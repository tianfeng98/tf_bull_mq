import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'support/delegating_queue_backend.dart';
import 'support/integration_fixture.dart';
import 'support/manual_runtime.dart';

void main() {
  // 验证 Worker forceClose 会等待 in-flight claim 完成后再进入 recovery 路径：
  // 如果 claim 被外部 barrier 阻塞，forceClose 不会跳过或中断它。
  test('shutdown 会在恢复前等待正在进行的 claim 完成', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('shutdown-claim');
    addTearDown(queue.close);

    // 让 claimJob 在 barrier 完成前阻塞——但第一次调用返回 null，让 start() 能完成
    final claimBarrier = Completer<void>();
    var claimCallCount = 0;
    final originalBackend = fixture.backend;
    final blockingBackend = _BlockingClaimBackend(
      originalBackend,
      barrier: claimBarrier,
      shouldBlock: () => ++claimCallCount > 1, // 第二次及以后调用才阻塞
    );

    // 用 blocking backend 创建 Worker
    final worker = Worker(
      queueName: 'shutdown-claim',
      backend: blockingBackend,
      signal: fixture.signal,
      processor: (ActiveJob job) async {
        // Processor 不应执行：claim 被阻塞
        throw StateError('should not run');
      },
      logSink: const SilentWorkerLogSink(),
      now: fixture.runtime.now,
      idGenerator: fixture.runtime.nextId,
      delay: fixture.runtime.delay,
    );
    addTearDown(worker.close);
    await worker.start(); // 第一次 claim 返回 null，start() 完成

    // 添加一个 Job → Signal 触发第二次 claim → 被 barrier 阻塞
    await queue.add('job', jobId: 'job-1', data: null);
    await fixture.runtime.flushMicrotasks();

    // 现在 claim 应该在 barrier 上等待
    expect(worker.activeCount, 0);

    // 开始 forceClose
    final closingFuture = worker.forceClose();
    await fixture.runtime.flushMicrotasks();
    expect(worker.state, WorkerState.closing);

    // forceClose Future 不应立即完成：claim 还在等待 barrier
    // 用 timeout 验证——200ms 内完成说明 forceClose 没有正确等待 in-flight claim
    final caught = <Object>[];
    try {
      await closingFuture.timeout(const Duration(milliseconds: 200));
      fail('closingFuture completed without waiting for the in-flight claim');
    } catch (e) {
      caught.add(e);
    }
    expect(caught, hasLength(1));
    expect(caught.single, isA<TimeoutException>());

    // 释放 barrier，claim 完成，然后 Worker 进入 recovery 并关闭
    claimBarrier.complete();
    await fixture.runtime.flushMicrotasks();

    await closingFuture;
    expect(worker.state, WorkerState.closed);
  });

  // 验证 forceClose 的 recovery 与 Processor 的迟到终态提交之间只有一个事实：
  // 如果 recovery 让 Job 变成 waiting（等待重试），那么 Processor 之后再尝试
  // complete/fail 都会因为 execution 已失去而被丢弃。
  test(
    '强制恢复和终态完成只会产生一个终态事实',
    () async {
      final fixture = await IntegrationFixture.open();
      addTearDown(fixture.close);
      final queue = fixture.queue('single-fact');
      addTearDown(queue.close);

      final processorDone = Completer<void>();
      final worker = fixture.worker(
        queueName: 'single-fact',
        options: const WorkerOptions(concurrency: 1),
        processor: (ActiveJob job) async {
          // Processor 先不要完成，等 forceClose 来放弃我们
          await processorDone.future;
          return const <String, Object?>{'result': 'late'};
        },
      );
      addTearDown(worker.close);
      await worker.start();

      final job = await queue.add('job', jobId: 'job-1', data: null);
      await fixture.runtime.flushMicrotasks();

      // 执行 forceClose → 放弃执行 + recovery
      await worker.forceClose();
      await fixture.runtime.flushMicrotasks();

      // 此时 recovery 应该让 Job 变成 waiting 或其他非 active 状态
      final recordBefore = await job.refresh();
      expect(recordBefore.status, isNot(JobStatus.active));

      // 现在让旧 Processor 继续执行（迟到的 terminal）
      processorDone.complete();
      await fixture.runtime.flushMicrotasks();

      // 最终状态：Job 不应是 completed with result={'result': 'late'}
      final recordAfter = await job.refresh();
      if (recordAfter.status == JobStatus.completed) {
        expect(
          recordAfter.result,
          isNot(equals(const <String, Object?>{'result': 'late'})),
        );
      }
    },
  );

  // 验证 Job.pause() 公开入口：在 active Job 上调用 pause()，请求被写入 Backend，
  // 持有 execution 的 ActiveJob 能观察到这个控制请求，确认后 Job 进入 paused。
  test('ActiveJob 可以观察并确认 pause 请求', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('pause-test');
    addTearDown(queue.close);

    // Processor 观察控制请求并确认 pause
    final observedControl = Completer<void>();
    final worker = fixture.worker(
      queueName: 'pause-test',
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        // 等待控制请求到达（ActiveJobControlState.pauseRequested 枚举值）
        await job.controlStream.firstWhere(
          (ActiveJobControlState state) =>
              state == ActiveJobControlState.pauseRequested,
        );
        if (!observedControl.isCompleted) observedControl.complete();
        // 确认暂停：这会让 Job 进入 paused
        await job.confirmPaused();
        return null;
      },
    );
    addTearDown(worker.close);
    await worker.start();

    final job = await queue.add('pause-job', jobId: 'pause-1', data: null);
    await fixture.runtime.flushMicrotasks();

    // Job 应该已经被领取，状态是 active
    expect((await job.refresh()).status, JobStatus.active);

    // 通过公开 API 请求协作式暂停
    final paused = await job.pause();
    expect(paused.controlRequest?.action, JobControlAction.pause);
    expect(paused.status, JobStatus.active); // pause 请求后仍保持 active

    // Processor 应该能观察到控制请求并确认暂停
    await withWatchdog(
      'observed pause control request',
      observedControl.future,
      phase: () => worker.state.name,
      snapshot: () async => (await job.refresh()).status.name,
    );
    await fixture.runtime.flushMicrotasks();

    // Processor 确认暂停后，Job 应该进入 paused 状态
    final finalRecord = await job.refresh();
    expect(finalRecord.status, JobStatus.paused);

    await worker.close();
  });
}

/// 选择性阻塞 claim 的 backend 代理：在 barrier 完成前对指定调用返回阻塞的 claim。
///
/// [shouldBlock] 决定每次 claimJob 调用是否阻塞。返回 false 时直接转发给 inner。
final class _BlockingClaimBackend extends DelegatingQueueBackend {
  _BlockingClaimBackend(
    super.inner, {
    required this.barrier,
    required this.shouldBlock,
  });

  final Completer<void> barrier;
  final bool Function() shouldBlock;

  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(
    ClaimJobRequest request,
  ) async {
    if (shouldBlock()) {
      // 等待 barrier 完成后再真正 claim
      await barrier.future;
    }
    return inner.claimJob(request);
  }
}
