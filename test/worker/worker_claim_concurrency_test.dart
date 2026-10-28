import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/worker/index.dart';

import 'support/recording_backend.dart';
import 'support/worker_fixture.dart';

/// `WorkerOptions.pollInterval` 的默认值。
const Duration poll = Duration(seconds: 1);

/// 一个声明 distributed Profile 的能力集合。
BackendCapabilities distributedCapabilities() => BackendCapabilities(
  profile: BackendProfile.distributed,
  durability: Durability.durable,
  delayedJobs: true,
  priorities: true,
  atomicFlows: true,
  eventDelivery: EventDelivery.replayable,
);

void main() {
  // --- 启动与发现路径：验证 Worker 启动阶段的订阅、claim、能力校验与故障容错 ---

  test('start 假定 backend 已打开，且绝不会调用 open', () async {
    // 验证 Worker 假设 Backend 已打开，start 时不调用 open；默认不关闭注入的 Backend 和 Signal
    final fixture = WorkerFixture();
    final worker = fixture.worker();

    await worker.start();
    expect(fixture.backend.calls.first, 'claimJob');
    expect(fixture.backend.countOf('open'), 0);

    await worker.close();
    expect(worker.state, WorkerState.closed);
    // 默认不关闭注入的 Backend 与 Signal。
    expect(fixture.backend.countOf('close'), 0);
    expect(fixture.signal.closeCount, 0);
  });

  test('start 会在首次 claim pump 前完成订阅', () async {
    // 验证 Worker 在首次 claim 之前完成 Signal 订阅，保证后续唤醒不丢失
    final fixture = WorkerFixture();
    var subscribedWhenClaimed = false;
    fixture.backend.onClaimJob = (ClaimJobRequest request) {
      subscribedWhenClaimed = fixture.signal.subscribedQueues.contains(
        'emails',
      );
      return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
    };

    final worker = fixture.worker();
    await worker.start();

    expect(subscribedWhenClaimed, isTrue);
    expect(fixture.signal.subscribedQueues, <String>['emails']);
    await worker.close();
  });

  test('同步订阅失败会记入日志，轮询仍会启动', () async {
    // 验证 Signal 订阅同步失败只记日志，Worker 仍进入 running 并保持独立的周期轮询
    final fixture = WorkerFixture();
    fixture.signal.subscribeError = LifecycleError(
      'RecordingSignal',
      'subscribe',
      'signal is closed',
    );

    final worker = fixture.worker();
    await worker.start();

    expect(worker.state, WorkerState.running);
    expect(fixture.logSink.forOperation('subscribe'), isNotEmpty);
    expect(fixture.backend.countOf('claimJob'), 1);

    // 独立周期轮询仍然工作。
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks();
    expect(fixture.backend.countOf('claimJob'), 2);

    await worker.close();
  });

  test(
    'start 会在 claim 前拒绝搭配 process signal 的 distributed backend',
    () async {
      // 验证 distributed Profile 的 Backend 与 process scope Signal 组合在 start 前被拒绝，且无任何副作用
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final worker = fixture.worker(
        options: WorkerOptions(
          leaseDuration: const Duration(seconds: 10),
          renewInterval: const Duration(seconds: 3),
        ),
      );

      await expectLater(worker.start(), throwsA(isA<UnsupportedCapability>()));

      expect(worker.state, WorkerState.created);
      // 能力不足必须在任何副作用之前拒绝。
      expect(fixture.signal.subscribedQueues, isEmpty);
      expect(fixture.backend.calls, isEmpty);
      expect(fixture.runtime.pendingDelays, isEmpty);
    },
  );

  test(
    'signal 唤醒会请求 backend claim，但不会被视为工作本身',
    () async {
      // 验证 Signal 唤醒只触发一次 claim，不会把 Backend 的返回值当作已领取的 Job
      final fixture = WorkerFixture();
      final processed = <JobKey>[];
      final worker = fixture.worker(
        processor: (ActiveJob job) {
          processed.add(job.key);
          return null;
        },
      );
      await worker.start();
      expect(fixture.backend.countOf('claimJob'), 1);

      fixture.signal.emit('emails');
      await fixture.runtime.flushMicrotasks();

      expect(fixture.backend.countOf('claimJob'), 2);
      // Backend 没有返回 Job：Signal 只是重新检查的提示，本身不构成工作。
      expect(processed, isEmpty);
      expect(worker.activeCount, 0);

      await worker.close();
    },
  );

  test('所有 signal 提示都丢失后，poll 仍会 claim Job', () async {
    // 验证所有 Signal 提示都丢失时，周期轮询仍然能发现 Job 并进入 Processor
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    fixture.backend.onClaimJob = (ClaimJobRequest request) {
      // 首次 claim 返回「无 Job」，模拟所有 Signal 提示都丢失。
      if (fixture.backend.countOf('claimJob') == 1) {
        return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
      }
      return Future<BackendWriteResult<ClaimedJob?>>.value(pool.take());
    };
    final entered = Completer<JobKey>();
    final release = Completer<void>();
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        entered.complete(job.key);
        await release.future;
        return null;
      },
    );

    await worker.start();
    expect(fixture.backend.countOf('claimJob'), 1);
    expect(entered.isCompleted, isFalse);

    // 不做任何 emit，只推进一个 pollInterval。
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks();

    // 所有 Signal 提示都丢失时，周期轮询仍然发现了 Job 并进入 Processor。
    expect(fixture.backend.countOf('claimJob'), 2);
    expect(entered.isCompleted, isTrue);
    expect(await entered.future, pool.jobs.single.record.key);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test('signal stream 错误会记入日志，轮询仍会继续', () async {
    // 验证 Signal 订阅流报错只记日志，Worker 仍保持 running 且继续周期轮询
    final fixture = WorkerFixture();
    final worker = fixture.worker();
    await worker.start();
    expect(fixture.backend.countOf('claimJob'), 1);

    fixture.signal.emitError('emails', StateError('signal exploded'));
    await fixture.runtime.flushMicrotasks();

    expect(fixture.logSink.forOperation('subscribe'), isNotEmpty);
    expect(worker.state, WorkerState.running);

    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks();
    expect(fixture.backend.countOf('claimJob'), 2);

    await worker.close();
  });

  test(
    '临时 claim 遇到 BackendUnavailable 时会记入日志，并在下一轮 poll 重试',
    () async {
      // 验证 claim 临时失败只记日志，Worker 保持 running 并在下一次 poll 重试
      final fixture = WorkerFixture();
      var attempts = 0;
      fixture.backend.onClaimJob = (ClaimJobRequest request) {
        attempts++;
        if (attempts == 1) {
          return Future<BackendWriteResult<ClaimedJob?>>.error(
            BackendUnavailable(null, 'backend is down'),
          );
        }
        return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
      };

      final worker = fixture.worker();
      await worker.start();

      expect(attempts, 1);
      expect(worker.state, WorkerState.running);
      expect(fixture.logSink.forOperation('claimJob'), hasLength(1));

      fixture.runtime.completeNext(poll);
      await fixture.runtime.flushMicrotasks();
      expect(attempts, 2);

      await worker.close();
    },
  );

  test(
    'poll Delay 失败时只记录一次日志，并升级为强制关闭，不会陷入忙循环',
    () async {
      // 验证 poll 延迟失败只记一次日志并直接升级为 forceClose，不存在「失败后原地重试」的热循环
      final fixture = WorkerFixture();
      final worker = fixture.worker();
      await worker.start();
      expect(fixture.backend.countOf('claimJob'), 1);

      fixture.runtime.failNext(poll);
      await fixture.runtime.flushMicrotasks();

      expect(fixture.logSink.forOperation('poll'), hasLength(1));
      expect(worker.state, WorkerState.closed);
      // 没有重新登记 poll 延迟：不存在「失败后原地重试」的热循环。
      expect(fixture.runtime.pendingDelays, isEmpty);
      expect(fixture.backend.countOf('claimJob'), 1);
    },
  );

  test('backend 已关闭时调用 start 会抛出 LifecycleError 并取消订阅', () async {
    // 验证 Backend 已关闭时 claim 抛 LifecycleError，start 失败后取消 Signal 订阅且不保留 poll 延迟
    final fixture = WorkerFixture();
    fixture.backend.onClaimJob = (ClaimJobRequest request) =>
        Future<BackendWriteResult<ClaimedJob?>>.error(
          LifecycleError('MemoryQueueBackend', 'claimJob', 'backend is closed'),
        );

    final worker = fixture.worker();
    await expectLater(worker.start(), throwsA(isA<LifecycleError>()));

    expect(worker.state, WorkerState.created);
    expect(fixture.signal.subscribedQueues, <String>['emails']);
    expect(fixture.signal.cancelCount, 1);

    // 回滚之后周期轮询不再产生新的 claim。
    if (fixture.runtime.pendingDelays.isNotEmpty) {
      fixture.runtime.completeNext(poll);
    }
    await fixture.runtime.flushMicrotasks();
    expect(fixture.backend.countOf('claimJob'), 1);
  });

  // --- 并发槽：验证 Worker 对并发槽位的填充、释放、暂停与关闭行为 ---

  test(
    '会在释放 Processor 前准确 claim concurrency 个 Job',
    () async {
      // 验证 Worker 启动后会填满到 concurrency 上限的槽位，每个槽位占一个 Job
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 4);
      fixture.installSuccessfulTransitions();
      final release = Completer<void>();
      final started = <JobKey>[];
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 2),
        processor: (ActiveJob job) async {
          started.add(job.key);
          await release.future;
          return null;
        },
      );

      await worker.start();
      await fixture.runtime.flushMicrotasks();

      expect(fixture.backend.countOf('claimJob'), 2);
      expect(pool.served, 2);
      expect(worker.activeCount, 2);
      expect(started, hasLength(2));

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('signal 与 poll 竞争时不会超额 claim', () async {
    // 验证 Signal 唤醒与周期 poll 并发时不会超领槽位：两次唤醒被合并成一次额外 claim
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 5);
    final secondClaim = Completer<void>();
    var claimCalls = 0;
    fixture.backend.onClaimJob = (ClaimJobRequest request) {
      claimCalls++;
      if (claimCalls == 2) {
        return secondClaim.future.then((void _) => pool.take());
      }
      return Future<BackendWriteResult<ClaimedJob?>>.value(pool.take());
    };
    final blocking = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 3),
      processor: (ActiveJob job) async {
        await blocking.future;
        return null;
      },
    );

    final starting = worker.start();
    await fixture.runtime.flushMicrotasks();
    expect(claimCalls, 2);

    // Signal 与周期 poll 同时唤醒，但第二次 claim 仍在 in flight。
    fixture.signal.emit('emails');
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks();
    expect(claimCalls, 2);

    secondClaim.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await starting;

    // 两次唤醒被合并成一次额外的填槽，而不是各发一次 claim。
    expect(claimCalls, 3);
    expect(worker.activeCount, 3);

    await worker.forceClose();
  });

  test('只释放一个槽位一次，并 claim 一个替代 Job', () async {
    // 验证 Processor 完成后释放一个槽位并立即领取一个替代 Job，保持槽位持续被填满
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 3);
    fixture.installSuccessfulTransitions();
    final release = List<Completer<void>>.generate(
      3,
      (int _) => Completer<void>(),
    );
    var index = 0;
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 2),
      processor: (ActiveJob job) async {
        final current = index++;
        await release[current].future;
        return null;
      },
    );

    await worker.start();
    expect(pool.served, 2);
    expect(worker.activeCount, 2);

    release[0].complete();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(pool.served, 3);
    expect(worker.activeCount, 2);

    release[1].complete();
    release[2].complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test(
    '一个 Processor 完成正在提交时，activeCount 保持稳定',
    () async {
      // 验证 Processor 已返回但 transitionJob 被阻塞时，activeCount 保持不变直到提交成功
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 2);
      final commitBarrier = Completer<void>();
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        await commitBarrier.future;
        return changedWrite(
          fixture.record(
            queueName: request.key.queueName,
            jobId: request.key.jobId,
            status: request.targetStatus,
            revision: request.expectedRevision + 1,
          ),
        );
      };
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 1),
        processor: (ActiveJob job) => null,
      );

      await worker.start();
      await fixture.runtime.flushMicrotasks();

      // Processor 已经返回，但终态提交被 barrier 阻塞。
      expect(fixture.backend.countOf('transitionJob'), 1);
      expect(worker.activeCount, 1);
      expect(pool.served, 1);
      expect(fixture.backend.countOf('claimJob'), 1);

      commitBarrier.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('暂停的 Worker 会刷新 active control，但不会 claim Job', () async {
    // 验证 paused Worker 仍会通过 Signal 和 poll 刷新 active Job 的控制请求，但不会领取新 Job；resume 后恢复领取
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 2);
    fixture.installSuccessfulTransitions();
    final blocking = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        await blocking.future;
        return null;
      },
    );

    await worker.start();
    expect(pool.served, 1);
    await worker.pause();
    final getJobBefore = fixture.backend.countOf('getJob');

    fixture.signal.emit('emails');
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);

    // paused 仍然刷新 active 控制请求，但绝不领取新 Job。
    expect(fixture.backend.countOf('claimJob'), 1);
    expect(fixture.backend.countOf('getJob'), greaterThan(getJobBefore));
    expect(pool.served, 1);

    blocking.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.resume();
    await fixture.runtime.flushMicrotasks(turns: 24);

    // resume 之后恢复领取：耗尽池之前至少多领取一个 Job。
    expect(fixture.backend.countOf('claimJob'), greaterThanOrEqualTo(2));
    expect(pool.served, 2);

    await worker.close();
  });

  test('pause、close 或 force close 开始后不会再 claim Job', () async {
    // 验证 pause、close、forceClose 之后 Worker 不再领取新 Job，槽位保持当前状态直到清理完成
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 4);
    final blocking = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        await blocking.future;
        return null;
      },
    );

    await worker.start();
    expect(pool.served, 1);

    await worker.pause();
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(pool.served, 1);

    final closing = worker.forceClose();
    fixture.signal.emit('emails');
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(pool.served, 1);

    await closing;
    expect(worker.state, WorkerState.closed);
    expect(worker.activeCount, 0);
  });
}
