import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'support/integration_fixture.dart';
import 'support/manual_runtime.dart';

void main() {
  // 验证 Processor 失败后的重试行为：首次失败触发 retry，第二次成功时 Job 最终变为 completed。
  test('Processor 首次失败后自动 retry，重试成功时 Job 最终 completed', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('retry');
    addTearDown(queue.close);
    var attempts = 0;
    final secondAttempt = Completer<void>();
    final worker = fixture.worker(
      queueName: 'retry',
      processor: (job) {
        attempts++;
        if (attempts == 2 && !secondAttempt.isCompleted) {
          secondAttempt.complete();
        }
        if (attempts == 1) throw StateError('try again');
        return const <String, Object?>{'ok': true};
      },
    );
    addTearDown(worker.close);
    await worker.start();
    final job = await queue.add(
      'retry-job',
      jobId: 'job-1',
      data: null,
      options: JobOptions(retry: RetryPolicy(maxAttempts: 2)),
    );
    await withWatchdog(
      'second attempt',
      secondAttempt.future,
      phase: () => worker.state.name,
      snapshot: () => attempts,
    );
    await fixture.runtime.flushMicrotasks();
    expect((await job.refresh()).status, JobStatus.completed);
    expect((await job.refresh()).attemptsMade, 2);
  });

  // 验证最终失败后的一致性：超过最大重试次数后 Job 状态变为 failed，failure 字段有值，
  // 且 backend 自动写入一条系统级 error JobLog。
  test('最终失败时 Job 暴露 failure，backend 自动写入 error JobLog', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('final-failure');
    addTearDown(queue.close);
    final entered = Completer<void>();
    final worker = fixture.worker(
      queueName: 'final-failure',
      processor: (job) {
        if (!entered.isCompleted) entered.complete();
        throw StateError('permanent failure');
      },
    );
    addTearDown(worker.close);
    await worker.start();
    final job = await queue.add('fails', jobId: 'job-1', data: null);
    await withWatchdog(
      'failed processor entry',
      entered.future,
      phase: () => worker.state.name,
      snapshot: () => job.snapshot.status,
    );
    await fixture.runtime.flushMicrotasks();
    final record = await job.refresh();
    final logs = await job.queryLogs(
      levels: <JobLogLevel>{JobLogLevel.error},
      channels: <JobLogChannel>{JobLogChannel.system},
    );
    expect(record.status, JobStatus.failed);
    expect(record.failure, isNotNull);
    expect(logs.items, hasLength(1));
    expect(logs.items.single.message, record.failure!.message);
  });

  // 验证带延迟的 retry：首次失败后 Job 进入 delayed，手动推进时钟让它到期，
  // 再触发 Worker poll 领取，第二次尝试成功时 Job 最终 completed。
  test('手动时钟和 poll 会解除延迟 retry', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('delayed-retry');
    addTearDown(queue.close);

    var attempts = 0;
    final worker = fixture.worker(
      queueName: 'delayed-retry',
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        attempts++;
        if (attempts == 1) {
          // 让第一次尝试失败，Job 会进入 delayed
          throw StateError('not yet');
        }
        return const <String, Object?>{'success': true};
      },
    );
    addTearDown(worker.close);
    await worker.start();

    final job = await queue.add(
      'delayed-job',
      jobId: 'delayed-1',
      data: null,
      options: JobOptions(
        retry: RetryPolicy(maxAttempts: 2, delay: const Duration(seconds: 5)),
      ),
    );

    // 等第一次尝试完成（失败）
    await fixture.runtime.flushMicrotasks();

    // 第一次尝试后 Job 应该进入 delayed 状态
    var record = await job.refresh();
    expect(record.status, JobStatus.delayed);
    expect(record.availableAt, isNotNull);
    // ignore: unnecessary_non_null_assertion
    final delayedAvailableAt = record.availableAt!;

    // 验证 Job 在时钟到期前不能被领取：触发 poll 但不推进时钟
    fixture.runtime.completeNext(const Duration(seconds: 1));
    await fixture.runtime.flushMicrotasks();
    record = await job.refresh();
    // 仍应保持 delayed（时钟未推进）
    expect(record.status, JobStatus.delayed);

    // 现在推进手动时钟到 availableAt 之后，让 Job 到期可领取
    fixture.runtime.advance(
      delayedAvailableAt.difference(fixture.runtime.now()) +
          const Duration(seconds: 1),
    );
    // 触发 poll
    fixture.runtime.completeNext(const Duration(seconds: 1));
    await fixture.runtime.flushMicrotasks();

    // 第二次尝试现在应该完成（成功）
    record = await job.refresh();
    expect(record.status, JobStatus.completed);
    expect(record.attemptsMade, 2);
    expect(attempts, 2);
  });

  // 验证 stale execution 回调不会覆盖已恢复的结果：Processor 已被放弃（通过 forceClose）
  // 但仍在运行，它的迟到结果在 Backend 看到 execution 已恢复后被丢弃。
  test(
    '过期的执行回调不能覆盖已恢复的结果',
    () async {
      final fixture = await IntegrationFixture.open();
      addTearDown(fixture.close);
      final queue = fixture.queue('stale-callback');
      addTearDown(queue.close);

      final processorBarrier = Completer<void>();
      var processorSettled = false;
      final worker = fixture.worker(
        queueName: 'stale-callback',
        options: const WorkerOptions(concurrency: 1),
        processor: (ActiveJob job) async {
          // 先让 Worker 放弃我们（通过 forceClose），然后我们继续跑
          await processorBarrier.future;
          processorSettled = true;
          return const <String, Object?>{'stale': true};
        },
      );
      addTearDown(worker.close);
      await worker.start();

      final job = await queue.add('stale-job', jobId: 'stale-1', data: null);
      await fixture.runtime.flushMicrotasks();

      // Worker forceClose 放弃执行，让 Backend 恢复
      await worker.forceClose();
      await fixture.runtime.flushMicrotasks();

      // processorBarrier.complete() 让旧 Processor 继续执行——但此时 execution 已被放弃，
      // 它的结果应该被丢弃（LeaseLost）
      processorBarrier.complete();
      await fixture.runtime.flushMicrotasks();

      final record = await job.refresh();
      // stale callback 的结果绝对不能是 completed with {'stale': true}
      if (record.status == JobStatus.completed) {
        expect(
          record.result,
          isNot(equals(const <String, Object?>{'stale': true})),
        );
      }
      // 无论如何，旧 Processor 已经跑了
      expect(processorSettled, isTrue);
    },
  );

  // 验证两个并发 Job 的日志序列各自隔离：Job A 的日志始终是 1,2,3...，
  // Job B 的日志也始终是 1,2,3...，不会交叉或混合。
  test('并发写入 JobLog 时，各 Job 的 sequence 会彼此隔离', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queueA = fixture.queue('logs-a');
    addTearDown(queueA.close);
    final queueB = fixture.queue('logs-b');
    addTearDown(queueB.close);

    final workerA = fixture.worker(
      queueName: 'logs-a',
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'a-log1',
          ),
        );
        await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'a-log2',
          ),
        );
        await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'a-log3',
          ),
        );
        return null;
      },
    );
    final workerB = fixture.worker(
      queueName: 'logs-b',
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'b-log1',
          ),
        );
        await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'b-log2',
          ),
        );
        await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'b-log3',
          ),
        );
        return null;
      },
    );
    addTearDown(workerA.close);
    addTearDown(workerB.close);

    await workerA.start();
    await workerB.start();

    final jobA = await queueA.add('job-a', jobId: 'job-a-1', data: null);
    final jobB = await queueB.add('job-b', jobId: 'job-b-1', data: null);

    await fixture.runtime.flushMicrotasks();

    final recordA = await jobA.refresh();
    final recordB = await jobB.refresh();
    expect(recordA.status, JobStatus.completed);
    expect(recordB.status, JobStatus.completed);

    final logsA = await jobA.queryLogs();
    final logsB = await jobB.queryLogs();

    expect(logsA.items, hasLength(3));
    expect(logsB.items, hasLength(3));

    // 验证 Job A 的日志都来自 Job A
    for (final log in logsA.items) {
      expect(log.jobKey, jobA.key);
      expect(log.message, startsWith('a-log'));
    }
    // 验证 Job B 的日志都来自 Job B
    for (final log in logsB.items) {
      expect(log.jobKey, jobB.key);
      expect(log.message, startsWith('b-log'));
    }

    // 验证两个 Job 的日志各自 sequence 连续且不重叠
    expect(logsA.items.map((log) => log.sequence).toList(), [1, 2, 3]);
    expect(logsB.items.map((log) => log.sequence).toList(), [1, 2, 3]);

    await workerA.close();
    await workerB.close();
  });
}
