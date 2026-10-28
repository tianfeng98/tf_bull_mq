import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'support/integration_fixture.dart';
import 'support/manual_runtime.dart';
import 'support/wait_until.dart';

void main() {
  // 端到端管线测试：Queue 生产 → Worker 消费 Processor 执行 → QueueEvents 观察事件。
  test(
    'Queue Signal Worker Processor Backend QueueEvents 完整管线完成一个 Job',
    () async {
      final fixture = await IntegrationFixture.open();
      addTearDown(fixture.close);
      final queue = fixture.queue('pipeline');
      addTearDown(queue.close);
      final queueEvents = fixture.queueEvents('pipeline');
      addTearDown(queueEvents.close);
      final processorEntered = Completer<void>();
      final processorFinished = Completer<void>();
      final events = <PositionedJobEvent>[];
      addTearDown(() {
        if (!processorEntered.isCompleted) processorEntered.complete();
      });
      // 订阅 QueueEvents，记录所有 PositionedJobEvent 用于后续验证。
      final subscription = queueEvents.events.listen((positioned) {
        events.add(positioned);
      });
      addTearDown(subscription.cancel);
      final worker = fixture.worker(
        queueName: 'pipeline',
        processor: (job) async {
          if (!processorEntered.isCompleted) processorEntered.complete();
          if (!processorFinished.isCompleted) processorFinished.complete();
          return const <String, Object?>{'ok': true};
        },
      );
      addTearDown(worker.close);

      // 启动 QueueEvents 监听和 Worker 消费循环，然后生产一个 Job。
      await queueEvents.start();
      await worker.start();
      final job = await queue.add(
        'pipeline-job',
        jobId: 'job-1',
        data: const <String, Object?>{'input': 1},
      );
      await withWatchdog(
        'pipeline processor entry',
        processorEntered.future,
        phase: () => worker.state.name,
        snapshot: () => job.snapshot.status,
      );
      await withWatchdog(
        'pipeline processor completion',
        processorFinished.future,
        phase: () => worker.state.name,
        snapshot: () => job.snapshot.status,
      );
      await fixture.runtime.flushMicrotasks();
      final record = await job.refresh();
      expect(record.status, JobStatus.completed);
      expect(record.result, const <String, Object?>{'ok': true});
      expect(
        events.map((positioned) => positioned.event.type),
        // 完整管线应包含 added、active 和 completed 三类事件。
        containsAll(<JobEventType>[
          JobEventType.added,
          JobEventType.active,
          JobEventType.completed,
        ]),
      );
    },
  );

  // 验证 QueueEvents 能即时观察到 completed 终态事件，无需等待额外 poll 周期。
  // 这确保了 Task 04 修复的 drain handoff 和 completed 事件即时可见性。
  test(
    'QueueEvents 会观察到管线中的最终 completed 事件',
    () async {
      final fixture = await IntegrationFixture.open();
      addTearDown(fixture.close);
      final queue = fixture.queue('completed-obs');
      addTearDown(queue.close);
      final queueEvents = fixture.queueEvents('completed-obs');
      addTearDown(queueEvents.close);

      final events = <PositionedJobEvent>[];
      final completedObserved = Completer<void>();
      final subscription = queueEvents.events.listen((positioned) {
        events.add(positioned);
        if (positioned.event.type == JobEventType.completed &&
            !completedObserved.isCompleted) {
          completedObserved.complete();
        }
      });
      addTearDown(subscription.cancel);

      final processorDone = Completer<void>();
      final worker = fixture.worker(
        queueName: 'completed-obs',
        processor: (job) async {
          processorDone.complete();
          return const <String, Object?>{'done': true};
        },
      );
      addTearDown(worker.close);

      await queueEvents.start();
      await worker.start();

      final job = await queue.add(
        'completed-obs-job',
        jobId: 'job-1',
        data: null,
      );

      // 等 Processor 完成
      await processorDone.future;
      await fixture.runtime.flushMicrotasks();

      // 关键断言：completed 事件应该即时到达，不需要任何额外 poll。
      // 如果 drain handoff 有问题，completedObserved 不会被触发。
      await completedObserved.future.timeout(
        const Duration(seconds: 2),
        onTimeout: () => fail('completed event was never observed'),
      );

      final record = await job.refresh();
      expect(record.status, JobStatus.completed);
      expect(
        events.map((e) => e.event.type).toList(),
        contains(JobEventType.completed),
      );
    },
  );

  // 双 Worker 竞争同一队列的 claim 互斥性：两个 Worker 同时启动、共享 backend，
  // 但一个 Job 只能被一个 Worker 领走，processor 只执行一次。
  test('两个 Worker 共享同一队列时，同一个 Job 仅触发一次 Processor', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('single-claim');
    addTearDown(queue.close);
    var processorCount = 0;
    var processorEntered = false;
    Object? processor(ActiveJob job) {
      processorCount++;
      processorEntered = true;
      return <String, Object?>{'worker': job.workerId};
    }

    final workerA = fixture.worker(
      queueName: 'single-claim',
      options: const WorkerOptions(workerId: 'worker-a'),
      processor: processor,
    );
    final workerB = fixture.worker(
      queueName: 'single-claim',
      options: const WorkerOptions(workerId: 'worker-b'),
      processor: processor,
    );
    addTearDown(workerA.close);
    addTearDown(workerB.close);
    await workerA.start();
    await workerB.start();
    final job = await queue.add('single', jobId: 'job-1', data: null);
    await waitUntil(
      'single claim completion',
      () => processorEntered,
      snapshot: () => '$processorCount/${job.snapshot.status.name}',
    );
    // 无论 claim 竞争多激烈，Processor 必须且仅执行一次。
    expect(processorCount, 1);
  });
}
