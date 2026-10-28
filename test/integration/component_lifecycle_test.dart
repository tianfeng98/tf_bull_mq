import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'support/delegating_queue_backend.dart';
import 'support/integration_fixture.dart';
import 'support/manual_runtime.dart';

void main() {
  // 验证组件生命周期：Queue 在 Worker 启动前可以成功 add Job，Worker 随后启动后能正确 claim 并完成；
  // 多实例共享同一 backend 时，Queue / QueueEvents 的关闭不影响 backend 存活。
  test('Queue 在 Worker 启动前可以 add Job，Worker 启动后能正确 claim 并完成', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('before-worker');
    addTearDown(queue.close);
    final entered = Completer<void>();
    addTearDown(() {
      if (!entered.isCompleted) entered.complete();
    });
    final worker = fixture.worker(
      queueName: 'before-worker',
      processor: (job) {
        if (!entered.isCompleted) entered.complete();
        return <String, Object?>{'id': job.key.jobId};
      },
    );
    addTearDown(worker.close);

    // 先 add Job，再启动 Worker，验证 Worker 能独立发现 waiting 的 Job。
    final job = await queue.add(
      'before-worker-job',
      jobId: 'job-1',
      data: const <String, Object?>{},
    );
    expect(job.snapshot.status, JobStatus.waiting);

    await worker.start();
    await withWatchdog(
      'job processor',
      entered.future,
      phase: () => worker.state.name,
      snapshot: () => job.snapshot.status,
    );
    await fixture.runtime.flushMicrotasks();
    expect((await job.refresh()).status, JobStatus.completed);
  });

  test('共享 backend 的 Queue 和 QueueEvents 关闭时不会连带关闭 backend 本身', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final queue = fixture.queue('shared');
    addTearDown(queue.close);
    final events = fixture.queueEvents('shared');
    addTearDown(events.close);
    final subscription = events.events.listen((_) {});
    addTearDown(subscription.cancel);

    await events.start();
    await events.close();
    await queue.close();

    // 从同一 backend 再开一个 peer queue，仍然能正常 add Job，说明 backend 未被关闭。
    final peer = fixture.queue('shared');
    addTearDown(peer.close);
    final job = await peer.add('still-open', data: null, jobId: 'job-1');
    expect(job.snapshot.status, JobStatus.waiting);
  });

  // 验证 FlowProducer 和 FlowQuery 共享 backend/signal 时的生命周期：
  // 它们关闭后 backend 和 signal 仍存活，其他组件不受影响。
  test('FlowProducer 和 FlowQuery 共享 backend 关闭后不影响 backend 存活', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);

    final producer = fixture.flowProducer();
    final query = fixture.flowQuery();

    await producer.close();
    await query.close();

    // backend 和 signal 仍应存活——可以创建新的 Queue 并 add Job
    final queue = fixture.queue('after-flow-close');
    addTearDown(queue.close);
    final job = await queue.add('alive', data: null, jobId: 'job-1');
    expect(job.snapshot.status, JobStatus.waiting);
  });

  // 验证 FlowProducer 的 capability preflight：当 Backend 不支持 atomicFlows 时
  // add 会在任何 Backend 写入之前抛 UnsupportedCapability。
  test(
    'FlowProducer 在不支持 atomicFlows 的 backend 上抛 UnsupportedCapability',
    () async {
      // 构造一个伪造的不支持 atomicFlows 的 backend
      final runtime = ManualRuntime();
      final inner = MemoryQueueBackend(
        now: runtime.now,
        idGenerator: runtime.nextId,
      );
      await inner.open();
      addTearDown(inner.close);

      final signal = InProcessQueueSignal();
      addTearDown(signal.close);

      final backend = _NoAtomicFlowBackend(inner);
      final producer = FlowProducer(
        backend: backend,
        signal: signal,
        now: runtime.now,
      );

      expect(
        () => producer.add(
          FlowJob(
            key: const JobKey(queueName: 'q', jobId: 'j'),
            name: 'j',
            data: null,
          ),
        ),
        throwsA(isA<UnsupportedCapability>()),
      );
      await producer.close();
    },
  );

  // 验证同一 QueueSignal 可以被多个 QueueEvents 实例同时 subscribe（duplicate Signal），
  // 它们都能收到各自队列的 notify 事件，互不干扰。
  test('多个 QueueEvents 可同时 subscribe 同一个 InProcessQueueSignal', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);

    // 两个不同队列各自创建 QueueEvents，都共享同一个 signal
    final eventsA = fixture.queueEvents('queue-a');
    final eventsB = fixture.queueEvents('queue-b');
    addTearDown(eventsA.close);
    addTearDown(eventsB.close);

    final receivedA = <String>[];
    final receivedB = <String>[];
    final subA = eventsA.events.listen((e) => receivedA.add(e.event.type.name));
    final subB = eventsB.events.listen((e) => receivedB.add(e.event.type.name));
    addTearDown(subA.cancel);
    addTearDown(subB.cancel);

    await eventsA.start();
    await eventsB.start();

    // 往 queue-a 加一个 Job → 只有 eventsA 应该收到事件
    final queueA = fixture.queue('queue-a');
    addTearDown(queueA.close);
    await queueA.add('job-a', jobId: 'a1', data: null);
    await fixture.runtime.flushMicrotasks();

    expect(receivedA, isNotEmpty);
    expect(receivedB, isEmpty);
  });
}

/// 包装 QueueBackend 但把 atomicFlows 设为 false，用于 capability preflight 测试。
final class _NoAtomicFlowBackend extends DelegatingQueueBackend {
  _NoAtomicFlowBackend(super.inner);

  @override
  BackendCapabilities get capabilities => BackendCapabilities(
    profile: inner.capabilities.profile,
    durability: inner.capabilities.durability,
    delayedJobs: inner.capabilities.delayedJobs,
    priorities: inner.capabilities.priorities,
    atomicFlows: false,
    eventDelivery: inner.capabilities.eventDelivery,
  );
}
