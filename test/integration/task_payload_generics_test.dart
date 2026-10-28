import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

final class _SilentLogSink implements WorkerLogSink {
  const _SilentLogSink();

  @override
  Future<void> write(WorkerLogEntry entry) async {}
}

final class _FailingSignal implements QueueSignal {
  final failure = StateError('notification failed');
  final _signal = InProcessQueueSignal();

  @override
  SignalScope get scope => _signal.scope;

  @override
  Future<void> notify(String queueName) async => throw failure;

  @override
  Stream<void> subscribe(String queueName) => _signal.subscribe(queueName);

  @override
  Future<void> close() => _signal.close();
}

void main() {
  test('不同的 payload 类型在 Queue、Worker 和 refresh 后仍保持不变', () async {
    final backend = MemoryQueueBackend();
    final signal = InProcessQueueSignal();
    await backend.open();
    final queue = Queue<String, num, int>(
      queueName: 'typed',
      backend: backend,
      signal: signal,
    );
    final completed = Completer<void>();
    final terminalEvent = Completer<PositionedJobEvent>();
    final progressEvent = Completer<PositionedJobEvent>();
    final events = QueueEvents(
      queueName: 'typed',
      backend: backend,
      signal: signal,
    );
    final subscription = events.events.listen((event) {
      if (event.event.type == JobEventType.completed) {
        terminalEvent.complete(event);
      } else if (event.event.type == JobEventType.progress) {
        progressEvent.complete(event);
      }
    });
    final worker = Worker<String, num, int>(
      queueName: 'typed',
      backend: backend,
      signal: signal,
      processor: (ActiveJob<String, num, int> job) async {
        final String? input = job.data;
        expect(input, 'input');
        // updateProgress 返回 JobRecord（ActiveJob 层），progress 为 Object?
        final JobRecord updated = await job.updateProgress(0.5);
        final num? progress = updated.progress as num?;
        expect(progress, 0.5);
        await job.updateData('updated');
        // JobLog 是原始 JSON-like，data 为 Object?
        final JobLog log = await job.log(
          JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'typed log',
            data: input,
          ),
        );
        final String? logData = log.data as String?;
        expect(logData, 'input');
        completed.complete();
        return 42;
      },
      logSink: const _SilentLogSink(),
    );
    addTearDown(() async {
      await worker.close();
      await events.close();
      await subscription.cancel();
      await queue.close();
      await signal.close();
      await backend.close();
    });
    final Job<String, num, int> job = await queue.add(
      'work',
      data: 'input',
      jobId: 'one',
    );
    await events.start();
    await worker.start();
    await completed.future.timeout(const Duration(seconds: 5));
    for (var attempt = 0; attempt < 100; attempt++) {
      await job.refresh();
      if (job.snapshot.status == JobStatus.completed) break;
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    final String? data = job.snapshot.data;
    final num? progress = job.snapshot.progress;
    final int? result = job.snapshot.result;
    expect(data, 'updated');
    expect(progress, 0.5);
    expect(result, 42);
    final PositionedJobEvent event = await terminalEvent.future.timeout(
      const Duration(seconds: 5),
    );
    final num? eventData = event.event.data as num?;
    expect(eventData, isNull);
    final num? progressData =
        (await progressEvent.future.timeout(
              const Duration(seconds: 5),
            )).event.data
            as num?;
    expect(progressData, 0.5);
    // JobLogPage 是原始 JSON-like；queryLogs() 不再需要类型参数
    final JobLogPage logs = await job.queryLogs();
    expect(logs.items.single.data, 'input');
    final Job<String, num, int>? found = await queue.getJob('one');
    expect(found!.snapshot.result, 42);
    final QueueJobPage<String, num, int> page = await queue.query();
    expect(page.items.single.snapshot.data, 'updated');
    final records = await queue.addBulk([
      QueueJobDraft<String>(name: 'nullable', data: null, jobId: 'two'),
    ]);
    expect(records.single.snapshot.data, isNull);
    expect(records.single.snapshot.result, isNull);
    final untyped = Queue(queueName: 'typed', backend: backend, signal: signal);
    addTearDown(untyped.close);
    final Job<Object?, Object?, Object?>? untypedJob = await untyped.getJob(
      'one',
    );
    expect(untypedJob!.snapshot.rawRecord, same(await backend.getJob(job.key)));
    final wrongType = Queue<int, num, int>(
      queueName: 'typed',
      backend: backend,
      signal: signal,
    );
    addTearDown(wrongType.close);
    // 错误类型 Queue 读取 Backend 里不匹配的数据（String → int 解码失败），
    // 抛 PayloadCodecException（PayloadCodec 包装的解码失败）。
    await expectLater(
      wrongType.getJob('one'),
      throwsA(isA<PayloadCodecException>()),
    );
  });

  test('泛型事件和 flow 会保留冻结的 JSON-like 值', () {
    const key = JobKey(queueName: 'typed', jobId: 'root');
    // JobEvent 是原始 JSON-like，data 为 Object?
    final event = JobEvent(
      eventId: 'event',
      jobKey: key,
      revision: 1,
      type: JobEventType.completed,
      occurredAt: DateTime.utc(2026),
      data: 'done',
    );
    final String? eventData = event.data as String?;
    expect(eventData, 'done');
    final spec = FlowJob<String>(
      key: key,
      name: 'root',
      data: 'input',
    ).toFlowJobSpec();
    final String? input = spec.jobs.single.data as String?;
    expect(input, 'input');
    final original = <String, Object?>{
      'nested': <Object?>[1],
    };
    // JobSpec 是原始 JSON-like 构造，data 为 Object?，构造时 freezeJsonLike
    final frozen = JobSpec(key: key, name: 'map', data: original);
    (original['nested']! as List<Object?>).add(2);
    expect(frozen.data, {
      'nested': [1],
    });
    expect(
      () => (frozen.data as Map<String, Object?>)['new'] = 2,
      throwsUnsupportedError,
    );
  });

  test(
    '类型化 flow 投影和独立子级结果在执行后仍会保留',
    () async {
      final backend = MemoryQueueBackend();
      final signal = InProcessQueueSignal();
      await backend.open();
      final producer = FlowProducer<String, num, int>(
        backend: backend,
        signal: signal,
      );
      final query = FlowQuery<String, num, int>(backend: backend);
      const root = JobKey(queueName: 'flow-typed', jobId: 'root');
      const child = JobKey(queueName: 'flow-typed', jobId: 'child');
      final entered = Completer<void>();
      final worker = Worker<String, num, int>(
        queueName: root.queueName,
        backend: backend,
        signal: signal,
        processor: (job) async {
          if (job.key == child) return 7;
          final Map<JobKey, int?> results = (await job.getDependenciesResult());
          expect(results[child], 7);
          expect(() => results.clear(), throwsUnsupportedError);
          entered.complete();
          return results[child]! + 1;
        },
        logSink: const _SilentLogSink(),
      );
      addTearDown(() async {
        await worker.close();
        await producer.close();
        await query.close();
        await signal.close();
        await backend.close();
      });
      // FlowSnapshot 是原始 JSON-like
      final FlowSnapshot snapshot = await producer.add(
        FlowJob<String>(
          key: root,
          name: 'parent',
          data: 'root-data',
          children: [
            FlowChild<String>(
              job: FlowJob<String>(
                key: child,
                name: 'child',
                data: 'child-data',
              ),
            ),
          ],
        ),
      );
      final String? rootData = snapshot.jobs.first.data as String?;
      expect(rootData, 'root-data');
      await worker.start();
      await entered.future.timeout(const Duration(seconds: 5));
      FlowProjection<String, num, int>? projection;
      for (var attempt = 0; attempt < 100; attempt++) {
        projection = await query.get(
          root,
          depth: FlowProjectionDepth.recursive,
        );
        if (projection!.root.job.status == JobStatus.completed) break;
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      final int? result = projection!.root.job.result;
      final int? childResult = projection.root.childResults.single.result;
      expect(result, 8);
      expect(childResult, 7);
      final FlowProjection<String, num, int>? direct = await query.get(root);
      expect(direct!.root.children.single.node.job.data, 'child-data');
    },
  );

  test('payload 请求会保留声明的类型和可空值', () {
    const key = JobKey(queueName: 'typed', jobId: 'request');
    const credential = ExecutionCredential(executionId: 'execution');
    // Backend 请求 DTO 都是非泛型的，字段为 Object?
    final data = UpdateJobDataRequest(
      key: key,
      data: null,
      expectedRevision: 1,
      credential: credential,
      occurredAt: DateTime.utc(2026),
    );
    final progress = UpdateJobProgressRequest(
      key: key,
      progress: 0.5,
      expectedRevision: 1,
      credential: credential,
      occurredAt: DateTime.utc(2026),
    );
    final result = TransitionJobRequest(
      key: key,
      result: 7,
      targetStatus: JobStatus.completed,
      expectedRevision: 1,
      credential: credential,
      occurredAt: DateTime.utc(2026),
    );
    final String? typedData = data.data as String?;
    final num? typedProgress = progress.progress as num?;
    final int? typedResult = result.result as int?;
    expect(typedData, isNull);
    expect(typedProgress, 0.5);
    expect(typedResult, 7);
    // JobEvent 构造时 freezeJsonLike，传非法值（非 JSON-like）抛 ArgumentError
    expect(
      () => JobEvent(
        eventId: 'invalid',
        jobKey: key,
        revision: 1,
        type: JobEventType.progress,
        occurredAt: DateTime.utc(2026),
        data: 1,
      ),
      returnsNormally, // int 是合法 JSON-like，构造成功
    );
    expect(
      () => JobSpec(key: key, name: 'invalid', data: DateTime.utc(2026)),
      throwsArgumentError,
    );
  });

  test(
    'flow 通知失败时会保留类型化的已提交事实和原因',
    () async {
      final backend = MemoryQueueBackend();
      final signal = _FailingSignal();
      await backend.open();
      final producer = FlowProducer<String, num, int>(
        backend: backend,
        signal: signal,
      );
      addTearDown(() async {
        await producer.close();
        await signal.close();
        await backend.close();
      });
      const key = JobKey(queueName: 'failed-notification', jobId: 'root');
      try {
        await producer.add(
          FlowJob<String>(key: key, name: 'root', data: 'committed'),
        );
        fail('notification must fail');
      } on FlowNotificationFailure catch (error) {
        // committedFlow 是非泛型原始 FlowSnapshot
        final FlowSnapshot committed = error.committedFlow;
        final String? data = committed.jobs.single.data as String?;
        expect(data, 'committed');
        expect(error.failures.single.error, same(signal.failure));
        expect(error.backendCommitted, isTrue);
        expect((await backend.getJob(key))!.data, 'committed');
      }
      final mismatched = FlowProducer<String, double, int>(
        backend: backend,
        signal: signal,
      );
      addTearDown(mismatched.close);
      try {
        await mismatched.add(
          FlowJob<String>(
            key: const JobKey(
              queueName: 'failed-notification',
              jobId: 'mismatch',
            ),
            name: 'mismatch',
            data: 'committed',
          ),
        );
        fail(
          'notification must fail even when the snapshot type is incompatible',
        );
      } on FlowNotificationFailure catch (error) {
        expect(error.failures.single.error, same(signal.failure));
        // committedFlow 是原始 JSON-like，不会抛 TypeError
        expect(error.committedFlow, isA<FlowSnapshot>());
      }
    },
  );
}
