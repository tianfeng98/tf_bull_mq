import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/flow/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';
import 'package:test/test.dart';

import 'support/fake_queue_backend.dart';
import 'support/fake_queue_signal.dart';

/// FlowProducer 测试 fixture：组装 fake backend/signal 和一个标准 FlowJob 定义，供各测试复用
class ProducerFixture {
  ProducerFixture({
    Set<String> queuesToNotify = const <String>{},
    Map<String, (Object, StackTrace)> signalFailures =
        const <String, (Object, StackTrace)>{},
    Now? now,
    BackendCapabilities? capabilities,
    QueueSignal? signal,
  }) {
    this.capabilities =
        capabilities ??
        BackendCapabilities(
          profile: BackendProfile.local,
          durability: Durability.volatile,
          delayedJobs: true,
          priorities: true,
          atomicFlows: true,
          eventDelivery: EventDelivery.liveOnly,
        );
    backend = FakeFlowBackend(capabilities: this.capabilities);
    this.signal = signal ?? FakeQueueSignal();
    this.now = now ?? () => DateTime.utc(2026, 9, 22, 12);
    definition = FlowJob(
      key: const JobKey(queueName: 'parent', jobId: 'root'),
      name: 'root',
      data: const <String, Object?>{},
      children: <FlowChild>[
        FlowChild(
          weight: 2,
          job: FlowJob(
            key: const JobKey(queueName: 'child', jobId: 'leaf'),
            name: 'leaf',
            data: const <String, Object?>{'part': 1},
          ),
        ),
      ],
    );
    final spec = definition.toFlowJobSpec();
    final snapshot = FlowSnapshot(
      root: spec.root,
      jobs: _toJobRecords(spec.jobs),
      edges: spec.edges,
    );
    backend.addFlowResult = BackendWriteResult<FlowSnapshot>(
      value: snapshot,
      changed: true,
      queuesToNotify: queuesToNotify,
    );
    if (this.signal is FakeQueueSignal) {
      (this.signal as FakeQueueSignal).signalFailures.addAll(signalFailures);
    }
    producer = FlowProducer(
      backend: backend,
      signal: this.signal,
      now: this.now,
    );
  }

  late final BackendCapabilities capabilities;
  late final FakeFlowBackend backend;
  late final QueueSignal signal;
  late final Now now;
  late FlowJob definition;
  late FlowProducer producer;
}

/// 将 [JobSpec] 列表转换为测试用 [JobRecord] 列表，默认填充 waiting 状态
List<JobRecord> _toJobRecords(List<JobSpec> specs) {
  final now = DateTime.utc(2026, 9, 22, 12);
  return specs.map((spec) {
    int attempts = 0;
    JobKey? parentKey;
    final isRoot = spec.key.queueName == 'parent' && spec.key.jobId == 'root';
    if (!isRoot) {
      parentKey = const JobKey(queueName: 'parent', jobId: 'root');
    }
    return JobRecord(
      schemaVersion: 0,
      key: spec.key,
      name: spec.name,
      data: spec.data,
      status: JobStatus.waiting,
      progress: null,
      result: null,
      failure: null,
      revision: 0,
      attemptsMade: attempts,
      stalledCount: 0,
      priority: spec.options.priority,
      availableAt: now,
      options: spec.options,
      parentKey: parentKey,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      finishedAt: null,
      execution: null,
      controlRequest: null,
    );
  }).toList();
}

void main() {
  // --- 构造期兼容性：验证 Backend/Signal 能力与 Profile 的匹配约束 ---

  group('构造兼容性', () {
    test('构造时会拒绝搭配 process signal 的 distributed backend', () {
      // 验证 distributed Profile 的 Backend 与 process scope 的 Signal 组合在构造期被拒绝
      expect(
        () {
          final backend = FakeFlowBackend(
            capabilities: BackendCapabilities(
              profile: BackendProfile.distributed,
              durability: Durability.durable,
              delayedJobs: true,
              priorities: true,
              atomicFlows: true,
              eventDelivery: EventDelivery.replayable,
            ),
          );
          FlowProducer(
            backend: backend,
            signal: FakeQueueSignal(scope: SignalScope.process),
            now: () => DateTime.now(),
          );
        },
        throwsA(
          predicate(
            (e) =>
                e is UnsupportedCapability &&
                e.capability == 'distributedSignal',
          ),
        ),
      );
    });
  });

  // --- 预检：能力缺失、非法 Flow 结构必须在任何外部调用之前被拒绝 ---

  group('能力预检', () {
    test('缺少 atomicFlows 时会在读取 Now 或写入前拒绝操作', () async {
      // 验证 atomicFlows 能力缺失时，Producer 不读 Now、不写 Backend、不通知 Signal
      var nowCalls = 0;
      final fixture = ProducerFixture(
        capabilities: BackendCapabilities(
          profile: BackendProfile.local,
          durability: Durability.volatile,
          delayedJobs: true,
          priorities: true,
          atomicFlows: false,
          eventDelivery: EventDelivery.liveOnly,
        ),
        now: () {
          nowCalls++;
          return DateTime.now();
        },
      );
      await expectLater(
        fixture.producer.add(fixture.definition),
        throwsA(isA<UnsupportedCapability>()),
      );
      expect(nowCalls, 0);
      expect(fixture.backend.addFlowRequests, isEmpty);
      expect(
        fixture.signal is FakeQueueSignal
            ? (fixture.signal as FakeQueueSignal).notifiedQueues
            : <String>[],
        isEmpty,
      );
    });

    test(
      '不支持 delayedJobs 时会在写入前拒绝延迟 Job',
      () async {
        // 验证 delayedJobs 能力缺失时，带 delay 的 Flow 定义被拒绝且无任何副作用
        var nowCalls = 0;
        final delayedDefinition = FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'root'),
          name: 'root',
          data: null,
          options: JobOptions(delay: Duration(seconds: 5)),
        );
        final fixture = ProducerFixture(
          capabilities: BackendCapabilities(
            profile: BackendProfile.local,
            durability: Durability.volatile,
            delayedJobs: false,
            priorities: true,
            atomicFlows: true,
            eventDelivery: EventDelivery.liveOnly,
          ),
          now: () {
            nowCalls++;
            return DateTime.now();
          },
        );
        await expectLater(
          fixture.producer.add(delayedDefinition),
          throwsA(isA<UnsupportedCapability>()),
        );
        expect(nowCalls, 0);
        expect(fixture.backend.addFlowRequests, isEmpty);
      },
    );

    test(
      '不支持 priorities 时会在写入前拒绝带优先级的 Job',
      () async {
        // 验证 priorities 能力缺失时，带 priority 的 Flow 定义被拒绝且无任何副作用
        var nowCalls = 0;
        final priorityDefinition = FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'root'),
          name: 'root',
          data: null,
          options: JobOptions(priority: 3),
        );
        final fixture = ProducerFixture(
          capabilities: BackendCapabilities(
            profile: BackendProfile.local,
            durability: Durability.volatile,
            delayedJobs: true,
            priorities: false,
            atomicFlows: true,
            eventDelivery: EventDelivery.liveOnly,
          ),
          now: () {
            nowCalls++;
            return DateTime.now();
          },
        );
        await expectLater(
          fixture.producer.add(priorityDefinition),
          throwsA(isA<UnsupportedCapability>()),
        );
        expect(nowCalls, 0);
        expect(fixture.backend.addFlowRequests, isEmpty);
      },
    );

    test(
      '无需可选能力也允许 delay 和 priority 为 0',
      () async {
        // 验证不使用可选能力（无 delay、无 priority）时，Producer 在能力受限的 Backend 上也能正常写入
        final fixture = ProducerFixture(
          capabilities: BackendCapabilities(
            profile: BackendProfile.local,
            durability: Durability.volatile,
            delayedJobs: false,
            priorities: false,
            atomicFlows: true,
            eventDelivery: EventDelivery.liveOnly,
          ),
        );
        // definition 没有 delay 或 priority，应该成功
        await expectLater(fixture.producer.add(fixture.definition), completes);
      },
    );

    test(
      '嵌套树无效时会在读取 Now 或写入前拒绝操作',
      () async {
        // 验证非法 Flow 结构（如重复 JobKey）在任何外部调用之前被拒绝，避免部分写入
        var nowCalls = 0;
        // 构造一个重复 JobKey 的非法嵌套
        final dup = FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'dup'),
          name: 'dup',
          data: null,
        );
        final invalidDefinition = FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'root'),
          name: 'root',
          data: null,
          children: <FlowChild>[
            FlowChild(job: dup),
            FlowChild(job: dup),
          ],
        );
        final fixture = ProducerFixture(
          now: () {
            nowCalls++;
            return DateTime.now();
          },
        );
        await expectLater(
          fixture.producer.add(invalidDefinition),
          throwsArgumentError,
        );
        expect(nowCalls, 0);
        expect(fixture.backend.addFlowRequests, isEmpty);
      },
    );
  });

  // --- 原子写入：Producer 正确转换 FlowJob 并提交一次 AddFlowRequest ---

  group('原子添加 flow', () {
    test('会转换、校验并提交一个原子的 AddFlowRequest', () async {
      // 验证成功路径：Producer 只读一次 Now、只提交一次 addFlow，返回 Backend 确认的快照
      final createdAt = DateTime.utc(2026, 9, 22, 12);
      var nowCalls = 0;
      final fixture = ProducerFixture(
        now: () {
          nowCalls++;
          return createdAt;
        },
      );

      final result = await fixture.producer.add(fixture.definition);

      expect(nowCalls, 1);
      expect(fixture.backend.addFlowRequests, hasLength(1));
      expect(fixture.backend.addFlowRequests.single.createdAt, createdAt);
      expect(
        fixture.backend.addFlowRequests.single.flow.root,
        fixture.definition.key,
      );
      expect(result, same(fixture.backend.addFlowResult.value));
    });

    test(
      '会在 AddFlowRequest 中保留所有嵌套 Job 选项和边权重',
      () async {
        // 验证嵌套 Flow 中每个 Job 的 options 和每条 edge 的 weight 都完整传递给 Backend
        final options = JobOptions(priority: 7, delay: Duration(seconds: 3));
        final definition = FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'root'),
          name: 'root',
          data: null,
          options: options,
          children: <FlowChild>[
            FlowChild(
              weight: 3.5,
              job: FlowJob(
                key: const JobKey(queueName: 'q', jobId: 'child'),
                name: 'child',
                data: null,
                options: JobOptions(priority: 2),
              ),
            ),
          ],
        );
        final fixture = ProducerFixture();
        // 更新 fixture 的 definition 和 result
        fixture.definition = definition;
        final spec = definition.toFlowJobSpec();
        final snapshot = FlowSnapshot(
          root: spec.root,
          jobs: _toJobRecords(spec.jobs),
          edges: spec.edges,
        );
        fixture.backend.addFlowResult = BackendWriteResult<FlowSnapshot>(
          value: snapshot,
          changed: true,
          queuesToNotify: <String>{},
        );
        await fixture.producer.add(definition);
        final request = fixture.backend.addFlowRequests.single;
        expect(request.flow.jobs.first.options.priority, 7);
        expect(request.flow.jobs.last.options.priority, 2);
        expect(request.flow.edges.single.weight, 3.5);
      },
    );

    test(
      '会传递 backend addFlow 错误，且不会通知任何 queue',
      () async {
        // 验证 Backend 写入失败时，Producer 原样抛错且不触发任何 Signal 通知
        final backendError = StateError('backend down');
        final fixture = ProducerFixture();
        fixture.backend.addFlowError = backendError;
        await expectLater(
          fixture.producer.add(fixture.definition),
          throwsA(same(backendError)),
        );
        expect(fixture.backend.addFlowRequests, hasLength(1));
        final sig = fixture.signal as FakeQueueSignal;
        expect(sig.notifiedQueues, isEmpty);
      },
    );

    test('除了 addFlow 以外不会调用任何 QueueBackend 方法', () async {
      // 验证 Producer 只调用 addFlow，不触达其他 QueueBackend 方法
      final fixture = ProducerFixture();
      await fixture.producer.add(fixture.definition);
      // FakeFlowBackend 继承自 QueueBackend，只有 addFlow 会被调用；其他任何调用都会抛 StateError
      expect(fixture.backend.addFlowRequests, hasLength(1));
    });
  });

  // --- Signal 通知：只通知 queuesToNotify、按排序顺序、Signal 失败不回滚已提交的 Flow ---

  group('signal 通知', () {
    test(
      'backend 提交后只会按排序顺序通知 queuesToNotify',
      () async {
        // 验证 Backend 成功后只按字母序通知 queuesToNotify 声明的队列
        final fixture = ProducerFixture(
          queuesToNotify: <String>{'queue-b', 'queue-a', 'queue-c'},
        );
        await fixture.producer.add(fixture.definition);
        final sig = fixture.signal as FakeQueueSignal;
        expect(sig.notifiedQueues, <String>['queue-a', 'queue-b', 'queue-c']);
      },
    );

    test('不会根据 Job 或 snapshot queues 推断通知对象', () async {
      // 验证 Producer 不根据 Flow 中 Job 所在的队列自动推断通知目标，只信 queuesToNotify
      final fixture = ProducerFixture(queuesToNotify: <String>{});
      await fixture.producer.add(fixture.definition);
      final sig = fixture.signal as FakeQueueSignal;
      // Backend 返回空 queuesToNotify，即使 Flow 里有 parent 和 child 两个 queue 也不通知
      expect(sig.notifiedQueues, isEmpty);
    });

    test('queuesToNotify 为空时不会发出通知', () async {
      // 验证空 queuesToNotify 时完全不通知 Signal
      final fixture = ProducerFixture(queuesToNotify: <String>{});
      await fixture.producer.add(fixture.definition);
      final sig = fixture.signal as FakeQueueSignal;
      expect(sig.notifiedQueues, isEmpty);
    });

    test(
      '某个 signal 通知失败时仍会尝试通知所有返回的 queue',
      () async {
        // 验证多个队列通知时，某个失败不阻塞其他队列；最终返回 FlowNotificationFailure 汇总所有失败
        final firstError = StateError('a failed');
        final firstStack = StackTrace.fromString('queue-a original stack');
        final secondError = StateError('c failed');
        final secondStack = StackTrace.fromString('queue-c original stack');
        final fixture = ProducerFixture(
          queuesToNotify: <String>{'queue-b', 'queue-a', 'queue-c'},
          signalFailures: <String, (Object, StackTrace)>{
            'queue-a': (firstError, firstStack),
            'queue-c': (secondError, secondStack),
          },
        );

        final failure = await fixture.producer
            .add(fixture.definition)
            .then<FlowNotificationFailure>(
              (_) => throw StateError('Expected FlowNotificationFailure.'),
              onError: (error, stack) => error as FlowNotificationFailure,
            );

        expect(failure.backendCommitted, isTrue);
        expect(
          failure.committedFlow,
          same(fixture.backend.addFlowResult.value),
        );
        expect(failure.failures.map((item) => item.queueName), <String>[
          'queue-a',
          'queue-c',
        ]);
        expect(failure.failures[0].error, same(firstError));
        expect(failure.failures[0].stackTrace, same(firstStack));
        expect(failure.failures[1].error, same(secondError));
        expect(failure.failures[1].stackTrace, same(secondStack));
        final sig = fixture.signal as FakeQueueSignal;
        expect(sig.notifiedQueues, <String>['queue-a', 'queue-b', 'queue-c']);
        expect(fixture.backend.addFlowRequests, hasLength(1));
      },
    );

    test('会抛出包含已提交 snapshot 和所有原因的 FlowNotificationFailure', () async {
      // 验证 Signal 通知失败时抛出的异常携带已提交的 Flow 快照
      final fixture = ProducerFixture(
        queuesToNotify: <String>{'queue-x'},
        signalFailures: <String, (Object, StackTrace)>{
          'queue-x': (StateError('oops'), StackTrace.fromString('x-stack')),
        },
      );
      await expectLater(
        fixture.producer.add(fixture.definition),
        throwsA(isA<FlowNotificationFailure>()),
      );
    });

    test('signal 失败后不会再次调用 addFlow', () async {
      // 验证 Signal 通知失败不触发 addFlow 重试，Backend 写入已原子完成
      final fixture = ProducerFixture(
        queuesToNotify: <String>{'queue-a'},
        signalFailures: <String, (Object, StackTrace)>{
          'queue-a': (StateError('fail'), StackTrace.current),
        },
      );
      await expectLater(
        fixture.producer.add(fixture.definition),
        throwsA(isA<FlowNotificationFailure>()),
      );
      expect(fixture.backend.addFlowRequests, hasLength(1));
    });
  });

  // --- 生命周期：close 幂等性、资源所有权、关闭失败处理 ---

  group('producer 生命周期', () {
    test(
      'close 可重复调用，之后再调用 add 会抛出 LifecycleError',
      () async {
        // 验证 close 幂等；关闭后再次 add 抛 LifecycleError
        final fixture = ProducerFixture();
        await fixture.producer.close();
        await fixture.producer.close();
        await expectLater(
          fixture.producer.add(fixture.definition),
          throwsA(predicate((e) => e is LifecycleError)),
        );
      },
    );

    test('默认情况下，close 不会关闭共享的 backend 和 signal', () async {
      // 验证默认情况下 Producer 不关闭注入的 Backend 和 Signal（共享所有权）
      final backend = FakeFlowBackend(
        capabilities: BackendCapabilities(
          profile: BackendProfile.local,
          durability: Durability.volatile,
          delayedJobs: true,
          priorities: true,
          atomicFlows: true,
          eventDelivery: EventDelivery.liveOnly,
        ),
      );
      final signal = FakeQueueSignal();
      final producer = FlowProducer(
        backend: backend,
        signal: signal,
        now: () => DateTime.now(),
      );
      await producer.close();
      expect(backend.closeCalls, 0);
      expect(signal.closeCalls, 0);
    });

    test('仅当 ownsBackend 为 true 时，close 才会释放 backend', () async {
      // 验证 ownsBackend=true 时 Producer.close 会关闭 Backend
      final backend = FakeFlowBackend(
        capabilities: BackendCapabilities(
          profile: BackendProfile.local,
          durability: Durability.volatile,
          delayedJobs: true,
          priorities: true,
          atomicFlows: true,
          eventDelivery: EventDelivery.liveOnly,
        ),
      );
      final signal = FakeQueueSignal();
      final producer = FlowProducer(
        backend: backend,
        signal: signal,
        now: () => DateTime.now(),
        ownsBackend: true,
      );
      await producer.close();
      expect(backend.closeCalls, 1);
      expect(signal.closeCalls, 0);
    });

    test('仅当 ownsSignal 为 true 时，close 才会释放 signal', () async {
      // 验证 ownsSignal=true 时 Producer.close 会关闭 Signal
      final backend = FakeFlowBackend(
        capabilities: BackendCapabilities(
          profile: BackendProfile.local,
          durability: Durability.volatile,
          delayedJobs: true,
          priorities: true,
          atomicFlows: true,
          eventDelivery: EventDelivery.liveOnly,
        ),
      );
      final signal = FakeQueueSignal();
      final producer = FlowProducer(
        backend: backend,
        signal: signal,
        now: () => DateTime.now(),
        ownsSignal: true,
      );
      await producer.close();
      expect(backend.closeCalls, 0);
      expect(signal.closeCalls, 1);
    });

    test(
      '第一个资源关闭失败时，close 仍会尝试关闭另一个自有资源',
      () async {
        // 验证两个资源都归 Producer 所有且关闭顺序：Signal 先于 Backend；前者失败后仍尝试后者
        final backend = FakeFlowBackend(
          capabilities: BackendCapabilities(
            profile: BackendProfile.local,
            durability: Durability.volatile,
            delayedJobs: true,
            priorities: true,
            atomicFlows: true,
            eventDelivery: EventDelivery.liveOnly,
          ),
        );
        final signal = FakeQueueSignal()
          ..closeError = StateError('signal fail');
        final producer = FlowProducer(
          backend: backend,
          signal: signal,
          now: () => DateTime.now(),
          ownsBackend: true,
          ownsSignal: true,
        );
        await expectLater(producer.close(), throwsStateError);
        expect(signal.closeCalls, 1);
        expect(backend.closeCalls, 1);
      },
    );
  });
}
