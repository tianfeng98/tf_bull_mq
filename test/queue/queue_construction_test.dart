import 'dart:mirrors';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';

import 'support/fake_queue_backend.dart';
import 'support/fake_queue_signal.dart';

void main() {
  group('QueueOptions', () {
    /// 验证默认 QueueOptions 不拥有 Backend 和 Signal（共享模式），
    /// defaultPolicy 中所有策略字段为 null（沿用 Backend 默认行为）。
    test('QueueOptions 默认使用共享资源和空 policy', () {
      final QueueOptions options = QueueOptions();

      expect(options.ownsBackend, isFalse);
      expect(options.ownsSignal, isFalse);
      expect(options.defaultPolicy.defaultJobLogRetention, isNull);
      expect(options.defaultPolicy.maxEventCount, isNull);
      expect(options.defaultPolicy.eventRetention, isNull);
    });

    /// 验证 QueueOptions 只暴露 3 个字段：defaultPolicy、ownsBackend、ownsSignal，
    /// 不携带任何 JobOptions 默认策略——Job 级配置应由 add() 单独传入。
    test(
      'QueueOptions 包含 policy 和 ownership，但不包含 JobOptions 默认值',
      () {
        final QueuePolicy policy = QueuePolicy(maxEventCount: 50);
        final QueueOptions options = QueueOptions(
          defaultPolicy: policy,
          ownsBackend: true,
          ownsSignal: true,
        );

        expect(options.defaultPolicy, same(policy));
        expect(options.ownsBackend, isTrue);
        expect(options.ownsSignal, isTrue);

        final List<String> fields =
            reflectClass(QueueOptions).declarations.values
                .whereType<VariableMirror>()
                .map(
                  (VariableMirror field) =>
                      MirrorSystem.getName(field.simpleName),
                )
                .toList()
              ..sort();
        expect(fields, <String>['defaultPolicy', 'ownsBackend', 'ownsSignal']);
      },
    );
  });

  group('QueueJobDraft', () {
    /// 验证 QueueJobDraft 的两个入参校验：name/jobId 不得为纯空白。
    /// JSON-like 校验已迁移到 Queue 层，由 PayloadCodec 在写入 Backend 前执行。
    test('QueueJobDraft 会验证 name 和 jobId', () {
      expect(
        () => QueueJobDraft(name: '   ', data: null),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => QueueJobDraft(name: 'task', data: null, jobId: '  '),
        throwsA(isA<ArgumentError>()),
      );
      // QueueJobDraft 不再校验 JSON-like data——允许任意 Dart 对象，
      // Queue 层 PayloadCodec 编码时才会做 freezeJsonLike 校验。
      QueueJobDraft(name: 'task', data: Object());
      QueueJobDraft(name: 'task', data: const <Object?>[Object()]);

      final QueueJobDraft draft = QueueJobDraft(
        name: 'task',
        data: const <String, Object?>{'id': 1},
        jobId: 'raw-id',
      );
      expect(draft.name, 'task');
      expect(draft.jobId, 'raw-id');
      expect(draft.options.priority, 0);
      expect(draft.options.delay, Duration.zero);
    });
  });

  group('QueueJobPage', () {
    /// 验证 QueueJobPage.items 做防御性拷贝：外部传入的 list 被 copy，
    /// 返回的 items 不可变（clear 抛 UnsupportedError）。
    test('QueueJobPage 会防御性地复制 Job 句柄', () {
      final List<Job> source = <Job>[];
      final QueueJobPage page = QueueJobPage(
        items: source,
        nextCursor: 'cursor-1',
      );

      expect(page.items, isNot(same(source)));
      expect(page.items, isEmpty);
      expect(page.nextCursor, 'cursor-1');
      expect(() => page.items.clear(), throwsUnsupportedError);
    });
  });

  group('Queue 构造', () {
    /// 验证 queueName 为空白字符串（含全空格、制表符、换行）时构造直接抛
    /// ArgumentError，Backend 和 Signal 完全不被触碰。
    test('构造时会拒绝空或仅包含空白字符的 queueName', () {
      final FakeQueueBackend backend = FakeQueueBackend();
      final FakeQueueSignal signal = FakeQueueSignal();

      for (final String queueName in <String>['', '   ', '\t\n']) {
        expect(
          () => Queue(
            queueName: queueName,
            backend: backend,
            signal: signal,
            idGenerator: () => 'job-1',
            now: () => DateTime.utc(2026, 1, 1),
          ),
          throwsA(isA<ArgumentError>()),
          reason: 'queueName $queueName must be rejected',
        );
      }
      expect(backend.calls, isEmpty);
    });

    /// 验证 distributed Backend 与 process Signal 组合不兼容——
    /// 分布式场景要求 Signal 也能跨进程工作，构造时抛 UnsupportedCapability。
    test('构造时会拒绝搭配 process signal 的 distributed backend', () {
      expect(
        () => Queue(
          queueName: 'work',
          backend: FakeQueueBackend(
            capabilities: FakeQueueBackend.distributedCapabilities(),
          ),
          signal: FakeQueueSignal(scope: SignalScope.process),
          idGenerator: () => 'job-1',
          now: () => DateTime.utc(2026, 1, 1),
        ),
        throwsA(
          isA<UnsupportedCapability>().having(
            (UnsupportedCapability error) => error.capability,
            'capability',
            'distributedSignal',
          ),
        ),
      );
    });

    /// 验证两个合法组合都能成功构造：local Backend + process Signal、
    /// distributed Backend + distributed Signal，isClosed 均为 false。
    test('接受 local backend 与 process signal、distributed backend 与 distributed signal 的组合', () {
      final Queue local = Queue(
        queueName: 'work',
        backend: FakeQueueBackend(),
        signal: FakeQueueSignal(scope: SignalScope.process),
        idGenerator: () => 'job-1',
        now: () => DateTime.utc(2026, 1, 1),
      );
      final Queue distributed = Queue(
        queueName: 'work',
        backend: FakeQueueBackend(
          capabilities: FakeQueueBackend.distributedCapabilities(),
        ),
        signal: FakeQueueSignal(scope: SignalScope.distributed),
        idGenerator: () => 'job-1',
        now: () => DateTime.utc(2026, 1, 1),
      );

      expect(local.isClosed, isFalse);
      expect(distributed.isClosed, isFalse);
    });

    /// 验证 Queue 构造是纯内存操作：不打开 Backend、不关闭 Signal，
    /// 完全不触碰任何外部资源生命周期。
    test('构造 Queue 时不会打开或关闭 backend', () {
      final FakeQueueBackend backend = FakeQueueBackend();
      final FakeQueueSignal signal = FakeQueueSignal();
      final Queue queue = Queue(
        queueName: 'work',
        backend: backend,
        signal: signal,
        idGenerator: () => 'job-1',
        now: () => DateTime.utc(2026, 1, 1),
      );

      expect(queue.queueName, 'work');
      expect(queue.isClosed, isFalse);
      expect(backend.calls, isEmpty);
      expect(backend.openCalls, 0);
      expect(backend.closeCalls, 0);
      expect(signal.closeCalls, 0);
    });

    /// 验证 queueName 不做 trim 或规范化——保持调用方原值，
    /// 即使包含前后空白也原样传递（构造期只拒绝全空白）。
    test('会保留原始 queueName，不执行去空格或规范化', () {
      final Queue queue = Queue(
        queueName: ' work.queue ',
        backend: FakeQueueBackend(),
        signal: FakeQueueSignal(),
        idGenerator: () => 'job-1',
        now: () => DateTime.utc(2026, 1, 1),
      );

      expect(queue.queueName, ' work.queue ');
      expect(queue.options.ownsBackend, isFalse);
      expect(queue.options.ownsSignal, isFalse);
    });
  });
}
