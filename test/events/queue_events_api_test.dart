import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/events/index.dart';

import 'support/event_fixtures.dart';
import 'support/fake_queue_backend.dart';
import 'support/fake_queue_signal.dart';
import 'support/manual_delay.dart';

void main() {
  // QueueEventsOptions 单测：验证默认值和参数校验。
  group('QueueEventsOptions', () {
    // 验证默认配置字段的稳定性，供外部用户作为文档参考。
    test('暴露稳定的默认值', () {
      final options = QueueEventsOptions();
      expect(options.initialCursor, isNull);
      expect(options.pollInterval, const Duration(seconds: 5));
      expect(options.pageSize, 100);
      expect(options.ownsBackend, isFalse);
      expect(options.ownsSignal, isFalse);
    });

    // pollInterval 必须为正值，pageSize 必须在 1..1000 范围内，否则构造时即抛 ArgumentError。
    test('拒绝无效的 poll 和 page 值', () {
      expect(
        () => QueueEventsOptions(pollInterval: Duration.zero),
        throwsArgumentError,
      );
      expect(
        () => QueueEventsOptions(pollInterval: const Duration(seconds: -1)),
        throwsArgumentError,
      );
      expect(() => QueueEventsOptions(pageSize: 0), throwsArgumentError);
      expect(() => QueueEventsOptions(pageSize: 1001), throwsArgumentError);
    });
  });

  // QueueEvents 构造和生命周期 API 面的单测：验证输入校验、单订阅约束、重复 start 拒绝等。
  group('QueueEvents 构造', () {
    late FakeQueueBackend backend;
    late FakeQueueSignal signal;
    late ManualDelay delay;

    setUp(() {
      backend = FakeQueueBackend(
        capabilities: eventCapabilities(),
        onReadEvents: (_) async =>
            const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
      );
      signal = FakeQueueSignal();
      delay = ManualDelay();
    });

    // queueName 为空字符串时构造应抛 ArgumentError，防止无意义的组件实例被创建。
    test('拒绝空 queue name', () {
      expect(
        () => QueueEvents(
          queueName: '',
          backend: backend,
          signal: signal,
          delay: delay.call,
        ),
        throwsArgumentError,
      );
    });

    // initialCursor 必须与 queueName 匹配，否则属于跨队列消费，构造时拒绝。
    test('拒绝属于其他 queue 的初始 cursor', () {
      expect(
        () => QueueEvents(
          queueName: 'queue_a',
          backend: backend,
          signal: signal,
          delay: delay.call,
          options: QueueEventsOptions(
            initialCursor: EventCursor(queueName: 'queue_b', position: 1),
          ),
        ),
        throwsArgumentError,
      );
    });

    // 对外公开的 API 面必须稳定：queueName / events / start / close 四个成员缺一不可。
    test('暴露稳定的公共 API', () {
      final queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        delay: delay.call,
      );
      verifyQueueEventsApi(queueEvents);
    });

    // events stream 只允许一次 listen，第二次必须抛 StateError，保障 single-subscription 语义。
    test('events stream 只接受一个 listener', () {
      final queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        delay: delay.call,
      );
      expect(() => queueEvents.events.listen((_) {}), returnsNormally);
      expect(() => queueEvents.events.listen((_) {}), throwsStateError);
    });

    // start() 已进入 running 状态后再次调用必须抛 LifecycleError，防止重复启动轮询。
    test('第二次调用 start 会抛出 LifecycleError', () async {
      final queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        delay: delay.call,
      );
      final subs = queueEvents.events.listen((_) {});
      await queueEvents.start();
      await expectLater(queueEvents.start(), throwsA(isA<LifecycleError>()));
      await subs.cancel();
    });
  });
}

/// 验证 QueueEvents 实例的公开 API 面完整且未意外减少成员。
void verifyQueueEventsApi(QueueEvents queueEvents) {
  final String queueName = queueEvents.queueName;
  final Stream<PositionedJobEvent> events = queueEvents.events;
  final Future<void> Function() start = queueEvents.start;
  final Future<void> Function() close = queueEvents.close;
  expect(<Object>[queueName, events, start, close], hasLength(4));
}
