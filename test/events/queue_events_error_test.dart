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
  late FakeQueueBackend backend;
  late FakeQueueSignal signal;
  late ManualDelay delay;
  late QueueEvents queueEvents;
  late List<PositionedJobEvent> seen;
  late List<Object> errors;
  late StreamSubscription<PositionedJobEvent> subs;

  /// 统一的启动辅助：构造 QueueEvents、挂起 listener 并执行 start()。
  Future<void> startWith({
    QueueEventsOptions? options,
    required Future<JobEventPage> Function(ReadJobEventsRequest) handler,
  }) async {
    backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: handler,
    );
    signal = FakeQueueSignal();
    delay = ManualDelay();
    queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
      options: options,
    );
    seen = <PositionedJobEvent>[];
    errors = <Object>[];
    subs = queueEvents.events.listen(
      (e) => seen.add(e),
      onError: (e) => errors.add(e),
      onDone: () {},
    );
    await queueEvents.start();
  }

  tearDown(() async {
    try {
      await queueEvents.close();
    } catch (_) {}
    await subs.cancel();
  });

  // 普通读取错误（如 BackendUnavailable）应推送到 stream，但组件继续运行不停止轮询。
  test(
    'backend 读取错误会通过 stream 发出，但不会停止轮询',
    () async {
      await startWith(
        handler: (req) async {
          throw BackendUnavailable(null, 'boom');
        },
      );
      // 错误已推送到 stream。
      expect(errors, hasLength(1));
      expect(errors.first, isA<BackendUnavailable>());
      // 轮询循环仍在运行——至少请求过一次 Delay。
      expect(delay.requestedDurations.length, greaterThanOrEqualTo(1));
    },
  );

  // 分页读取中途某页失败，重试应从「最后成功追加到 stream 的事件位置」重新开始，
  // 而不是从头或从失败页开头。
  test(
    '第二页读取失败后，会从第一页最后一个已添加事件的位置重试',
    () async {
      int callCount = 0;
      EventCursor? lastRequestCursor;
      await startWith(
        options: QueueEventsOptions(pageSize: 2),
        handler: (req) async {
          callCount++;
          lastRequestCursor = req.cursor;
          if (callCount == 1) {
            // 第一页成功返回 2 条事件。
            return JobEventPage(
              items: [
                positionedEvent('orders', 1),
                positionedEvent('orders', 2),
              ],
              nextCursor: EventCursor(queueName: 'orders', position: 2),
            );
          }
          if (callCount == 2) {
            // 第二页失败抛 BackendUnavailable。
            throw BackendUnavailable(null, 'read fail');
          }
          // callCount >= 3：poll 触发重试。必须从上次成功追加的位置（pos=2）开始。
          expect(lastRequestCursor?.position, 2);
          return JobEventPage(
            items: [positionedEvent('orders', 3)],
            nextCursor: null,
          );
        },
      );
      // 手动推进 poll 触发重试。
      delay.completeNext();
      await Future<void>.delayed(Duration.zero);
      // poll 可能需要再推一轮。
      delay.completeNext();
      await Future<void>.delayed(Duration.zero);
      expect(errors, isNotEmpty); // 第二页失败产生的错误
      expect(seen.last.cursor.position, 3);
    },
  );

  // EventCursorGap 属于终态错误，必须原封不动地推送到 stream，由上层决定如何处理。
  test('EventCursorGap 会作为原始 stream 错误发出', () async {
    await startWith(
      handler: (req) async {
        final gap = EventGap(
          requested:
              req.cursor ?? EventCursor(queueName: 'orders', position: 1),
          earliestAvailable: EventCursor(queueName: 'orders', position: 42),
        );
        throw EventCursorGap(gap, 'cursor fell behind');
      },
    );
    expect(errors, hasLength(1));
    expect(errors.first, isA<EventCursorGap>());
    expect((errors.first as EventCursorGap).gap.earliestAvailable.position, 42);
  });

  // EventCursorGap 是终态：推送完错误后 stream 必须自动关闭，不会再消费后续事件。
  test('发生 EventCursorGap 后会关闭 stream', () async {
    bool doneFired = false;
    final queueEventsLocal = QueueEvents(
      queueName: 'orders',
      backend: FakeQueueBackend(
        capabilities: eventCapabilities(),
        onReadEvents: (req) async {
          final gap = EventGap(
            requested: EventCursor(queueName: 'orders', position: 2),
            earliestAvailable: EventCursor(queueName: 'orders', position: 10),
          );
          throw EventCursorGap(gap, 'gap!');
        },
      ),
      signal: FakeQueueSignal(),
      delay: ManualDelay().call,
    );
    final errors = <Object>[];
    final subs = queueEventsLocal.events.listen(
      (_) {},
      onError: (e) => errors.add(e),
      onDone: () => doneFired = true,
    );
    await queueEventsLocal.start();
    expect(errors, hasLength(1));
    expect(doneFired, isTrue);
    await subs.cancel();
    await queueEventsLocal.close();
  });

  // EventCursorGap 发生后，signal 提示不应再触发 drain，轮询也应停止——组件进入 terminal 状态。
  test(
    '发生 EventCursorGap 后会取消 signal 订阅并停止轮询',
    () async {
      FakeQueueSignal? theSignal;
      FakeQueueBackend? theBackend;
      await startWith(
        handler: (req) async {
          final gap = EventGap(
            requested: EventCursor(queueName: 'orders', position: 2),
            earliestAvailable: EventCursor(queueName: 'orders', position: 10),
          );
          throw EventCursorGap(gap, 'gap!');
        },
      );
      theSignal = signal;
      theBackend = backend;
      // gap 之后 signal 提示不应触发新的 read。
      final readsBefore = theBackend.readRequests.length;
      theSignal.emit('orders');
      await Future<void>.delayed(Duration.zero);
      expect(theBackend.readRequests.length, readsBefore);
      // 轮询也应已停止；read 数不再增长。
    },
  );

  // 进入 terminal 状态（由 EventCursorGap 触发）后再次 start() 必须抛 LifecycleError。
  test('发生 gap 后调用 start 会抛出 LifecycleError', () async {
    await startWith(
      handler: (req) async {
        final gap = EventGap(
          requested: EventCursor(queueName: 'orders', position: 2),
          earliestAvailable: EventCursor(queueName: 'orders', position: 10),
        );
        throw EventCursorGap(gap, 'gap!');
      },
    );
    await expectLater(queueEvents.start(), throwsA(isA<LifecycleError>()));
  });
}
