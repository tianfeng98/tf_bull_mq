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
  // 在 start 之前就 close 的情况下，组件不应访问任何外部依赖。
  test(
    'start 前调用 close 会结束 stream，且不会读取 backend',
    () async {
      final backend = FakeQueueBackend(
        capabilities: eventCapabilities(),
        onReadEvents: (_) async =>
            const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
      );
      final signal = FakeQueueSignal();
      final delay = ManualDelay();
      final queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        delay: delay.call,
      );
      final subs = queueEvents.events.listen((_) {});
      await queueEvents.close();
      await subs.cancel();
      // 未 start 即 close，不应产生任何 read 请求，也不应调用 backend.open。
      expect(backend.readRequests, isEmpty);
      expect(backend.openCalls, 0);
    },
  );

  // close() 必须等待内部 stream controller 关闭后才返回，保证调用方看到 done。
  test(
    'close 会取消 signal 订阅并关闭 events stream',
    () async {
      final backend = FakeQueueBackend(
        capabilities: eventCapabilities(),
        onReadEvents: (_) async => JobEventPage(
          items: [positionedEvent('orders', 1)],
          nextCursor: null,
        ),
      );
      final signal = FakeQueueSignal();
      final delay = ManualDelay();
      final queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        delay: delay.call,
      );
      final seen = <PositionedJobEvent>[];
      final done = Completer<void>();
      queueEvents.events.listen(seen.add, onDone: done.complete);
      await queueEvents.start();
      expect(seen, isNotEmpty);
      await queueEvents.close();
      // close() 内部必须等待 controller done 后再返回。
      expect(done.isCompleted, isTrue);
    },
  );

  // close() 在 closing 状态和 closed 状态都应幂等返回同一个 Future，不得抛错。
  test('close 在关闭过程中和关闭后均可重复调用', () async {
    final backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final signal = FakeQueueSignal();
    final delay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
    );
    final subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
    final f1 = queueEvents.close();
    final f2 = queueEvents.close();
    await Future.wait([f1, f2]);
    // 两次 close 都应无错误完成。
    await subs.cancel();
  });

  // 组件已进入 closed 状态后再调用 start() 必须抛 LifecycleError。
  test('close 后调用 start 会抛出 LifecycleError', () async {
    final backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final signal = FakeQueueSignal();
    final delay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
    );
    final subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
    await queueEvents.close();
    await expectLater(queueEvents.start(), throwsA(isA<LifecycleError>()));
    await subs.cancel();
  });

  // 组件已关闭后再获取 events getter 必须抛 LifecycleError，防止外部拿到已关闭 stream。
  test('close 后获取 events 会抛出 LifecycleError', () async {
    final backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final signal = FakeQueueSignal();
    final delay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
    );
    await queueEvents.start();
    await queueEvents.close();
    expect(() => queueEvents.events, throwsA(isA<LifecycleError>()));
  });

  // ownsBackend / ownsSignal 为 false 时，close() 不应关闭被注入的 backend 和 signal。
  test('默认情况下，close 不会关闭注入的 backend 和 signal', () async {
    final backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final signal = FakeQueueSignal();
    final delay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
    );
    final subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
    await queueEvents.close();
    await subs.cancel();
    expect(backend.closeCalls, 0);
    expect(signal.closeCalls, 0);
    expect(backend.openCalls, 0);
  });

  // ownsBackend 为 true 时 close() 必须且仅关闭一次 backend，组件自己不会提前 open。
  test('ownsBackend 会且仅会关闭已打开的 backend 一次', () async {
    final backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final signal = FakeQueueSignal();
    final delay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
      options: QueueEventsOptions(ownsBackend: true),
    );
    final subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
    await queueEvents.close();
    await subs.cancel();
    expect(backend.closeCalls, 1);
    expect(backend.openCalls, 0);
  });

  // ownsSignal 为 true 时 close() 必须且仅关闭一次 signal。
  test('ownsSignal 会且仅会关闭 signal 一次', () async {
    final backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final signal = FakeQueueSignal();
    final delay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
      options: QueueEventsOptions(ownsSignal: true),
    );
    final subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
    await queueEvents.close();
    await subs.cancel();
    expect(signal.closeCalls, 1);
  });

  // close() 必须等待正在执行的 drain 完成后才真正关闭 stream，防止漏掉 in-flight 读到的事件。
  test(
    'close 会等待正在进行的 drain 完成后再关闭 stream',
    () async {
      final backend = FakeQueueBackend(
        capabilities: eventCapabilities(),
        onReadEvents: (req) async {
          final base = req.cursor?.position ?? 0;
          return JobEventPage(
            items: [positionedEvent('orders', base + 1)],
            nextCursor: null,
          );
        },
      );
      final signal = FakeQueueSignal();
      final delay = ManualDelay();
      final queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        delay: delay.call,
      );
      final seen = <PositionedJobEvent>[];
      queueEvents.events.listen(seen.add);
      await queueEvents.start();

      // 先挂起一次 drain，让 close 观察到 in-flight 状态。
      backend.enqueueReadCompleter();
      signal.emit('orders');
      await Future<void>.delayed(Duration.zero);
      final closeFuture = queueEvents.close();
      // close() 不应在 pending read 完成之前就 resolve。
      bool closeDone = false;
      closeFuture.then((_) => closeDone = true);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(closeDone, isFalse);
      // 手动完成挂起的 read，close 随后正常完成。
      backend.completeNextRead(
        ReadJobEventsRequest(queueName: 'orders', cursor: null, limit: 100),
      );
      await closeFuture;
      expect(seen.isNotEmpty, isTrue);
    },
  );
}
