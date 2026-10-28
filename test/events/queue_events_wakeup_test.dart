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
  late StreamSubscription<PositionedJobEvent> subs;

  /// 统一的启动辅助：构造 QueueEvents 并执行 start()。
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
    subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
  }

  tearDown(() async {
    try {
      await queueEvents.close();
    } catch (_) {}
    await subs.cancel();
  });

  // start() 必须完成一次初始 drain，保证监听器拿到已存在的历史事件。
  test('start 会初始读取一次 backend', () async {
    int reads = 0;
    await startWith(
      handler: (req) async {
        reads++;
        return JobEventPage(items: [], nextCursor: null);
      },
    );
    expect(reads, 1);
  });

  // signal 提示只触发 drain，不直接推送数据——所有事件都必须来自 Backend 读取结果。
  test(
    'signal 提示会触发 backend 读取，但不会直接发出数据',
    () async {
      int reads = 0;
      await startWith(
        handler: (req) async {
          reads++;
          return JobEventPage(items: [], nextCursor: null);
        },
      );
      // start 之后已有 1 次 read。再 emit signal 应触发第二次 drain。
      signal.emit('orders');
      // drain 是异步的，让出 microtask 让其执行。
      await Future<void>.delayed(Duration.zero);
      expect(reads, greaterThan(1));
    },
  );

  // poll 定时器触发后也必须执行一次 drain，作为 signal 的兜底机制。
  test('poll delay 完成后会触发 backend 读取', () async {
    int reads = 0;
    await startWith(
      handler: (req) async {
        reads++;
        return JobEventPage(items: [], nextCursor: null);
      },
    );
    final readsAfterStart = reads;
    delay.completeNext();
    await Future<void>.delayed(Duration.zero);
    expect(reads, greaterThan(readsAfterStart));
  });

  // signal 断裂后，signal 提示不再触发 drain；但 poll 必须继续作为补偿机制。
  test('下一次轮询会弥补丢失的 signal', () async {
    await startWith(
      handler: (req) async {
        // 每次返回空页——cursor 始终为 null。
        return JobEventPage(items: [], nextCursor: null);
      },
    );
    final readsAfterStart = backend.readRequests.length;
    // 模拟 signal 断裂。
    signal.emitError('orders', StateError('signal down'));
    await Future<void>.delayed(Duration.zero);
    // signal 断裂后 emit 不应触发 read。
    signal.emit('orders');
    await Future<void>.delayed(Duration.zero);
    expect(backend.readRequests.length, readsAfterStart);
    // poll 补偿触发新的 read。
    delay.completeNext();
    await Future<void>.delayed(Duration.zero);
    expect(backend.readRequests.length, greaterThan(readsAfterStart));
  });

  // single-flight 语义：drain 正在执行期间到达的多个 signal 提示必须合并成一次 drain，
  // 不能并发发起多次 readJobEvents。
  test('signal 风暴期间最多只有一次 backend 读取在进行', () async {
    final stormBackend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    final stormSignal = FakeQueueSignal();
    final stormDelay = ManualDelay();
    final queueEvents = QueueEvents(
      queueName: 'orders',
      backend: stormBackend,
      signal: stormSignal,
      delay: stormDelay.call,
    );
    final subs = queueEvents.events.listen((_) {});
    await queueEvents.start();
    // 先挂起一次 read，让后续 drain 保持 in-flight。
    final heldCompleter = stormBackend.enqueueReadCompleter();
    stormSignal.emit('orders');
    await Future<void>.delayed(Duration.zero);
    // 再连续触发 100 个 signal 提示——它们必须合并到同一个 in-flight drain。
    for (var i = 0; i < 100; i++) {
      stormSignal.emit('orders');
    }
    stormDelay.completeNext();
    await Future<void>.delayed(Duration.zero);
    // 峰值并发 read 数必须为 1，说明 single-flight 生效。
    expect(stormBackend.maxActiveReads, 1);
    heldCompleter.complete(
      JobEventPage(items: const <PositionedJobEvent>[], nextCursor: null),
    );
    await Future<void>.delayed(Duration.zero);
    expect(stormBackend.maxActiveReads, 1);
    await subs.cancel();
    await queueEvents.close();
  });

  // 轮询循环请求的每次 delay 间隔必须与 QueueEventsOptions.pollInterval 一致。
  test('每轮 poll 请求都会使用配置的间隔', () async {
    await startWith(
      options: QueueEventsOptions(pollInterval: const Duration(seconds: 42)),
      handler: (req) async => JobEventPage(items: [], nextCursor: null),
    );
    // start 会触发第一次 poll delay，手动完成两次推进多轮。
    delay.completeNext();
    await Future<void>.delayed(Duration.zero);
    delay.completeNext();
    await Future<void>.delayed(Duration.zero);
    // 所有请求的 delay 都应等于配置值。
    for (final d in delay.requestedDurations) {
      expect(d, const Duration(seconds: 42));
    }
  });

  // delay 是可选参数：省略时必须回退到组件内部的默认等待实现（真实计时器），
  // 轮询兜底仍然生效。用极短的 pollInterval 避免测试真实等待数秒。
  test(
    '未提供 delay 时会回退到默认计时器，轮询仍会继续',
    () async {
      var reads = 0;
      backend = FakeQueueBackend(
        capabilities: eventCapabilities(),
        onReadEvents: (_) async {
          reads++;
          return JobEventPage(
            items: const <PositionedJobEvent>[],
            nextCursor: null,
          );
        },
      );
      signal = FakeQueueSignal();
      queueEvents = QueueEvents(
        queueName: 'orders',
        backend: backend,
        signal: signal,
        options: QueueEventsOptions(
          pollInterval: const Duration(milliseconds: 10),
        ),
      );
      subs = queueEvents.events.listen((_) {});
      await queueEvents.start();
      // 首次 drain 仍然发生，且不经过计时路径。
      expect(reads, 1);
      // 留出远大于一个轮询周期的裕度，默认计时器必须完成并触发后续 read。
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(reads, greaterThan(1));
    },
  );
}
