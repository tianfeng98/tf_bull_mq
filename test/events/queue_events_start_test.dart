import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/events/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';

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

  /// 统一构造 QueueEvents 并挂起 listener（不执行 start）。
  void build({
    BackendProfile profile = BackendProfile.local,
    EventDelivery delivery = EventDelivery.liveOnly,
    SignalScope scope = SignalScope.process,
    Object? subscribeSyncError,
  }) {
    backend = FakeQueueBackend(
      capabilities: eventCapabilities(profile: profile, delivery: delivery),
      onReadEvents: (_) async =>
          const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
    );
    signal = FakeQueueSignal(
      scope: scope,
      subscribeSyncError: subscribeSyncError,
    );
    delay = ManualDelay();
    queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
    );
    subs = queueEvents.events.listen((_) {});
  }

  tearDown(() async {
    try {
      await queueEvents.close();
    } catch (_) {}
    await subs.cancel();
  });

  // QueueEvents 不应依赖注入 Backend 的 open——组件不接管 Backend 生命周期。
  test('start 不会打开注入的 backend', () async {
    build();
    await queueEvents.start();
    expect(backend.openCalls, 0);
  });

  // distributed Backend + process Signal 属于能力不兼容，start() 必须在任何副作用之前
  // （read / subscribe / delay 请求）抛出 UnsupportedCapability。
  test(
    'start 在产生副作用前会拒绝搭配 process signal 的 distributed backend',
    () async {
      build(
        profile: BackendProfile.distributed,
        delivery: EventDelivery.replayable,
        scope: SignalScope.process,
      );
      await expectLater(
        queueEvents.start(),
        throwsA(
          isA<UnsupportedCapability>().having(
            (error) => error.capability,
            'capability',
            'distributedSignal',
          ),
        ),
      );
      expect(backend.readRequests, isEmpty);
      expect(signal.subscribeCalls, 0);
      expect(delay.requestedDurations, isEmpty);
    },
  );

  // distributed Backend + distributed Signal 属于兼容组合，start() 应正常完成。
  test('start 接受搭配 distributed signal 的 distributed backend', () async {
    build(
      profile: BackendProfile.distributed,
      delivery: EventDelivery.replayable,
      scope: SignalScope.distributed,
    );
    await queueEvents.start();
    expect(backend.openCalls, 0);
  });

  // local Backend + process Signal 属于默认兼容组合。
  test('start 接受搭配 process signal 的 local backend', () async {
    build(profile: BackendProfile.local, scope: SignalScope.process);
    await queueEvents.start();
    expect(backend.openCalls, 0);
  });

  // subscribe 同步抛错时，组件必须降级为只依赖轮询继续运行，不得让 start() 失败。
  test(
    'signal subscribe 抛错时，start 仍可继续轮询',
    () async {
      build(subscribeSyncError: StateError('signal down'));
      await queueEvents.start();
      expect(signal.subscribeCalls, greaterThanOrEqualTo(1));
      expect(delay.requestedDurations.length, greaterThanOrEqualTo(1));
      expect(backend.openCalls, 0);
    },
  );

  // Signal 流收到错误（onError + cancelOnError）后，轮询循环应继续请求新的 delay，
  // 证明组件不会因为 signal 断裂而停止 poll。
  test('signal stream 出错后，start 仍可继续轮询', () async {
    build();
    await queueEvents.start();
    final delayCountBefore = delay.requestedDurations.length;
    signal.emitError('orders', StateError('boom'));
    delay.completeNext();
    await Future<void>.delayed(Duration.zero);
    // 轮询循环即使在 signal 出错后也必须继续请求新的 delay。
    expect(delay.requestedDurations.length, greaterThan(delayCountBefore));
    expect(signal.subscribeCalls, 1);
  });

  // Signal 流出错后 subscription 已被取消，后续 signal.emit() 不应再触发新的 drain。
  test('signal stream 出错后会取消失败的订阅，并忽略后续提示', () async {
    build();
    await queueEvents.start();
    signal.emitError('orders', StateError('boom'));
    final readCountBefore = backend.readRequests.length;
    signal.emit('orders');
    signal.emit('orders');
    await Future<void>.delayed(Duration.zero);
    expect(backend.readRequests.length, equals(readCountBefore));
  });

  // 兼容性拒绝发生在副作用之前，拒绝后再次 start 也应同样抛出 UnsupportedCapability
  // 而不是抛 LifecycleError（因为状态仍留在 created）。
  test('兼容性检查拒绝后实例仍已创建，重复调用 start 会得到相同拒绝结果', () async {
    build(
      profile: BackendProfile.distributed,
      delivery: EventDelivery.replayable,
      scope: SignalScope.process,
    );
    await expectLater(
      queueEvents.start(),
      throwsA(isA<UnsupportedCapability>()),
    );
    await expectLater(
      queueEvents.start(),
      throwsA(
        isA<UnsupportedCapability>().having(
          (error) => error.capability,
          'capability',
          'distributedSignal',
        ),
      ),
    );
    expect(backend.readRequests, isEmpty);
    expect(signal.subscribeCalls, 0);
    expect(delay.requestedDurations, isEmpty);
  });

  // running 状态下再次 start() 必须抛 LifecycleError。
  test('第二次调用 start 会抛出 LifecycleError', () async {
    build();
    await queueEvents.start();
    await expectLater(queueEvents.start(), throwsA(isA<LifecycleError>()));
  });
}
