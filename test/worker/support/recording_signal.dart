import 'dart:async';

import 'package:tf_bull_mq/src/signal/index.dart';

/// 可发提示、可失败、可关闭的 `QueueSignal` 测试替身。
///
/// `notify` 只记录调用，不会自动唤醒订阅者：测试必须显式调用 [emit]，才能精确
/// 区分「Signal 唤醒」与「周期轮询」两条发现路径。
final class RecordingSignal implements QueueSignal {
  RecordingSignal({this.scope = SignalScope.process});

  @override
  final SignalScope scope;

  /// `true` 时 `notify` 返回失败的 Future。
  bool notifyFails = false;

  /// 非空时 `subscribe` 同步抛出该错误。
  Object? subscribeError;

  /// 非空时 `close` 在该次调用中抛出该错误。
  ///
  /// 用于验证「Signal 关闭失败不会阻断 Backend 关闭」。
  Object? closeError;

  /// `close` 被调用时的回调，用于记录跨资源的关闭顺序（例如 Signal 先于 Backend）。
  Future<void> Function()? onClose;

  final List<String> notifyCalls = <String>[];
  final List<String> subscribedQueues = <String>[];
  int closeCount = 0;
  int cancelCount = 0;
  bool closed = false;

  final Map<String, StreamController<void>> _controllers =
      <String, StreamController<void>>{};

  /// 当前处于订阅状态的队列数量。
  int get activeSubscriptionCount => _controllers.values
      .where((StreamController<void> controller) => controller.hasListener)
      .length;

  @override
  Stream<void> subscribe(String queueName) {
    subscribedQueues.add(queueName);
    final error = subscribeError;
    if (error != null) throw error;
    final existing = _controllers[queueName];
    if (existing != null && !existing.isClosed) return existing.stream;
    final controller = StreamController<void>.broadcast(
      sync: true,
      onCancel: () => cancelCount++,
    );
    _controllers[queueName] = controller;
    return controller.stream;
  }

  /// 手工发出一次唤醒提示。没有订阅者时静默忽略。
  void emit(String queueName) {
    final controller = _controllers[queueName];
    if (controller != null && !controller.isClosed && controller.hasListener) {
      controller.add(null);
    }
  }

  /// 手工向订阅者推送一个 stream error。
  void emitError(String queueName, Object error) {
    final controller = _controllers[queueName];
    if (controller != null && !controller.isClosed) {
      controller.addError(error, StackTrace.current);
    }
  }

  /// 手工结束订阅流，模拟 Signal 连接断开。
  void emitDone(String queueName) {
    final controller = _controllers[queueName];
    if (controller != null && !controller.isClosed) {
      unawaited(controller.close());
    }
  }

  @override
  Future<void> notify(String queueName) {
    notifyCalls.add(queueName);
    if (notifyFails) {
      return Future<void>.error(StateError('signal notify failed'));
    }
    return Future<void>.value();
  }

  @override
  Future<void> close() async {
    closeCount++;
    final order = onClose;
    if (order != null) await order();
    final error = closeError;
    if (error != null) throw error;
    closed = true;
    for (final controller in _controllers.values) {
      if (!controller.isClosed) await controller.close();
    }
  }
}
