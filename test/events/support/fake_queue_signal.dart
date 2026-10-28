import 'dart:async';

import 'package:tf_bull_mq/src/signal/index.dart';

/// 基于 per-queue broadcast StreamController 的 [QueueSignal] 测试替身。
///
/// 每个队列名对应一个独立的 StreamController，[emit] / [emitError] 同步触发通知。
/// 支持模拟 subscribe 同步失败（如握手前网络断开），以及 close 抛错，用于覆盖
/// QueueEvents 在 signal 异常时的降级和恢复路径。
final class FakeQueueSignal implements QueueSignal {
  FakeQueueSignal({this.scope = SignalScope.process, this.subscribeSyncError});

  @override
  final SignalScope scope;

  /// 非 null 时 [subscribe] 在返回 Stream 之前同步抛出此错误。
  /// 用于模拟 subscribe 调用本身都无法产生 Stream 的场景（如握手前连接断开）。
  final Object? subscribeSyncError;

  /// [subscribe] 被调用的次数。
  int subscribeCalls = 0;

  /// QueueEvents 主动取消 listener 订阅的次数——fake 自己不会取消，仅作计数。
  int canceledSubscriptions = 0;

  /// [close] 被调用的次数，验证 ownsSignal 语义。
  int closeCalls = 0;

  /// 当非 null 时，下一次 [close] 调用会抛出此错误。
  Object? closeError;

  /// 每个队列名对应一个 broadcast StreamController，sync: true 保证通知同步传递。
  final Map<String, StreamController<void>> _controllers =
      <String, StreamController<void>>{};

  StreamController<void> _controllerFor(String queueName) {
    return _controllers.putIfAbsent(
      queueName,
      () => StreamController<void>.broadcast(sync: true),
    );
  }

  /// 同步通知订阅了 [queueName] 的所有当前 listener。
  void emit(String queueName) {
    final controller = _controllers[queueName];
    if (controller != null && !controller.isClosed) {
      controller.add(null);
    }
  }

  /// 同步向 [queueName] 的所有当前 listener 推送错误。
  void emitError(String queueName, Object error, [StackTrace? stackTrace]) {
    final controller = _controllers[queueName];
    if (controller != null && !controller.isClosed) {
      controller.addError(error, stackTrace);
    }
  }

  @override
  Future<void> notify(String queueName) async {
    emit(queueName);
  }

  @override
  Stream<void> subscribe(String queueName) {
    subscribeCalls++;
    final syncError = subscribeSyncError;
    if (syncError != null) {
      throw syncError;
    }
    // 直接返回 broadcast stream，QueueEvents 可正常 listen。
    return _controllerFor(queueName).stream;
  }

  @override
  Future<void> close() async {
    closeCalls++;
    for (final controller in _controllers.values) {
      await controller.close();
    }
    _controllers.clear();
    final error = closeError;
    if (error != null) {
      closeError = null;
      throw error;
    }
  }
}
