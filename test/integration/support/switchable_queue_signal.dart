import 'package:tf_bull_mq/tf_bull_mq.dart';

/// 可切换是否丢弃通知的 [QueueSignal] 装饰器。
///
/// 默认正常透传所有 notify 到 delegate；当 [dropNotifications] 置为 true 时，
/// notify 直接以已完成 Future 返回，让调用方以为通知已送达但实际被吞掉。
/// 用于验证 Worker 的独立周期 poll 能够在 Signal 完全不可用的情况下仍然 claim Job。
final class SwitchableQueueSignal implements QueueSignal {
  SwitchableQueueSignal(this._delegate);

  final QueueSignal _delegate;

  /// 为 true 时，[notify] 被静默丢弃，不转发给 delegate。
  bool dropNotifications = false;

  @override
  SignalScope get scope => _delegate.scope;

  @override
  Future<void> notify(String queueName) =>
      dropNotifications ? Future<void>.value() : _delegate.notify(queueName);

  @override
  Stream<void> subscribe(String queueName) => _delegate.subscribe(queueName);

  @override
  Future<void> close() => _delegate.close();
}
