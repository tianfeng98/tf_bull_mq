import 'dart:async';

import '../core/errors.dart';
import 'queue_signal.dart';

/// 进程内 Signal 实现：使用 Broadcast Stream 在同一 Dart VM 内传播提示。
///
/// 特点：
/// - 单进程可见，不跨进程
/// - 提示丢失时消费者必须依赖独立的周期轮询
/// - 关闭后所有 [notify] 和 [subscribe] 抛 [LifecycleError]
final class InProcessQueueSignal implements QueueSignal {
  /// 每个队列名对应一个 Broadcast StreamController，用于广播提示。
  final Map<String, StreamController<void>> _controllers = {};

  /// 关闭标记；关闭后不再接受 notify/subscribe。
  bool _closed = false;

  @override
  SignalScope get scope => SignalScope.process;

  @override
  Stream<void> subscribe(String queueName) {
    if (_closed) {
      // 关闭后不允许再订阅
      throw LifecycleError('InProcessQueueSignal', 'subscribe', 'Signal 已关闭');
    }
    // getOrCreate：首次订阅时为该队列创建 broadcast controller
    final controller = _controllers.putIfAbsent(
      queueName,
      () => StreamController<void>.broadcast(sync: true),
    );
    return controller.stream;
  }

  @override
  Future<void> notify(String queueName) {
    if (_closed) {
      // 关闭后 notify 立即返回 error Future
      return Future<void>.error(
        LifecycleError('InProcessQueueSignal', 'notify', 'Signal 已关闭'),
      );
    }
    // 立即发送通知；无监听器时 add 是 no-op
    _controllers[queueName]?.add(null);
    return Future<void>.value();
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    // 关闭所有已创建的 controller
    final controllers = _controllers.values.toList(growable: false);
    _controllers.clear();
    for (final controller in controllers) {
      await controller.close();
    }
  }
}
