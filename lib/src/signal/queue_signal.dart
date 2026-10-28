/// QueueSignal 的作用域。
///
/// - `process`：单进程内 Signal，通知只能在同一 Dart VM 内传播
/// - `distributed`：跨进程 Signal（如 Redis Pub/Sub），多实例可互相感知
enum SignalScope { process, distributed }

/// Queue 提示通道的冻结抽象接口。
///
/// 当 Backed 写入产生新事实（Job 添加、状态转换、事件产出）时，高层组件
/// 调用 [notify] 唤醒同一队列上的消费者；消费者通过 [subscribe] 监听提示，
/// 再去 Backed 读取最新事实。Signal 本身不承载事实，只承载唤醒提示。
///
/// 接口契约：
/// - 提示可能丢失（消费者必须有独立的周期轮询机制）
/// - 提示也可能重复（消费者必须能容忍重复处理）
/// - [close] 幂等
abstract interface class QueueSignal {
  /// Signal 作用域（process / distributed）。
  SignalScope get scope;

  /// 发送队列提示。
  ///
  /// [queueName]：被唤醒的队列名称。实现类应尽量快速返回，
  /// 避免阻塞 Backed 写入的主路径。
  Future<void> notify(String queueName);

  /// 订阅队列提示流。
  ///
  /// 返回 broadcast Stream——调用方可多次监听，每次监听都会收到之后的提示。
  /// 实现类在 Stream 关闭或 [close] 调用后应让已订阅的监听器停止推送。
  Stream<void> subscribe(String queueName);

  /// 关闭 Signal 通道。幂等。
  ///
  /// 关闭后所有 [notify] 和 [subscribe] 调用的行为由实现类定义
  /// （通常抛 [LifecycleError] 或返回空 Future/Stream）。
  Future<void> close();
}
