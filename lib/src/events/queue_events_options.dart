import '../core/index.dart';

/// [QueueEvents] 实例的配置项。
///
/// 本类承载调用方拥有的调优参数和资源所有权标记。队列名称本身不在这里设置，
/// 它属于 [QueueEvents]，因为组件需要校验自身 [QueueEvents.queueName] 与
/// 调用方提供的 [initialCursor] 中的 queueName 是否一致。
final class QueueEventsOptions {
  /// 创建新的配置项。
  ///
  /// 当 [pollInterval] 非正值、或 [pageSize] 超出契约范围 `[1, 1000]` 时抛 [ArgumentError]。
  QueueEventsOptions({
    this.initialCursor,
    this.pollInterval = const Duration(seconds: 5),
    this.pageSize = 100,
    this.ownsBackend = false,
    this.ownsSignal = false,
  }) {
    if (pollInterval <= Duration.zero) {
      throw ArgumentError.value(pollInterval, 'pollInterval', '必须严格为正值');
    }
    if (pageSize < 1 || pageSize > 1000) {
      throw ArgumentError.value(pageSize, 'pageSize', '必须在 1..1000 范围内');
    }
  }

  /// 可选的调用方已消费到的事件游标。
  ///
  /// 非空时 [QueueEvents] 会从此游标的 position 之后开始读取，且 queueName
  /// 必须匹配本组件的 queueName。为空时组件从 Backend 认为最早可读的位置开始。
  final EventCursor? initialCursor;

  /// 周期性轮询唤醒之间的间隔。必须严格为正值。
  final Duration pollInterval;

  /// 每次 [QueueBackend.readJobEvents] 请求的最大事件数量，契约范围 `[1, 1000]`。
  final int pageSize;

  /// 为 `true` 时，[QueueEvents.close()] 会同时关闭注入的 Backend。
  ///
  /// 组件从不主动 `open()` Backend——调用方有责任在构造 [QueueEvents] 之前
  /// 确保 Backend 已经打开。
  final bool ownsBackend;

  /// 为 `true` 时，[QueueEvents.close()] 会同时关闭注入的 Signal。
  final bool ownsSignal;
}
