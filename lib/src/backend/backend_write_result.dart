/// Backend 写入操作的统一返回结果。
///
/// 每次 Backend 写入（添加、转换、删除等）返回本类型，携带：
/// - 实际写入的值 `value`（泛型，随操作类型变化）
/// - Backend 是否产生了持久化变更 `changed`
/// - 变更影响的队列集合 `queuesToNotify`（高层组件据此发送 Signal 提示）
///
/// 构造约束：如果 `changed == false`，则 `queuesToNotify` 必须为空——
/// 没有实际变更就没有提示的必要。
final class BackendWriteResult<T> {
  BackendWriteResult({
    required this.value,
    required this.changed,
    required Set<String> queuesToNotify,
  }) : queuesToNotify = Set<String>.unmodifiable(queuesToNotify) {
    if (!changed && this.queuesToNotify.isNotEmpty) {
      throw ArgumentError('未变更的写入不能产生通知');
    }
  }

  /// Backed 返回的实际值（JobRecord、List、QueueState 等）。
  final T value;

  /// Backed 是否产生了持久化变更。
  ///
  /// 用于高层组件判断是否需要发送 Signal 提示；幂等操作（如 canceled 的 Job
  /// 再 cancel）会返回 `changed == false` 和空通知集合。
  final bool changed;

  /// 本次变更需要通知的队列集合（不可变）。
  ///
  /// 高层组件（Queue、Worker）在 Backend 写入成功后，对这些队列名逐个调用
  /// `QueueSignal.notify()`，让消费者感知新事件。
  final Set<String> queuesToNotify;
}
