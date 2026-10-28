import '../core/index.dart';

/// 单次 Signal.notify 失败记录。
///
/// [queueName] 是触发失败的队列名；[error] 与 [stackTrace] 直接来自
/// Signal 抛出的原始异常，Producer 不做转换或包装，供调用方完整
/// 观测失败上下文。
final class FlowNotificationError {
  const FlowNotificationError({
    required this.queueName,
    required this.error,
    required this.stackTrace,
  });

  final String queueName;
  final Object error;
  final StackTrace stackTrace;
}

/// Backend 已提交但一个或多个 Signal.notify 失败的公开异常。
///
/// Flow 的写入在 Backend 的 FlowSnapshot 中原子完成且不可回滚；[failures]
/// 收集了每个 notify 失败的原始 error/stack trace。[backendCommitted]
/// 始终为 `true`，用于与"写入前预检失败"或"Backend 本身失败"区分。
///
/// 异常本身不携带业务泛型：[committedFlow] 是 Backend 原始 JSON-like 快照。
/// 调用方若需要类型化视图，可用自己的 Codec 重新解码。
final class FlowNotificationFailure implements Exception {
  FlowNotificationFailure({
    required FlowSnapshot committedFlow,
    Iterable<FlowNotificationError> failures = const <FlowNotificationError>[],
  }) : _committedFlow = committedFlow,
       failures = List<FlowNotificationError>.unmodifiable(failures) {
    if (this.failures.isEmpty) {
      throw ArgumentError(
        'FlowNotificationFailure requires at least one failure.',
      );
    }
  }

  final FlowSnapshot _committedFlow;

  /// Backend 已提交的原始 JSON-like Flow 快照。
  FlowSnapshot get committedFlow => _committedFlow;

  final List<FlowNotificationError> failures;

  bool get backendCommitted => true;

  @override
  String toString() {
    final names = failures.map((item) => item.queueName).join(', ');
    return 'FlowNotificationFailure: Backend commit succeeded but Signal notification failed '
        'for queues [$names]. The Flow is persisted in committedFlow.';
  }
}
