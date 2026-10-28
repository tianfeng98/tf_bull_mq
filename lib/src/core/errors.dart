import '../core/job_event.dart';
import '../core/job_key.dart';
import '../core/job_record.dart';
import '../core/job_status.dart';

/// 所有框架异常的基类。
///
/// 具体子类都实现 [message] 来携带人类可读的错误描述。
sealed class TfBullMqException implements Exception {
  const TfBullMqException(this.message);

  /// 人类可读的错误描述。
  final String message;

  @override
  String toString() => '$runtimeType: $message';
}

/// Job 状态机转换不合法。
///
/// 由 Backend 在收到违反状态机冻结规则的转换请求时抛出，
/// 例如从 `completed` 转到 `failed`、从 `waiting` 转到 `active` 但没有 claim。
final class InvalidJobTransition extends TfBullMqException {
  const InvalidJobTransition(this.key, this.from, this.to, super.message);

  /// 目标 Job 的复合标识。
  final JobKey key;

  /// 转换前的状态。
  final JobStatus from;

  /// 请求的目标状态。
  final JobStatus to;
}

/// Backend 乐观并发冲突：调用方持有的 revision 已过期。
///
/// [expected] 是调用方传入的 revision，[actual] 是 Backend 中的最新 revision。
final class RevisionConflict extends TfBullMqException {
  const RevisionConflict(this.key, this.expected, this.actual, super.message);

  /// 目标 Job 的复合标识。
  final JobKey key;

  /// 调用方期望的 revision。
  final int expected;

  /// Backend 中的实际 revision。
  final int actual;
}

/// Queue 级乐观并发冲突。
final class QueueRevisionConflict extends TfBullMqException {
  const QueueRevisionConflict(
    this.queueName,
    this.expected,
    this.actual,
    super.message,
  );

  /// 目标队列名称。
  final String queueName;

  /// 调用方期望的 revision。
  final int expected;

  /// Backend 中的实际 revision。
  final int actual;
}

/// Job 执行的租约/所有权已丢失。
///
/// 可能由租约过期、Backend 抢占、或 Backend 拒绝 credential 导致。
final class LeaseLost extends TfBullMqException {
  const LeaseLost(this.key, this.executionId, super.message);

  /// 目标 Job 的复合标识。
  final JobKey key;

  /// 丢失的执行标识。
  final String executionId;
}

/// Backend 暂时不可用。
final class BackendUnavailable extends TfBullMqException {
  const BackendUnavailable(this.cause, super.message);

  /// Backend 抛出的原始错误对象（可空）。
  final Object? cause;
}

/// Backend 不支持请求的能力。
///
/// 例如向不支持 `delayedJobs` 的 Backend 提交带 delay 的 Job。
final class UnsupportedCapability extends TfBullMqException {
  const UnsupportedCapability(this.capability, super.message);

  /// 缺失的能力名（如 `delayedJobs`、`atomicFlows`）。
  final String capability;
}

/// Job 执行日志写入失败。
final class JobLogWriteFailure extends TfBullMqException {
  const JobLogWriteFailure(this.key, this.cause, super.message);

  /// 目标 Job 的复合标识。
  final JobKey key;

  /// 原始错误对象（可空）。
  final Object? cause;
}

/// Processor 业务失败。
///
/// Worker 在 Processor 抛异常或返回非 JSON-like 值时构造此异常，
/// 封装在 [JobFailure] 中提交给 Backend。
final class ProcessorFailure extends TfBullMqException {
  const ProcessorFailure(this.failure, super.message);

  /// Backend 要持久化的失败摘要。
  final JobFailure failure;
}

/// Flow 依赖失败：子 Job 失败/取消导致父 Job 无法继续。
///
/// [dependencies] 列出了直接失败的子 Job。
final class FlowDependencyFailure extends TfBullMqException {
  FlowDependencyFailure(this.key, List<JobKey> dependencies, super.message)
    : dependencies = List<JobKey>.unmodifiable(dependencies);

  /// 父 Job 的复合标识。
  final JobKey key;

  /// 直接失败的依赖子 Job 列表（不可变）。
  final List<JobKey> dependencies;
}

/// 组件生命周期错误：在不允许的状态下调用方法。
///
/// 例如对已关闭的 Worker 调用 `start()`。
final class LifecycleError extends TfBullMqException {
  const LifecycleError(this.component, this.operation, super.message);

  /// 发生错误的组件名（如 `Worker`、`Queue`）。
  final String component;

  /// 触发错误的操作名（如 `start`、`close`）。
  final String operation;
}

/// Job 在 Backend 中不存在。
final class JobNotFound extends TfBullMqException {
  const JobNotFound(this.key, super.message);

  /// 找不到的 Job 复合标识。
  final JobKey key;
}

/// 创建 Job 时发现 JobKey 已存在。
final class DuplicateJob extends TfBullMqException {
  const DuplicateJob(this.key, super.message);

  /// 重复的 Job 复合标识。
  final JobKey key;
}

/// 事件游标缺失：Backed 事件历史已被裁剪，请求的游标位置不可达。
final class EventCursorGap extends TfBullMqException {
  const EventCursorGap(this.gap, super.message);

  /// 描述缺失范围的 gap 信息。
  final EventGap gap;
}
