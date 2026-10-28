import 'dart:async';

import '../core/job_key.dart';
import '../core/json_like.dart';

/// Worker 运行日志的上下文信息。
///
/// 所有字段在构造时冻结，用于标识日志产生的 Worker、队列、操作，
/// 以及可选的 Job 执行标识。
final class WorkerLogContext {
  const WorkerLogContext({
    required this.workerId,
    required this.queueName,
    required this.operation,
    this.jobKey,
    this.attempt,
    this.executionId,
  });

  /// 产生日志的 Worker 标识。非空。
  final String workerId;

  /// 产生日志的队列名称。非空。
  final String queueName;

  /// 触发日志的高层操作名（如 `addJob`、`claimJob`、`transitionJob`）。
  final String operation;

  /// 关联的 Job 复合标识。非 Job 级别日志时为 `null`。
  final JobKey? jobKey;

  /// 当前尝试序号。非 Job 级别日志时为 `null`。
  final int? attempt;

  /// 当前执行标识。非 Job 级别或尚未 claim 时为 `null`。
  final String? executionId;
}

/// Worker 运行日志的级别。
enum WorkerLogLevel { debug, info, warning, error }

/// 一条 Worker 运行日志条目。
///
/// 由 Worker 在处理各种基础设施和业务事件时创建，通过 [WorkerLogSink.write]
/// 推送。构造时校验 [data] 必须为 JSON-like。
final class WorkerLogEntry {
  WorkerLogEntry({
    required this.level,
    required this.message,
    required this.occurredAt,
    required this.context,
    this.error,
    this.stackTrace,
    Object? data,
  }) : data = freezeJsonLike(data, argumentName: 'data');

  /// 日志级别。
  final WorkerLogLevel level;

  /// 人类可读的日志消息。
  final String message;

  /// 日志发生的时间点。
  final DateTime occurredAt;

  /// 日志产生时的上下文信息。
  final WorkerLogContext context;

  /// 关联的异常对象（如有）。
  final Object? error;

  /// 关联的异常堆栈（如有）。
  final StackTrace? stackTrace;

  /// 结构化的 JSON-like 附加数据（如有）。
  final Object? data;
}

/// Worker 运行日志的抽象接口。
///
/// Worker 通过本接口推送运行日志；实现类可以选择同步或异步写入，
/// 也可以选择丢弃或持久化。sink 的失败不能影响 Job 状态、重试目标或
/// Worker 生命周期。
abstract interface class WorkerLogSink {
  /// 写入一条运行日志。
  ///
  /// 可以返回 `void` 或 `Future<void>`。同步抛错或异步失败都由 Worker 内部
  /// 捕获并忽略，不会向上传播。
  FutureOr<void> write(WorkerLogEntry entry);
}
