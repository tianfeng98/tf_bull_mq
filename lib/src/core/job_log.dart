import 'job_key.dart';
import 'json_like.dart';

/// Job 执行日志的草稿：写入 Backend 之前的输入形态（非泛型）。
///
/// 构造时校验 [data] 必须为 JSON-like。
class JobLogDraft {
  JobLogDraft({
    required this.level,
    required this.channel,
    required this.message,
    Object? data,
  }) : data = freezeJsonLike(data, argumentName: 'data');

  /// 日志级别。
  final JobLogLevel level;

  /// 日志通道：标识这条日志的产生来源（应用、stdout、stderr 或系统）。
  final JobLogChannel channel;

  /// 人类可读的日志消息。
  final String message;

  /// 结构化的已冻结 JSON-like 附加数据（可空）。
  final Object? data;
}

/// Job 日志的级别。
enum JobLogLevel { debug, info, warning, error }

/// Job 日志的通道。
///
/// - `application`：业务代码通过 `Job.log()` 显式写入
/// - `normalOutput`：处理器的 stdout
/// - `errorOutput`：处理器的 stderr
/// - `system`：Worker 在失败时自动写入的系统日志
enum JobLogChannel { application, normalOutput, errorOutput, system }

/// Backend 持久化的 Job 执行日志（原始 JSON-like 模型，非泛型）。
///
/// 构造时校验 [logId] 非空、[sequence] >= 1、[attempt] >= 1，
/// 以及 [data] 必须为 JSON-like。
class JobLog {
  JobLog({
    required this.logId,
    required this.jobKey,
    required this.sequence,
    required this.attempt,
    required this.executionId,
    required this.level,
    required this.channel,
    required this.message,
    required Object? data,
    required this.occurredAt,
  }) : data = freezeJsonLike(data, argumentName: 'data') {
    if (logId.isEmpty || sequence < 1 || attempt < 1) {
      throw ArgumentError('日志标识和计数器非法');
    }
  }

  /// Backend 分配的唯一日志标识。非空。
  final String logId;

  /// 日志关联的 Job 复合标识。
  final JobKey jobKey;

  /// Backend 为该 Job 分配的全局递增序号。从 1 开始。
  final int sequence;

  /// 产生这条日志的尝试序号。从 1 开始。
  final int attempt;

  /// 产生这条日志的执行标识。非空。
  final String executionId;

  /// 日志级别。
  final JobLogLevel level;

  /// 日志通道。
  final JobLogChannel channel;

  /// 人类可读的日志消息。
  final String message;

  /// 结构化的已冻结 JSON-like 附加数据。
  final Object? data;

  /// 日志发生的时间点。
  final DateTime occurredAt;
}
