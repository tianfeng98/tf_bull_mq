import 'job_key.dart';
import 'job_options.dart';
import 'job_status.dart';
import 'json_like.dart';
import 'queue_state.dart';

/// Job 失败摘要：Backend 持久化的失败原因。
///
/// 当 Processor 抛异常或 Backed 因依赖失败/stalled 等原因标记 Job 失败时，
/// 本类型携带失败类型、消息、堆栈、发生时间以及导致失败的依赖列表。
final class JobFailure {
  JobFailure({
    required this.type,
    required this.message,
    required this.failedAt,
    this.stackTrace,
    List<JobKey> dependencyKeys = const <JobKey>[],
  }) : dependencyKeys = List<JobKey>.unmodifiable(dependencyKeys);

  /// 失败类型标识（如 `ArgumentError`、`FlowDependencyFailure`）。非空字符串。
  final String type;

  /// 人类可读的失败描述。
  final String message;

  /// 失败时的原始堆栈字符串（可空）。由 Worker 按 `stackTraceLimit` 裁剪后持久化。
  final String? stackTrace;

  /// 失败发生的时间点。
  final DateTime failedAt;

  /// 导致本次失败的 Flow 依赖子 Job 列表（不可变）。非 Flow 失败时为空列表。
  final List<JobKey> dependencyKeys;
}

/// Job 执行凭证：Worker claim Job 或续租 Backed 返回的所有权证明。
///
/// [executionId] 在 claim 时确定且不会因续租改变；[fencingToken] 用于
/// distributed Backed 的租约 fencing 机制，防止过期凭证提交写入。
final class ExecutionCredential {
  const ExecutionCredential({
    required this.executionId,
    this.leaseId,
    this.fencingToken,
    this.leaseExpiresAt,
  }) : assert(executionId != '');

  /// 本次执行的唯一标识。非空。
  final String executionId;

  /// distributed Backend 的租约 ID（可空，local Backend 不使用）。
  final String? leaseId;

  /// fencing token（可空，local Backend 不使用）。每次续租递增。
  final int? fencingToken;

  /// 租约到期时间（可空，local Backend 不使用）。租约到期后 Worker 必须停止续租。
  final DateTime? leaseExpiresAt;
}

/// Job 执行所有者信息。
///
/// 记录 claim 时 Worker 标识、凭证以及执行开始时间。
final class ExecutionOwner {
  const ExecutionOwner({
    required this.workerId,
    required this.credential,
    required this.startedAt,
  });

  /// claim 该 Job 的 Worker 标识。
  final String workerId;

  /// claim 时 Backend 返回的执行凭证。
  final ExecutionCredential credential;

  /// 执行开始时间（claim 发生时间）。
  final DateTime startedAt;
}

/// Backend 持久化的 Job 完整事实（非泛型原始 JSON-like 模型）。
///
/// 该类型是 Backend 对外暴露的核心数据模型：所有高层组件（Queue、Worker）
/// 都通过 Backend API 读写本类型来获取或修改 Job 事实。构造时做范围和 JSON-like 校验。
/// data / progress / result 字段始终是已冻结的 JSON-like Object?。
/// 业务层通过 PayloadCodec 把这些原始值解码为业务模型。
final class JobRecord {
  JobRecord({
    required this.schemaVersion,
    required this.key,
    required this.name,
    required Object? data,
    required this.status,
    required Object? progress,
    required Object? result,
    required this.failure,
    required this.revision,
    required this.attemptsMade,
    required this.stalledCount,
    required this.priority,
    required this.availableAt,
    required this.options,
    required this.parentKey,
    required this.createdAt,
    required this.updatedAt,
    required this.startedAt,
    required this.finishedAt,
    required this.execution,
    required this.controlRequest,
  }) : data = freezeJsonLike(data, argumentName: 'data'),
       progress = freezeJsonLike(progress, argumentName: 'progress'),
       result = freezeJsonLike(result, argumentName: 'result') {
    // 校验计数器非负
    if (schemaVersion < 0 ||
        revision < 0 ||
        attemptsMade < 0 ||
        stalledCount < 0) {
      throw ArgumentError('record counters must be non-negative');
    }
    // 进度值必须在 [0, 1] 范围内
    if (progress is num && progress < 0 || progress is num && progress > 1) {
      throw ArgumentError.value(progress, 'progress');
    }
  }

  /// 数据模型的 schema 版本，用于 Backed 升级。当前为 1。
  final int schemaVersion;

  /// 复合标识（queueName + jobId）。
  final JobKey key;

  /// Job 名称。由调用方创建时指定。
  final String name;

  /// Job 业务输入数据，已冻结的 JSON-like，可空。
  final Object? data;

  /// 当前生命周期状态。
  final JobStatus status;

  /// 业务进度，已冻结的 JSON-like。数值需在 [0, 1]，非数值可作为任意业务数据。
  final Object? progress;

  /// 成功结果，已冻结的 JSON-like，可空。仅 Job 成功完成后有值。
  final Object? result;

  /// 失败摘要，可空。仅 Job 处于 failed 状态时有值。
  final JobFailure? failure;

  /// Backend 内部的乐观并发版本号。每次写入递增。
  final int revision;

  /// 已完成的尝试次数（从 0 开始；首次 claim 后为 1）。
  final int attemptsMade;

  /// Stall 恢复计数。超过 [JobOptions.maxStalledCount] 后 Backend 将 Job 标记为 failed。
  final int stalledCount;

  /// 优先级，数值越大越优先领取。
  final int priority;

  /// 下次可领取的时间点。delay Job 为创建时间 + delay；stalled 恢复或 retry 时更新。
  final DateTime availableAt;

  /// Job 级运行选项（重试策略、delay、stackTraceLimit 等）。
  final JobOptions options;

  /// Flow 父 Job 的复合标识；无 Flow 时为 null。
  final JobKey? parentKey;

  /// Job 创建时间。
  final DateTime createdAt;

  /// 最后一次 Backend 写入该 Job 的时间。
  final DateTime updatedAt;

  /// 首次 claim 的时间；Job 从未 claim 时为 null。
  final DateTime? startedAt;

  /// 到达终态（completed/failed/canceled）的时间；非终态时为 null。
  final DateTime? finishedAt;

  /// 当前执行所有者信息；Job 未 claim 时为 null。
  final ExecutionOwner? execution;

  /// 未处理的暂停/取消控制请求；无请求时为 null。
  final JobControlRequest? controlRequest;
}

/// Worker claim Backed 返回的结果（非泛型原始模型）。
///
/// 同时携带持久化的 JobRecord 和执行凭证；Worker 用凭证进行后续的续租和写入操作。
final class ClaimedJob {
  const ClaimedJob({required this.record, required this.credential});

  /// claim 后的 Job 完整记录（原始 JSON-like 模型）。
  final JobRecord record;

  /// Backend 分配的执行凭证。Worker 需妥善保存并用于后续写入。
  final ExecutionCredential credential;
}
