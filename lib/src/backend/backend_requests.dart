import '../core/flow.dart';
import '../core/job_key.dart';
import '../core/job_log.dart';
import '../core/job_record.dart';
import '../core/job_status.dart';
import '../core/json_like.dart';
import '../core/queue_state.dart';

/// Backend 写入请求 DTO 集合。
///
/// 所有请求类型都是不可变的参数载体，构造时做输入校验（如非空检查、
/// 范围检查、JSON-like 检查），确保传给 Backed 的输入都是合法的。

/// 添加单个 Job 的请求（非泛型原始模型）。
final class AddJobRequest {
  AddJobRequest({
    required this.job,
    required this.createdAt,
    required this.availableAt,
  });

  /// Job 输入规格。
  final JobSpec job;

  /// Job 创建时间（由调用方时钟决定）。
  final DateTime createdAt;

  /// Job 可领取时间（createdAt + JobOptions.delay）。
  final DateTime availableAt;
}

/// 批量添加 Job 的请求。构造时校验 JobKey 不重复。
final class AddJobsRequest {
  AddJobsRequest({required List<JobSpec> jobs, required this.createdAt})
    : jobs = List<JobSpec>.unmodifiable(jobs) {
    final keys = <JobKey>{};
    for (final job in this.jobs) {
      if (!keys.add(job.key)) throw ArgumentError('重复的 JobKey');
    }
  }

  /// Job 输入规格列表（不可变）。
  final List<JobSpec> jobs;

  /// 所有 Job 共享的创建时间。
  final DateTime createdAt;
}

/// 原子添加 Flow 的请求（非泛型原始模型）。
final class AddFlowRequest {
  AddFlowRequest({required this.flow, required this.createdAt});

  /// Flow 输入规格。
  final FlowJobSpec flow;

  /// 创建时间。
  final DateTime createdAt;
}

/// 设置队列暂停状态的请求（乐观并发）。
final class SetQueuePausedRequest {
  const SetQueuePausedRequest({
    required this.queueName,
    required this.paused,
    required this.expectedRevision,
    required this.occurredAt,
  });

  /// 目标队列名称。
  final String queueName;

  /// 目标暂停状态（true = 暂停，false = 恢复）。
  final bool paused;

  /// 调用方期望的 QueueState revision（乐观并发）。
  final int expectedRevision;

  /// 请求到达时间。
  final DateTime occurredAt;
}

/// 设置队列策略的请求（乐观并发）。
final class SetQueuePolicyRequest {
  const SetQueuePolicyRequest({
    required this.queueName,
    required this.policy,
    required this.expectedRevision,
    required this.occurredAt,
  });

  /// 目标队列名称。
  final String queueName;

  /// 新的队列策略。
  final QueuePolicy policy;

  /// 调用方期望的 QueueState revision（乐观并发）。
  final int expectedRevision;

  /// 请求到达时间。
  final DateTime occurredAt;
}

/// Worker claim Job 的请求。
final class ClaimJobRequest {
  const ClaimJobRequest({
    required this.queueName,
    required this.workerId,
    required this.claimedAt,
    this.leaseDuration,
  });

  /// 目标队列名称。
  final String queueName;

  /// 发起 claim 的 Worker 标识。
  final String workerId;

  /// claim 发生时间。
  final DateTime claimedAt;

  /// distributed Backend 的租约时长（local Backend 不传）。
  final Duration? leaseDuration;
}

/// distributed Backend 租约续租请求。
final class RenewLeaseRequest {
  const RenewLeaseRequest({
    required this.key,
    required this.credential,
    required this.renewedAt,
    required this.leaseDuration,
  });

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 当前执行凭证。
  final ExecutionCredential credential;

  /// 续租发生时间。
  final DateTime renewedAt;

  /// 新租约时长。
  final Duration leaseDuration;
}

/// 更新 active Job 进度的请求（乐观并发 + 凭证，非泛型原始模型）。
final class UpdateJobProgressRequest {
  UpdateJobProgressRequest({
    required this.key,
    required Object? progress,
    required this.expectedRevision,
    required this.credential,
    required this.occurredAt,
  }) : progress = freezeJsonLike(progress, argumentName: 'progress');

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 新的进度值，已冻结 JSON-like，数值需在 [0, 1]。
  final Object? progress;

  /// 调用方期望的 JobRecord revision。
  final int expectedRevision;

  /// 当前执行凭证。
  final ExecutionCredential credential;

  /// 更新发生时间。
  final DateTime occurredAt;
}

/// 更新 active Job 业务数据的请求（乐观并发 + 凭证，非泛型原始模型）。
final class UpdateJobDataRequest {
  UpdateJobDataRequest({
    required this.key,
    required Object? data,
    required this.expectedRevision,
    required this.credential,
    required this.occurredAt,
  }) : data = freezeJsonLike(data, argumentName: 'data');

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 新的业务数据，已冻结 JSON-like。
  final Object? data;

  /// 调用方期望的 JobRecord revision。
  final int expectedRevision;

  /// 当前执行凭证。
  final ExecutionCredential credential;

  /// 更新发生时间。
  final DateTime occurredAt;
}

/// Job 状态转换请求（乐观并发 + 可选凭证，非泛型原始模型）。
///
/// 构造时校验：如果指定了 [automaticErrorLog] 则必须同时指定 [failure]。
final class TransitionJobRequest {
  TransitionJobRequest({
    required this.key,
    required this.targetStatus,
    required this.expectedRevision,
    required this.occurredAt,
    this.credential,
    Object? result,
    this.failure,
    this.availableAt,
    this.automaticErrorLog,
  }) : result = freezeJsonLike(result, argumentName: 'result') {
    if (automaticErrorLog != null && failure == null) {
      throw ArgumentError('自动错误日志要求同时提供 failure');
    }
  }

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 目标状态（completed / failed / waiting / delayed / paused / canceled）。
  final JobStatus targetStatus;

  /// 调用方期望的 JobRecord revision。
  final int expectedRevision;

  /// 转换发生时间。
  final DateTime occurredAt;

  /// 执行凭证（active Job 转换时必须提供）。
  final ExecutionCredential? credential;

  /// 成功结果（仅 completed 时使用）。已冻结 JSON-like。
  final Object? result;

  /// 失败摘要（仅 failed 时使用）。
  final JobFailure? failure;

  /// 下次可领取时间（仅 delayed 时使用）。
  final DateTime? availableAt;

  /// 与 failure 同一次提交的自动错误日志（error/system 通道）。
  final JobLogDraft? automaticErrorLog;
}

/// 请求对 Job 执行控制操作的请求（乐观并发）。
final class RequestJobControlRequest {
  const RequestJobControlRequest({
    required this.key,
    required this.action,
    required this.expectedRevision,
    required this.requestedAt,
  });

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 控制操作类型（pause 或 cancel）。
  final JobControlAction action;

  /// 调用方期望的 JobRecord revision。
  final int expectedRevision;

  /// 请求发生时间。
  final DateTime requestedAt;
}

/// 恢复 stalled Job 的请求（local Backend 使用）。
final class RecoverStalledJobsRequest {
  const RecoverStalledJobsRequest({
    required this.queueName,
    required this.now,
    this.workerId,
    this.executionIds,
  });

  /// 目标队列名称。
  final String queueName;

  /// Backend 判断 stalled 的当前时间。
  final DateTime now;

  /// 可选的 Worker 过滤条件（只恢复指定 Worker 持有的 execution）。
  final String? workerId;

  /// 可选的精确 execution 过滤集合（只恢复指定的 execution）。
  final Set<String>? executionIds;
}

/// 删除 Job（及 Backend 判定的 Flow 子树）的请求。
final class RemoveJobRequest {
  const RemoveJobRequest({
    required this.key,
    required this.expectedRevision,
    required this.removedAt,
  });

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 调用方期望的 JobRecord revision。
  final int expectedRevision;

  /// 删除发生时间。
  final DateTime removedAt;
}

/// 追加 Job 执行日志的请求（非泛型原始模型）。
final class AppendJobLogRequest {
  const AppendJobLogRequest({
    required this.key,
    required this.credential,
    required this.attempt,
    required this.log,
    required this.occurredAt,
  });

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 当前执行凭证。
  final ExecutionCredential credential;

  /// 日志对应的尝试序号（从 1 开始）。
  final int attempt;

  /// 日志草稿。
  final JobLogDraft log;

  /// 日志发生时间。
  final DateTime occurredAt;
}

/// 清理 Job 执行日志的请求。
///
/// 构造要求 [keepLatest] >= 0。
final class ClearJobLogsRequest {
  const ClearJobLogsRequest({required this.key, this.keepLatest = 0})
    : assert(keepLatest >= 0);

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 保留最近 N 条日志。`0` 表示全部清空。
  final int keepLatest;
}
