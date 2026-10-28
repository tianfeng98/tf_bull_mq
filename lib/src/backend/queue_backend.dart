import '../core/backend_capabilities.dart';
import '../core/flow.dart';
import '../core/job_key.dart';
import '../core/job_log.dart';
import '../core/job_record.dart';
import '../core/queue_state.dart';
import 'backend_queries.dart';
import 'backend_requests.dart';
import 'backend_write_result.dart';

/// Queue Backend 的冻结抽象接口。
///
/// 所有持久化事实的读写都通过本接口完成。阶段 01 契约冻结：
/// - Queue/Worker/Flow 等高层组件不感知底层实现（内存/Redis/PostgreSQL）
/// - 所有写入方法返回 [BackendWriteResult] 携带 Backend 通知集合
/// - 所有查询方法只读，不产生通知
///
/// 实现类必须保证：
/// - 同一 Job 的 revision 严格单调递增
/// - 状态转换符合 Backend 冻结的状态机（违反时抛 [InvalidJobTransition]）
/// - 乐观并发冲突抛 [RevisionConflict] / [QueueRevisionConflict]
/// - distributed Backend 必须声明 `BackendProfile.distributed`
abstract interface class QueueBackend {
  /// Backend 能力声明。高层组件据此做 capability preflight。
  BackendCapabilities get capabilities;

  /// 打开 Backend 连接。Queue/Worker 构造前必须完成。
  Future<void> open();

  /// 关闭 Backend 连接。幂等。
  Future<void> close();

  /// 原子添加单个 Job。返回 Backed 持久化后的记录。
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request);

  /// 原子批量添加多个 Job。返回顺序与输入一致。
  Future<BackendWriteResult<List<JobRecord>>> addJobs(AddJobsRequest request);

  /// 原子添加一个 Flow（整棵依赖树一次性写入）。
  Future<BackendWriteResult<FlowSnapshot>> addFlow(AddFlowRequest request);

  /// 读取单个 Job 记录；不存在时返回 null（不是错误）。
  Future<JobRecord?> getJob(JobKey key);

  /// 分页查询队列中的 Job。[JobQuery] 携带过滤条件。
  Future<JobPage> queryJobs(JobQuery query);

  /// 读取队列级状态（paused、policy、revision）。
  Future<QueueState> getQueueState(String queueName);

  /// 设置队列暂停状态（乐观并发）。
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  );

  /// 设置队列级策略（乐观并发）。
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  );

  /// Worker claim 一个可领取的 Job。返回 claim 后的记录和执行凭证。
  Future<BackendWriteResult<ClaimedJob?>> claimJob(ClaimJobRequest request);

  /// distributed Backend 的租约续租（local Backend 抛 UnsupportedCapability）。
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  );

  /// 更新 active Job 的进度。
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  );

  /// 更新 active Job 的业务数据。
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  );

  /// 执行 Job 状态转换（完成/失败/重试/暂停/取消等）。
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  );

  /// 请求对 Job 执行控制操作（pause 或 cancel）。
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  );

  /// 恢复 stalled 的 active Job（Worker 心跳超时后调用）。
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  );

  /// 删除 Job（Flow 时 Backed 原子删除整棵子树）。
  Future<BackendWriteResult<List<JobKey>>> removeJob(RemoveJobRequest request);

  /// 追加一条 Job 执行日志。
  Future<BackendWriteResult<JobLog>> appendJobLog(AppendJobLogRequest request);

  /// 分页查询 Job 的执行日志。
  Future<JobLogPage> queryJobLogs(JobLogQuery query);

  /// 清理 Job 的执行日志（保留最近 N 条）。
  Future<BackendWriteResult<int>> clearJobLogs(ClearJobLogsRequest request);

  /// 分页读取队列事件流。
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request);

  /// 读取一个 Flow 的完整快照；无 Flow 时返回 null。
  Future<FlowSnapshot?> getFlow(JobKey root);
}
