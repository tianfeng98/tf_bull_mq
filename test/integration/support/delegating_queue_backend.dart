import 'package:tf_bull_mq/tf_bull_mq.dart';

/// 完整转发 [QueueBackend] 接口到内部 backend 的测试夹具基类。
///
/// 与 `FakeQueueBackend` 不同，`DelegatingQueueBackend` **不复制事实**——
/// 所有读取和写入都直接交给 [inner] 完成，子类只通过覆盖特定方法插入
/// Completer barrier 或单次故障（例如让 `recoverStalledJobs` 恰好一次
/// 抛错，或让 `appendJobLog` 在某个 Completer 上等待）。
///
/// 这种“代理 + 选择性拦截”的模式避免了在测试代码中手动复制 Backend 的
/// 冻结行为，使我们可以只关注待验证的时序边界（例如 recovery 中途失败
/// 后 Worker 保持 closing 状态）。
///
/// 典型用法：
/// ```dart
/// final class FailingRecovery extends DelegatingQueueBackend {
///   FailingRecovery(super.inner);
///   @override
///   Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
///     RecoverStalledJobsRequest request,
///   ) async {
///     throw StateError('boom');
///   }
/// }
/// ```
abstract class DelegatingQueueBackend implements QueueBackend {
  /// 内部持有并被所有方法转发到的真实 Backend。
  ///
  /// 测试通常传入一个已经 `open()` 的 [MemoryQueueBackend]；子类可按需
  /// 覆盖构造函数接受额外依赖（例如 Completer barrier）。
  DelegatingQueueBackend(this.inner);

  final QueueBackend inner;

  @override
  BackendCapabilities get capabilities => inner.capabilities;

  @override
  Future<void> open() => inner.open();

  @override
  Future<void> close() => inner.close();

  @override
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request) =>
      inner.addJob(request);

  @override
  Future<BackendWriteResult<List<JobRecord>>> addJobs(AddJobsRequest request) =>
      inner.addJobs(request);

  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(AddFlowRequest request) =>
      inner.addFlow(request);

  @override
  Future<JobRecord?> getJob(JobKey key) => inner.getJob(key);

  @override
  Future<JobPage> queryJobs(JobQuery query) => inner.queryJobs(query);

  @override
  Future<QueueState> getQueueState(String queueName) =>
      inner.getQueueState(queueName);

  @override
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  ) => inner.setQueuePaused(request);

  @override
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  ) => inner.setQueuePolicy(request);

  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(ClaimJobRequest request) =>
      inner.claimJob(request);

  @override
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  ) => inner.renewLease(request);

  @override
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  ) => inner.updateJobProgress(request);

  @override
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  ) => inner.updateJobData(request);

  @override
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  ) => inner.transitionJob(request);

  @override
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  ) => inner.requestJobControl(request);

  @override
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  ) => inner.recoverStalledJobs(request);

  @override
  Future<BackendWriteResult<List<JobKey>>> removeJob(
    RemoveJobRequest request,
  ) => inner.removeJob(request);

  @override
  Future<BackendWriteResult<JobLog>> appendJobLog(
    AppendJobLogRequest request,
  ) => inner.appendJobLog(request);

  @override
  Future<JobLogPage> queryJobLogs(JobLogQuery query) =>
      inner.queryJobLogs(query);

  @override
  Future<BackendWriteResult<int>> clearJobLogs(ClearJobLogsRequest request) =>
      inner.clearJobLogs(request);

  @override
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request) =>
      inner.readJobEvents(request);

  @override
  Future<FlowSnapshot?> getFlow(JobKey root) => inner.getFlow(root);
}
