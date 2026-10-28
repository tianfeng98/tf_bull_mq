import 'dart:async';

import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

/// 实现冻结 `QueueBackend` 的可编程测试替身。
///
/// 设计约束：
///
/// - 每个方法都显式实现并记录调用名与请求对象，测试用 [calls] / `...Requests`
///   断言精确的调用次数与参数，不使用动态方法转发兜底。
/// - 读取类操作（`getJob`、`getFlow`、分页查询、`claimJob`）默认返回空值，
///   因此不会遮蔽 Worker 的正常流程。
/// - 写入类操作默认抛 `StateError('Unexpected <operation> call')`，让任何计划外
///   的写入立刻让测试失败。
final class RecordingQueueBackend implements QueueBackend {
  RecordingQueueBackend({
    BackendCapabilities? capabilities,
    List<BackendWriteResult<ClaimedJob?>>? pendingClaims,
  }) : _capabilities =
           capabilities ??
           BackendCapabilities(
             profile: BackendProfile.local,
             durability: Durability.volatile,
             delayedJobs: true,
             priorities: true,
             atomicFlows: true,
             eventDelivery: EventDelivery.liveOnly,
           ),
       pendingClaims = pendingClaims ?? <BackendWriteResult<ClaimedJob?>>[];

  final BackendCapabilities _capabilities;

  /// 依次返回的 claim 结果。用尽后 `claimJob` 返回「无 Job」结果。
  /// 每个元素在被取用后被移除。
  final List<BackendWriteResult<ClaimedJob?>> pendingClaims;

  /// 按调用顺序记录的 operation 名称。
  final List<String> calls = <String>[];

  final List<ClaimJobRequest> claimJobRequests = <ClaimJobRequest>[];
  final List<JobKey> getJobRequests = <JobKey>[];
  final List<JobKey> getFlowRequests = <JobKey>[];
  final List<TransitionJobRequest> transitionJobRequests =
      <TransitionJobRequest>[];
  final List<RenewLeaseRequest> renewLeaseRequests = <RenewLeaseRequest>[];
  final List<UpdateJobProgressRequest> updateJobProgressRequests =
      <UpdateJobProgressRequest>[];
  final List<UpdateJobDataRequest> updateJobDataRequests =
      <UpdateJobDataRequest>[];
  final List<AppendJobLogRequest> appendJobLogRequests =
      <AppendJobLogRequest>[];
  final List<RecoverStalledJobsRequest> recoverStalledJobsRequests =
      <RecoverStalledJobsRequest>[];
  final List<SetQueuePausedRequest> setQueuePausedRequests =
      <SetQueuePausedRequest>[];

  /// 指定 operation 被调用的次数。
  int countOf(String operation) =>
      calls.where((String call) => call == operation).length;

  @override
  BackendCapabilities get capabilities => _capabilities;

  // --- 可编程 handler -------------------------------------------------------

  Future<void> Function()? onOpen;
  Future<void> Function()? onClose;
  Future<BackendWriteResult<JobRecord>> Function(AddJobRequest)? onAddJob;
  Future<BackendWriteResult<List<JobRecord>>> Function(AddJobsRequest)?
  onAddJobs;
  Future<BackendWriteResult<FlowSnapshot>> Function(AddFlowRequest)? onAddFlow;
  Future<JobRecord?> Function(JobKey)? onGetJob;
  Future<JobPage> Function(JobQuery)? onQueryJobs;
  Future<QueueState> Function(String)? onGetQueueState;
  Future<BackendWriteResult<QueueState>> Function(SetQueuePausedRequest)?
  onSetQueuePaused;
  Future<BackendWriteResult<QueueState>> Function(SetQueuePolicyRequest)?
  onSetQueuePolicy;
  Future<BackendWriteResult<ClaimedJob?>> Function(ClaimJobRequest)? onClaimJob;
  Future<BackendWriteResult<ExecutionCredential>> Function(RenewLeaseRequest)?
  onRenewLease;
  Future<BackendWriteResult<JobRecord>> Function(UpdateJobProgressRequest)?
  onUpdateJobProgress;
  Future<BackendWriteResult<JobRecord>> Function(UpdateJobDataRequest)?
  onUpdateJobData;
  Future<BackendWriteResult<JobRecord>> Function(TransitionJobRequest)?
  onTransitionJob;
  Future<BackendWriteResult<JobRecord>> Function(RequestJobControlRequest)?
  onRequestJobControl;
  Future<BackendWriteResult<List<JobRecord>>> Function(
    RecoverStalledJobsRequest,
  )?
  onRecoverStalledJobs;
  Future<BackendWriteResult<List<JobKey>>> Function(RemoveJobRequest)?
  onRemoveJob;
  Future<BackendWriteResult<JobLog>> Function(AppendJobLogRequest)?
  onAppendJobLog;
  Future<JobLogPage> Function(JobLogQuery)? onQueryJobLogs;
  Future<BackendWriteResult<int>> Function(ClearJobLogsRequest)? onClearJobLogs;
  Future<JobEventPage> Function(ReadJobEventsRequest)? onReadJobEvents;
  Future<FlowSnapshot?> Function(JobKey)? onGetFlow;

  // --- QueueBackend 实现 ----------------------------------------------------

  @override
  Future<void> open() {
    calls.add('open');
    final handler = onOpen;
    return handler == null ? Future<void>.value() : handler();
  }

  @override
  Future<void> close() {
    calls.add('close');
    final handler = onClose;
    return handler == null ? Future<void>.value() : handler();
  }

  @override
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request) {
    calls.add('addJob');
    final handler = onAddJob;
    if (handler == null) return _unexpected('addJob');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<List<JobRecord>>> addJobs(AddJobsRequest request) {
    calls.add('addJobs');
    final handler = onAddJobs;
    if (handler == null) return _unexpected('addJobs');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(AddFlowRequest request) {
    calls.add('addFlow');
    final handler = onAddFlow;
    if (handler == null) return _unexpected('addFlow');
    return handler(request);
  }

  @override
  Future<JobRecord?> getJob(JobKey key) {
    calls.add('getJob');
    getJobRequests.add(key);
    final handler = onGetJob;
    if (handler == null) return Future<JobRecord?>.value();
    return handler(key);
  }

  @override
  Future<JobPage> queryJobs(JobQuery query) {
    calls.add('queryJobs');
    final handler = onQueryJobs;
    if (handler == null) {
      return Future<JobPage>.value(
        const JobPage(items: <JobRecord>[], nextCursor: null),
      );
    }
    return handler(query);
  }

  @override
  Future<QueueState> getQueueState(String queueName) {
    calls.add('getQueueState');
    final handler = onGetQueueState;
    if (handler == null) {
      return Future<QueueState>.value(
        QueueState(
          queueName: queueName,
          paused: false,
          policy: QueuePolicy(),
          revision: 0,
        ),
      );
    }
    return handler(queueName);
  }

  @override
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  ) {
    calls.add('setQueuePaused');
    setQueuePausedRequests.add(request);
    final handler = onSetQueuePaused;
    if (handler == null) return _unexpected('setQueuePaused');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  ) {
    calls.add('setQueuePolicy');
    final handler = onSetQueuePolicy;
    if (handler == null) return _unexpected('setQueuePolicy');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(ClaimJobRequest request) {
    calls.add('claimJob');
    claimJobRequests.add(request);
    final handler = onClaimJob;
    if (handler != null) return handler(request);
    if (pendingClaims.isNotEmpty) {
      return Future<BackendWriteResult<ClaimedJob?>>.value(
        pendingClaims.removeAt(0),
      );
    }
    return Future<BackendWriteResult<ClaimedJob?>>.value(
      BackendWriteResult<ClaimedJob?>(
        value: null,
        changed: false,
        queuesToNotify: const <String>{},
      ),
    );
  }

  @override
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  ) {
    calls.add('renewLease');
    renewLeaseRequests.add(request);
    final handler = onRenewLease;
    if (handler == null) return _unexpected('renewLease');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  ) {
    calls.add('updateJobProgress');
    updateJobProgressRequests.add(request);
    final handler = onUpdateJobProgress;
    if (handler == null) return _unexpected('updateJobProgress');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  ) {
    calls.add('updateJobData');
    updateJobDataRequests.add(request);
    final handler = onUpdateJobData;
    if (handler == null) return _unexpected('updateJobData');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  ) {
    calls.add('transitionJob');
    transitionJobRequests.add(request);
    final handler = onTransitionJob;
    if (handler == null) return _unexpected('transitionJob');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  ) {
    calls.add('requestJobControl');
    final handler = onRequestJobControl;
    if (handler == null) return _unexpected('requestJobControl');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  ) {
    calls.add('recoverStalledJobs');
    recoverStalledJobsRequests.add(request);
    final handler = onRecoverStalledJobs;
    // recovery 是 forceClose 的内部协调步骤，大多数测试并不直接验证它；
    // 未配置 handler 时默认返回空的成功结果，让 forceClose 能正常完成。
    if (handler == null) {
      return Future<BackendWriteResult<List<JobRecord>>>.value(
        BackendWriteResult<List<JobRecord>>(
          value: const <JobRecord>[],
          changed: true,
          queuesToNotify: const <String>{},
        ),
      );
    }
    return handler(request);
  }

  @override
  Future<BackendWriteResult<List<JobKey>>> removeJob(RemoveJobRequest request) {
    calls.add('removeJob');
    final handler = onRemoveJob;
    if (handler == null) return _unexpected('removeJob');
    return handler(request);
  }

  @override
  Future<BackendWriteResult<JobLog>> appendJobLog(AppendJobLogRequest request) {
    calls.add('appendJobLog');
    appendJobLogRequests.add(request);
    final handler = onAppendJobLog;
    if (handler == null) return _unexpected('appendJobLog');
    return handler(request);
  }

  @override
  Future<JobLogPage> queryJobLogs(JobLogQuery query) {
    calls.add('queryJobLogs');
    final handler = onQueryJobLogs;
    if (handler == null) {
      return Future<JobLogPage>.value(
        const JobLogPage(items: <JobLog>[], nextSequence: null),
      );
    }
    return handler(query);
  }

  @override
  Future<BackendWriteResult<int>> clearJobLogs(ClearJobLogsRequest request) {
    calls.add('clearJobLogs');
    final handler = onClearJobLogs;
    if (handler == null) return _unexpected('clearJobLogs');
    return handler(request);
  }

  @override
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request) {
    calls.add('readJobEvents');
    final handler = onReadJobEvents;
    if (handler == null) {
      return Future<JobEventPage>.value(
        const JobEventPage(items: <PositionedJobEvent>[], nextCursor: null),
      );
    }
    return handler(request);
  }

  @override
  Future<FlowSnapshot?> getFlow(JobKey root) {
    calls.add('getFlow');
    getFlowRequests.add(root);
    final handler = onGetFlow;
    if (handler == null) return Future<FlowSnapshot?>.value();
    return handler(root);
  }

  Future<T> _unexpected<T>(String operation) =>
      Future<T>.error(StateError('Unexpected $operation call'));
}

/// 构造一个「已变更」的写入结果。
BackendWriteResult<T> changedWrite<T>(
  T value, {
  Set<String> queuesToNotify = const <String>{},
}) => BackendWriteResult<T>(
  value: value,
  changed: true,
  queuesToNotify: queuesToNotify,
);

/// 构造一个「未变更」的写入结果。
BackendWriteResult<T> unchangedWrite<T>(T value) => BackendWriteResult<T>(
  value: value,
  changed: false,
  queuesToNotify: const <String>{},
);

/// 构造一次成功 claim 的结果。
BackendWriteResult<ClaimedJob?> claimedWrite(
  ClaimedJob job, {
  Set<String> queuesToNotify = const <String>{},
}) => changedWrite<ClaimedJob?>(job, queuesToNotify: queuesToNotify);

/// 构造「本次没有可领取 Job」的结果。
BackendWriteResult<ClaimedJob?> noClaim() => unchangedWrite<ClaimedJob?>(null);
