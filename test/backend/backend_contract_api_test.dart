import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

void main() {
  // 验证 BackendWriteResult 暴露的 queuesToNotify 是不可变集合，
  // 防止外部修改 result 内部的通知集合，保证 Backend 写结果的只读语义。
  test('BackendWriteResult 的 queuesToNotify 是不可变集合', () {
    final queues = <String>{'mail'};
    final result = BackendWriteResult<void>(
      value: null,
      changed: true,
      queuesToNotify: queues,
    );
    queues.add('other');
    // 外部原集合的修改不应影响 result 内部快照。
    expect(result.queuesToNotify, <String>{'mail'});
    // 直接对 result.queuesToNotify add 也应抛 UnsupportedError。
    expect(() => result.queuesToNotify.add('x'), throwsUnsupportedError);
  });

  // 验证 changed=false 的写结果必须携带空的 queuesToNotify：
  // 没有事实变更就不应产生通知，这是 Backend 的显式不变量。
  test('changed=false 的 BackendWriteResult 必须携带空 queuesToNotify', () {
    expect(
      () => BackendWriteResult<void>(
        value: null,
        changed: false,
        queuesToNotify: <String>{'mail'},
      ),
      throwsArgumentError,
    );
  });

  // 验证 AddJobsRequest 在构造期拒绝重复的 JobKey，
  // 避免同一次批量添加中两个 Job 竞争同一个 key，语义不明确。
  test('AddJobsRequest 会在后端 I/O 前拒绝重复 JobKey', () {
    final key = const JobKey(queueName: 'mail', jobId: '1');
    expect(
      () => AddJobsRequest(
        jobs: <JobSpec>[
          JobSpec(key: key, name: 'a', data: null),
          JobSpec(key: key, name: 'b', data: null),
        ],
        createdAt: DateTime.utc(2026),
      ),
      throwsArgumentError,
    );
  });

  // 验证 TransitionJobRequest 的不变量：automaticErrorLog 只有在 targetStatus 为 failed
  // 且 failure 非空时才允许携带，避免错误日志与失败信息不一致。
  test('TransitionJobRequest 拒绝携带 automaticErrorLog 但没有 failure 的失败转移', () {
    expect(
      () => TransitionJobRequest(
        key: const JobKey(queueName: 'mail', jobId: '1'),
        targetStatus: JobStatus.failed,
        expectedRevision: 1,
        occurredAt: DateTime.utc(2026),
        automaticErrorLog: JobLogDraft(
          level: JobLogLevel.error,
          channel: JobLogChannel.system,
          message: 'failed',
        ),
      ),
      throwsArgumentError,
    );
  });

  // 验证查询请求的 limit 边界：JobQuery.limit 必须 >= 1，
  // JobLogQuery.limit 必须 <= 1000，超过边界应抛 ArgumentError。
  test('查询 limit 必须在 1 到 1000 之间', () {
    expect(() => JobQuery(queueName: 'mail', limit: 0), throwsArgumentError);
    expect(
      () => JobLogQuery(
        key: const JobKey(queueName: 'mail', jobId: '1'),
        limit: 1001,
      ),
      throwsArgumentError,
    );
  });

  // 验证 QueueBackend 接口暴露了冻结的核心方法列表（open / addJob / claimJob / readJobEvents / getFlow），
  // 使用签名探测类直接绑定方法，确保未来改动不会悄悄改变接口签名。
  test('QueueBackend 暴露冻结的核心方法签名', () {
    void verify(QueueBackend backend) {
      final Future<void> Function() open = backend.open;
      final Future<BackendWriteResult<JobRecord>> Function(AddJobRequest)
      addJob = backend.addJob;
      final Future<BackendWriteResult<ClaimedJob?>> Function(ClaimJobRequest)
      claimJob = backend.claimJob;
      final Future<JobEventPage> Function(ReadJobEventsRequest) readEvents =
          backend.readJobEvents;
      final Future<FlowSnapshot?> Function(JobKey) getFlow = backend.getFlow;
      expect(<Object>[
        open,
        addJob,
        claimJob,
        readEvents,
        getFlow,
      ], hasLength(5));
    }

    verify(_SignatureProbe());
  });
}

/// 仅用于接口签名探测的 QueueBackend 空实现。
///
/// 它不提供任何功能，所有方法都抛 UnimplementedError，
/// 唯一目的是验证 [QueueBackend] 接口的方法签名数量没有意外变化。
final class _SignatureProbe implements QueueBackend {
  @override
  BackendCapabilities get capabilities => BackendCapabilities(
    profile: BackendProfile.local,
    durability: Durability.volatile,
    delayedJobs: true,
    priorities: true,
    atomicFlows: true,
    eventDelivery: EventDelivery.liveOnly,
  );

  @override
  Future<void> open() async {}
  @override
  Future<void> close() async {}
  @override
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request) =>
      throw UnimplementedError();
  @override
  Future<BackendWriteResult<List<JobRecord>>> addJobs(AddJobsRequest request) =>
      throw UnimplementedError();
  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(AddFlowRequest request) =>
      throw UnimplementedError();
  @override
  Future<JobRecord?> getJob(JobKey key) => throw UnimplementedError();
  @override
  Future<JobPage> queryJobs(JobQuery query) => throw UnimplementedError();
  @override
  Future<QueueState> getQueueState(String queueName) =>
      throw UnimplementedError();
  @override
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(ClaimJobRequest request) =>
      throw UnimplementedError();
  @override
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<List<JobKey>>> removeJob(
    RemoveJobRequest request,
  ) => throw UnimplementedError();
  @override
  Future<BackendWriteResult<JobLog>> appendJobLog(
    AppendJobLogRequest request,
  ) => throw UnimplementedError();
  @override
  Future<JobLogPage> queryJobLogs(JobLogQuery query) =>
      throw UnimplementedError();
  @override
  Future<BackendWriteResult<int>> clearJobLogs(ClearJobLogsRequest request) =>
      throw UnimplementedError();
  @override
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request) =>
      throw UnimplementedError();
  @override
  Future<FlowSnapshot?> getFlow(JobKey root) => throw UnimplementedError();
}
