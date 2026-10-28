import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/observability/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';
import 'package:tf_bull_mq/src/worker/active_job.dart';
import 'package:tf_bull_mq/src/worker/index.dart';

import 'manual_runtime.dart';
import 'recording_backend.dart';
import 'recording_log_sink.dart';
import 'recording_signal.dart';

/// 组合公开 core/backend 类型、测试替身与手工运行时的 Worker 测试夹具。
///
/// 只使用公开 API 与冻结契约构造 Job/claim/Worker，不访问 `MemoryQueueBackend`
/// 的内部状态。`active_job.dart` 的 controller 工厂是 Worker 模块内部实现，
/// 因此夹具直接导入该文件，而不是从模块 `index.dart` 取。
final class WorkerFixture {
  WorkerFixture({BackendCapabilities? capabilities})
    : backend = RecordingQueueBackend(capabilities: capabilities);

  /// 注入 Worker 的假时钟、递增 ID 生成器与手工延迟。
  final ManualRuntime runtime = ManualRuntime();

  /// 记录全部 Backend 调用的可编程替身。
  final RecordingQueueBackend backend;

  /// 需要显式 `emit` 才会唤醒的 Signal 替身。
  final RecordingSignal signal = RecordingSignal();

  /// 收集 Worker 运行日志的 sink 替身。
  final RecordingLogSink logSink = RecordingLogSink();

  /// 夹具默认的 Worker 标识。
  String workerId = 'worker-1';

  int _revision = 0;
  int _executionSeq = 0;
  int _jobSeq = 0;

  /// 下一个单调递增的 Job revision。
  int nextRevision() => ++_revision;

  /// 下一个 execution 标识。
  String nextExecutionId() => 'exec-${++_executionSeq}';

  /// 下一个队列内 Job 标识。
  String nextJobId() => 'job-${++_jobSeq}';

  /// 构造合法的 `JobRecord`，默认处于 active 且已带 execution。
  JobRecord record({
    String queueName = 'emails',
    String? jobId,
    String name = 'job',
    JobStatus status = JobStatus.active,
    int? revision,
    int attemptsMade = 1,
    Object? data,
    Object? progress,
    Object? result,
    JobFailure? failure,
    JobOptions? options,
    ExecutionOwner? execution,
    JobControlRequest? controlRequest,
    JobKey? parentKey,
    DateTime? availableAt,
    DateTime? finishedAt,
  }) => JobRecord(
    schemaVersion: 1,
    key: JobKey(queueName: queueName, jobId: jobId ?? nextJobId()),
    name: name,
    data: data,
    status: status,
    progress: progress,
    result: result,
    failure: failure,
    revision: revision ?? nextRevision(),
    attemptsMade: attemptsMade,
    stalledCount: 0,
    priority: 0,
    availableAt: availableAt ?? runtime.now(),
    options: options ?? JobOptions(),
    parentKey: parentKey,
    createdAt: runtime.now(),
    updatedAt: runtime.now(),
    startedAt: status == JobStatus.active ? runtime.now() : null,
    finishedAt: finishedAt,
    execution: execution,
    controlRequest: controlRequest,
  );

  /// 构造合法的 `ClaimedJob`：record 为 active，且 execution 与 credential 一致。
  ClaimedJob claim({
    String queueName = 'emails',
    String? jobId,
    String name = 'job',
    int? revision,
    int attemptsMade = 1,
    Object? data,
    Object? progress,
    Object? result,
    JobOptions? options,
    JobKey? parentKey,
    String? executionId,
    String? ownerWorkerId,
    JobControlRequest? controlRequest,
    Duration? leaseDuration,
    DateTime? leaseExpiresAt,
  }) {
    final credential = ExecutionCredential(
      executionId: executionId ?? nextExecutionId(),
      leaseExpiresAt:
          leaseExpiresAt ??
          (leaseDuration == null ? null : runtime.now().add(leaseDuration)),
    );
    final owner = ExecutionOwner(
      workerId: ownerWorkerId ?? workerId,
      credential: credential,
      startedAt: runtime.now(),
    );
    return ClaimedJob(
      record: record(
        queueName: queueName,
        jobId: jobId,
        name: name,
        revision: revision,
        attemptsMade: attemptsMade,
        data: data,
        progress: progress,
        result: result,
        options: options,
        parentKey: parentKey,
        execution: owner,
        controlRequest: controlRequest,
      ),
      credential: credential,
    );
  }

  /// 构造注入替身的 Worker。
  Worker worker({
    String queueName = 'emails',
    Processor? processor,
    WorkerOptions? options,
    QueueSignal? signal,
    RecordingQueueBackend? backend,
    WorkerLogSink? logSink,
    IdGenerator? idGenerator,
    ManualRuntime? runtime,
    bool ownsBackend = false,
    bool ownsSignal = false,

    /// 为 `true` 时不传 `delay`，让 Worker 回退到内部默认等待实现。
    /// 仅用于验证「delay 可选」的用例；其余用例都必须注入假时钟。
    bool omitDelay = false,
  }) {
    final clock = runtime ?? this.runtime;
    return Worker(
      queueName: queueName,
      backend: backend ?? this.backend,
      signal: signal ?? this.signal,
      processor: processor ?? (ActiveJob job) => null,
      options: options ?? const WorkerOptions(),
      logSink: logSink ?? this.logSink,
      now: clock.now,
      idGenerator: idGenerator ?? clock.nextId,
      delay: omitDelay ? null : clock.delay,
      ownsBackend: ownsBackend,
      ownsSignal: ownsSignal,
    );
  }

  /// 为一次 claim 直接构造 execution controller，用于 ActiveJob 单元测试。
  ActiveJobController controllerFor(
    ClaimedJob claimed, {
    RecordingSignal? signal,
    String? workerId,
    Future<void> Function(Set<String> queuesToNotify)? notifyQueues,
  }) {
    final target = signal ?? this.signal;
    return createActiveJobController(
      backend: backend,
      claimed: claimed,
      workerId: workerId ?? this.workerId,
      now: runtime.now,
      notifyQueues:
          notifyQueues ??
          (Set<String> queues) async {
            for (final queue in queues) {
              try {
                await target.notify(queue);
              } catch (_) {
                // 与 Worker 一致：Signal 失败不影响已提交的 Backend 事实。
              }
            }
          },
    );
  }

  /// 让 `transitionJob` 始终原子成功，并让 execution 从记录中清除。
  ///
  /// 用于验证「Processor 完成后槽位被释放」这类流程：若终态无法提交，槽位会
  /// 按设计保持占用，直到下一次 poll 重试成功。
  void installSuccessfulTransitions() {
    backend.onTransitionJob = (TransitionJobRequest request) async =>
        changedWrite(
          record(
            queueName: request.key.queueName,
            jobId: request.key.jobId,
            status: request.targetStatus,
            revision: request.expectedRevision + 1,
            finishedAt: runtime.now(),
          ),
        );
  }

  /// 预先构造 [count] 个可领取 Job，并让 `claimJob` / `getJob` 返回它们。
  ///
  /// `claimJob` 按顺序发出这些 Job，用尽后返回「无 Job」；`getJob` 默认返回对应
  /// Job 仍然 active 的 claim 记录，使控制 refresh 不会误判为失去所有权。
  ClaimPool installClaimPool({
    int count = 1,
    String queueName = 'emails',
    int attemptsMade = 1,
    JobOptions? options,
  }) {
    final pool = ClaimPool._(
      fixture: this,
      count: count,
      queueName: queueName,
      attemptsMade: attemptsMade,
      options: options,
    );
    backend.onClaimJob = (ClaimJobRequest request) =>
        Future<BackendWriteResult<ClaimedJob?>>.value(pool.take());
    backend.onGetJob = (JobKey key) =>
        Future<JobRecord?>.value(pool.byKey[key]?.record);
    return pool;
  }
}

/// 一组预先构造、按序发出的 claim 结果。
final class ClaimPool {
  ClaimPool._({
    required WorkerFixture fixture,
    required int count,
    required String queueName,
    required int attemptsMade,
    JobOptions? options,
  }) : _queueName = queueName,
       _fixture = fixture {
    for (var index = 1; index <= count; index++) {
      final claimed = _fixture.claim(
        queueName: queueName,
        jobId: 'job-$index',
        attemptsMade: attemptsMade,
        options: options,
      );
      jobs.add(claimed);
      byKey[claimed.record.key] = claimed;
    }
  }

  final WorkerFixture _fixture;
  final String _queueName;

  /// 按 jobId 顺序排列的 Job。
  final List<ClaimedJob> jobs = <ClaimedJob>[];

  /// 按 JobKey 索引的 Job。
  final Map<JobKey, ClaimedJob> byKey = <JobKey, ClaimedJob>{};

  int _cursor = 0;

  /// 已经发出的 claim 数量。
  int get served => _cursor;

  /// 尚未发出的 claim 数量。
  int get remaining => jobs.length - _cursor;

  /// 取下一个 claim 结果；耗尽后返回「无 Job」。
  BackendWriteResult<ClaimedJob?> take() {
    if (_cursor >= jobs.length) return noClaim();
    return claimedWrite(jobs[_cursor++]);
  }

  /// 按队列内 jobId 取 Job。
  ClaimedJob byJobId(String jobId) =>
      byKey[JobKey(queueName: _queueName, jobId: jobId)]!;
}
