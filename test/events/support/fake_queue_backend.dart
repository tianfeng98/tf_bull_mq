import 'dart:async';

import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

/// 处理单次 readJobEvents 请求的函数类型。
typedef ReadEventsHandler = Future<JobEventPage> Function(
  ReadJobEventsRequest request,
);

/// 仅实现 QueueEvents 会调用的接口成员的 [QueueBackend] 测试替身。
///
/// 设计意图：
/// 1. QueueEvents 只依赖 readJobEvents / open / close，其余接口成员全部抛
///    [UnsupportedError]，而非通过 noSuchMethod 静默吞掉，这样一旦 QueueEvents
///    内部错误地调用了它不应该依赖的方法，编译期或运行期立即暴露；
/// 2. 提供 [enqueueReadCompleter] / [completeNextRead] / [failNextRead] 等手动控制点，
///    让测试精确控制 readJobEvents 的完成时机和错误注入；
/// 3. 记录所有 read 请求的入队顺序、并发数峰值，便于验证 single-flight 语义。
final class FakeQueueBackend implements QueueBackend {
  FakeQueueBackend({required this.capabilities, required this.onReadEvents});

  @override
  final BackendCapabilities capabilities;

  /// 每次 [readJobEvents] 被调用时的业务处理函数，由测试提供，决定返回的页面内容。
  final ReadEventsHandler onReadEvents;

  /// 按到达顺序记录的所有 readJobEvents 请求，测试可断言游标传递是否正确。
  final List<ReadJobEventsRequest> readRequests = <ReadJobEventsRequest>[];

  /// 当前正在执行的 readJobEvents 调用数，用于验证 single-flight 语义。
  int activeReads = 0;

  /// 历史并发 readJobEvents 调用数的峰值。
  int maxActiveReads = 0;

  /// [open] 被调用次数。QueueEvents 不应调用 open，测试可用作反向断言。
  int openCalls = 0;

  /// [close] 被调用次数，验证 ownsBackend 语义。
  int closeCalls = 0;

  /// 测试可预设的 [Completer] 队列——每个 enqueue 的 completer 会让下一次
  /// readJobEvents 挂起，直到测试主动 completeNextRead / failNextRead。
  final List<Completer<JobEventPage>> _pendingReads =
      <Completer<JobEventPage>>[];

  /// 当非 null 时，下一次 [close] 调用会抛出此错误。
  Object? closeError;

  /// 入队一个 [Completer]，下一次 [readJobEvents] 调用会 await 它而非直接调用 onReadEvents。
  Completer<JobEventPage> enqueueReadCompleter() {
    final completer = Completer<JobEventPage>();
    _pendingReads.add(completer);
    return completer;
  }

  /// 用 [onReadEvents] 产出的页面完成最早挂起的 read。
  void completeNextRead(ReadJobEventsRequest request) {
    if (_pendingReads.isEmpty) {
      throw StateError('no pending read to complete');
    }
    final completer = _pendingReads.removeAt(0);
    completer.complete(onReadEvents(request));
  }

  /// 用错误完成最早挂起的 read，模拟 Backend 读取失败。
  void failNextRead(Object error, [StackTrace? stackTrace]) {
    if (_pendingReads.isEmpty) {
      throw StateError('no pending read to fail');
    }
    final completer = _pendingReads.removeAt(0);
    completer.completeError(error, stackTrace);
  }

  @override
  Future<void> open() async {
    openCalls++;
  }

  @override
  Future<void> close() async {
    closeCalls++;
    final error = closeError;
    if (error != null) {
      closeError = null;
      throw error;
    }
  }

  @override
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request) async {
    readRequests.add(request);
    activeReads++;
    if (activeReads > maxActiveReads) {
      maxActiveReads = activeReads;
    }
    try {
      if (_pendingReads.isNotEmpty) {
        return await _pendingReads.first.future;
      }
      return await onReadEvents(request);
    } finally {
      activeReads--;
    }
  }

  @override
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request) {
    throw UnsupportedError('QueueEvents test fake does not implement addJob.');
  }

  @override
  Future<BackendWriteResult<List<JobRecord>>> addJobs(AddJobsRequest request) {
    throw UnsupportedError('QueueEvents test fake does not implement addJobs.');
  }

  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(AddFlowRequest request) {
    throw UnsupportedError('QueueEvents test fake does not implement addFlow.');
  }

  @override
  Future<JobRecord?> getJob(JobKey key) {
    throw UnsupportedError('QueueEvents test fake does not implement getJob.');
  }

  @override
  Future<JobPage> queryJobs(JobQuery query) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement queryJobs.',
    );
  }

  @override
  Future<QueueState> getQueueState(String queueName) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement getQueueState.',
    );
  }

  @override
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement setQueuePaused.',
    );
  }

  @override
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement setQueuePolicy.',
    );
  }

  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(ClaimJobRequest request) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement claimJob.',
    );
  }

  @override
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement renewLease.',
    );
  }

  @override
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement updateJobProgress.',
    );
  }

  @override
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement updateJobData.',
    );
  }

  @override
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement transitionJob.',
    );
  }

  @override
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement requestJobControl.',
    );
  }

  @override
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  ) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement recoverStalledJobs.',
    );
  }

  @override
  Future<BackendWriteResult<List<JobKey>>> removeJob(RemoveJobRequest request) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement removeJob.',
    );
  }

  @override
  Future<BackendWriteResult<JobLog>> appendJobLog(AppendJobLogRequest request) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement appendJobLog.',
    );
  }

  @override
  Future<JobLogPage> queryJobLogs(JobLogQuery query) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement queryJobLogs.',
    );
  }

  @override
  Future<BackendWriteResult<int>> clearJobLogs(ClearJobLogsRequest request) {
    throw UnsupportedError(
      'QueueEvents test fake does not implement clearJobLogs.',
    );
  }

  @override
  Future<FlowSnapshot?> getFlow(JobKey root) {
    throw UnsupportedError('QueueEvents test fake does not implement getFlow.');
  }
}
