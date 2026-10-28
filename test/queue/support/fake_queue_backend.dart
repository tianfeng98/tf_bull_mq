import 'dart:async';

import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

/// 一次 Backend 调用的记录：方法名 + 原始入参。
final class FakeQueueCall {
  const FakeQueueCall(this.method, this.argument);

  /// 被调用的 Backend 方法名，用于断言调用集合与顺序。
  final String method;

  /// 调用时传入的冻结请求/查询 DTO；无参方法为 null。
  final Object? argument;
}

/// 尚未被排入队列时的兜底响应函数。
typedef FakeQueueResponder = FutureOr<Object?> Function(
  String method,
  Object? argument,
);

/// 以“抛出错误 + 指定堆栈”作为响应的包装器。
///
/// 用于断言通知失败时原始 error/stack trace 被完整保留，
/// 而不是由这里重新生成堆栈。
final class FakeQueueFailure {
  const FakeQueueFailure(this.error, [this.stackTrace]);

  /// 需要抛出的原始错误。
  final Object error;

  /// 需要与错误一起抛出的堆栈；为 null 时使用当前位置堆栈。
  final StackTrace? stackTrace;
}

/// 冻结 [QueueBackend] 的可编程 recording fake。
///
/// 只负责记录调用并按配置返回冻结类型，不实现任何产品语义：
/// 不模拟调度、不模拟原子性、不模拟 Flow 或 Worker 行为。
/// 未配置响应的方法抛 `StateError('No fake response for <method>.')`，
/// 使测试遗漏配置时立即暴露，而不是静默返回错误结果。
final class FakeQueueBackend implements QueueBackend {
  /// 构造 fake；[capabilities] 省略时使用 local + volatile 全能力配置。
  FakeQueueBackend({BackendCapabilities? capabilities})
    : capabilities = capabilities ?? localCapabilities();

  /// local + volatile、delayed/priority/atomicFlows 全支持的默认能力。
  static BackendCapabilities localCapabilities() => BackendCapabilities(
    profile: BackendProfile.local,
    durability: Durability.volatile,
    delayedJobs: true,
    priorities: true,
    atomicFlows: true,
    eventDelivery: EventDelivery.liveOnly,
  );

  /// distributed + durable + replayable 能力，用于构造兼容性断言。
  static BackendCapabilities distributedCapabilities() => BackendCapabilities(
    profile: BackendProfile.distributed,
    durability: Durability.durable,
    delayedJobs: true,
    priorities: true,
    atomicFlows: true,
    eventDelivery: EventDelivery.replayable,
  );

  /// 当前暴露给 Queue 的能力声明；测试可直接替换以模拟能力缺失。
  @override
  BackendCapabilities capabilities;

  /// 按调用顺序记录的方法名，供 `calls.contains(...)`、`where(...)` 风格断言使用。
  final List<String> calls = <String>[];

  /// 按调用顺序记录的完整调用（方法名 + 入参）。
  final List<FakeQueueCall> invocations = <FakeQueueCall>[];

  /// 未被排入、也未设置 default 的方法的兜底响应。
  FakeQueueResponder? responder;

  /// Lifecycle 调用计数：产品代码不应触发它们。
  int openCalls = 0;
  int closeCalls = 0;

  /// close 需要抛出的错误；为 null 表示正常关闭。
  Object? closeError;

  /// [closeError] 对应的原始堆栈。
  StackTrace? closeErrorStack;

  final Map<String, List<Object?>> _queued = <String, List<Object?>>{};
  final Map<String, Object?> _defaults = <String, Object?>{};
  final Map<String, Object? Function()> _defaultBuilders =
      <String, Object? Function()>{};

  /// 为该方法的接下来若干次调用排入响应（先进先出）。
  ///
  /// 排入 [FakeQueueFailure] 或 Error/Exception 实例表示该次调用抛错。
  void respond(String method, Object? response) {
    (_queued[method] ??= <Object?>[]).add(response);
  }

  /// 为该方法的接下来若干次调用排入“抛错”响应。
  void respondWithFailure(
    String method,
    Object error, [
    StackTrace? stackTrace,
  ]) {
    respond(method, FakeQueueFailure(error, stackTrace));
  }

  /// 为该方法的**所有**调用设置持久默认响应（队列响应优先）。
  void setDefault(String method, Object? response) {
    _defaults[method] = response;
  }

  /// 为该方法的**所有**调用设置动态默认响应。
  ///
  /// 与 [setDefault] 的区别：每次调用时重新求值，
  /// 适合“默认响应依赖 fixture 当前状态”的场景（例如 getQueueState）。
  void setDefaultBuilder(String method, Object? Function() builder) {
    _defaultBuilders[method] = builder;
  }

  /// 某方法是否被调用过。
  bool called(String method) => calls.contains(method);

  /// 某方法被调用的次数。
  int countOf(String method) => calls.where((call) => call == method).length;

  /// 某方法全部调用的入参列表，按调用顺序。
  List<Object?> argumentsOf(String method) => invocations
      .where((call) => call.method == method)
      .map((call) => call.argument)
      .toList();

  /// 某方法最后一次调用的入参；未被调用时抛 [StateError]。
  Object? lastArgument(String method) {
    final List<Object?> arguments = argumentsOf(method);
    if (arguments.isEmpty) {
      throw StateError('No recorded call for $method.');
    }
    return arguments.last;
  }

  FutureOr<Object?> _record(String method, Object? argument) {
    calls.add(method);
    invocations.add(FakeQueueCall(method, argument));
    final List<Object?>? queued = _queued[method];
    if (queued != null && queued.isNotEmpty) {
      return _resolve(queued.removeAt(0));
    }
    if (_defaults.containsKey(method)) {
      return _resolve(_defaults[method]);
    }
    final Object? Function()? builder = _defaultBuilders[method];
    if (builder != null) {
      return _resolve(builder());
    }
    final FakeQueueResponder? custom = responder;
    if (custom != null) {
      return custom(method, argument);
    }
    throw StateError('No fake response for $method.');
  }

  Object? _resolve(Object? response) {
    if (response is FakeQueueFailure) {
      Error.throwWithStackTrace(
        response.error,
        response.stackTrace ?? StackTrace.current,
      );
    }
    if (response is Error) {
      Error.throwWithStackTrace(response, StackTrace.current);
    }
    if (response is Exception) {
      Error.throwWithStackTrace(response, StackTrace.current);
    }
    return response;
  }

  @override
  Future<void> open() async {
    openCalls += 1;
  }

  @override
  Future<void> close() async {
    closeCalls += 1;
    final Object? error = closeError;
    if (error != null) {
      Error.throwWithStackTrace(error, closeErrorStack ?? StackTrace.current);
    }
  }

  @override
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request) async =>
      (await _record('addJob', request)) as BackendWriteResult<JobRecord>;

  @override
  Future<BackendWriteResult<List<JobRecord>>> addJobs(
    AddJobsRequest request,
  ) async =>
      (await _record('addJobs', request))
          as BackendWriteResult<List<JobRecord>>;

  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(
    AddFlowRequest request,
  ) async =>
      (await _record('addFlow', request)) as BackendWriteResult<FlowSnapshot>;

  @override
  Future<JobRecord?> getJob(JobKey key) async =>
      (await _record('getJob', key)) as JobRecord?;

  @override
  Future<JobPage> queryJobs(JobQuery query) async =>
      (await _record('queryJobs', query)) as JobPage;

  @override
  Future<QueueState> getQueueState(String queueName) async =>
      (await _record('getQueueState', queueName)) as QueueState;

  @override
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  ) async =>
      (await _record('setQueuePaused', request))
          as BackendWriteResult<QueueState>;

  @override
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  ) async =>
      (await _record('setQueuePolicy', request))
          as BackendWriteResult<QueueState>;

  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(
    ClaimJobRequest request,
  ) async =>
      (await _record('claimJob', request)) as BackendWriteResult<ClaimedJob?>;

  @override
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  ) async =>
      (await _record('renewLease', request))
          as BackendWriteResult<ExecutionCredential>;

  @override
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  ) async =>
      (await _record('updateJobProgress', request))
          as BackendWriteResult<JobRecord>;

  @override
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  ) async =>
      (await _record('updateJobData', request))
          as BackendWriteResult<JobRecord>;

  @override
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  ) async =>
      (await _record('transitionJob', request))
          as BackendWriteResult<JobRecord>;

  @override
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  ) async =>
      (await _record('requestJobControl', request))
          as BackendWriteResult<JobRecord>;

  @override
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  ) async =>
      (await _record('recoverStalledJobs', request))
          as BackendWriteResult<List<JobRecord>>;

  @override
  Future<BackendWriteResult<List<JobKey>>> removeJob(
    RemoveJobRequest request,
  ) async =>
      (await _record('removeJob', request)) as BackendWriteResult<List<JobKey>>;

  @override
  Future<BackendWriteResult<JobLog>> appendJobLog(
    AppendJobLogRequest request,
  ) async =>
      (await _record('appendJobLog', request)) as BackendWriteResult<JobLog>;

  @override
  Future<JobLogPage> queryJobLogs(JobLogQuery query) async =>
      (await _record('queryJobLogs', query)) as JobLogPage;

  @override
  Future<BackendWriteResult<int>> clearJobLogs(
    ClearJobLogsRequest request,
  ) async =>
      (await _record('clearJobLogs', request)) as BackendWriteResult<int>;

  @override
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request) async =>
      (await _record('readJobEvents', request)) as JobEventPage;

  @override
  Future<FlowSnapshot?> getFlow(JobKey root) async =>
      (await _record('getFlow', root)) as FlowSnapshot?;
}
