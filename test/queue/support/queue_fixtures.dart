import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';

import 'fake_queue_backend.dart';
import 'fake_queue_signal.dart';

/// Queue/Job 模块测试的统一夹具。
///
/// 提供三件测试稳定性保障：
/// 1. 固定时钟（每次读取计数）——禁止依赖真实时间；
/// 2. 递增 ID 生成器（generated-1、generated-2 ...）——禁止随机 ID；
/// 3. 已 open 语义的 recording fake Backend/Signal——不触碰 Memory Backend。
///
/// 默认 Backend 能力为 local + volatile 全能力；默认 Queue state 为
/// revision 0 + 空 policy，使“未显式配置默认策略”的用例不会产生 policy 写入。
final class QueueFixture {
  /// 构造夹具。
  ///
  /// [queueName]：Queue 名称，默认 `work`。
  /// [options]：Queue 级配置；省略时使用 `QueueOptions()`。
  /// [capabilities]：Backend 能力；省略时使用 local 全能力。
  /// [signalScope]：Signal 作用域，用于构造兼容性断言。
  /// [time]：固定时钟初值；省略时为 2026-09-22 09:30 UTC。
  /// [queueState]：默认 getQueueState 返回值；省略时为 revision 0 + 空 policy。
  QueueFixture({
    this.queueName = 'work',
    QueueOptions? options,
    BackendCapabilities? capabilities,
    SignalScope signalScope = SignalScope.process,
    DateTime? time,
    QueueState? queueState,
  }) : backend = FakeQueueBackend(capabilities: capabilities),
       signal = FakeQueueSignal(scope: signalScope),
       time = time ?? DateTime.utc(2026, 9, 22, 9, 30),
       queueState =
           queueState ??
           QueueState(
             queueName: queueName,
             paused: false,
             policy: QueuePolicy(),
             revision: 0,
           ) {
    var generated = 0;
    idGenerator = () => 'generated-${++generated}';
    // getQueueState 使用动态默认值：测试可通过替换 queueState 字段改变后续读取结果。
    // 这里必须写 this.queueState：构造器体内同名标识符会解析到参数而不是字段。
    backend.setDefaultBuilder('getQueueState', () => this.queueState);
    queue = Queue(
      queueName: queueName,
      backend: backend,
      signal: signal,
      idGenerator: () => idGenerator(),
      now: readNow,
      options: options,
    );
  }

  /// 固定 Queue 名称。
  final String queueName;

  /// recording fake Backend。
  final FakeQueueBackend backend;

  /// recording fake Signal。
  final FakeQueueSignal signal;

  /// 固定时钟当前值；测试可直接替换以模拟时间推进。
  DateTime time;

  /// 默认 getQueueState 返回值；测试可直接替换（例如改成非零 revision）。
  QueueState queueState;

  /// Now 被读取的次数；用于断言“恰好一次 Now”之类的时序约束。
  int nowReads = 0;

  /// 可替换的 ID 生成器；默认返回 generated-1、generated-2 ...
  late String Function() idGenerator;

  /// 被测 Queue 句柄。
  late final Queue queue;

  /// 唯一时钟入口；每次调用计数并返回当前固定时间。
  DateTime readNow() {
    nowReads += 1;
    return time;
  }

  /// 构造本 Queue 作用域下的 [JobKey]。
  JobKey key(String jobId) => JobKey(queueName: queueName, jobId: jobId);

  /// 通过 `getJob` 取得一个受控 snapshot 的 Job 句柄。
  ///
  /// 把构造出的记录排入 `getJob` 响应队列，因此调用时机决定该句柄看到的 revision/status；
  /// 后续 mutation 的响应由各测试自行排入。
  Future<Job> attachJob({
    String jobId = 'job-1',
    JobStatus status = JobStatus.waiting,
    int revision = 1,
    JobControlRequest? controlRequest,
    DateTime? availableAt,
  }) async {
    backend.respond(
      'getJob',
      record(
        jobId: jobId,
        status: status,
        revision: revision,
        controlRequest: controlRequest,
        availableAt: availableAt,
      ),
    );
    final Job? job = await queue.getJob(jobId);
    if (job == null) {
      throw StateError('fixture could not attach job $jobId');
    }
    return job;
  }

  /// 构造 [QueueState]，默认沿用空 policy。
  QueueState state({
    bool paused = false,
    QueuePolicy? policy,
    int revision = 0,
  }) => QueueState(
    queueName: queueName,
    paused: paused,
    policy: policy ?? QueuePolicy(),
    revision: revision,
  );

  /// 构造 [JobRecord]，未指定字段取“刚创建完成的 waiting Job”语义。
  JobRecord record({
    String? queueName,
    String jobId = 'generated-1',
    String name = 'task',
    Object? data = const <String, Object?>{},
    JobStatus status = JobStatus.waiting,
    Object? progress = 0,
    Object? result,
    JobFailure? failure,
    int revision = 1,
    int attemptsMade = 0,
    int stalledCount = 0,
    int priority = 0,
    JobOptions? options,
    DateTime? availableAt,
    JobKey? parentKey,
    DateTime? createdAt,
    DateTime? updatedAt,
    DateTime? startedAt,
    DateTime? finishedAt,
    ExecutionOwner? execution,
    JobControlRequest? controlRequest,
  }) {
    final String resolvedQueueName = queueName ?? this.queueName;
    final DateTime resolvedCreatedAt = createdAt ?? time;
    return JobRecord(
      schemaVersion: 1,
      key: JobKey(queueName: resolvedQueueName, jobId: jobId),
      name: name,
      data: data,
      status: status,
      progress: progress,
      result: result,
      failure: failure,
      revision: revision,
      attemptsMade: attemptsMade,
      stalledCount: stalledCount,
      priority: priority,
      availableAt: availableAt ?? resolvedCreatedAt,
      options: options ?? JobOptions(),
      parentKey: parentKey,
      createdAt: resolvedCreatedAt,
      updatedAt: updatedAt ?? resolvedCreatedAt,
      startedAt: startedAt,
      finishedAt: finishedAt,
      execution: execution,
      controlRequest: controlRequest,
    );
  }

  /// 构造成功的 [BackendWriteResult]，默认 changed 且不通知任何 queue。
  BackendWriteResult<T> writeResult<T>(
    T value, {
    Set<String> queuesToNotify = const <String>{},
    bool changed = true,
  }) => BackendWriteResult<T>(
    value: value,
    changed: changed,
    queuesToNotify: queuesToNotify,
  );

  /// 构造单条 [JobLog]。
  JobLog log({
    String logId = 'log-1',
    String jobId = 'generated-1',
    int sequence = 1,
    int attempt = 1,
    String executionId = 'execution-1',
    JobLogLevel level = JobLogLevel.info,
    JobLogChannel channel = JobLogChannel.application,
    String message = 'hello',
    Object? data,
    DateTime? occurredAt,
  }) => JobLog(
    logId: logId,
    jobKey: key(jobId),
    sequence: sequence,
    attempt: attempt,
    executionId: executionId,
    level: level,
    channel: channel,
    message: message,
    data: data,
    occurredAt: occurredAt ?? time,
  );

  /// 构造一页 [JobLogPage]。
  JobLogPage logPage({
    List<JobLog> items = const <JobLog>[],
    int? nextSequence,
  }) => JobLogPage(items: items, nextSequence: nextSequence);
}
