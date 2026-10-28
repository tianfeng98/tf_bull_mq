import '../backend/index.dart';
import '../core/index.dart';
import 'queue_context.dart';

/// 以 [JobKey] 定位持久化事实的轻量句柄。
///
/// Job 不保存 Job 事实：它只缓存最近一次成功读取或写入的原始 [JobRecord]，
/// 每次 `refresh()` 或 mutation 都会重新访问 Backend。不同的 `Queue.add`/
/// `getJob`/`query` 调用会得到不同的句柄实例，但相同 [JobKey] 表达同一持久化身份，
/// 句柄之间不共享 snapshot。
///
/// 句柄持有一个 JobPayloadCodecs bundle，用于把 Backend 原始事实解码为业务类型化的
/// [JobSnapshot]。业务字段（data/progress/result）从 snapshot 读取；其余元数据
/// 通过只读 getter 委托给 rawRecord。
///
/// Job 没有公共构造器：只能由 `Queue` 依据 Backend 返回的记录创建。
final class Job<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  Job._({
    required QueueContext queueContext,
    required JobRecord rawRecord,
    required JobPayloadCodecs<TData, TProgress, TResult> codecs,
  }) : _context = queueContext,
       _codecs = codecs,
       _rawRecord = rawRecord,
       _snapshot = JobSnapshot<TData, TProgress, TResult>(
         rawRecord: rawRecord,
         codecs: codecs,
       );

  final QueueContext _context;
  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;
  JobRecord _rawRecord;
  JobSnapshot<TData, TProgress, TResult> _snapshot;

  /// 复合身份（queueName + jobId），在整个句柄生命周期内不变。
  JobKey get key => _snapshot.key;

  /// Job 所属 Queue 名称，取自 [key]。
  String get queueName => _snapshot.key.queueName;

  /// Job ID，取自 [key]。
  String get jobId => _snapshot.key.jobId;

  /// 本句柄最近一次成功读取或写入的类型化 snapshot。
  ///
  /// 它只是快照而非权威事实：Backend 可能已被其他句柄推进，
  /// 需要基于最新状态操作时应先显式调用 `refresh()`。
  JobSnapshot<TData, TProgress, TResult> get snapshot => _snapshot;

  /// 从 Backend 重新读取本 Job 的最新记录。
  ///
  /// 每次调用都按本句柄的复合 [key] 访问 Backend（不使用缓存推断），
  /// 成功时用返回记录替换本句柄的最近 snapshot 并返回它。
  ///
  /// 失败语义：Backend 中不存在该 Job 时抛 [JobNotFound]，
  /// 且保留调用前的 snapshot（不会用 null 或伪造状态覆盖）。
  /// 其它 Backend 错误原样传播，同样不修改 snapshot。
  ///
  /// 副作用：只读取 Backend，不产生 Signal 提示；其他句柄的 snapshot 不受影响。
  Future<JobSnapshot<TData, TProgress, TResult>> refresh() {
    _context.ensureOpen('job.refresh');
    final JobKey jobKey = _snapshot.key;
    return _context.run('job.refresh', () async {
      final JobRecord? record = await _context.backend.getJob(jobKey);
      if (record == null) {
        throw JobNotFound(jobKey, 'job is not present in ${jobKey.queueName}');
      }
      _replaceSnapshot(record);
      return _snapshot;
    });
  }

  /// 手动重试 failed 或 canceled Job。
  ///
  /// [delay]：重试延迟；为零表示立即重试（目标状态 waiting），
  /// 大于零表示延迟重试（目标状态 delayed，`availableAt = occurredAt + delay`）。
  /// [expectedRevision]：可选乐观并发前提；为 null 时使用本句柄最近 snapshot 的 revision。
  ///
  /// 该方法是"手动重试"语义：用于 failed 或 canceled Job，是否允许重试由 Backend 的冻结状态机判定
  /// （例如依赖失败的父 Job 会被 Backend 以 [FlowDependencyFailure] 拒绝，本句柄原样传播，
  /// 不改写错误、不通知 Signal、不更新 snapshot）。
  ///
  /// 对 canceled Job 重试：Backend 会重置 progress，清除 failure、
  /// execution 和 controlRequest，但保留 data、attemptsMade、stalledCount、
  /// startedAt 以及历史 JobLog；若该 Job 仍有未完成的 Flow 子依赖，
  /// 实际进入 waitingChildren 而非 waiting，需等子全部完成后再推进。
  /// completed Job 仍不可重试，重复执行成功业务必须创建新 Job。
  ///
  /// 失败语义与顺序：
  /// - 负 [delay] -> [ArgumentError]，且发生在任何 Backend capability 访问、Backend 方法
  ///   调用与 `Now` 读取之前；
  /// - delay 大于零但 Backend 不支持 `delayedJobs` -> `UnsupportedCapability`，零 Backend 写入；
  /// - Backend 拒绝或冲突 -> 原样传播，snapshot 不变、零通知。
  ///
  /// 副作用：成功时先更新本句柄 snapshot，再按 Backend 返回的通知集合发送提示。
  Future<JobSnapshot<TData, TProgress, TResult>> retry({
    Duration delay = Duration.zero,
    int? expectedRevision,
  }) {
    // 负延迟必须在读取能力、调用 Backend 或读取时钟之前失败。
    if (delay.isNegative) {
      throw ArgumentError.value(delay, 'delay', 'delay must not be negative');
    }
    _context.ensureOpen('job.retry');
    final JobRecord current = _rawRecord;
    final int revision = expectedRevision ?? current.revision;
    final DateTime occurredAt = _context.now();
    final bool delayed = delay > Duration.zero;
    if (delayed && !_context.backend.capabilities.delayedJobs) {
      throw UnsupportedCapability(
        'delayedJobs',
        'job.retry requires a backend that supports delayed jobs',
      );
    }
    final TransitionJobRequest request = TransitionJobRequest(
      key: current.key,
      targetStatus: delayed ? JobStatus.delayed : JobStatus.waiting,
      expectedRevision: revision,
      occurredAt: occurredAt,
      availableAt: occurredAt.add(delay),
    );
    return _context
        .commit<JobRecord>(
          operation: 'job.retry',
          write: () => _context.backend.transitionJob(request),
          onCommitted: _replaceSnapshot,
        )
        .then((record) => _snapshot);
  }

  /// 取消本 Job。
  ///
  /// [expectedRevision]：可选乐观并发前提；为 null 时使用本句柄最近 snapshot 的 revision。
  ///
  /// 状态分派（只依据调用开始时的 snapshot，不自动 refresh）：
  /// - active：只调用 `requestJobControl(cancel)`，状态保持 active，直到持有执行凭证的
  ///   Worker/Processor 确认取消；本句柄绝不本地伪造 canceled；
  /// - waiting、delayed、waitingChildren、paused、canceled：调用 `transitionJob(canceled)`，
  ///   其中 canceled 再 cancel 由 Backend 返回 `changed == false` 的幂等结果且零通知；
  /// - completed、failed：同样交给 Backend 判定并由其明确拒绝（[InvalidJobTransition]）。
  ///
  /// 失败语义：Backend 错误原样传播，snapshot 不变、零通知；
  /// 成功但提示失败时抛 [QueueNotificationFailure]，此时 snapshot 已是已提交记录。
  Future<JobSnapshot<TData, TProgress, TResult>> cancel({
    int? expectedRevision,
  }) {
    _context.ensureOpen('job.cancel');
    final JobRecord current = _rawRecord;
    final int revision = expectedRevision ?? current.revision;
    final DateTime requestedAt = _context.now();
    if (current.status == JobStatus.active) {
      final RequestJobControlRequest request = RequestJobControlRequest(
        key: current.key,
        action: JobControlAction.cancel,
        expectedRevision: revision,
        requestedAt: requestedAt,
      );
      return _context
          .commit<JobRecord>(
            operation: 'job.cancel',
            write: () => _context.backend.requestJobControl(request),
            onCommitted: _replaceSnapshot,
          )
          .then((record) => _snapshot);
    }
    final TransitionJobRequest request = TransitionJobRequest(
      key: current.key,
      targetStatus: JobStatus.canceled,
      expectedRevision: revision,
      occurredAt: requestedAt,
    );
    return _context
        .commit<JobRecord>(
          operation: 'job.cancel',
          write: () => _context.backend.transitionJob(request),
          onCommitted: _replaceSnapshot,
        )
        .then((record) => _snapshot);
  }

  /// 请求协作式暂停当前正在执行的 Job。
  ///
  /// [expectedRevision]：可选乐观并发前提；为 null 时使用本句柄最近 snapshot 的 revision。
  ///
  /// 语义：只允许 snapshot.status 为 [JobStatus.active] 时调用；其他状态（waiting、
  /// delayed、failed 等）的句柄直接抛 [LifecycleError]，不产生 Backend 写入。
  /// 内部调用 `requestJobControl(JobControlAction.pause)` 写入协作式暂停请求，
  /// Job 状态保持 active，直到持有 execution 的 Worker/Processor 调用 confirmPaused 完成暂停；
  /// 若 Processor 选择忽略并继续执行，本请求不会自动提升为取消。
  /// Backend 已保证相同 action 的 [requestJobControl] 幂等：重复 pause 不会产生额外事件或通知。
  ///
  /// 失败语义与 cancel 一致：Backend 错误原样传播、snapshot 不变；
  /// 成功但提示失败时 snapshot 已更新为已提交记录。
  Future<JobSnapshot<TData, TProgress, TResult>> pause({
    int? expectedRevision,
  }) {
    _context.ensureOpen('job.pause');
    final JobRecord current = _rawRecord;
    if (current.status != JobStatus.active) {
      // 返回已失败的 Future——让调用方可以选择 await 或 try/catch，
      // 与 cancel/retry 等其他 mutation 的异步失败语义保持一致。
      return Future<JobSnapshot<TData, TProgress, TResult>>.error(
        LifecycleError(
          'Job',
          'pause',
          'Job ${current.key} cannot request pause from ${current.status.name}.',
        ),
      );
    }
    final int revision = expectedRevision ?? current.revision;
    final DateTime requestedAt = _context.now();
    final RequestJobControlRequest request = RequestJobControlRequest(
      key: current.key,
      action: JobControlAction.pause,
      expectedRevision: revision,
      requestedAt: requestedAt,
    );
    return _context
        .commit<JobRecord>(
          operation: 'job.pause',
          write: () => _context.backend.requestJobControl(request),
          onCommitted: _replaceSnapshot,
        )
        .then((record) => _snapshot);
  }

  /// 恢复被单独暂停的 Job。
  ///
  /// [expectedRevision]：可选乐观并发前提；为 null 时使用本句柄最近 snapshot 的 revision。
  ///
  /// 语义：只用于 paused Job，通过 `transitionJob(waiting)` 恢复，`availableAt = occurredAt`；
  /// 是否合法由 Backend 判定，非 paused 的 snapshot 会得到 [InvalidJobTransition]。
  /// 本方法不涉及 Queue 级暂停（Queue 恢复不会恢复单独暂停的 Job）。
  ///
  /// 失败语义与其他 mutation 一致：Backend 错误零通知且 snapshot 不变；
  /// 成功但提示失败时 snapshot 已更新为已提交记录。
  Future<JobSnapshot<TData, TProgress, TResult>> resume({
    int? expectedRevision,
  }) {
    _context.ensureOpen('job.resume');
    final JobRecord current = _rawRecord;
    final int revision = expectedRevision ?? current.revision;
    final DateTime occurredAt = _context.now();
    final TransitionJobRequest request = TransitionJobRequest(
      key: current.key,
      targetStatus: JobStatus.waiting,
      expectedRevision: revision,
      occurredAt: occurredAt,
      availableAt: occurredAt,
    );
    return _context
        .commit<JobRecord>(
          operation: 'job.resume',
          write: () => _context.backend.transitionJob(request),
          onCommitted: _replaceSnapshot,
        )
        .then((record) => _snapshot);
  }

  /// 删除本 Job（及 Backend 判定的整棵目标子树）。
  ///
  /// [expectedRevision]：可选乐观并发前提；为 null 时使用本句柄最近 snapshot 的 revision。
  ///
  /// 语义：只调用冻结的 `removeJob`，由 Backend 原子完成子树删除、active 拒绝、
  /// 日志与关系清理以及祖先推进；Queue 模块不读取 Flow、不拆分删除、不做部分删除。
  ///
  /// 返回 Backend 给出的完整被删除 key 列表；通知只使用 Backend 返回的通知集合，
  /// 绝不根据被删除 key 或父子关系推导通知目标。
  ///
  /// 副作用：不修改本句柄 snapshot（删除结果不是 Job 记录）；删除后继续 `refresh()`
  /// 或 mutation 仍会访问 Backend 并得到 [JobNotFound] 或 Backend 判定结果。
  Future<List<JobKey>> remove({int? expectedRevision}) {
    _context.ensureOpen('job.remove');
    final JobRecord current = _rawRecord;
    final int revision = expectedRevision ?? current.revision;
    final RemoveJobRequest request = RemoveJobRequest(
      key: current.key,
      expectedRevision: revision,
      removedAt: _context.now(),
    );
    return _context.commit<List<JobKey>>(
      operation: 'job.remove',
      write: () => _context.backend.removeJob(request),
    );
  }

  /// 查询本 Job 的执行日志（原始 JSON-like 非泛型模型）。
  ///
  /// 日志附加数据是独立于任务 payload 的原始 JSON-like Object?，不复用任务 Codec。
  ///
  /// [attempt]：按尝试次数过滤，null 表示不过滤。
  /// [levels]：日志级别过滤集合，空集合表示不过滤。
  /// [channels]：日志通道过滤集合，空集合表示不过滤。
  /// [afterSequence]：独占日志游标，只返回 sequence > afterSequence 的日志。
  /// [limit]：每页数量，由冻结的 [JobLogQuery] 校验（1..1000），非法值抛 [ArgumentError]。
  ///
  /// 语义：普通日志查询直接使用冻结的 `queryJobLogs`，过滤器与游标原样传递，
  /// 返回 Backend 给出的日志页；不修改本句柄 snapshot，不产生 Signal 提示。
  /// 失败语义：Backend 错误原样传播，snapshot 不变。
  Future<JobLogPage> queryLogs({
    int? attempt,
    Set<JobLogLevel> levels = const <JobLogLevel>{},
    Set<JobLogChannel> channels = const <JobLogChannel>{},
    int? afterSequence,
    int limit = 100,
  }) {
    _context.ensureOpen('job.queryLogs');
    final JobLogQuery logQuery = JobLogQuery(
      key: _snapshot.key,
      attempt: attempt,
      levels: levels,
      channels: channels,
      afterSequence: afterSequence,
      limit: limit,
    );
    return _context.run(
      'job.queryLogs',
      () => _context.backend.queryJobLogs(logQuery),
    );
  }

  /// 清理本 Job 的执行日志。
  ///
  /// [keepLatest]：保留最近 N 条日志，`0` 表示全部清空；负数由冻结的
  /// [ClearJobLogsRequest] 拒绝（断言）。
  ///
  /// 语义：只使用冻结的 `clearJobLogs`，其请求 DTO **没有** revision 字段，
  /// 因此本方法不伪造 `expectedRevision`；日志清理也不改变 Job 的 revision、status
  /// 或本句柄 snapshot（日志不是 Job 状态的一部分）。
  ///
  /// 返回 Backend 给出的删除数量；通知只使用 Backend 返回的通知集合。
  /// 失败语义：Backend 错误原样传播且零通知；成功但提示失败时抛
  /// [QueueNotificationFailure]，其 `committedValue` 为已提交的删除数量。
  Future<int> clearLogs({int keepLatest = 0}) {
    _context.ensureOpen('job.clearLogs');
    final ClearJobLogsRequest request = ClearJobLogsRequest(
      key: _snapshot.key,
      keepLatest: keepLatest,
    );
    return _context.commit<int>(
      operation: 'job.clearLogs',
      write: () => _context.backend.clearJobLogs(request),
    );
  }

  /// mutation 成功后替换本句柄的最近 snapshot。
  ///
  /// 只在 Backend 已提交对应事实后调用，绝不用输入参数伪造 revision、状态、
  /// 控制请求、日志或删除结果。替换同时更新 rawRecord 和解码后的 JobSnapshot。
  void _replaceSnapshot(JobRecord record) {
    _rawRecord = record;
    _snapshot = JobSnapshot<TData, TProgress, TResult>(
      rawRecord: record,
      codecs: _codecs,
    );
  }
}

/// 模块内部 Job 工厂：用 Backend 返回的原始记录创建句柄。
///
/// 该顶层函数不是 `queue/index.dart` 的导出内容（出口只 `show Job`），
/// 因此模块外无法凭空构造 Job 句柄，必须通过 Queue 的生产或查询 API 获得。
Job<TData, TProgress, TResult> createJobHandle<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
>({
  required QueueContext queueContext,
  required JobRecord rawRecord,
  required JobPayloadCodecs<TData, TProgress, TResult> codecs,
}) => Job<TData, TProgress, TResult>._(
  queueContext: queueContext,
  rawRecord: rawRecord,
  codecs: codecs,
);
