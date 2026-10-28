import '../backend/index.dart';
import '../core/index.dart';
import '../signal/index.dart';
import 'job.dart';
import 'queue_context.dart';
import 'queue_models.dart';

/// 固定 `queueName` 的 Queue 生产与管理句柄。
///
/// Queue 只是编排层：它把阶段 01 冻结的 [QueueBackend] 与 [QueueSignal] 组合成
/// 生产、查询、暂停、恢复与 Job 句柄入口，自身不保存 Job 事实、不 claim Job、
/// 不执行 Processor、不维护 Job 集合，也不实现 Flow。
///
/// 构造器假设注入的 [QueueBackend] 已经 open：它既不调用 `open()`，
/// 也不探测或恢复 Backend 生命周期；只有 [QueueOptions.ownsBackend]
/// 与 [QueueOptions.ownsSignal] 为 true 时，[close] 才会关闭对应资源。
///
/// [codecs] 用于把业务载荷（TData）编码为 Backend 原始 JSON-like，
/// 以及把 Backend 事实解码为业务视图。省略时使用默认 JSON-like Codec
/// （透传，仅 freezeJsonLike 校验）。
final class Queue<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  /// 构造 Queue 句柄。
  ///
  /// [queueName]：固定队列名，必须非空白；合法名称原样保留，不做 trim 或归一化。
  /// [backend]：已 open 的 Backend。
  /// [signal]：提示通道；提示失败不会回滚 Backend 事实。
  /// [codecs]：业务 ↔ JSON-like 编解码器；省略时使用默认 JSON-like 透传。
  /// [idGenerator]：缺省 jobId 的唯一来源；显式 jobId 不会触发它。
  ///   省略时回退到 [defaultIdGenerator]。
  /// [now]：所有请求时间戳（createdAt、availableAt、occurredAt 等）的唯一来源。
  ///   省略时回退到 [defaultNow]。
  /// [options]：Queue 级配置；为 null 时使用 `QueueOptions()` 默认值。
  ///
  /// 构造期同步失败的情形：
  /// - `queueName` 为空白 -> [ArgumentError]；
  /// - distributed Backend 与 process Signal 组合 -> `UnsupportedCapability('distributedSignal')`。
  ///
  /// 副作用：无。构造不产生任何 Backend 调用，也不 open/close 任何资源。
  Queue({
    required this.queueName,
    required QueueBackend backend,
    required QueueSignal signal,
    JobPayloadCodecs<TData, TProgress, TResult>? codecs,
    IdGenerator idGenerator = defaultIdGenerator,
    Now now = defaultNow,
    QueueOptions? options,
  }) : _context = QueueContext(
         queueName: queueName,
         backend: backend,
         signal: signal,
         idGenerator: idGenerator,
         now: now,
         options: options,
       ),
       _codecs =
           codecs ?? JobPayloadCodecs<TData, TProgress, TResult>.jsonLike() {
    if (queueName.trim().isEmpty) {
      throw ArgumentError.value(queueName, 'queueName', 'must not be blank');
    }
    // distributed Backend 只能与 distributed Signal 组合：跨进程提示无法由
    // 进程内 Signal 表达，静默降级会让消费者以为提示已送达。
    if (backend.capabilities.profile == BackendProfile.distributed &&
        signal.scope == SignalScope.process) {
      throw UnsupportedCapability(
        'distributedSignal',
        'a distributed backend cannot be paired with a process scoped signal',
      );
    }
  }

  /// 本句柄固定的队列名，原样保留调用方传入的值。
  final String queueName;

  final QueueContext _context;

  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;

  /// 本句柄的 Queue 级配置。
  QueueOptions get options => _context.options;

  /// 本句柄是否已进入关闭流程；为 true 后所有方法都会抛 [LifecycleError]。
  bool get isClosed => _context.isClosed;

  /// 生产单个 Job。
  ///
  /// [name]：Job 名称，必须非空白。
  /// [data]：业务载荷；Queue 会通过 [codecs.data] 编码后写入 Backend。
  /// [jobId]：显式 Job ID；为 null 时由注入的 `IdGenerator` 生成。
  /// [options]：Job 级选项，原样透传；[JobOptions.delay] 只影响 `availableAt`，
  /// 不改变 JobOptions 自身语义。
  ///
  /// 返回以 Backend 已提交记录为初始 snapshot 的 Job 句柄。
  ///
  /// 失败语义（全部发生在任何 Backend 写入之前）：
  /// - 句柄已关闭 -> [LifecycleError]；
  /// - 输入非法或生成的 jobId 为空白 -> [ArgumentError]；
  /// - Codec 编码失败 -> [PayloadCodecException]；
  /// - 需要 delay 但 Backend 不支持 `delayedJobs`，或 priority > 0 但
  ///   不支持 `priorities` -> `UnsupportedCapability`（绝不静默移除）。
  /// Backend 写入失败时不会有任何 Signal 通知；写入成功但提示失败时抛
  /// [QueueNotificationFailure]，其 `committedValue` 即已提交的 [JobRecord]。
  Future<Job<TData, TProgress, TResult>> add(
    String name, {
    required TData? data,
    String? jobId,
    JobOptions? options,
  }) {
    _context.ensureOpen('add');
    // 输入验证、capability preflight、ID 生成与 DTO 构造全部同步完成，
    // 因此坏输入/能力不足/ID 失败都不会留下 policy-only 或部分写入。
    final QueueJobDraft<TData> draft = QueueJobDraft<TData>(
      name: name,
      data: data,
      jobId: jobId,
      options: options,
    );
    final AddJobRequest request = _prepareAddRequest(draft);
    return _context.run('add', () async {
      await _context.ensureDefaultPolicy(operation: 'add');
      final JobRecord record = await _context.commit<JobRecord>(
        operation: 'add',
        write: () => _context.backend.addJob(request),
      );
      // 句柄只用 Backend 返回的已提交记录构造，绝不用输入 JobSpec 伪造状态与 revision。
      return createJobHandle<TData, TProgress, TResult>(
        queueContext: _context,
        rawRecord: record,
        codecs: _codecs,
      );
    });
  }

  /// 原子批量生产 Job。
  ///
  /// [jobs]：Job 草稿列表；空列表抛 [ArgumentError]。输入被防御性复制，
  /// 调用方之后修改原列表不影响本次批量。
  ///
  /// 语义：整批只调用一次 `QueueBackend.addJobs`，任一输入、重复 JobKey、
  /// ID 生成、Codec 编码、capability 或 Backend 错误都使整批抛出，绝不退化为循环 `addJob`
  /// 而产生部分成功。所有 Job 使用同一个 `createdAt`，各 Job 的 delay 由 Backend
  /// 依据各自的 `JobOptions.delay` 计算 `availableAt`。
  ///
  /// 返回顺序与输入顺序一致、元素数量与 Backend 返回记录一致的 Job 句柄列表。
  /// 失败语义与副作用与 [add] 相同（同样会先完成 default policy 初始化）。
  Future<List<Job<TData, TProgress, TResult>>> addBulk(
    List<QueueJobDraft<TData>> jobs,
  ) {
    _context.ensureOpen('addBulk');
    final List<QueueJobDraft<TData>> drafts = List<QueueJobDraft<TData>>.of(
      jobs,
    );
    if (drafts.isEmpty) {
      throw ArgumentError.value(jobs, 'jobs', 'must not be empty');
    }
    final AddJobsRequest request = _prepareAddJobsRequest(drafts);
    return _context.run('addBulk', () async {
      await _context.ensureDefaultPolicy(operation: 'addBulk');
      final List<JobRecord> records = await _context.commit<List<JobRecord>>(
        operation: 'addBulk',
        write: () => _context.backend.addJobs(request),
      );
      return List<Job<TData, TProgress, TResult>>.unmodifiable(
        records.map(
          (JobRecord record) => createJobHandle<TData, TProgress, TResult>(
            queueContext: _context,
            rawRecord: record,
            codecs: _codecs,
          ),
        ),
      );
    });
  }

  /// 按 jobId 查询单个 Job 句柄。
  ///
  /// [jobId]：必须非空白，且原样保留——Queue 只负责补上本句柄固定的 `queueName`
  /// 组成复合 [JobKey]，不做裁剪、前缀或归一化。
  ///
  /// 返回以 Backend 当前记录为初始 snapshot 的新句柄；Backend 中不存在时返回 null
  /// （不是错误，也不伪造 removed 状态）。每次调用都返回独立实例。
  ///
  /// 副作用：只读取 Backend，不产生 Signal 提示，也不安装默认 policy。
  Future<Job<TData, TProgress, TResult>?> getJob(String jobId) {
    _context.ensureOpen('getJob');
    if (jobId.trim().isEmpty) {
      throw ArgumentError.value(jobId, 'jobId', 'must not be blank');
    }
    final JobKey jobKey = JobKey(queueName: queueName, jobId: jobId);
    return _context.run('getJob', () async {
      final JobRecord? record = await _context.backend.getJob(jobKey);
      return record == null
          ? null
          : createJobHandle<TData, TProgress, TResult>(
              queueContext: _context,
              rawRecord: record,
              codecs: _codecs,
            );
    });
  }

  /// 分页查询本 Queue 的 Job。
  ///
  /// [statuses]：状态过滤集合，空集合表示不过滤。
  /// [cursor]：Backend 定义的下一页游标，原样透传。
  /// [limit]：每页数量，由冻结的 [JobQuery] 校验（1..1000），非法值抛 [ArgumentError]。
  ///
  /// 查询永远被限定在本句柄的 `queueName` 内，调用方无法跨 Queue 查询。
  /// 返回顺序与 Backend 返回顺序一致：Queue 不按 ID 重排、不过滤、不合并。
  ///
  /// 副作用：只读取 Backend，不产生 Signal 提示，也不安装默认 policy。
  Future<QueueJobPage<TData, TProgress, TResult>> query({
    Set<JobStatus> statuses = const <JobStatus>{},
    String? cursor,
    int limit = 100,
  }) {
    _context.ensureOpen('query');
    // 构造冻结 DTO 负责 limit 校验；这里同步失败，不产生任何 Backend 访问。
    final JobQuery jobQuery = JobQuery(
      queueName: queueName,
      statuses: statuses,
      cursor: cursor,
      limit: limit,
    );
    return _context.run('query', () async {
      final JobPage page = await _context.backend.queryJobs(jobQuery);
      return QueueJobPage<TData, TProgress, TResult>(
        items: page.items
            .map(
              (JobRecord record) => createJobHandle<TData, TProgress, TResult>(
                queueContext: _context,
                rawRecord: record,
                codecs: _codecs,
              ),
            )
            .toList(),
        nextCursor: page.nextCursor,
      );
    });
  }

  /// 读取本 Queue 的持久化状态。
  ///
  /// 返回 Backend 中的 queue state（paused、policy、revision）。
  /// 这是纯只读操作：不安装默认 policy、不修改任何 Job、不产生 Signal 提示。
  Future<QueueState> getState() =>
      _context.run('getState', () => _context.backend.getQueueState(queueName));

  /// 暂停本 Queue 的 Job 领取。
  ///
  /// [expectedRevision]：可选乐观并发前提；为 null 时使用 Backend 最新 revision。
  ///
  /// 语义：只写入 Backend 的 queue state，**不**枚举、不暂停、不 transition 任何 Job；
  /// 已经 active 的 Job 不受影响。Backend 判定 state 未变化时返回当前状态且零通知。
  ///
  /// 失败语义：
  /// - 句柄已关闭 -> [LifecycleError]；
  /// - 显式 revision 已过期 -> [QueueRevisionConflict]，且发生在任何 policy 写入之前；
  /// - Backend 写入失败 -> 原样传播，不产生 Signal 提示；
  /// - 写入成功但提示失败 -> [QueueNotificationFailure]，`committedValue` 为已提交的 [QueueState]。
  Future<QueueState> pause({int? expectedRevision}) => _setPaused(
    paused: true,
    expectedRevision: expectedRevision,
    operation: 'pause',
  );

  /// 恢复本 Queue 的 Job 领取。
  ///
  /// [expectedRevision]：语义与 [pause] 相同。
  ///
  /// 语义：只写入 Backend 的 queue state；不会恢复那些被单独暂停（paused）的 Job，
  /// 也不会修改任何 Job snapshot。Backend 判定 state 未变化时返回当前状态且零通知。
  /// 失败语义与 [pause] 一致，[QueueNotificationFailure.operation] 为 `resume`。
  Future<QueueState> resume({int? expectedRevision}) => _setPaused(
    paused: false,
    expectedRevision: expectedRevision,
    operation: 'resume',
  );

  /// [pause] 与 [resume] 的共用实现。
  ///
  /// 固定顺序：先冻结 `occurredAt` 与调用方 revision，再走 default policy 初始化
  /// （显式旧 revision 会在此处以 [QueueRevisionConflict] 失败且不写 policy），
  /// 最后用 policy 写返回的最新 revision 构造并提交 [SetQueuePausedRequest]。
  /// 之所以不在 policy 写之前构造主请求：policy 写会推进 Queue revision，
  /// 沿用旧 revision 会让主写必然冲突。
  Future<QueueState> _setPaused({
    required bool paused,
    required int? expectedRevision,
    required String operation,
  }) {
    _context.ensureOpen(operation);
    final DateTime occurredAt = _context.now();
    return _context.run(operation, () async {
      final QueueState state = await _context.ensureDefaultPolicy(
        operation: operation,
        expectedRevision: expectedRevision,
      );
      final SetQueuePausedRequest request = SetQueuePausedRequest(
        queueName: queueName,
        paused: paused,
        expectedRevision: state.revision,
        occurredAt: occurredAt,
      );
      return _context.commit<QueueState>(
        operation: operation,
        write: () => _context.backend.setQueuePaused(request),
      );
    });
  }

  /// 校验单 Job 的 capability 并构造冻结 [AddJobRequest]。
  ///
  /// 只在确实需要 delay 或 priority 时才读取 `capabilities`，避免无意义的
  /// 能力依赖；[JobOptions.delay] 与 `availableAt` 使用同一个 `createdAt` 基准。
  /// 业务 data 通过 codecs.data 编码为 JSON-like。
  AddJobRequest _prepareAddRequest(QueueJobDraft<TData> draft) {
    _preflightJobOptions(draft.options, operation: 'add');
    final String resolvedJobId = _resolveJobId(draft.jobId);
    final DateTime createdAt = _context.now();
    // 通过 Codec 编码业务数据为 JSON-like
    final Object? encodedData = _codecs.data.encodeNullable(draft.data);
    return AddJobRequest(
      job: JobSpec(
        key: JobKey(queueName: queueName, jobId: resolvedJobId),
        name: draft.name,
        data: encodedData,
        options: draft.options,
      ),
      createdAt: createdAt,
      availableAt: createdAt.add(draft.options.delay),
    );
  }

  /// 校验整批 draft 的 capability 并构造冻结 [AddJobsRequest]。
  ///
  /// 先按输入顺序补齐缺省 ID 再交给 DTO 做重复校验，因此显式重复、生成重复
  /// 以及两者交叉重复都会在任何 Backend 调用之前被拒绝。
  /// 业务 data 通过 codecs.data 编码为 JSON-like。
  AddJobsRequest _prepareAddJobsRequest(List<QueueJobDraft<TData>> drafts) {
    final List<JobSpec> specs = <JobSpec>[];
    for (final QueueJobDraft<TData> draft in drafts) {
      _preflightJobOptions(draft.options, operation: 'addBulk');
      final String resolvedJobId = _resolveJobId(draft.jobId);
      final Object? encodedData = _codecs.data.encodeNullable(draft.data);
      specs.add(
        JobSpec(
          key: JobKey(queueName: queueName, jobId: resolvedJobId),
          name: draft.name,
          data: encodedData,
          options: draft.options,
        ),
      );
    }
    return AddJobsRequest(jobs: specs, createdAt: _context.now());
  }

  /// 解析 Job ID：显式值原样使用，缺省值只来自注入的 `IdGenerator`。
  String _resolveJobId(String? explicitJobId) {
    final String resolved = explicitJobId ?? _context.idGenerator();
    if (resolved.trim().isEmpty) {
      throw ArgumentError.value(resolved, 'jobId', 'job id must not be blank');
    }
    return resolved;
  }

  /// 依据 Backend 能力声明 preflight Job 的 delay 与 priority 需求。
  ///
  /// 能力不足时抛阶段 01 的 [UnsupportedCapability]：绝不静默移除 delay
  /// 或把优先级降级为普通 Job。
  void _preflightJobOptions(JobOptions options, {required String operation}) {
    final bool needsDelay = options.delay > Duration.zero;
    final bool needsPriority = options.priority > 0;
    if (!needsDelay && !needsPriority) {
      return;
    }
    final BackendCapabilities capabilities = _context.backend.capabilities;
    if (needsDelay && !capabilities.delayedJobs) {
      throw UnsupportedCapability(
        'delayedJobs',
        '$operation requires a backend that supports delayed jobs',
      );
    }
    if (needsPriority && !capabilities.priorities) {
      throw UnsupportedCapability(
        'priorities',
        '$operation requires a backend that supports priorities',
      );
    }
  }

  /// 关闭本 Queue 句柄。
  ///
  /// 顺序与语义：先同步拒绝对新 Queue/Job 操作，再等待已开始的操作（含其全部通知）
  /// settle，最后只在 [QueueOptions.ownsBackend]/[QueueOptions.ownsSignal] 为 true 时
  /// 关闭对应资源。关闭不影响任何 Job 事实——不删除、不取消、不暂停、不停止 Job。
  ///
  /// 幂等且并发安全：所有并发/后续 close 共享同一个 close Future，
  /// 因此 owned 资源最多关闭一次，且首个关闭错误被一致地观察。
  Future<void> close() => _context.close();
}
