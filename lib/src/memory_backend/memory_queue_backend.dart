// ignore_for_file: curly_braces_in_flow_control_structures

import '../backend/index.dart';
import '../core/index.dart';
import 'memory_store.dart';
import 'memory_transaction.dart';

/// 单进程内存 Backend 实现。
///
/// 特点：
/// - BackendProfile: local
/// - Durability: volatile（进程重启数据丢失）
/// - 支持 delayedJobs、priorities、atomicFlows
/// - 事件投递: liveOnly（不支持回放）
/// - 不支持 distributed 租约机制（renewLease 直接抛 UnsupportedCapability）
///
/// 使用 copy-on-write 事务保证写入原子性；同进程内单线程执行，写入不可重入。
final class MemoryQueueBackend implements QueueBackend {
  MemoryQueueBackend({this.now = defaultNow, this.idGenerator = defaultIdGenerator});

  /// 时钟注入。
  final Now now;

  /// ID 生成器注入。
  final IdGenerator idGenerator;

  /// 能力声明（构造后不变）。
  final BackendCapabilities _capabilities = BackendCapabilities(
    profile: BackendProfile.local,
    durability: Durability.volatile,
    delayedJobs: true,
    priorities: true,
    atomicFlows: true,
    eventDelivery: EventDelivery.liveOnly,
  );

  /// 当前 committed store（每次写入用 copy-on-write 替换）。
  MemoryStore _store = MemoryStore();

  /// 是否已 open。
  bool _open = false;

  /// 是否在写入中（防止 reentrant write）。
  bool _inWrite = false;

  @override
  BackendCapabilities get capabilities => _capabilities;

  @override
  Future<void> open() async => _open = true;

  @override
  Future<void> close() async {
    // 关闭时清空 store，数据丢弃
    _store = MemoryStore();
    _open = false;
  }

  /// 同步门禁：Backend 未 open 时抛 LifecycleError。
  void _ensureOpen(String operation) {
    if (!_open)
      throw LifecycleError('MemoryQueueBackend', operation, 'Backend 已关闭');
  }

  /// 原子写入的统一入口。
  ///
  /// 保证：
  /// 1. 检查已 open
  /// 2. 保证写入不可重入（防止递归调用污染 store）
  /// 3. 通过 MemoryTransaction 执行 copy-on-write mutation
  Future<T> _write<T>(String operation, MemoryMutation<T> mutation) async {
    _ensureOpen(operation);
    if (_inWrite) throw StateError('内存 Backend 写入不可重入');
    _inWrite = true;
    try {
      final result = MemoryTransaction.write(
        _store,
        mutation,
        (draft) => _store = draft,
      );
      return result;
    } finally {
      _inWrite = false;
    }
  }

  /// 获取或创建指定队列的 QueueState（revision=0 表示未初始化）。
  QueueState _queue(MemoryStore store, String name) => store.queues.putIfAbsent(
    name,
    () => QueueState(
      queueName: name,
      paused: false,
      policy: QueuePolicy(),
      revision: 0,
    ),
  );

  /// 构造一个 changed=true 的 BackendWriteResult 快捷方法。
  BackendWriteResult<T> _result<T>(T value, Set<String> queues) =>
      BackendWriteResult(value: value, changed: true, queuesToNotify: queues);

  /// 用 JobSpec 创建一条持久化 JobRecord。
  JobRecord _makeRecord(
    MemoryStore store,
    JobSpec spec,
    DateTime createdAt,
    DateTime availableAt,
    JobStatus status,
  ) => JobRecord(
    schemaVersion: 1,
    key: spec.key,
    name: spec.name,
    data: spec.data,
    status: status,
    progress: 0,
    result: null,
    failure: null,
    revision: 1,
    attemptsMade: 0,
    stalledCount: 0,
    priority: spec.options.priority,
    availableAt: availableAt,
    options: spec.options,
    parentKey: store.parentByChild[spec.key],
    createdAt: createdAt,
    updatedAt: createdAt,
    startedAt: null,
    finishedAt: null,
    execution: null,
    controlRequest: null,
  );

  /// 在指定队列产出一条 Job 事件。
  ///
  /// 自动分配递增 position，追加到事件历史，并按队列策略裁剪旧事件。
  PositionedJobEvent _event(
    MemoryStore store,
    JobKey key,
    int revision,
    JobEventType type,
    Object? data,
  ) {
    final queue = key.queueName;
    // 为该队列分配下一个事件位置
    final position = (store.nextEventPositionByQueue[queue] ?? 0) + 1;
    store.nextEventPositionByQueue[queue] = position;
    final item = PositionedJobEvent(
      cursor: EventCursor(queueName: queue, position: position),
      event: JobEvent(
        eventId: idGenerator(),
        jobKey: key,
        revision: revision,
        type: type,
        occurredAt: now(),
        data: data,
      ),
    );
    (store.eventsByQueue[queue] ??= <PositionedJobEvent>[]).add(item);
    _trimEvents(store, queue);
    return item;
  }

  /// 按队列策略裁剪事件历史。
  ///
  /// 裁剪后更新 [store.trimmedEventPositionByQueue]，记录已被裁剪掉的
  /// 最大 position。这样即使事件列表为空，后续游标 gap 检测仍能正确工作。
  void _trimEvents(MemoryStore store, String queue) {
    final events = store.eventsByQueue[queue];
    if (events == null || events.isEmpty) return;
    final policy = _queue(store, queue).policy;
    // 记录裁剪前最早事件位置，用于更新下界
    final firstBefore = events.first.cursor.position;
    // 按 maxEventCount 裁剪最早事件
    while (policy.maxEventCount != null &&
        events.length > policy.maxEventCount!) {
      final removed = events.removeAt(0);
      _updateTrimmed(store, queue, removed.cursor.position);
    }
    // 按 eventRetention 裁剪超时事件
    if (policy.eventRetention != null) {
      final cutoff = now().subtract(policy.eventRetention!);
      events.removeWhere((item) {
        final removed = item.event.occurredAt.isBefore(cutoff);
        if (removed) _updateTrimmed(store, queue, item.cursor.position);
        return removed;
      });
    }
    // 如果整个列表被清空，确保下界至少更新到裁剪前最早位置
    if (events.isEmpty && firstBefore > 0) {
      _updateTrimmed(store, queue, firstBefore - 1);
    }
  }

  /// 更新某队列的裁剪下界：记录已被裁剪掉的最大事件 position。
  void _updateTrimmed(MemoryStore store, String queue, int position) {
    final current = store.trimmedEventPositionByQueue[queue] ?? 0;
    if (position > current) {
      store.trimmedEventPositionByQueue[queue] = position;
    }
  }

  /// 向 store 添加一条 Job 事实并产出 added 事件。
  void _addFact(MemoryStore store, JobRecord record, JobEventType type) {
    store.jobs[record.key] = record;
    _queue(store, record.key.queueName);
    _event(store, record.key, record.revision, type, null);
  }

  // ==================== QueueBackend 接口实现 ====================

  @override
  Future<BackendWriteResult<JobRecord>> addJob(AddJobRequest request) =>
      _write('addJob', (store) {
        if (store.jobs.containsKey(request.job.key))
          throw DuplicateJob(request.job.key, 'Job 已存在');
        final status = request.availableAt.isAfter(request.createdAt)
            ? JobStatus.delayed
            : JobStatus.waiting;
        final record = _makeRecord(
          store,
          request.job,
          request.createdAt,
          request.availableAt,
          status,
        );
        _addFact(store, record, JobEventType.added);
        return _result(record, <String>{record.key.queueName});
      });

  @override
  Future<BackendWriteResult<List<JobRecord>>> addJobs(AddJobsRequest request) =>
      _write('addJobs', (store) {
        // 先做全部重复检查，避免部分写入
        for (final job in request.jobs)
          if (store.jobs.containsKey(job.key))
            throw DuplicateJob(job.key, 'Job 已存在');
        final records = <JobRecord>[];
        for (final job in request.jobs) {
          final available = request.createdAt.add(job.options.delay);
          final record = _makeRecord(
            store,
            job,
            request.createdAt,
            available,
            available.isAfter(request.createdAt)
                ? JobStatus.delayed
                : JobStatus.waiting,
          );
          records.add(record);
          _addFact(store, record, JobEventType.added);
        }
        return _result(
          records,
          records.map((record) => record.key.queueName).toSet(),
        );
      });

  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(AddFlowRequest request) =>
      _write('addFlow', (store) {
        // 先做全部重复检查
        for (final job in request.flow.jobs)
          if (store.jobs.containsKey(job.key))
            throw DuplicateJob(job.key, 'Job 已存在');
        // 建立 Flow 边索引
        for (final edge in request.flow.edges) {
          (store.childrenByParent[edge.parent] ??= <FlowEdge>[]).add(edge);
          store.parentByChild[edge.child] = edge.parent;
        }
        final records = <JobRecord>[];
        for (final job in request.flow.jobs) {
          final available = request.createdAt.add(job.options.delay);
          // 有子节点的父 Job 初始状态为 waitingChildren
          final status = (store.childrenByParent[job.key]?.isNotEmpty ?? false)
              ? JobStatus.waitingChildren
              : (available.isAfter(request.createdAt)
                    ? JobStatus.delayed
                    : JobStatus.waiting);
          final record = _makeRecord(
            store,
            job,
            request.createdAt,
            available,
            status,
          );
          records.add(record);
          _addFact(store, record, JobEventType.added);
        }
        return _result(
          FlowSnapshot(
            root: request.flow.root,
            jobs: records,
            edges: request.flow.edges,
          ),
          records.map((record) => record.key.queueName).toSet(),
        );
      });

  @override
  Future<JobRecord?> getJob(JobKey key) async {
    _ensureOpen('getJob');
    return _store.jobs[key];
  }

  @override
  Future<JobPage> queryJobs(JobQuery query) async {
    _ensureOpen('queryJobs');
    final items =
        _store.jobs.values
            .where(
              (job) =>
                  job.key.queueName == query.queueName &&
                  (query.statuses.isEmpty ||
                      query.statuses.contains(job.status)),
            )
            .toList()
          // 先按 createdAt 升序，再按 JobKey 字典序排序
          ..sort(
            (a, b) => a.createdAt.compareTo(b.createdAt) != 0
                ? a.createdAt.compareTo(b.createdAt)
                : a.key.compareTo(b.key),
          );
    // 处理游标跳过已读取的项
    var start = 0;
    if (query.cursor != null) {
      final index = items.indexWhere(
        (job) =>
            '${job.createdAt.microsecondsSinceEpoch}:${job.key}' ==
            query.cursor,
      );
      start = index < 0 ? 0 : index + 1;
    }
    final selected = items.skip(start).take(query.limit).toList();
    return JobPage(
      items: List<JobRecord>.unmodifiable(selected),
      nextCursor: selected.length == query.limit
          ? '${selected.last.createdAt.microsecondsSinceEpoch}:${selected.last.key}'
          : null,
    );
  }

  @override
  Future<QueueState> getQueueState(String queueName) async {
    _ensureOpen('getQueueState');
    return _queue(_store, queueName);
  }

  /// 校验 Queue revision 乐观并发。
  void _checkQueueRevision(QueueState state, int expected) {
    if (state.revision != expected)
      throw QueueRevisionConflict(
        state.queueName,
        expected,
        state.revision,
        '队列 revision 过期',
      );
  }

  @override
  Future<BackendWriteResult<QueueState>> setQueuePaused(
    SetQueuePausedRequest request,
  ) => _write('setQueuePaused', (store) {
    final state = _queue(store, request.queueName);
    _checkQueueRevision(state, request.expectedRevision);
    // 幂等：状态未变时直接返回 changed=false
    if (state.paused == request.paused)
      return BackendWriteResult(
        value: state,
        changed: false,
        queuesToNotify: <String>{},
      );
    final updated = QueueState(
      queueName: state.queueName,
      paused: request.paused,
      policy: state.policy,
      revision: state.revision + 1,
    );
    store.queues[request.queueName] = updated;
    return _result(updated, <String>{request.queueName});
  });

  @override
  Future<BackendWriteResult<QueueState>> setQueuePolicy(
    SetQueuePolicyRequest request,
  ) => _write('setQueuePolicy', (store) {
    final state = _queue(store, request.queueName);
    _checkQueueRevision(state, request.expectedRevision);
    // 幂等：策略未变时直接返回 changed=false
    if (state.policy == request.policy)
      return BackendWriteResult(
        value: state,
        changed: false,
        queuesToNotify: <String>{},
      );
    final updated = QueueState(
      queueName: state.queueName,
      paused: state.paused,
      policy: request.policy,
      revision: state.revision + 1,
    );
    store.queues[request.queueName] = updated;
    return _result(updated, <String>{request.queueName});
  });

  @override
  Future<BackendWriteResult<ClaimedJob?>> claimJob(ClaimJobRequest request) =>
      _write('claimJob', (store) {
        // 队列暂停时不领取
        if (_queue(store, request.queueName).paused)
          return BackendWriteResult(
            value: null,
            changed: false,
            queuesToNotify: <String>{},
          );
        // 找出所有可领取的 Job（waiting 或到期的 delayed）
        final candidates = store.jobs.values
            .where(
              (job) =>
                  job.key.queueName == request.queueName &&
                  (job.status == JobStatus.waiting ||
                      job.status == JobStatus.delayed &&
                          !job.availableAt.isAfter(request.claimedAt)),
            )
            .toList();
        if (candidates.isEmpty)
          return BackendWriteResult(
            value: null,
            changed: false,
            queuesToNotify: <String>{},
          );
        // 排序：优先级 > availableAt > createdAt > JobKey
        candidates.sort((a, b) {
          final p = b.priority.compareTo(a.priority);
          if (p != 0) return p;
          final d = a.availableAt.compareTo(b.availableAt);
          if (d != 0) return d;
          final c = a.createdAt.compareTo(b.createdAt);
          return c != 0 ? c : a.key.compareTo(b.key);
        });
        var job = candidates.first;
        // delayed 先转正为 waiting
        if (job.status == JobStatus.delayed) {
          job = _replace(
            store,
            job,
            status: JobStatus.waiting,
            revision: job.revision + 1,
            updatedAt: request.claimedAt,
          );
          _event(store, job.key, job.revision, JobEventType.waiting, null);
        }
        // 创建执行凭证并把状态转为 active
        final credential = ExecutionCredential(executionId: idGenerator());
        final claimed = _replace(
          store,
          job,
          status: JobStatus.active,
          revision: job.revision + 1,
          attemptsMade: job.attemptsMade + 1,
          updatedAt: request.claimedAt,
          startedAt: request.claimedAt,
          execution: ExecutionOwner(
            workerId: request.workerId,
            credential: credential,
            startedAt: request.claimedAt,
          ),
        );
        _event(store, job.key, claimed.revision, JobEventType.active, null);
        return _result(
          ClaimedJob(record: claimed, credential: credential),
          <String>{job.key.queueName},
        );
      });

  @override
  Future<BackendWriteResult<ExecutionCredential>> renewLease(
    RenewLeaseRequest request,
  ) => Future.error(
    const UnsupportedCapability('leases', 'local Backend 没有租约机制'),
  );

  @override
  Future<BackendWriteResult<JobRecord>> updateJobProgress(
    UpdateJobProgressRequest request,
  ) => _updateActive(
    request.key,
    request.expectedRevision,
    request.credential,
    request.occurredAt,
    progress: request.progress,
    event: JobEventType.progress,
  );

  @override
  Future<BackendWriteResult<JobRecord>> updateJobData(
    UpdateJobDataRequest request,
  ) => _updateActive(
    request.key,
    request.expectedRevision,
    request.credential,
    request.occurredAt,
    data: request.data,
  );

  /// 更新 active Job 的内部实现：统一做 revision 和 credential 校验，
  /// 然后用新 progress/data 替换 JobRecord 的对应字段。
  ///
  /// 使用 sentinel 语义：只有实际传入的字段才会覆盖 record 原值，
  /// 未传入的字段保留。这样 execution、startedAt 等字段在多次调用间保持不变。
  Future<BackendWriteResult<JobRecord>> _updateActive(
    JobKey key,
    int expected,
    ExecutionCredential credential,
    DateTime time, {
    Object? progress = _preserve,
    Object? data = _preserve,
    JobEventType? event,
  }) => _write('updateActive', (store) {
    final job = _job(store, key);
    _checkRevision(job, expected);
    _checkCredential(job, credential);
    final updated = _replace(
      store,
      job,
      revision: job.revision + 1,
      updatedAt: time,
      progress: progress,
      data: data,
    );
    // 有指定事件类型时产出事件
    if (event != null) _event(store, key, updated.revision, event, progress);
    return _result(updated, <String>{key.queueName});
  });

  @override
  Future<BackendWriteResult<JobRecord>> transitionJob(
    TransitionJobRequest request,
  ) => _write('transitionJob', (store) {
    final job = _job(store, request.key);
    _checkRevision(job, request.expectedRevision);
    if (request.credential != null) _checkCredential(job, request.credential!);
    // 幂等：canceled 再 cancel 不变
    if (job.status == request.targetStatus &&
        request.targetStatus == JobStatus.canceled)
      return BackendWriteResult(
        value: job,
        changed: false,
        queuesToNotify: <String>{},
      );
    // Flow 依赖失败的 Job 不能手动 retry，必须由失败子 Job 触发
    if ((request.targetStatus == JobStatus.waiting ||
            request.targetStatus == JobStatus.delayed) &&
        job.status == JobStatus.failed &&
        job.failure?.type == 'FlowDependencyFailure')
      throw FlowDependencyFailure(
        job.key,
        _failedDependencies(store, job.key),
        '依赖失败必须由其失败子 Job 触发重试',
      );
    // 校验状态转换合法性
    if (!_canTransition(job.status, request.targetStatus))
      throw InvalidJobTransition(
        job.key,
        job.status,
        request.targetStatus,
        '非法的 Job 状态转换',
      );
    // failed 状态必须提供 failure
    if (request.targetStatus == JobStatus.failed && request.failure == null)
      throw ArgumentError('failed 转换必须提供 failure');
    // canceled retry 是一次新的执行开始：重置 progress，
    // 但保留 data、attemptsMade、stalledCount、startedAt 等业务数据和身份字段。
    final isCanceledRetry =
        job.status == JobStatus.canceled &&
        (request.targetStatus == JobStatus.waiting ||
            request.targetStatus == JobStatus.delayed);
    // 对 canceled retry 决定实际目标状态，按顺序检查三个条件：
    // 1. 存在 failed 或 canceled 子依赖 → FlowDependencyFailure 拒绝（依赖已断裂，
    //    必须先 retry 子 Job 恢复依赖关系，才能重新激活父 Job）；
    // 2. 存在未完成子（状态不是 completed/canceled）→ waitingChildren，
    //    等待子全部完成后再推进；
    // 3. 无子依赖或子全部完成 → 使用请求的目标状态 waiting/delayed。
    JobStatus actualTargetStatus = request.targetStatus;
    if (isCanceledRetry) {
      final childrenEdges =
          store.childrenByParent[request.key] ?? const <FlowEdge>[];
      if (childrenEdges.isNotEmpty) {
        final failed = _failedDependencies(store, request.key);
        if (failed.isNotEmpty)
          throw FlowDependencyFailure(
            request.key,
            failed,
            '依赖失败必须由其失败或已取消子 Job 触发重试',
          );
        final hasIncompleteChild = childrenEdges.any((edge) {
          final s = store.jobs[edge.child]?.status;
          return s != null &&
              s != JobStatus.completed &&
              s != JobStatus.canceled;
        });
        if (hasIncompleteChild) actualTargetStatus = JobStatus.waitingChildren;
      }
    }
    // 构造更新后的 JobRecord
    final updated = _replace(
      store,
      job,
      status: actualTargetStatus,
      revision: job.revision + 1,
      updatedAt: request.occurredAt,
      progress: isCanceledRetry ? null : _preserve,
      result: request.result ?? job.result,
      failure: actualTargetStatus == JobStatus.failed ? request.failure : null,
      availableAt: request.availableAt ?? job.availableAt,
      finishedAt:
          actualTargetStatus == JobStatus.completed ||
              actualTargetStatus == JobStatus.failed ||
              actualTargetStatus == JobStatus.canceled
          ? request.occurredAt
          : null,
      execution: actualTargetStatus == JobStatus.active ? job.execution : null,
      controlRequest: null,
    );
    // 产出事件
    _event(
      store,
      job.key,
      updated.revision,
      _eventFor(actualTargetStatus, job.status),
      null,
    );
    // 自动附加错误日志
    if (request.automaticErrorLog != null)
      _appendLog(
        store,
        request.key,
        job.execution!.credential.executionId,
        request.automaticErrorLog!,
        request.occurredAt,
        job.attemptsMade,
      );
    // 确定通知集合：Flow 终态需要通知父级重新计算；
    // canceled 子 Job retry 同样需要重新计算祖先，以便依赖失败的父 Job 恢复。
    final queues = <String>{job.key.queueName};
    if (request.targetStatus == JobStatus.completed ||
        request.targetStatus == JobStatus.failed ||
        request.targetStatus == JobStatus.canceled ||
        (((job.status == JobStatus.failed ||
                job.status == JobStatus.canceled) &&
            (request.targetStatus == JobStatus.waiting ||
                request.targetStatus == JobStatus.delayed)))) {
      queues.addAll(_recomputeAncestors(store, job.key, request.occurredAt));
    }
    return _result(updated, queues);
  });

  /// Backend 冻结的状态机：定义每种状态允许转移到哪些目标状态。
  ///
  /// 注意：canceled → waitingChildren 这条边不在此处显式声明——它由
  /// [transitionJob] 内部在 canceled retry 时根据 Flow 依赖关系覆盖目标状态得到，
  /// 此处只列出对 [waiting] / [delayed] 的显式授权。
  bool _canTransition(JobStatus from, JobStatus to) => switch (from) {
    JobStatus.active =>
      to == JobStatus.completed ||
          to == JobStatus.waiting ||
          to == JobStatus.delayed ||
          to == JobStatus.failed ||
          to == JobStatus.paused ||
          to == JobStatus.canceled,
    JobStatus.paused => to == JobStatus.waiting || to == JobStatus.canceled,
    JobStatus.failed ||
    JobStatus.canceled => to == JobStatus.waiting || to == JobStatus.delayed,
    JobStatus.waiting ||
    JobStatus.delayed ||
    JobStatus.waitingChildren => to == JobStatus.canceled,
    JobStatus.completed => false,
  };

  /// 根据目标状态和前一状态确定应产出的事件类型。
  JobEventType _eventFor(JobStatus status, JobStatus previous) =>
      status == JobStatus.waiting && previous == JobStatus.paused
      ? JobEventType.resumed
      : switch (status) {
          JobStatus.waiting => JobEventType.waiting,
          JobStatus.delayed => JobEventType.delayed,
          JobStatus.active => JobEventType.active,
          JobStatus.paused => JobEventType.paused,
          JobStatus.completed => JobEventType.completed,
          JobStatus.failed => JobEventType.failed,
          JobStatus.canceled => JobEventType.canceled,
          JobStatus.waitingChildren => JobEventType.waiting,
        };

  @override
  Future<BackendWriteResult<JobRecord>> requestJobControl(
    RequestJobControlRequest request,
  ) => _write('requestJobControl', (store) {
    final job = _job(store, request.key);
    _checkRevision(job, request.expectedRevision);

    final existing = job.controlRequest;
    final newAction = request.action;

    // 相同 action：幂等返回 changed=false，revision 不变，空通知
    if (existing != null && existing.action == newAction) {
      return BackendWriteResult(
        value: job,
        changed: false,
        queuesToNotify: <String>{},
      );
    }

    // pause 不能覆盖 cancel（阻止降级）
    if (existing != null &&
        existing.action == JobControlAction.cancel &&
        newAction == JobControlAction.pause) {
      return BackendWriteResult(
        value: job,
        changed: false,
        queuesToNotify: <String>{},
      );
    }
    // cancel 可以覆盖 pause（升级）→ 继续执行更新

    final updated = _replace(
      store,
      job,
      revision: job.revision + 1,
      updatedAt: request.requestedAt,
      controlRequest: JobControlRequest(
        action: request.action,
        requestedAt: request.requestedAt,
      ),
    );
    return _result(updated, <String>{job.key.queueName});
  });

  @override
  Future<BackendWriteResult<List<JobRecord>>> recoverStalledJobs(
    RecoverStalledJobsRequest request,
  ) => _write('recoverStalledJobs', (store) {
    // 筛选 stalled 条件：active、同队列、可选匹配 workerId、可选匹配 executionIds
    final jobs =
        store.jobs.values
            .where(
              (job) =>
                  job.key.queueName == request.queueName &&
                  job.status == JobStatus.active &&
                  (request.workerId == null ||
                      job.execution?.workerId == request.workerId) &&
                  (request.executionIds == null ||
                      request.executionIds!.contains(
                        job.execution?.credential.executionId,
                      )),
            )
            .toList()
          ..sort((a, b) => a.key.compareTo(b.key));
    if (jobs.isEmpty)
      return BackendWriteResult(
        value: <JobRecord>[],
        changed: false,
        queuesToNotify: <String>{},
      );
    final result = <JobRecord>[];
    final queues = <String>{request.queueName};
    for (final job in jobs) {
      final stalled = job.stalledCount + 1;
      // 超过 maxStalledCount 则终态 failed，否则回到 waiting
      final terminal = stalled > job.options.maxStalledCount;
      final updated = _replace(
        store,
        job,
        status: terminal ? JobStatus.failed : JobStatus.waiting,
        stalledCount: stalled,
        revision: job.revision + 1,
        updatedAt: request.now,
        execution: null,
        controlRequest: null,
        failure: terminal
            ? JobFailure(
                type: 'StalledLimitExceeded',
                message: 'stalled limit exceeded',
                failedAt: request.now,
              )
            : null,
        finishedAt: terminal ? request.now : null,
      );
      _event(store, job.key, updated.revision, JobEventType.stalled, null);
      // 终态额外产出 failed 事件
      _event(
        store,
        job.key,
        updated.revision,
        terminal ? JobEventType.failed : JobEventType.waiting,
        null,
      );
      result.add(updated);
      if (terminal)
        queues.addAll(_recomputeAncestors(store, job.key, request.now));
    }
    return _result(result, queues);
  });

  @override
  Future<BackendWriteResult<List<JobKey>>> removeJob(
    RemoveJobRequest request,
  ) => _write('removeJob', (store) {
    final root = _job(store, request.key);
    _checkRevision(root, request.expectedRevision);
    // BFS 收集整棵子树的所有 JobKey
    final keys = <JobKey>[request.key];
    for (var i = 0; i < keys.length; i++)
      keys.addAll(
        store.childrenByParent[keys[i]]?.map((e) => e.child) ??
            const <JobKey>[],
      );
    // 拒绝删除包含 active Job 的子树
    for (final key in keys) {
      final job = store.jobs[key]!;
      if (job.status == JobStatus.active)
        throw InvalidJobTransition(
          key,
          job.status,
          JobStatus.canceled,
          '不能删除包含 active Job 的子树',
        );
    }
    final externalParent = store.parentByChild[request.key];
    final queues = <String>{};
    // 逐个清理：产出 removed 事件、删除 Job、清理日志和 Flow 索引
    for (final key in keys) {
      final job = store.jobs[key]!;
      queues.add(key.queueName);
      _event(store, key, job.revision + 1, JobEventType.removed, null);
      store.jobs.remove(key);
      store.logsByJob.remove(key);
      store.nextLogSequenceByJob.remove(key);
      // 清理 Flow 索引
      final parent = store.parentByChild.remove(key);
      if (parent != null)
        store.childrenByParent[parent]?.removeWhere((e) => e.child == key);
      store.childrenByParent.remove(key);
    }
    // 如果被删除 Job 有外部父节点，触发父节点依赖失败
    if (externalParent != null && store.jobs[externalParent] != null)
      queues.addAll(
        _failDependency(store, externalParent, <JobKey>[
          request.key,
        ], request.removedAt),
      );
    return _result(keys, queues);
  });

  @override
  Future<BackendWriteResult<JobLog>> appendJobLog(
    AppendJobLogRequest request,
  ) => _write('appendJobLog', (store) {
    final job = _job(store, request.key);
    _checkCredential(job, request.credential);
    return _result(
      _appendLog(
        store,
        request.key,
        request.credential.executionId,
        request.log,
        request.occurredAt,
        request.attempt,
      ),
      <String>{request.key.queueName},
    );
  });

  /// 内部日志追加：分配 sequence、写入日志列表、按策略裁剪。
  JobLog _appendLog(
    MemoryStore store,
    JobKey key,
    String executionId,
    JobLogDraft draft,
    DateTime time,
    int attempt,
  ) {
    final sequence = (store.nextLogSequenceByJob[key] ?? 0) + 1;
    store.nextLogSequenceByJob[key] = sequence;
    final log = JobLog(
      logId: idGenerator(),
      jobKey: key,
      sequence: sequence,
      attempt: attempt,
      executionId: executionId,
      level: draft.level,
      channel: draft.channel,
      message: draft.message,
      data: draft.data,
      occurredAt: time,
    );
    final logs = (store.logsByJob[key] ??= <JobLog>[])..add(log);
    // 按 Job 级或 Queue 级策略裁剪日志
    final job = store.jobs[key]!;
    final retention =
        job.options.logRetention ??
        _queue(store, key.queueName).policy.defaultJobLogRetention;
    if (retention != null && retention > 0)
      while (logs.length > retention) logs.removeAt(0);
    return log;
  }

  @override
  Future<JobLogPage> queryJobLogs(JobLogQuery query) async {
    _ensureOpen('queryJobLogs');
    final source = _store.logsByJob[query.key] ?? const <JobLog>[];
    final items = source
        .where(
          (log) =>
              (query.attempt == null || log.attempt == query.attempt) &&
              (query.levels.isEmpty || query.levels.contains(log.level)) &&
              (query.channels.isEmpty ||
                  query.channels.contains(log.channel)) &&
              (query.afterSequence == null ||
                  log.sequence > query.afterSequence!),
        )
        .take(query.limit)
        .toList();
    return JobLogPage(
      items: List<JobLog>.unmodifiable(items),
      nextSequence: items.length == query.limit ? items.last.sequence : null,
    );
  }

  @override
  Future<BackendWriteResult<int>> clearJobLogs(ClearJobLogsRequest request) =>
      _write('clearJobLogs', (store) {
        final logs = store.logsByJob[request.key] ?? <JobLog>[];
        final count = logs.length;
        if (request.keepLatest == 0)
          logs.clear();
        else if (logs.length > request.keepLatest)
          logs.removeRange(0, logs.length - request.keepLatest);
        return BackendWriteResult(
          value: count - logs.length,
          changed: count != logs.length,
          queuesToNotify: count != logs.length
              ? <String>{request.key.queueName}
              : <String>{},
        );
      });

  @override
  Future<JobEventPage> readJobEvents(ReadJobEventsRequest request) async {
    _ensureOpen('readJobEvents');
    _trimEvents(_store, request.queueName);
    final items =
        _store.eventsByQueue[request.queueName] ?? const <PositionedJobEvent>[];
    final cursor = request.cursor;
    // 跨队列游标拒绝
    if (cursor != null && cursor.queueName != request.queueName)
      throw ArgumentError('事件游标属于其他队列');
    // 游标 gap 检测：使用 trimmedEventPositionByQueue 记录的裁剪下界。
    // 即使事件列表为空，只要 cursor.position <= 下界，就说明它指向的
    // 事件已被裁剪，必须抛 EventCursorGap。
    if (cursor != null) {
      final trimmed =
          _store.trimmedEventPositionByQueue[request.queueName] ?? 0;
      final earliestAvailable = items.isNotEmpty
          ? items.first.cursor.position
          : trimmed;
      if (cursor.position < earliestAvailable || cursor.position <= trimmed) {
        throw EventCursorGap(
          EventGap(
            requested: cursor,
            earliestAvailable: cursor.position <= trimmed
                ? EventCursor(
                    queueName: request.queueName,
                    position: trimmed + 1,
                  )
                : items.first.cursor,
          ),
          '事件历史已被裁剪',
        );
      }
    }
    final selected = items
        .where(
          (item) => cursor == null || item.cursor.position > cursor.position,
        )
        .take(request.limit)
        .toList();
    return JobEventPage(
      items: selected,
      nextCursor: selected.isEmpty ? cursor : selected.last.cursor,
    );
  }

  @override
  Future<FlowSnapshot?> getFlow(JobKey root) async {
    _ensureOpen('getFlow');
    // Job 不存在或不是 Flow 根（无子无父边）时返回 null
    if (!_store.jobs.containsKey(root) ||
        (!_store.childrenByParent.containsKey(root) &&
            !_store.parentByChild.containsKey(root)))
      return null;
    // BFS 收集从 root 可达的子树 jobs 和 edges（避免跨 Flow 污染）
    final keys = <JobKey>[root];
    final edges = <FlowEdge>[];
    for (var i = 0; i < keys.length; i++) {
      final edgeList = _store.childrenByParent[keys[i]];
      if (edgeList == null) continue;
      for (final edge in edgeList) {
        edges.add(edge);
        keys.add(edge.child);
      }
    }
    return FlowSnapshot(
      root: root,
      jobs: keys.map((key) => _store.jobs[key]!).toList(),
      edges: edges,
    );
  }

  // ==================== 私有辅助方法 ====================

  /// 用于区分 `_replace` 中「未提供字段」和「显式清空」的 sentinel 值。
  ///
  /// 未传入时保留 record 原值；显式传 `null` 时清空；传实际值时替换。
  static const Object _preserve = Object();

  /// 查找 Job，不存在则抛 JobNotFound。
  JobRecord _job(MemoryStore store, JobKey key) =>
      store.jobs[key] ?? (throw JobNotFound(key, 'Job 不存在'));

  /// 校验 Job revision 乐观并发。
  void _checkRevision(JobRecord job, int expected) {
    if (job.revision != expected)
      throw RevisionConflict(job.key, expected, job.revision, 'revision 过期');
  }

  /// 校验 active Job 的执行凭证匹配。
  void _checkCredential(JobRecord job, ExecutionCredential credential) {
    if (job.execution?.credential.executionId != credential.executionId)
      throw LeaseLost(job.key, credential.executionId, '执行所有权已丢失');
  }

  /// 用指定覆盖字段创建新的 JobRecord，并更新 store。
  ///
  /// 用指定覆盖字段创建新的 JobRecord，并更新 store。
  ///
  /// 使用 sentinel 语义区分三种情况：
  /// - 未传入（默认值为 [_preserve]）→ 保留 record 原值
  /// - 显式传入 `null` → 清空字段
  /// - 传入实际值 → 替换字段
  ///
  /// [revision] 由调用方显式传入，不受 sentinel 影响（默认仍保留原值）。
  JobRecord _replace(
    MemoryStore store,
    JobRecord job, {
    JobStatus? status,
    Object? data = _preserve,
    Object? progress = _preserve,
    Object? result = _preserve,
    Object? failure = _preserve,
    int? revision,
    int? attemptsMade,
    int? stalledCount,
    DateTime? availableAt,
    DateTime? updatedAt,
    Object? startedAt = _preserve,
    Object? finishedAt = _preserve,
    Object? execution = _preserve,
    Object? controlRequest = _preserve,
  }) {
    final updated = JobRecord(
      schemaVersion: job.schemaVersion,
      key: job.key,
      name: job.name,
      data: identical(data, _preserve) ? job.data : data,
      status: status ?? job.status,
      progress: identical(progress, _preserve) ? job.progress : progress,
      result: identical(result, _preserve) ? job.result : result,
      failure: identical(failure, _preserve)
          ? job.failure
          : failure as JobFailure?,
      revision: revision ?? job.revision,
      attemptsMade: attemptsMade ?? job.attemptsMade,
      stalledCount: stalledCount ?? job.stalledCount,
      priority: job.priority,
      availableAt: availableAt ?? job.availableAt,
      options: job.options,
      parentKey: job.parentKey,
      createdAt: job.createdAt,
      updatedAt: updatedAt ?? job.updatedAt,
      startedAt: identical(startedAt, _preserve)
          ? job.startedAt
          : startedAt as DateTime?,
      finishedAt: identical(finishedAt, _preserve)
          ? job.finishedAt
          : finishedAt as DateTime?,
      execution: identical(execution, _preserve)
          ? job.execution
          : execution as ExecutionOwner?,
      controlRequest: identical(controlRequest, _preserve)
          ? job.controlRequest
          : controlRequest as JobControlRequest?,
    );
    store.jobs[job.key] = updated;
    return updated;
  }

  /// 返回指定父 Job 的失败/取消子 Job 列表。
  List<JobKey> _failedDependencies(MemoryStore store, JobKey parent) =>
      (store.childrenByParent[parent] ?? const <FlowEdge>[])
          .map((e) => e.child)
          .where(
            (key) =>
                store.jobs[key]?.status == JobStatus.failed ||
                store.jobs[key]?.status == JobStatus.canceled,
          )
          .toList();

  /// 从 [child] 向上遍历 Flow 树，对祖先做状态重新计算。
  Set<String> _recomputeAncestors(
    MemoryStore store,
    JobKey child,
    DateTime time,
  ) {
    final queues = <String>{};
    var parent = store.parentByChild[child];
    while (parent != null) {
      queues.addAll(_recomputeParent(store, parent, time));
      parent = store.parentByChild[parent];
    }
    return queues;
  }

  /// 重新计算单个父 Job 的 Flow 状态。
  ///
  /// 检查：
  /// 1. 父已被用户取消或已完成 -> 保持 no-op（终态不可被子 Job retry 复活）
  /// 2. 有失败子 -> 父标记为 FlowDependencyFailure
  /// 3. 父已等待子 -> 检查是否所有子都 completed，若是则父脱离 waitingChildren
  Set<String> _recomputeParent(MemoryStore store, JobKey key, DateTime time) {
    final parent = store.jobs[key];
    if (parent == null) return <String>{};
    // 父 Job 已被用户取消或已完成时，不得因子 Job 状态变化而复活。
    if (parent.status == JobStatus.canceled ||
        parent.status == JobStatus.completed)
      return <String>{};
    final failed = _failedDependencies(store, key);
    // 有失败子 -> 父标记为依赖失败
    if (failed.isNotEmpty) return _failDependency(store, key, failed, time);
    final childrenEdges = store.childrenByParent[key] ?? const <FlowEdge>[];
    // 判断是否还有未完成子（completed / canceled 视为已完成；其余均为未完成）。
    final hasIncompleteChild = childrenEdges.any((edge) {
      final s = store.jobs[edge.child]?.status;
      return s != null && s != JobStatus.completed && s != JobStatus.canceled;
    });
    // 父已处于依赖失败之外的状态，且还有子未完成 -> 不处理
    if (childrenEdges.isEmpty || hasIncompleteChild) {
      if (parent.status != JobStatus.failed ||
          parent.failure?.type != 'FlowDependencyFailure')
        return <String>{};
      // 否则继续走到下方把父从 failed 恢复
    }
    // 有未完成子 -> waitingChildren；所有子 completed（或父因依赖失败现已恢复）-> waiting/delayed
    final status = (childrenEdges.isNotEmpty && hasIncompleteChild)
        ? JobStatus.waitingChildren
        : (parent.availableAt.isAfter(time)
              ? JobStatus.delayed
              : JobStatus.waiting);
    final updated = _replace(
      store,
      parent,
      status: status,
      revision: parent.revision + 1,
      updatedAt: time,
      failure: null,
      finishedAt: null,
      execution: null,
      controlRequest: null,
    );
    _event(
      store,
      key,
      updated.revision,
      status == JobStatus.delayed ? JobEventType.delayed : JobEventType.waiting,
      null,
    );
    return <String>{key.queueName};
  }

  /// 将父 Job 标记为 FlowDependencyFailure。
  Set<String> _failDependency(
    MemoryStore store,
    JobKey key,
    List<JobKey> dependencies,
    DateTime time,
  ) {
    final job = store.jobs[key];
    if (job == null ||
        job.status == JobStatus.completed ||
        job.status == JobStatus.canceled ||
        (job.status == JobStatus.failed &&
            job.failure?.type != 'FlowDependencyFailure'))
      return <String>{};
    final updated = _replace(
      store,
      job,
      status: JobStatus.failed,
      revision: job.revision + 1,
      updatedAt: time,
      failure: JobFailure(
        type: 'FlowDependencyFailure',
        message: 'dependency failed',
        failedAt: time,
        dependencyKeys: dependencies,
      ),
      finishedAt: time,
      execution: null,
      controlRequest: null,
    );
    _event(store, key, updated.revision, JobEventType.failed, null);
    return <String>{key.queueName};
  }
}
