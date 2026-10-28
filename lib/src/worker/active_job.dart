// ignore_for_file: prefer_initializing_formals

import 'dart:async';

// 相邻模块只允许通过各自的 index.dart 依赖；不得直引其内部实现文件。
import '../backend/index.dart';
import '../core/index.dart';

/// Processor 可观察的执行控制状态。
///
/// 状态只能沿 `running -> pauseRequested -> cancelRequested -> leaseLost`、
/// `running -> cancelRequested -> leaseLost` 或 `running -> leaseLost` 前进，
/// 不会回退。`pauseRequested` / `cancelRequested` 只表示"Backend 记录了请求"，
/// 不表示 Job 已经暂停或取消：只有 Processor 显式调用 `confirmPaused()` /
/// `confirmCanceled()` 才会产生状态转换。
enum ActiveJobControlState {
  /// 没有未处理的控制请求，Processor 可以继续执行业务。
  running,

  /// Backend 记录了暂停请求。Processor 应自行决定在何时确认暂停。
  pauseRequested,

  /// Backend 记录了取消请求。约束强于暂停，可以覆盖 [pauseRequested]。
  cancelRequested,

  /// 本次执行的 credential 已失效（租约到期、被抢占或 Backend 拒绝）。
  /// 之后所有依赖 credential 的操作都会失败，Processor 必须停止业务写入。
  leaseLost,
}

/// Worker 模块内部的"执行基础设施失败"标记。
///
/// 该类型不是公开 API：`worker/index.dart` 不导出它。它的唯一用途是把 Backend、
/// 续租、Job 日志写入、依赖读取、refresh 与终态提交产生的错误与 Processor 的
/// 业务异常区分开。Worker 只把**不带**该标记的异常归类为 `JobFailure`，因此
/// `BackendUnavailable`、`RevisionConflict`、`LeaseLost` 和未知 Backend 错误
/// 永远不会被伪装成业务失败，也不会增加 Job 的 attempts。
///
/// [original] 保留 Backend 或基础设施抛出的原始错误对象，[stackTrace] 保留其
/// 原始堆栈对象身份，供 Worker 完整写入 `WorkerLogSink`。
final class ActiveJobInfrastructureFailure implements Exception {
  ActiveJobInfrastructureFailure({
    required this.operation,
    required this.original,
    required this.stackTrace,
  });

  /// 触发的模块内操作名，例如 `getJob`、`updateJobProgress`、`transitionJob`。
  final String operation;

  /// 原始错误。为 `LeaseLost` 时表示本次执行的所有权已经失效。
  final Object original;

  /// 原始错误对应的堆栈，保持对象身份。
  final StackTrace stackTrace;

  @override
  String toString() => 'ActiveJobInfrastructureFailure($operation): $original';
}

/// 一次执行的终态意图（不可变）。
///
/// 由 Worker 在 Processor 结束或业务失败时创建一次，之后 Processor 永不重跑。
/// 当终态提交遇到可重试的基础设施错误时，同一 intent 会在后续 Signal 或周期
/// poll 中被重新提交，因此 intent 内不能包含随时间变化的字段。
final class ActiveJobTerminalIntent {
  ActiveJobTerminalIntent({
    required this.targetStatus,
    required this.occurredAt,
    this.result,
    this.failure,
    this.availableAt,
    this.automaticErrorLog,
  });

  /// 目标 Job 状态：`completed`、`failed`、`waiting` 或 `delayed`。
  final JobStatus targetStatus;

  /// 转换发生时间。重试同一 intent 时保持不变，保证 Backend 事实可比对。
  final DateTime occurredAt;

  /// 成功结果（非 null 时将通过 result Codec 编码）。仅 `completed` 使用。
  final Object? result;

  /// 业务失败摘要，`failed`、`waiting`、`delayed` 使用。
  final JobFailure? failure;

  /// 下次可领取时间，仅 `delayed` 使用。
  final DateTime? availableAt;

  /// 与 [failure] 同一次原子提交的 `error/system` JobLogDraft。
  final JobLogDraft? automaticErrorLog;
}

/// 一次 claim 的执行上下文，交给 Processor 使用。
///
/// 实例由内部 controller 创建，只在该 execution 的 credential 有效期内可用。
/// 它不暴露 credential、Backend、Signal、Worker，也不提供公开构造器；Processor
/// 无法构造、替换、续期或读取可修改的 credential。
///
/// 业务类型通过构造时注入的 JobPayloadCodecs 解码，写入前通过 Codec 编码。
final class ActiveJob<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  /// Worker 内部构造：传入 controller（原始事实与 mutation 能力）和 codecs
  /// （业务 ↔ JSON-like 编解码）。该构造函数不从 worker/index.dart 导出。
  ActiveJob(this._controller, this._codecs);

  final ActiveJobController _controller;
  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;

  /// 最近一次 Backend 写入或控制 refresh 后的类型化 JobSnapshot。
  /// 其余派生 getter 都从这个 snapshot 读取，不缓存第二份业务事实。
  JobSnapshot<TData, TProgress, TResult> get snapshot =>
      JobSnapshot<TData, TProgress, TResult>(
        rawRecord: _controller.record,
        codecs: _codecs,
      );

  /// Backend 原始 JSON-like 记录。供 Worker 内部需要原始事实的场景使用。
  JobRecord get rawRecord => _controller.record;

  /// 本次执行所属的复合 Job 标识。
  JobKey get key => _controller.key;

  /// Worker 标识，来自 claim 该 Job 的 Worker。
  String get workerId => _controller.workerId;

  /// Job 名称，来自最新 snapshot。
  String get name => snapshot.name;

  /// Job 业务数据，已解码为业务类型；可能为 `null`。
  TData? get data => snapshot.data;

  /// Job 业务进度，已解码为业务类型；可能为 `null`。
  TProgress? get progress => snapshot.progress;

  /// 本次执行的尝试序号，等于 claim 后 snapshot 的 `attemptsMade`（从 1 开始）。
  int get attempt => snapshot.attemptsMade;

  /// 当前控制状态。调用方应先读该值再监听 [controlStream]。
  ActiveJobControlState get controlState => _controller.controlState;

  /// 同一 ActiveJob 内的 broadcast 控制状态流，不重放历史状态。
  Stream<ActiveJobControlState> get controlStream => _controller.controlStream;

  /// 写入业务进度并刷新 snapshot。
  ///
  /// 业务值通过 [codecs.progress] 编码为 JSON-like 后再提交 Backend。
  /// 返回写入后的原始 JobRecord。失败时抛出 `ActiveJobInfrastructureFailure`：
  /// 所有权失效时其 `original` 为 `LeaseLost`。
  Future<JobRecord> updateProgress(TProgress? progress) {
    final Object? encoded = _codecs.progress.encodeNullable(progress);
    return _controller.updateProgress(encoded);
  }

  /// 写入业务数据并刷新 snapshot。失败语义同 [updateProgress]。
  Future<JobRecord> updateData(TData? data) {
    final Object? encoded = _codecs.data.encodeNullable(data);
    return _controller.updateData(encoded);
  }

  /// 追加一条 Job 执行日志（原始 JSON-like 模型）。
  ///
  /// 日志追加不递增 Job revision，也不产生 JobEvent；成功后仍会重新读取
  /// `getJob` 刷新 snapshot。失败语义同 [updateProgress]。
  Future<JobLog> log(JobLogDraft log) => _controller.log(log);

  /// 确认暂停请求，提交 `JobStatus.paused`。
  ///
  /// 仅在 [controlState] 为 `pauseRequested` 时可用；其他状态调用抛出
  /// `LifecycleError` 且不写 Backend。成功后本次执行进入终态，后续业务写入
  /// 与控制确认都会失败。
  Future<JobRecord> confirmPaused() => _controller.confirmPaused();

  /// 确认取消请求，提交 `JobStatus.canceled`。语义同 [confirmPaused]。
  Future<JobRecord> confirmCanceled() => _controller.confirmCanceled();

  /// 读取直接子 Job 的结果，已通过 codecs.result 解码为 TResult 业务类型。
  ///
  /// 只返回 `FlowEdge.parent == record.key` 的直接子 Job 的 `result`，key 为直接
  /// `FlowEdge.child`，按 `JobKey` 稳定排序；允许某个直接子的结果为 `null`。
  /// 无 Flow 或无直接子时返回不可变空 Map。不递归聚合后代，也不写回父 Job。
  Future<Map<JobKey, TResult?>> getDependenciesResult() async {
    final results = await _controller.getDependenciesResult();
    return Map<JobKey, TResult?>.unmodifiable(
      results.map((key, rawValue) {
        final TResult? decoded = _codecs.result.decodeNullable(rawValue);
        return MapEntry(key, decoded);
      }),
    );
  }
}

/// 单次 claim 的执行状态机。
///
/// 该类型是 Worker 模块内部实现：`worker/index.dart` 不导出它，Processor 也无法
/// 取得它。它独占四类权限：
///
/// 1. credential 替换（claim 时的初始凭证、续租返回的新凭证）；
/// 2. `JobRecord` snapshot 刷新与控制状态发布；
/// 3. lease lost 的判定与单次发布；
/// 4. terminal gate 与终态意图提交。
///
/// 所有 refresh、业务 mutation、续租、终态提交和 leaseLost 都经同一个串行 tail
/// 执行（见 [_serialize]），因此 credential、snapshot、epoch 与 terminal 状态
/// 不会被并发修改。
final class ActiveJobController {
  ActiveJobController._({
    required QueueBackend backend,
    required ClaimedJob claimed,
    required String workerId,
    Now now = defaultNow,
    required Future<void> Function(Set<String> queuesToNotify) notifyQueues,
  }) : _backend = backend,
       _workerId = workerId,
       _now = now,
       _notifyQueues = notifyQueues,
       _key = claimed.record.key,
       _executionId = claimed.credential.executionId,
       _credential = claimed.credential,
       _record = claimed.record,
       _controlState = _readControlState(
         claimed.record.controlRequest,
         ActiveJobControlState.running,
       ) {
    // 在构造函数体中创建 _job，此时 this 已完全初始化。
    // 使用默认 jsonLike codecs 创建无泛型 ActiveJob，供直接访问 controller.job
    // 的场景（如 active_job_test）；Worker 层会用自己的 codecs 重新创建类型化版本。
    _job = ActiveJob<Object?, Object?, Object?>(
      this,
      JobPayloadCodecs<Object?, Object?, Object?>.jsonLike(),
    );
  }

  final QueueBackend _backend;
  final JobKey _key;

  /// claim 时确定的执行标识。续租替换 credential 但不会改变它，
  /// 因此它可以安全地作为 holder 身份与恢复请求的 executionId。
  final String _executionId;
  final String _workerId;
  final Now _now;
  final Future<void> Function(Set<String> queuesToNotify) _notifyQueues;

  late final ActiveJob _job;
  ExecutionCredential _credential;
  JobRecord _record;
  ActiveJobControlState _controlState;

  final StreamController<ActiveJobControlState> _controlController =
      StreamController<ActiveJobControlState>.broadcast(sync: true);

  /// 串行 tail。始终以成功完成，因此前一次失败不会阻断后续操作。
  Future<void> _tail = Future<void>.value();

  /// 每次 snapshot / credential / control / terminal / lease 状态改变时递增。
  /// 依赖读取用它判断 await 之后执行身份是否已经变化。
  int _epoch = 0;

  bool _leaseLost = false;
  Object? _leaseLostError;
  StackTrace? _leaseLostStackTrace;

  /// 本次执行已经提交过终态转换（completed / failed / waiting / delayed /
  /// paused / canceled）。之后不再接受新的业务写入或终态提交。
  bool _terminal = false;

  /// 已提交但尚未成功的终态意图，用于在 `RevisionConflict` 后判断"是否已经
  /// 由同一 execution 提交过"。
  ActiveJobTerminalIntent? _pendingIntent;

  /// 由 Worker 的强制关闭路径声明：该 execution 需要交给 Backend 的恢复机制。
  bool _abandoned = false;

  ActiveJob get job => _job;
  JobKey get key => _key;

  /// claim 时确定、续租不会改变的执行标识。
  String get executionId => _executionId;

  String get workerId => _workerId;
  ActiveJobControlState get controlState => _controlState;
  Stream<ActiveJobControlState> get controlStream => _controlController.stream;
  ExecutionCredential get credential => _credential;
  JobRecord get record => _record;
  int get epoch => _epoch;
  bool get isLeaseLost => _leaseLost;
  bool get isTerminal => _terminal;
  bool get isAbandoned => _abandoned;
  ActiveJobTerminalIntent? get pendingIntent => _pendingIntent;

  /// 把 [operation] 排入唯一的串行 tail 并返回其结果。
  ///
  /// [runWhenLeaseLost] 只供"标记 lease lost"这类清理操作使用：它们必须在
  /// 所有权已经失效后仍然可执行，且不会重复发布状态。
  Future<T> _serialize<T>(
    Future<T> Function() operation, {
    bool runWhenLeaseLost = false,
  }) {
    final completer = Completer<T>();
    _tail = _tail.then((void _) async {
      try {
        if (_leaseLost && !runWhenLeaseLost) {
          throw _leaseLostFailure();
        }
        final value = await operation();
        if (!completer.isCompleted) {
          completer.complete(value);
        }
      } catch (error, stackTrace) {
        if (!completer.isCompleted) {
          completer.completeError(error, stackTrace);
        }
      }
    });
    return completer.future;
  }

  /// [_serialize] 的 `void` 版本，避免把 `void` 当作值传递。
  Future<void> _serializeVoid(
    Future<void> Function() operation, {
    bool runWhenLeaseLost = false,
  }) => _serialize<Object?>(() async {
    await operation();
    return null;
  }, runWhenLeaseLost: runWhenLeaseLost);

  // --- Worker 侧生命周期入口 ------------------------------------------------

  /// 读取 Backend 权威事实，刷新 snapshot 与控制状态。
  ///
  /// 发现执行所有权变化（Job 消失、状态不再是 active、executionId 或
  /// fencingToken 不匹配）时在同一个 tail 内标记 lease lost 并失败。
  Future<void> refreshControl() =>
      _serializeVoid(() => _refreshControlInternal('getJob'));

  /// 标记本次执行的 lease 已丢失。
  Future<void> markLeaseLost(Object error, StackTrace stackTrace) =>
      _serializeVoid(() {
        _markLeaseLostInternal(error, stackTrace);
        return Future<void>.value();
      }, runWhenLeaseLost: true);

  /// 强制关闭路径使用：声明该 execution 需要由 Backend 恢复机制接管。
  ///
  /// 已经提交过终态的执行不会被打上该标记，因此不会被重复恢复。
  Future<void> abandonForRecovery(Object error, StackTrace stackTrace) =>
      _serializeVoid(() {
        if (!_terminal) {
          _abandoned = true;
          _markLeaseLostInternal(error, stackTrace);
        }
        return Future<void>.value();
      }, runWhenLeaseLost: true);

  /// distributed 续租的一次推进。
  ///
  /// 返回 `false` 表示 lease 已到期或被 Backend 拒绝，调用方必须停止续租并丢弃
  /// Processor 结果；返回 `true` 表示已用新凭证替换本次执行的 credential。
  /// 凭证替换不改变 `JobRecord`、不递增 Job revision，只递增内部 epoch。
  Future<bool> renewOrExpire(DateTime renewedAt, Duration leaseDuration) =>
      _serialize<bool>(() async {
        if (_terminal) return true;
        final expiresAt = _credential.leaseExpiresAt;
        if (expiresAt != null && !renewedAt.isBefore(expiresAt)) {
          _markLeaseLostInternal(
            LeaseLost(
              _key,
              _executionId,
              'Lease expired at ${expiresAt.toIso8601String()} before renewal.',
            ),
            StackTrace.current,
          );
          return false;
        }
        try {
          final result = await _backend.renewLease(
            RenewLeaseRequest(
              key: _key,
              credential: _credential,
              renewedAt: renewedAt,
              leaseDuration: leaseDuration,
            ),
          );
          _credential = result.value;
          _epoch++;
          await _notifyQueues(result.queuesToNotify);
          return true;
        } on LeaseLost catch (error, stackTrace) {
          _markLeaseLostInternal(error, stackTrace);
          return false;
        } catch (error, stackTrace) {
          throw _wrapOrRethrow('renewLease', error, stackTrace);
        }
      });

  /// 提交一个终态意图。Worker 在 Processor 结束后调用，且 Processor 永不重跑。
  ///
  /// 成功时本次执行进入终态；`RevisionConflict` 会先受控 refresh，再判定意图是否
  /// 已提交或是否可用新 revision 重试一次；其余基础设施错误以 marker 形式抛出，
  /// 由调用方保留 holder 与 intent 并在后续 poll 重试。
  Future<JobRecord> commitTerminal(ActiveJobTerminalIntent intent) =>
      _serialize<JobRecord>(() async {
        if (_terminal) {
          throw ActiveJobInfrastructureFailure(
            operation: 'transitionJob',
            original: LeaseLost(
              _key,
              _executionId,
              'Execution already committed a terminal state.',
            ),
            stackTrace: StackTrace.current,
          );
        }
        _throwIfLeaseExpired();
        return _commitIntentInternal(intent, 'transitionJob');
      });

  /// 释放内部资源。control stream 会被关闭，之后不再发布事件。
  Future<void> dispose() async {
    if (!_controlController.isClosed) {
      await _controlController.close();
    }
  }

  // --- ActiveJob 公开 API 的内部实现 ---------------------------------------

  Future<JobRecord> updateProgress(Object? progress) async {
    final result = await _mutateOwned(
      'updateJobProgress',
      (int revision, ExecutionCredential credential) =>
          _backend.updateJobProgress(
            UpdateJobProgressRequest(
              key: _key,
              progress: progress,
              expectedRevision: revision,
              credential: credential,
              occurredAt: _now(),
            ),
          ),
    );
    return result.value;
  }

  Future<JobRecord> updateData(Object? data) async {
    final result = await _mutateOwned(
      'updateJobData',
      (int revision, ExecutionCredential credential) => _backend.updateJobData(
        UpdateJobDataRequest(
          key: _key,
          data: data,
          expectedRevision: revision,
          credential: credential,
          occurredAt: _now(),
        ),
      ),
    );
    return result.value;
  }

  Future<JobLog> log(JobLogDraft log) => _serialize<JobLog>(() async {
    _throwIfLeaseLost();
    _throwIfTerminal();
    _throwIfLeaseExpired();
    final BackendWriteResult<JobLog> result;
    try {
      result = await _backend.appendJobLog(
        AppendJobLogRequest(
          key: _key,
          credential: _credential,
          attempt: _record.attemptsMade,
          log: log,
          occurredAt: _now(),
        ),
      );
    } catch (error, stackTrace) {
      throw _wrapOrRethrow('appendJobLog', error, stackTrace);
    }
    await _notifyQueues(result.queuesToNotify);
    // appendJobLog 不递增 Job revision，但成功后仍必须刷新 snapshot：
    // 刷新发现执行身份变化时立刻进入 leaseLost。
    final fresh = await _readJobOrFail('getJob');
    _acceptRefreshedRecord(fresh, 'appendJobLog');
    return result.value;
  });

  Future<JobRecord> confirmPaused() => _confirmTerminal(
    JobStatus.paused,
    ActiveJobControlState.pauseRequested,
    'confirmPaused',
  );

  Future<JobRecord> confirmCanceled() => _confirmTerminal(
    JobStatus.canceled,
    ActiveJobControlState.cancelRequested,
    'confirmCanceled',
  );

  Future<Map<JobKey, Object?>> getDependenciesResult() async {
    // getFlow 的读取不长期占用串行 tail：先在 tail 内捕获 epoch，读取完成后
    // 重新进入 tail 校验。epoch 变化说明执行身份或控制状态已经改变，必须丢弃
    // 结果并重试一次，绝不返回跨 execution 的旧依赖结果。
    for (var attempt = 0; attempt < 2; attempt++) {
      final capturedEpoch = await _serialize<int>(() async {
        _throwIfLeaseLost();
        _throwIfLeaseExpired();
        return _epoch;
      });
      final FlowSnapshot? flow;
      try {
        flow = await _backend.getFlow(_key);
      } catch (error, stackTrace) {
        throw _wrapOrRethrow('getFlow', error, stackTrace);
      }
      final unchanged = await _serialize<bool>(() async {
        _throwIfLeaseLost();
        return _epoch == capturedEpoch;
      });
      if (unchanged) {
        return _projectDirectChildren(flow);
      }
    }
    throw ActiveJobInfrastructureFailure(
      operation: 'getFlow',
      original: LeaseLost(
        _key,
        _executionId,
        'Execution identity changed while reading flow dependencies.',
      ),
      stackTrace: StackTrace.current,
    );
  }

  // --- 内部控制与提交 -------------------------------------------------------

  Future<JobRecord> _confirmTerminal(
    JobStatus target,
    ActiveJobControlState required,
    String operation,
  ) => _serialize<JobRecord>(() async {
    _throwIfLeaseLost();
    if (_terminal) {
      throw ActiveJobInfrastructureFailure(
        operation: operation,
        original: LeaseLost(
          _key,
          _executionId,
          'Execution already committed a terminal state.',
        ),
        stackTrace: StackTrace.current,
      );
    }
    if (_controlState != required) {
      throw LifecycleError(
        'ActiveJob',
        operation,
        'A ${required.name} control request is required before $operation.',
      );
    }
    _throwIfLeaseExpired();
    return _commitIntentInternal(
      ActiveJobTerminalIntent(targetStatus: target, occurredAt: _now()),
      operation,
    );
  });

  Future<BackendWriteResult<JobRecord>> _mutateOwned(
    String operation,
    Future<BackendWriteResult<JobRecord>> Function(
      int revision,
      ExecutionCredential credential,
    )
    write,
  ) => _serialize<BackendWriteResult<JobRecord>>(() async {
    _throwIfLeaseLost();
    _throwIfTerminal();
    _throwIfLeaseExpired();
    var revision = _record.revision;
    var retried = false;
    while (true) {
      try {
        final result = await write(revision, _credential);
        _applyWriteResult(result);
        await _notifyQueues(result.queuesToNotify);
        return result;
      } on RevisionConflict catch (error, stackTrace) {
        if (retried) {
          throw _wrapOrRethrow(operation, error, stackTrace);
        }
        retried = true;
        // 唯一一次受控 refresh：同 execution 仍在 active 时用新 revision 重试，
        // 否则 refresh 内部已经标记 lease lost。
        await _refreshControlInternal(operation);
        revision = _record.revision;
      } catch (error, stackTrace) {
        throw _wrapOrRethrow(operation, error, stackTrace);
      }
    }
  });

  Future<JobRecord> _commitIntentInternal(
    ActiveJobTerminalIntent intent,
    String operation,
  ) async {
    _throwIfLeaseLost();
    _pendingIntent = intent;
    var revision = _record.revision;
    var retried = false;
    while (true) {
      try {
        final result = await _backend.transitionJob(
          TransitionJobRequest(
            key: _key,
            targetStatus: intent.targetStatus,
            expectedRevision: revision,
            occurredAt: intent.occurredAt,
            credential: _credential,
            result: intent.result,
            failure: intent.failure,
            availableAt: intent.availableAt,
            automaticErrorLog: intent.automaticErrorLog,
          ),
        );
        _applyWriteResult(result);
        _pendingIntent = null;
        _terminal = true;
        _epoch++;
        await _notifyQueues(result.queuesToNotify);
        return result.value;
      } on RevisionConflict catch (error, stackTrace) {
        if (retried) {
          throw _wrapOrRethrow(operation, error, stackTrace);
        }
        retried = true;
        final JobRecord? alreadyCommitted = await _resolveTerminalConflict(
          operation,
        );
        if (alreadyCommitted != null) {
          _applyRecord(alreadyCommitted);
          _pendingIntent = null;
          _terminal = true;
          _epoch++;
          return alreadyCommitted;
        }
        revision = _record.revision;
      } on LeaseLost catch (error, stackTrace) {
        _markLeaseLostInternal(error, stackTrace);
        throw _wrapOrRethrow(operation, error, stackTrace);
      } catch (error, stackTrace) {
        throw _wrapOrRethrow(operation, error, stackTrace);
      }
    }
  }

  /// `RevisionConflict` 之后的受控解析。
  ///
  /// 返回非空 `JobRecord` 表示同一 execution 的同一 intent 已经提交成功。
  /// 返回 `null` 表示同一 execution 仍处于 active，调用方可用新 revision 重试
  /// 一次。其他情况（Job 消失、所有权转移、事实与 intent 不匹配）标记 lease
  /// lost 并失败。
  Future<JobRecord?> _resolveTerminalConflict(String operation) async {
    final fresh = await _readJobOrFail(operation);
    final execution = fresh.execution;
    final sameExecution =
        execution != null &&
        execution.credential.executionId == _executionId &&
        execution.credential.fencingToken == _credential.fencingToken;
    if (sameExecution && fresh.status == JobStatus.active) {
      _applyRecord(fresh);
      return null;
    }
    final intent = _pendingIntent;
    if (intent != null && _matchesTerminalIntent(intent, fresh)) {
      return fresh;
    }
    _markLeaseLostInternal(
      LeaseLost(
        _key,
        _executionId,
        'Execution ownership changed before $operation.',
      ),
      StackTrace.current,
    );
    throw _leaseLostFailure();
  }

  Future<void> _refreshControlInternal(String operation) async {
    // 终态已提交后不再读取控制请求：Backend 的 controlRequest 与 execution
    // 都已经与本执行无关。
    if (_terminal) return;
    _throwIfLeaseLost();
    final fresh = await _readJobOrFail(operation);
    _acceptRefreshedRecord(fresh, operation);
  }

  Future<JobRecord> _readJobOrFail(String operation) async {
    final JobRecord? fresh;
    try {
      fresh = await _backend.getJob(_key);
    } catch (error, stackTrace) {
      throw _wrapOrRethrow(operation, error, stackTrace);
    }
    if (fresh == null) {
      _markLeaseLostInternal(
        LeaseLost(_key, _executionId, 'Job no longer exists in the Backend.'),
        StackTrace.current,
      );
      throw _leaseLostFailure();
    }
    return fresh;
  }

  void _acceptRefreshedRecord(JobRecord fresh, String operation) {
    final execution = fresh.execution;
    final stillOwned =
        execution != null &&
        execution.credential.executionId == _executionId &&
        execution.credential.fencingToken == _credential.fencingToken;
    if (!stillOwned || fresh.status != JobStatus.active) {
      _markLeaseLostInternal(
        LeaseLost(
          _key,
          _executionId,
          'Execution ownership changed before $operation.',
        ),
        StackTrace.current,
      );
      throw _leaseLostFailure();
    }
    _applyRecord(fresh);
  }

  void _applyWriteResult(BackendWriteResult<JobRecord> result) {
    _applyRecord(result.value);
  }

  void _applyRecord(JobRecord record) {
    _record = record;
    _applyControlRequest(record.controlRequest);
    _epoch++;
  }

  /// 控制状态只允许单调前进：cancel 的约束强于 pause，两者都强于 running。
  void _applyControlRequest(JobControlRequest? request) {
    if (_leaseLost) return;
    final desired = _readControlState(request, ActiveJobControlState.running);
    if (desired.index <= _controlState.index) return;
    _controlState = desired;
    _epoch++;
    if (!_controlController.isClosed) {
      _controlController.add(desired);
    }
  }

  void _throwIfLeaseLost() {
    if (_leaseLost) {
      throw _leaseLostFailure();
    }
  }

  void _throwIfTerminal() {
    if (_terminal) {
      throw ActiveJobInfrastructureFailure(
        operation: 'terminal',
        original: LeaseLost(
          _key,
          _executionId,
          'Execution already committed a terminal state.',
        ),
        stackTrace: StackTrace.current,
      );
    }
  }

  /// 检查租约是否已到期。到期时在串行 tail 内标记 lease lost 并失败，绝不向
  /// Backend 提交业务结果。
  void _throwIfLeaseExpired() {
    final expiresAt = _credential.leaseExpiresAt;
    if (expiresAt == null || _now().isBefore(expiresAt)) return;
    _markLeaseLostInternal(
      LeaseLost(
        _key,
        _executionId,
        'Lease expired at ${expiresAt.toIso8601String()}.',
      ),
      StackTrace.current,
    );
    throw _leaseLostFailure();
  }

  /// 只在首次丢失时发布状态并关闭 control stream。
  void _markLeaseLostInternal(Object error, StackTrace stackTrace) {
    if (_leaseLost) return;
    _leaseLost = true;
    _leaseLostError = error;
    _leaseLostStackTrace = stackTrace;
    _controlState = ActiveJobControlState.leaseLost;
    _epoch++;
    if (!_controlController.isClosed) {
      _controlController.add(ActiveJobControlState.leaseLost);
      unawaited(_controlController.close());
    }
  }

  ActiveJobInfrastructureFailure _leaseLostFailure() {
    final error =
        _leaseLostError ??
        LeaseLost(_key, _executionId, 'Execution ownership has been lost.');
    return ActiveJobInfrastructureFailure(
      operation: 'leaseLost',
      original: error,
      stackTrace: _leaseLostStackTrace ?? StackTrace.current,
    );
  }

  Map<JobKey, Object?> _projectDirectChildren(FlowSnapshot? flow) {
    if (flow == null) return const <JobKey, Object?>{};
    final children =
        flow.edges
            .where((FlowEdge edge) => edge.parent == _key)
            .map((FlowEdge edge) => edge.child)
            .toList()
          ..sort((JobKey left, JobKey right) => left.compareTo(right));
    if (children.isEmpty) return const <JobKey, Object?>{};
    final records = <JobKey, JobRecord>{
      for (final record in flow.jobs) record.key: record,
    };
    final results = <JobKey, Object?>{};
    for (final child in children) {
      final record = records[child];
      if (record == null) {
        throw _wrapOrRethrow(
          'getFlow',
          StateError('Flow snapshot is missing the direct child $child.'),
          StackTrace.current,
        );
      }
      results[child] = record.result;
    }
    return Map<JobKey, Object?>.unmodifiable(results);
  }
}

/// 创建一次 claim 的执行 controller。
///
/// 该工厂是 Worker 模块内部实现，不从 `worker/index.dart` 导出。Worker 只把
/// controller 交给 Processor（通过 Worker 自己的 codecs 构造类型化 ActiveJob），
/// 因此 Processor 无法取得 controller、credential 或终态意图。
ActiveJobController createActiveJobController({
  required QueueBackend backend,
  required ClaimedJob claimed,
  required String workerId,
  Now now = defaultNow,
  required Future<void> Function(Set<String> queuesToNotify) notifyQueues,
}) => ActiveJobController._(
  backend: backend,
  claimed: claimed,
  workerId: workerId,
  now: now,
  notifyQueues: notifyQueues,
);

ActiveJobControlState _readControlState(
  JobControlRequest? request,
  ActiveJobControlState fallback,
) {
  if (request == null) return fallback;
  return request.action == JobControlAction.pause
      ? ActiveJobControlState.pauseRequested
      : ActiveJobControlState.cancelRequested;
}

/// 把任意非标记错误包装成 [ActiveJobInfrastructureFailure]；已经是标记的错误
/// 原样抛出，避免重复包装丢失原始错误类型。
Never _wrapOrRethrow(String operation, Object error, StackTrace stackTrace) {
  if (error is ActiveJobInfrastructureFailure) {
    throw error;
  }
  throw ActiveJobInfrastructureFailure(
    operation: operation,
    original: error,
    stackTrace: stackTrace,
  );
}

/// 判断 Backend 中残留的终态事实是否与本执行提交的 intent 一致。
///
/// 只在 `RevisionConflict` 后使用：一致说明该 intent 此前已经提交成功，Worker
/// 不应再次 transition。
bool _matchesTerminalIntent(ActiveJobTerminalIntent intent, JobRecord record) {
  if (record.status != intent.targetStatus) return false;
  switch (intent.targetStatus) {
    case JobStatus.completed:
      return _jsonLikeEquals(record.result, intent.result) &&
          record.failure == null;
    case JobStatus.failed:
      return _sameFailure(record.failure, intent.failure);
    case JobStatus.waiting:
      return record.result == null && record.failure == null;
    case JobStatus.delayed:
      return record.result == null &&
          record.failure == null &&
          record.availableAt == intent.availableAt;
    default:
      return true;
  }
}

bool _sameFailure(JobFailure? left, JobFailure? right) {
  if (left == null || right == null) return left == right;
  if (left.type != right.type ||
      left.message != right.message ||
      left.stackTrace != right.stackTrace ||
      left.failedAt != right.failedAt ||
      left.dependencyKeys.length != right.dependencyKeys.length) {
    return false;
  }
  for (var index = 0; index < left.dependencyKeys.length; index++) {
    if (left.dependencyKeys[index] != right.dependencyKeys[index]) return false;
  }
  return true;
}

bool _jsonLikeEquals(Object? left, Object? right) {
  if (identical(left, right)) return true;
  if (left is num && right is num) return left == right;
  if (left is List<Object?> && right is List<Object?>) {
    if (left.length != right.length) return false;
    for (var index = 0; index < left.length; index++) {
      if (!_jsonLikeEquals(left[index], right[index])) return false;
    }
    return true;
  }
  if (left is Map<Object?, Object?> && right is Map<Object?, Object?>) {
    if (left.length != right.length) return false;
    for (final entry in left.entries) {
      if (!right.containsKey(entry.key)) return false;
      if (!_jsonLikeEquals(entry.value, right[entry.key])) return false;
    }
    return true;
  }
  return left == right;
}
