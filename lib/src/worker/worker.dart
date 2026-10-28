// ignore_for_file: prefer_initializing_formals

import 'dart:async';

// 相邻模块只允许通过各自的 index.dart 依赖；不得直引其内部实现文件。
import '../backend/index.dart';
import '../core/index.dart';
import '../observability/index.dart';
import '../signal/index.dart';
import '../utils/index.dart';
import 'active_job.dart';
import 'processor.dart';
import 'worker_options.dart';

/// Worker 的生命周期状态。
///
/// `created -> running <-> paused`，任一时点都可以进入 `closing`，之后不可逆地
/// 进入 `closed`。只有 `running` 会 claim 新 Job；`paused` 仍会刷新 active
/// execution 的控制请求并保持 distributed 续租。
enum WorkerState { created, running, paused, closing, closed }

/// 队列消费者。
///
/// Worker 通过公开 `QueueBackend` 读取和原子修改事实，通过 `QueueSignal` 接收
/// 可丢失的唤醒提示。Signal 不是事实来源：即使订阅失败、Signal 丢失或发送失败，
/// 独立周期 poll 仍会继续从 Backend 领取 Job 并读取控制请求。
///
/// 构造要求 Backend 已经 `open()`；`start()` 不会调用 `backend.open()`。默认
/// 情况下 Worker 不关闭注入的 Backend 与 Signal，只有 [ownsBackend] /
/// [ownsSignal] 为 `true` 时才在关闭流程中关闭对应资源，两者独立判断。
///
/// ### [Delay] 的定位
/// Worker 有三条互不相同的计时路径，全部使用同一个 [Delay]（构造时注入；
/// 未提供时回退到默认的 `Future<void>.delayed`）：
///
/// 1. 周期轮询——按 [WorkerOptions.pollInterval] 计时，到期请求一次 pump，
///    是 Signal 之外的可靠性兜底；
/// 2. distributed 续租——每个 execution 一条，按 [WorkerOptions.renewInterval]
///    计时，到期续租或判定租约丢失；
/// 3. 优雅关闭宽限——按 [WorkerOptions.gracefulTimeout] 计时，超时升级为强制
///    关闭；没有 active execution 时不会创建这条等待。
///
/// [Delay] 不参与 Job 重试退避（`RetryPolicy` 只决定 `availableAt`），也不参与
/// claim 失败后的重试——那些都等下一次 pump。
final class Worker<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  Worker({
    required String queueName,
    required QueueBackend backend,
    required QueueSignal signal,
    required Processor<TData, TProgress, TResult> processor,
    WorkerOptions options = const WorkerOptions(),
    required WorkerLogSink logSink,
    Now now = defaultNow,
    IdGenerator idGenerator = defaultIdGenerator,

    /// 业务 ↔ JSON-like 编解码器 bundle。
    /// 省略时使用默认 JSON-like Codec（透传，仅 freezeJsonLike 校验）。
    JobPayloadCodecs<TData, TProgress, TResult>? codecs,

    /// 异步等待原语，可选。仅为三条计时路径提供等待：周期轮询
    /// [WorkerOptions.pollInterval]、distributed 续租 [WorkerOptions.renewInterval]、
    /// 优雅关闭宽限 [WorkerOptions.gracefulTimeout]。
    ///
    /// 为空时回退到 Worker 内部的默认实现 `_defaultDelay`（即
    /// `Future<void>.delayed`），生产环境通常无需注入。
    ///
    /// 需要注入的场景有两类：测试用假时钟推进时间，避免真实等待（默认
    /// `gracefulTimeout` 长达 30 秒）；以及把计时接入自有调度器或可观测埋点。
    /// 无论是否注入，关闭与续租都有停止 token 与等待竞争，未完成的等待
    /// 不会被取消，只是不再被等待。
    Delay? delay,
    bool ownsBackend = false,
    bool ownsSignal = false,
  }) : queueName = _requireNonEmpty(queueName, 'queueName'),
       _backend = backend,
       _signal = signal,
       _processor = processor,
       _codecs =
           codecs ?? JobPayloadCodecs<TData, TProgress, TResult>.jsonLike(),
       _logSink = logSink,
       _now = now,
       _delay = delay ?? _defaultDelay,
       _ownsBackend = ownsBackend,
       _ownsSignal = ownsSignal,
       options = validateWorkerOptions(options),
       workerId = _resolveWorkerId(options, idGenerator);

  /// Worker 消费的队列名称。非空，构造后不可变。
  final String queueName;

  /// Worker 在 Backend 中的标识。显式提供或由注入的 `IdGenerator` 生成一次。
  final String workerId;

  /// 生效的运行配置。已经通过 `validateWorkerOptions` 校验。
  final WorkerOptions options;

  final QueueBackend _backend;
  final QueueSignal _signal;
  final Processor<TData, TProgress, TResult> _processor;
  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;
  final WorkerLogSink _logSink;
  final Now _now;

  /// 计时使用的异步等待原语：调用方注入的实现，或未提供时回退的
  /// `_defaultDelay`。三条计时路径共用同一个实现；等待不被取消，
  /// 也不参与 claim / 提交失败后的退避（那些只依赖 [options.pollInterval]）。
  final Delay _delay;
  final bool _ownsBackend;
  final bool _ownsSignal;

  WorkerState _state = WorkerState.created;
  StreamSubscription<void>? _subscription;

  /// 当前 in-flight pump 的完成信号。关闭流程用它等待已经发出的 claim 返回。
  Future<void> _pumpIdle = Future<void>.value();
  bool _pumpInProgress = false;
  bool _pumpAgain = false;

  bool _canRefreshControls = false;
  bool _canClaim = false;

  /// 首次 `start()` 的 claim 遇到 `LifecycleError` 时需要回滚启动。
  bool _propagateClaimLifecycleError = false;

  final Map<String, _WorkerExecution> _executions =
      <String, _WorkerExecution>{};

  /// 合并本 Worker 中重叠的 close / forceClose 操作。
  final SingleFlightOperationCoordinator _closeCoordinator =
      SingleFlightOperationCoordinator();

  /// 当前关闭操作；未启动或上次失败时为空，成功后保留以维持幂等。
  Future<void>? _closeFuture;
  bool _forceRequested = false;
  final Completer<void> _forceEscalation = Completer<void>();

  bool _periodicStopped = false;
  final Completer<void> _periodicStop = Completer<void>();

  /// 当前生命周期状态。
  WorkerState get state => _state;

  /// 当前占用的 execution 槽位数量。关闭完成后必然为 0。
  int get activeCount => _executions.length;

  /// 启动消费循环。
  ///
  /// 只允许从 [WorkerState.created] 进入 [WorkerState.running]；重复 start 或
  /// closed 之后 start 抛出 `LifecycleError`。返回表示已经尝试订阅 Signal、
  /// 并已启动独立周期轮询与首次 claim pump，**不**表示 Processor 已经完成。
  ///
  /// 能力校验在产生任何副作用之前完成：distributed Backend 必须搭配
  /// `SignalScope.distributed` 与完整租约配置，local Backend 不得携带租约配置，
  /// 否则抛出 `UnsupportedCapability`。
  Future<void> start() async {
    if (_state != WorkerState.created) {
      throw LifecycleError(
        'Worker',
        'start',
        'Worker cannot start from ${_state.name}.',
      );
    }
    _assertCapabilities();
    _canRefreshControls = true;
    _canClaim = true;
    _state = WorkerState.running;
    _subscription = _subscribeSafely();
    _startPeriodicLoop();
    _propagateClaimLifecycleError = true;
    try {
      await _requestPump();
    } on LifecycleError {
      await _rollbackStart();
      rethrow;
    } finally {
      _propagateClaimLifecycleError = false;
    }
  }

  /// 停止本 Worker 领取新 Job。
  ///
  /// 不修改 Queue 的持久化暂停状态（不调用 `setQueuePaused`），也不停止已经运行
  /// 的 Processor 和 distributed 续租。重复调用幂等。`created` / `closing` /
  /// `closed` 状态下调用抛出 `LifecycleError`。
  Future<void> pause() async {
    if (_state == WorkerState.paused) return;
    if (_state != WorkerState.running) {
      throw LifecycleError(
        'Worker',
        'pause',
        'Worker cannot pause from ${_state.name}.',
      );
    }
    _state = WorkerState.paused;
    _canClaim = false;
  }

  /// 恢复领取新 Job，并立即请求一次 pump（不等待下一个 poll 间隔）。
  ///
  /// 重复调用幂等。非 `paused` 状态下调用抛出 `LifecycleError`。
  Future<void> resume() async {
    if (_state == WorkerState.running) return;
    if (_state != WorkerState.paused) {
      throw LifecycleError(
        'Worker',
        'resume',
        'Worker cannot resume from ${_state.name}.',
      );
    }
    _state = WorkerState.running;
    _canClaim = true;
    await _requestPump();
  }

  /// 优雅关闭：先停止领取，再等待当前 Processor 完成（宽限期内继续续租）。
  ///
  /// 幂等，并与 [forceClose] 共享同一个关闭 Future。没有 active execution 时
  /// 不会创建宽限期等待。
  Future<void> close() => _ensureCloseStarted();

  /// 强制关闭：中断优雅等待，放弃当前执行的租约并由 Backend 恢复机制接管。
  ///
  /// 可以升级正在进行中的优雅关闭。返回的 Future 与 [close] 相同。
  Future<void> forceClose() {
    _signalForce();
    return _ensureCloseStarted();
  }

  // --- 启动与消费循环 -------------------------------------------------------

  void _assertCapabilities() {
    if (_backend.capabilities.profile == BackendProfile.distributed) {
      if (_signal.scope != SignalScope.distributed) {
        throw const UnsupportedCapability(
          'distributedSignal',
          'Distributed workers require a distributed QueueSignal.',
        );
      }
      if (options.leaseDuration == null || options.renewInterval == null) {
        throw const UnsupportedCapability(
          'leases',
          'Distributed workers require leaseDuration and renewInterval.',
        );
      }
    } else if (options.leaseDuration != null || options.renewInterval != null) {
      throw const UnsupportedCapability(
        'leases',
        'Local workers do not renew leases.',
      );
    }
  }

  Future<void> _rollbackStart() async {
    _canRefreshControls = false;
    _canClaim = false;
    _stopPeriodic();
    await _cancelSubscription();
    _state = WorkerState.created;
  }

  /// 订阅 Signal。同步抛错只记录 warning 并以 `null` 订阅继续启动。
  ///
  /// Signal 的异步 error/done 同样只写运行日志，独立周期轮询不受影响。
  StreamSubscription<void>? _subscribeSafely() {
    try {
      final stream = _signal.subscribe(queueName);
      return stream.listen(
        (void _) => _requestPumpIfAllowed(),
        onError: (Object error, StackTrace stackTrace) {
          _writeLog(
            WorkerLogLevel.warning,
            'Signal stream reported an error; periodic polling continues.',
            error,
            stackTrace,
            operation: 'subscribe',
          );
        },
        onDone: () {
          _writeLog(
            WorkerLogLevel.warning,
            'Signal stream closed; periodic polling continues.',
            null,
            null,
            operation: 'subscribe',
          );
        },
        cancelOnError: false,
      );
    } catch (error, stackTrace) {
      _writeLog(
        WorkerLogLevel.warning,
        'Signal subscription failed; falling back to periodic polling.',
        error,
        stackTrace,
        operation: 'subscribe',
      );
      return null;
    }
  }

  Future<void> _cancelSubscription() async {
    final subscription = _subscription;
    _subscription = null;
    if (subscription == null) return;
    try {
      await subscription.cancel();
    } catch (error, stackTrace) {
      _writeLog(
        WorkerLogLevel.warning,
        'Failed to cancel the signal subscription.',
        error,
        stackTrace,
        operation: 'close',
      );
    }
  }

  /// 独立周期轮询。
  ///
  /// 与 Signal 完全解耦：即使所有提示丢失，也会按 `pollInterval` 继续领取 Job
  /// 并读取控制请求。关闭 token 与注入的 Delay 用 `Future.any` 竞争，因此未完成
  /// 的手工 Delay 不会阻塞关闭；迟到的 Delay 只结束一次已经失效的等待。
  void _startPeriodicLoop() {
    unawaited(_runPeriodicLoop());
  }

  Future<void> _runPeriodicLoop() async {
    while (!_periodicStopped) {
      final bool shouldPoll;
      try {
        // 计时路径 1：按 pollInterval 等待一个轮询周期，与停止 token 竞争。
        shouldPoll = await Future.any<bool>(<Future<bool>>[
          _periodicStop.future.then((void _) => false),
          _manualDelay(options.pollInterval).then((void _) => true),
        ]);
      } catch (error, stackTrace) {
        // 注入 Delay 失败时只记录一次并升级到 force 路径，绝不原地重试形成热循环。
        _writeLog(
          WorkerLogLevel.error,
          'Periodic poll delay failed; escalating to force close.',
          error,
          stackTrace,
          operation: 'poll',
        );
        _signalForce();
        await _ensureCloseStarted();
        return;
      }
      if (!shouldPoll || _periodicStopped) return;
      await _requestPump();
    }
  }

  void _stopPeriodic() {
    _periodicStopped = true;
    if (!_periodicStop.isCompleted) _periodicStop.complete();
  }

  // --- Pump 与槽位 ----------------------------------------------------------

  void _requestPumpIfAllowed() {
    if (!_canRefreshControls) return;
    unawaited(_requestPump());
  }

  /// 唯一的 pump 入口：Signal listener、start、resume、周期 poll 与槽位释放都
  /// 走这里。
  ///
  /// `_pumpInProgress` / `_pumpAgain` 合并重入，保证同一时刻只有一轮 claim，重复
  /// 唤醒不会超额领取。running 与 paused 都刷新 active 控制请求，只有 running
  /// 允许 claim；closing/closed 拒绝新的 pump 请求。
  Future<void> _requestPump() async {
    if (!_canRefreshControls) return;
    if (_pumpInProgress) {
      _pumpAgain = true;
      return;
    }
    final idle = Completer<void>();
    _pumpIdle = idle.future;
    _pumpInProgress = true;
    try {
      do {
        _pumpAgain = false;
        await _refreshActiveControls();
        if (!_canRefreshControls) break;
        await _retryPendingTerminalIntents();
        if (!_canRefreshControls) break;
        if (_canClaim) {
          await _fillAvailableSlots();
        }
      } while (_pumpAgain && _canRefreshControls);
    } finally {
      _pumpInProgress = false;
      if (!idle.isCompleted) idle.complete();
    }
  }

  Future<void> _refreshActiveControls() async {
    final holders = _executions.values.toList();
    for (final holder in holders) {
      if (!_canRefreshControls) return;
      final controller = holder.controller;
      if (controller.isLeaseLost || controller.isTerminal) continue;
      try {
        await controller.refreshControl();
      } on ActiveJobInfrastructureFailure catch (failure) {
        if (failure.original is LeaseLost) {
          _writeLog(
            WorkerLogLevel.warning,
            'Execution ownership was lost while refreshing controls.',
            failure.original,
            failure.stackTrace,
            operation: 'getJob',
            execution: holder,
          );
          _settleLeaseLost(holder);
        } else {
          _writeLog(
            WorkerLogLevel.warning,
            'Control refresh failed; retrying on the next poll.',
            failure.original,
            failure.stackTrace,
            operation: 'getJob',
            execution: holder,
          );
        }
      }
    }
  }

  /// 重试尚未提交成功的终态意图（例如上一次遇到 `BackendUnavailable`）。
  ///
  /// Processor 不会因此重跑：intent 在 Processor 结束时就已冻结。
  Future<void> _retryPendingTerminalIntents() async {
    final holders = _executions.values
        .where((holder) => holder.pendingIntent != null)
        .toList();
    for (final holder in holders) {
      if (!_canRefreshControls) return;
      final intent = holder.pendingIntent;
      if (intent == null) continue;
      await _commitIntent(holder, intent);
    }
  }

  Future<void> _fillAvailableSlots() async {
    while (_canClaim && _executions.length < options.concurrency) {
      final BackendWriteResult<ClaimedJob?> result;
      try {
        // local 不携带 leaseDuration；distributed 精确传入配置的租约时长。
        result = await _backend.claimJob(
          ClaimJobRequest(
            queueName: queueName,
            workerId: workerId,
            claimedAt: _now(),
            leaseDuration: options.leaseDuration,
          ),
        );
      } catch (error, stackTrace) {
        _writeLog(
          WorkerLogLevel.warning,
          'Claiming a job failed; retrying on the next poll.',
          error,
          stackTrace,
          operation: 'claimJob',
        );
        if (_propagateClaimLifecycleError && error is LifecycleError) {
          rethrow;
        }
        return;
      }
      final claimed = result.value;
      if (claimed == null) return;
      final holder = _registerExecution(claimed);
      if (result.queuesToNotify.isNotEmpty) {
        await _notifyQueues(result.queuesToNotify);
      }
      if (!_canClaim) {
        // 关闭流程已经开始：holder 只登记、不启动 Processor 与续租，
        // 由关闭协调器纳入精确 recovery / lease 放弃集合。
        _writeLog(
          WorkerLogLevel.info,
          'Claimed a job while the worker was closing; leaving it for recovery.',
          null,
          null,
          operation: 'claimJob',
          execution: holder,
        );
      }
    }
  }

  _WorkerExecution _registerExecution(ClaimedJob claimed) {
    final holder = _WorkerExecution(
      controller: createActiveJobController(
        backend: _backend,
        claimed: claimed,
        workerId: workerId,
        now: _now,
        notifyQueues: _notifyQueues,
      ),
      codecs: _codecs,
    );
    _executions[holder.executionId] = holder;
    if (_state == WorkerState.running || _state == WorkerState.paused) {
      _startProcessor(holder);
      if (options.leaseDuration != null && options.renewInterval != null) {
        _startRenewal(holder);
      }
    }
    return holder;
  }

  // --- Processor 驱动 -------------------------------------------------------

  void _startProcessor(_WorkerExecution holder) {
    holder.processorStarted = true;
    // ActiveJobController.job 是无泛型 ActiveJob<Object?, Object?, Object?>，
    // 但 Processor 期望 ActiveJob<TData, TProgress, TResult>。我们需要用 Worker 的
    // codecs 构造一个正确泛型的 ActiveJob——它共享同一个 controller 来访问原始
    // JobRecord 和 mutation 能力，同时用 Worker 的 codecs 解码 data/progress/result。
    final job = ActiveJob<TData, TProgress, TResult>(
      holder.controller,
      _codecs,
    );
    Future<TResult?>.sync(() => _processor(job)).then(
      (Object? result) {
        holder.processorSettled = true;
        return _handleProcessorResult(holder, result);
      },
      onError: (Object error, StackTrace stackTrace) {
        holder.processorSettled = true;
        return _handleProcessorError(holder, error, stackTrace);
      },
    );
  }

  Future<void> _handleProcessorResult(
    _WorkerExecution holder,
    Object? result,
  ) async {
    final controller = holder.controller;
    if (controller.isLeaseLost || controller.isTerminal) {
      _discardProcessorOutcome(
        holder,
        'Discarding the Processor result because the execution already ended.',
      );
      return;
    }
    // 用 result Codec 编码 Processor 返回值为 JSON-like；
    // 编码失败按 Processor 业务失败处理（Codec 错误也是业务层面的）。
    Object? encodedResult;
    try {
      encodedResult = holder.codecs.result.encodeNullable(result as TResult?);
    } on PayloadCodecException catch (error, stackTrace) {
      await _commitFailure(holder, error, stackTrace);
      return;
    }
    await _commitIntent(
      holder,
      ActiveJobTerminalIntent(
        targetStatus: JobStatus.completed,
        occurredAt: _now(),
        result: encodedResult,
      ),
    );
  }

  Future<void> _handleProcessorError(
    _WorkerExecution holder,
    Object error,
    StackTrace stackTrace,
  ) async {
    if (error is ActiveJobInfrastructureFailure) {
      // 执行基础设施错误绝不算作业务失败：完整错误只写 Worker 运行日志。
      _writeLog(
        WorkerLogLevel.error,
        'Processor aborted by an execution infrastructure failure.',
        error.original,
        error.stackTrace,
        operation: 'processor',
        execution: holder,
      );
      _discardProcessorOutcome(holder, null);
      return;
    }
    final controller = holder.controller;
    if (controller.isLeaseLost || controller.isTerminal) {
      _discardProcessorOutcome(
        holder,
        'Discarding the Processor failure because the execution already ended.',
      );
      return;
    }
    await _commitFailure(holder, error, stackTrace);
  }

  void _discardProcessorOutcome(_WorkerExecution holder, String? message) {
    if (message != null) {
      _writeLog(
        WorkerLogLevel.info,
        message,
        null,
        null,
        operation: 'processor',
        execution: holder,
      );
    }
    holder.pendingIntent = null;
    holder.workSettled = true;
    _maybeFinalize(holder);
  }

  Future<void> _commitFailure(
    _WorkerExecution holder,
    Object error,
    StackTrace stackTrace,
  ) async {
    final record = holder.controller.record;
    // failure 的 failedAt 与 transition 的 occurredAt 必须来自同一次 now()。
    final occurredAt = _now();
    // PayloadCodecException 是编码/解码时对底层异常的包装；持久化时 unwrap
    // 到底层 cause，让 failure.type 反映真正的业务失败类型（ArgumentError 等），
    // 同时 PayloadCodecException 本身仍会被完整写入 WorkerLogSink。
    final unwrappedError = error is PayloadCodecException ? error.cause : error;
    final failure = JobFailure(
      type: unwrappedError.runtimeType.toString(),
      message: _failureMessage(unwrappedError),
      failedAt: occurredAt,
      stackTrace: _limitStackTrace(stackTrace, record.options.stackTraceLimit),
    );
    final shouldRetry = record.attemptsMade < record.options.retry.maxAttempts;
    final retryDelay = shouldRetry
        ? record.options.retry.delayForAttempt(record.attemptsMade)
        : Duration.zero;
    final targetStatus = !shouldRetry
        ? JobStatus.failed
        : retryDelay == Duration.zero
        ? JobStatus.waiting
        : JobStatus.delayed;
    // 完整原始异常只进 WorkerLogSink，不进入持久化 JobFailure 或 JobLog data。
    _writeLog(
      WorkerLogLevel.error,
      'Processor failed.',
      error,
      stackTrace,
      operation: 'processor',
      execution: holder,
    );
    await _commitIntent(
      holder,
      ActiveJobTerminalIntent(
        targetStatus: targetStatus,
        occurredAt: occurredAt,
        failure: failure,
        availableAt: targetStatus == JobStatus.delayed
            ? occurredAt.add(retryDelay)
            : null,
        automaticErrorLog: JobLogDraft(
          level: JobLogLevel.error,
          channel: JobLogChannel.system,
          message: failure.message,
        ),
      ),
    );
  }

  /// 提交冻结的终态意图。Processor 永不因此重跑。
  Future<void> _commitIntent(
    _WorkerExecution holder,
    ActiveJobTerminalIntent intent,
  ) async {
    holder.pendingIntent = intent;
    try {
      await holder.controller.commitTerminal(intent);
      holder.pendingIntent = null;
      holder.workSettled = true;
      _maybeFinalize(holder);
    } on ActiveJobInfrastructureFailure catch (failure) {
      if (failure.original is LeaseLost) {
        _writeLog(
          WorkerLogLevel.warning,
          'Terminal commit aborted because the execution ownership was lost.',
          failure.original,
          failure.stackTrace,
          operation: 'transitionJob',
          execution: holder,
        );
        holder.pendingIntent = null;
        holder.workSettled = true;
        _maybeFinalize(holder);
        return;
      }
      // BackendUnavailable、第二次 RevisionConflict 或未知 Backend 错误：
      // 保留 holder、槽位与 intent，由后续 Signal 或周期 poll 重试。
      _writeLog(
        WorkerLogLevel.error,
        'Terminal commit failed; retrying after the next signal or poll.',
        failure.original,
        failure.stackTrace,
        operation: 'transitionJob',
        execution: holder,
      );
    }
  }

  // --- 续租 -----------------------------------------------------------------

  void _startRenewal(_WorkerExecution holder) {
    unawaited(_runRenewalLoop(holder));
  }

  Future<void> _runRenewalLoop(_WorkerExecution holder) async {
    final leaseDuration = options.leaseDuration!;
    final renewInterval = options.renewInterval!;
    final controller = holder.controller;
    while (!holder.renewalStopped) {
      final bool shouldRenew;
      try {
        // 计时路径 2：每个 execution 一条独立等待，按 renewInterval 计时，
        // 与该 execution 的续租停止 token（槽位释放或关闭）竞争。
        shouldRenew = await Future.any<bool>(<Future<bool>>[
          holder.renewalStop.future.then((void _) => false),
          _manualDelay(renewInterval).then((void _) => true),
        ]);
      } catch (error, stackTrace) {
        _writeLog(
          WorkerLogLevel.error,
          'Lease renewal delay failed; abandoning the execution.',
          error,
          stackTrace,
          operation: 'renewLease',
          execution: holder,
        );
        await _markHolderLeaseLost(holder);
        return;
      }
      if (!shouldRenew || holder.renewalStopped) return;
      try {
        final renewed = await controller.renewOrExpire(_now(), leaseDuration);
        if (!renewed) {
          _writeLog(
            WorkerLogLevel.warning,
            'Lease renewal could not be confirmed; abandoning the execution.',
            null,
            null,
            operation: 'renewLease',
            execution: holder,
          );
          _settleLeaseLost(holder);
          return;
        }
      } on ActiveJobInfrastructureFailure catch (failure) {
        if (failure.original is LeaseLost) {
          _writeLog(
            WorkerLogLevel.warning,
            'Lease renewal reported a lost lease.',
            failure.original,
            failure.stackTrace,
            operation: 'renewLease',
            execution: holder,
          );
          _settleLeaseLost(holder);
          return;
        }
        // 已确认的 leaseExpiresAt 之前允许继续尝试：记录 warning 后进入下一周期。
        _writeLog(
          WorkerLogLevel.warning,
          'Lease renewal failed; retrying at the next interval.',
          failure.original,
          failure.stackTrace,
          operation: 'renewLease',
          execution: holder,
        );
        final expiresAt = controller.credential.leaseExpiresAt;
        if (expiresAt != null && !_now().isBefore(expiresAt)) {
          await _markHolderLeaseLost(holder);
          return;
        }
      }
    }
  }

  Future<void> _markHolderLeaseLost(_WorkerExecution holder) async {
    try {
      await holder.controller.markLeaseLost(
        LeaseLost(
          holder.controller.key,
          holder.executionId,
          'Lease renewal could not be confirmed before leaseExpiresAt.',
        ),
        StackTrace.current,
      );
    } catch (error, stackTrace) {
      _writeLog(
        WorkerLogLevel.error,
        'Failed to mark the execution lease as lost.',
        error,
        stackTrace,
        operation: 'renewLease',
        execution: holder,
      );
    }
    _settleLeaseLost(holder);
  }

  void _settleLeaseLost(_WorkerExecution holder) {
    if (holder.workSettled) return;
    holder.pendingIntent = null;
    holder.workSettled = true;
    _maybeFinalize(holder);
  }

  // --- 槽位释放 -------------------------------------------------------------

  /// 只在「Processor 已结束」且「本次执行不再有未完成的写入」时释放槽位。
  ///
  /// 因此 leaseLost 但 Processor 仍在运行、或终态提交仍在重试时，槽位保持占用，
  /// Worker 不会超额 claim。
  void _maybeFinalize(_WorkerExecution holder) {
    if (holder.finalized) return;
    if (!holder.processorSettled || !holder.workSettled) return;
    holder.finalized = true;
    _executions.remove(holder.executionId);
    _stopRenewal(holder);
    if (!holder.completion.isCompleted) holder.completion.complete();
    _requestPumpIfAllowed();
  }

  void _stopRenewal(_WorkerExecution holder) {
    if (holder.renewalStopped) return;
    holder.renewalStopped = true;
    if (!holder.renewalStop.isCompleted) holder.renewalStop.complete();
  }

  // --- 关闭协调 -------------------------------------------------------------

  void _signalForce() {
    _forceRequested = true;
    if (!_forceEscalation.isCompleted) _forceEscalation.complete();
  }

  /// 启动或复用关闭操作；失败时清理缓存并将原错误传播给调用方。
  Future<void> _ensureCloseStarted() {
    final existing = _closeFuture;
    if (existing != null) return existing;
    late final Future<void> future;
    future = _closeCoordinator.run(_runCloseCoordinator).catchError((
      Object error,
      StackTrace stackTrace,
    ) {
      if (identical(_closeFuture, future)) _closeFuture = null;
      Error.throwWithStackTrace(error, stackTrace);
    });
    _closeFuture = future;
    return future;
  }

  /// 唯一的关闭协调器：close 与 forceClose 共享同一条路径与同一个 Future。
  Future<void> _runCloseCoordinator() async {
    // 先原子禁止新的 pump / claim，再等待 in-flight 的 pump 落地。
    _state = WorkerState.closing;
    _canRefreshControls = false;
    _canClaim = false;
    _stopPeriodic();
    await _cancelSubscription();
    await _pumpIdle;

    final holders = _executions.values.toList();
    final started = holders
        .where((holder) => holder.processorStarted)
        .toList(growable: false);
    // close 期间登记但尚未启动 Processor 的 claim 无法优雅完成，直接走恢复路径。
    var forced = _forceRequested || holders.length != started.length;
    if (!forced && started.isNotEmpty) {
      forced = await _awaitGracefulOrForce(started);
    }
    if (forced) {
      try {
        await _abandonAndRecover(holders);
      } catch (error, stackTrace) {
        // local profile 上的 recovery 失败：本次 close Future 以原错误失败，
        // Worker 保持 closing，保留 holders 与 executionIds；同时清除
        // 清除关闭 Future 让后续 forceClose 创建新的 recovery attempt。
        _writeLog(
          WorkerLogLevel.error,
          'Local recovery failed; worker remains closing for a retry.',
          error,
          stackTrace,
          operation: 'forceClose',
        );
        rethrow;
      }
    }
    for (final holder in holders) {
      try {
        await holder.controller.dispose();
      } catch (_) {
        // control stream 释放失败不影响关闭结果。
      }
    }
    await _releaseOwnedResources();
    _executions.clear();
    _state = WorkerState.closed;
  }

  /// 宽限期等待：Processor 全部完成即优雅结束；force 升级、超时或宽限 Delay
  /// 失败都进入强制路径。没有 active execution 时不会创建宽限 Delay。
  Future<bool> _awaitGracefulOrForce(List<_WorkerExecution> started) async {
    var processorFinished = false;
    var timedOut = false;
    var delayFailed = false;
    final completion =
        Future.wait<void>(
          started.map<Future<void>>(
            (_WorkerExecution holder) => holder.completion.future,
          ),
        ).then((void _) {
          processorFinished = true;
        });
    // 计时路径 3：宽限期超时。只在存在已启动 Processor 的 execution 时创建，
    // 与「全部 Processor 完成」「force 升级」三者竞争；注入的 Delay 失败按超时
    // 处理（delayFailed），同样进入强制路径，绝不原地重试。
    final timeout = _manualDelay(options.gracefulTimeout).then(
      (void _) {
        timedOut = true;
      },
      onError: (Object error, StackTrace stackTrace) {
        delayFailed = true;
      },
    );
    await Future.any<void>(<Future<void>>[
      completion,
      timeout,
      _forceEscalation.future,
    ]);
    if (processorFinished && !_forceRequested) return false;
    if (timedOut) {
      _writeLog(
        WorkerLogLevel.warning,
        'Graceful close timed out; escalating to force close.',
        null,
        null,
        operation: 'close',
      );
    } else if (delayFailed) {
      _writeLog(
        WorkerLogLevel.error,
        'Graceful timeout delay failed; escalating to force close.',
        null,
        null,
        operation: 'close',
      );
    }
    return true;
  }

  /// 强制路径：向精确的 holder 集合发布 leaseLost，并让 Backend 接管恢复。
  ///
  /// local Profile 仅在该集合非空时恰好一次调用 `recoverStalledJobs`，请求同时
  /// 携带 `workerId` 与这些 holder 当前 credential 的精确非空 `executionIds`。
  /// local Profile 下 Backend 抛错时**向上传播**（由 `_runCloseCoordinator` 捕获并
  /// 决定是否让 Worker 保持 closing 以允许重试）。distributed Profile 从不主动
  /// 恢复：只停止续租、发布 leaseLost，由 lease 到期后的 Backend 机制接管，
  /// 且 distributed 下 Backend 错误仅记日志不影响关闭路径。
  Future<void> _abandonAndRecover(List<_WorkerExecution> holders) async {
    for (final holder in holders) {
      try {
        await holder.controller.abandonForRecovery(
          LeaseLost(
            holder.controller.key,
            holder.executionId,
            'Worker force closed the execution.',
          ),
          StackTrace.current,
        );
      } catch (error, stackTrace) {
        _writeLog(
          WorkerLogLevel.warning,
          'Failed to abandon an execution during close.',
          error,
          stackTrace,
          operation: 'forceClose',
          execution: holder,
        );
      }
    }
    final executionIds = <String>{
      for (final holder in holders)
        if (holder.controller.isAbandoned) holder.executionId,
    };
    if (_backend.capabilities.profile != BackendProfile.local) return;
    if (executionIds.isEmpty) return;
    final result = await _backend.recoverStalledJobs(
      RecoverStalledJobsRequest(
        queueName: queueName,
        now: _now(),
        workerId: workerId,
        executionIds: Set<String>.unmodifiable(executionIds),
      ),
    );
    if (result.queuesToNotify.isNotEmpty) {
      await _notifyQueues(result.queuesToNotify);
    }
  }

  /// 按 Signal 后 Backend 的固定顺序释放自有资源。
  ///
  /// 两个 close 都会尝试执行，第一项失败不会跳过第二项；错误只写运行日志，不
  /// 改变 Worker 的 closed 状态。未声明所有权的资源保持打开。
  Future<void> _releaseOwnedResources() async {
    final failures = <Object>[];
    final stackTraces = <StackTrace?>[];
    if (_ownsSignal) {
      try {
        await _signal.close();
      } catch (error, stackTrace) {
        failures.add(error);
        stackTraces.add(stackTrace);
      }
    }
    if (_ownsBackend) {
      try {
        await _backend.close();
      } catch (error, stackTrace) {
        failures.add(error);
        stackTraces.add(stackTrace);
      }
    }
    for (var index = 0; index < failures.length; index++) {
      _writeLog(
        WorkerLogLevel.error,
        'Failed to release an owned resource.',
        failures[index],
        stackTraces[index],
        operation: 'close',
      );
    }
  }

  // --- 日志与通知 -----------------------------------------------------------

  /// 每个 Backend 写成功后只通知该结果声明的队列。
  ///
  /// 单个 notify 失败只记录 warning，后续队列仍会发送；Signal 故障不回滚 Backend
  /// 事实、不修改 Processor 结果、也不中止消费循环。
  Future<void> _notifyQueues(Set<String> queuesToNotify) async {
    for (final queue in queuesToNotify) {
      try {
        await _signal.notify(queue);
      } catch (error, stackTrace) {
        _writeLog(
          WorkerLogLevel.warning,
          'Signal notification failed.',
          error,
          stackTrace,
          operation: 'notify',
          data: <String, Object?>{'queueName': queue},
        );
      }
    }
  }

  /// 写入 Worker 运行日志。
  ///
  /// sink 的同步抛错与异步失败全部吞掉：sink 故障不得改变 Job 状态、重试目标、
  /// Worker 生命周期或资源关闭结果，也不得递归写同一个 sink。
  void _writeLog(
    WorkerLogLevel level,
    String message,
    Object? error,
    StackTrace? stackTrace, {
    required String operation,
    _WorkerExecution? execution,
    Object? data,
  }) {
    final entry = WorkerLogEntry(
      level: level,
      message: message,
      occurredAt: _now(),
      context: WorkerLogContext(
        workerId: workerId,
        queueName: queueName,
        operation: operation,
        jobKey: execution?.controller.key,
        attempt: execution?.controller.record.attemptsMade,
        executionId: execution?.executionId,
      ),
      error: error,
      stackTrace: stackTrace,
      data: data,
    );
    try {
      final result = _logSink.write(entry);
      if (result is Future<void>) {
        result.then((void _) {}, onError: (Object _) {});
      }
    } catch (_) {
      // 故意吞掉：运行日志失败不能影响业务结果。
    }
  }

  /// 默认的等待实现：直接委托给 `Future<void>.delayed`。
  ///
  /// 仅在构造时未提供 `delay` 时使用。它依赖真实计时器，因此测试必须注入假
  /// 时钟版本——否则续租与优雅关闭宽限都会真实等待（[WorkerOptions.gracefulTimeout]
  /// 默认为 30 秒），`close()` 也会被真实计时器拖住。
  static Future<void> _defaultDelay(Duration duration) =>
      Future<void>.delayed(duration);

  /// 唯一的 [Delay] 调用点：所有计时路径都必须经过它。
  ///
  /// `Future.sync` 包装把注入实现的**同步**抛错转成返回的 Future 错误，
  /// 这样所有调用方都能统一用 `await` / `Future.any` 捕获，而不必同时处理
  /// 同步抛出与异步失败两种形态。返回的 Future 不登记、不取消：停止 token
  /// 先完成时就地结束等待，未完成的等待自然悬挂到结束。
  Future<void> _manualDelay(Duration duration) =>
      Future<void>.sync(() => _delay(duration));
}

/// 单个 execution holder：一次 claim 对应的全部可变执行状态。
final class _WorkerExecution {
  _WorkerExecution({required this.controller, required this.codecs});

  final ActiveJobController controller;
  final JobPayloadCodecs codecs;

  /// claim 时确定的 execution 身份。续租替换 credential 不会改变它。
  String get executionId => controller.executionId;

  /// execution 完成的信号，供优雅关闭等待。
  final Completer<void> completion = Completer<void>();

  /// 续租循环的停止信号，可立即结束未完成的续租等待。
  final Completer<void> renewalStop = Completer<void>();

  bool processorStarted = false;
  bool processorSettled = false;
  bool workSettled = false;
  bool finalized = false;
  bool renewalStopped = false;

  /// 尚未成功提交的终态意图。非空表示需要在下一次 pump 重试提交，
  /// 且 Processor 不会重跑。
  ActiveJobTerminalIntent? pendingIntent;
}

String _requireNonEmpty(String value, String name) {
  if (value.isEmpty) {
    throw ArgumentError.value(value, name, 'must not be empty');
  }
  return value;
}

String _resolveWorkerId(WorkerOptions options, IdGenerator idGenerator) {
  final configured = options.workerId;
  if (configured != null) return configured;
  final generated = idGenerator();
  if (generated.isEmpty) {
    throw ArgumentError.value(
      generated,
      'idGenerator',
      'must generate a non empty workerId',
    );
  }
  return generated;
}

String _failureMessage(Object error) {
  if (error is ArgumentError) {
    final message = error.message;
    if (message != null) return message.toString();
  }
  return error.toString();
}

/// 按 `stackTraceLimit` 裁剪持久化堆栈；限制为 0 时持久化 `null`。
String? _limitStackTrace(StackTrace stackTrace, int limit) {
  if (limit <= 0) return null;
  final lines = stackTrace.toString().split('\n');
  while (lines.isNotEmpty && lines.last.isEmpty) {
    lines.removeLast();
  }
  return lines.take(limit).join('\n');
}
