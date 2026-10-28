import '../backend/index.dart';
import '../core/index.dart';
import '../signal/index.dart';
import 'queue_models.dart';

/// Queue 与 Job 共享的模块内部上下文。
///
/// 职责严格限定为三件事：
/// 1. 共享生命周期状态（closed 标记、in-flight 登记、唯一 close Future）；
/// 2. 默认 Queue policy 的初始化协调（只在 revision 为 0 时安装一次）；
/// 3. Backend 写入成功后的统一提示语义（只按 `queuesToNotify` 通知、全部 settle 后报错）。
///
/// 它不保存任何 Job 事实，也不从 `queue/index.dart` 导出：模块外既无法引用本类型，
/// 也无法通过它访问 Backend 或 Signal。
final class QueueContext {
  /// 构造共享上下文。
  ///
  /// [queueName]：所属 Queue 的名称，已由 [Queue] 构造完成非空白校验。
  /// [backend]：已 open 的 Backend；本上下文既不调用 open，也不探测其生命周期。
  /// [signal]：用于发送提示的 Signal。
  /// [idGenerator]：缺省 jobId 的唯一来源。省略时回退到 [defaultIdGenerator]。
  /// [now]：所有请求时间戳的唯一来源。省略时回退到 [defaultNow]。
  /// [options]：Queue 级配置，为 null 时取 `QueueOptions()` 默认值。
  QueueContext({
    required this.queueName,
    required this.backend,
    required this.signal,
    this.idGenerator = defaultIdGenerator,
    this.now = defaultNow,
    QueueOptions? options,
  }) : options = options ?? QueueOptions();

  /// 所属 Queue 名称，原样保留调用方传入的值。
  final String queueName;

  /// 注入的 Backend；构造器假设其已 open。
  final QueueBackend backend;

  /// 注入的 Signal。
  final QueueSignal signal;

  /// 缺省 ID 生成器。
  final IdGenerator idGenerator;

  /// 请求时间来源。
  final Now now;

  /// Queue 级配置（含默认 policy 与资源所有权）。
  final QueueOptions options;

  // 关闭状态与协调状态：_closed 用于同步门禁，_closeFuture 保证所有并发/后续 close
  // 共享同一结果，_inFlight 记录已开始但未 settle 的操作（含其全部通知）。
  bool _closed = false;
  Future<void>? _closeFuture;
  final Set<Future<void>> _inFlight = <Future<void>>{};

  /// 本上下文是否已进入关闭流程；为 true 后所有 Queue/Job 入口操作都会被拒绝。
  bool get isClosed => _closed;

  /// 关闭入口的同步门禁。
  ///
  /// 所有公开操作必须在触碰 Backend/Signal 之前先调用本方法，使关闭后的调用
  /// 立即（同步）得到 [LifecycleError]，而不会产生任何 Backend 访问。
  void ensureOpen(String operation) {
    if (_closed) {
      throw LifecycleError('Queue', operation, 'queue $queueName is closed');
    }
  }

  /// 执行一次只读操作，并把整个操作登记为 in-flight。
  ///
  /// 调用方必须已经完成 [ensureOpen]；本方法不重复做输入校验。
  /// 返回的 Future 与调用方共享同一结果，其错误只交给调用方，
  /// 关闭流程只等待它 settle、不接管它的异常。
  Future<T> run<T>(String operation, Future<T> Function() body) {
    ensureOpen(operation);
    final Future<T> result = Future<T>.sync(body);
    _track(result);
    return result;
  }

  /// 统一的 Backend mutation helper：commit 后按 `queuesToNotify` 通知。
  ///
  /// 执行顺序固定，不可调换：
  /// 1. 同步检查未关闭并登记 in-flight；
  /// 2. await [write]，失败时直接抛出（不执行 [onCommitted]、不通知任何 queue）；
  /// 3. 同步调用 [onCommitted]，让 Job 句柄先更新为已提交事实；
  /// 4. 通知集合为空则直接返回 committed value；
  /// 5. 否则**先同时启动全部** queue 通知，再以非 eager 的等待收集全部结果；
  /// 6. 存在失败时按排序后的启动顺序取第一个失败，用同一 committed value 构造
  ///    [QueueNotificationFailure]，并以该失败的原始堆栈抛出。
  ///
  /// 之所以“全部 settle 后才抛错”：Signal 只是提示通道，Backend 事实已经提交，
  /// 因此绝不能回滚、重试或改写 committed value；调用方需要基于完整结果决定下一步。
  Future<T> commit<T>({
    required String operation,
    required Future<BackendWriteResult<T>> Function() write,
    void Function(T value)? onCommitted,
  }) {
    ensureOpen(operation);
    final Future<T> result = _commit(
      operation: operation,
      write: write,
      onCommitted: onCommitted,
    );
    _track(result);
    return result;
  }

  /// 安装默认 Queue policy，并返回主 Queue mutation 应当使用的 Queue state。
  ///
  /// 只有 revision 为 0 且现有 policy 与 [QueueOptions.defaultPolicy] 不同才写入一次，
  /// 因此该默认值是初始化行为，绝不覆盖已存在或已被其他句柄配置的 Queue state。
  ///
  /// [operation]：调用方操作名，用于失败报告与生命周期错误。
  /// [expectedRevision]：调用方显式 revision；非 null 时先与读到的 state 比对，
  /// 过期立即抛 [QueueRevisionConflict] 且不写 policy（乐观并发预检）。
  ///
  /// 返回 Policy 写成功后返回的 state（其 revision 通常为 1），或无需写入时读到的 state；
  /// pause/resume 必须用该 state 的 revision 作为主写入的 expectedRevision。
  /// 只有在调用方省略 revision 时才会吞掉初始化竞态：此时重读一次并接受赢家已提交的 state。
  Future<QueueState> ensureDefaultPolicy({
    required String operation,
    int? expectedRevision,
  }) async {
    ensureOpen(operation);
    final QueueState state = await backend.getQueueState(queueName);
    if (expectedRevision != null && state.revision != expectedRevision) {
      throw QueueRevisionConflict(
        queueName,
        expectedRevision,
        state.revision,
        'queue revision is stale',
      );
    }
    if (state.revision != 0 ||
        _samePolicy(state.policy, options.defaultPolicy)) {
      return state;
    }
    final SetQueuePolicyRequest request = SetQueuePolicyRequest(
      queueName: queueName,
      policy: options.defaultPolicy,
      expectedRevision: state.revision,
      occurredAt: now(),
    );
    try {
      return await commit<QueueState>(
        operation: 'initializeDefaultPolicy',
        write: () => backend.setQueuePolicy(request),
      );
    } on QueueRevisionConflict {
      // 显式 revision 的调用方自己承担并发前提，不得改用赢家的 revision。
      if (expectedRevision != null) rethrow;
      // 省略 revision 的初始化允许输给并发赢家：接受其已提交的 state 后继续主写入。
      return backend.getQueueState(queueName);
    }
  }

  /// 关闭共享上下文：标记关闭、drain 已开始操作、按所有权关闭资源。
  ///
  /// 幂等且并发安全：首个调用启动关闭并在之后返回同一 Future，
  /// 因此所有并发/后续 close 观察到同一结果，owned 资源最多关闭一次。
  Future<void> close() => _closeFuture ??= _closeOnce();

  // commit 的实际执行体。
  //
  // 顺序不可调换：先 await 写入（失败即抛出、零副作用），再同步更新句柄 snapshot，
  // 最后才处理通知。通知阶段刻意“先全部启动、再全部等待、最后才抛错”：
  // 单个 queue 快速失败既不能阻止其他 queue 收到提示，也不能让本 helper 提前返回，
  // 否则调用方可能误以为通知已完成而跳过补偿查询。
  Future<T> _commit<T>({
    required String operation,
    required Future<BackendWriteResult<T>> Function() write,
    void Function(T value)? onCommitted,
  }) async {
    final BackendWriteResult<T> result = await write();
    onCommitted?.call(result.value);
    final Set<String> queuesToNotify = result.queuesToNotify;
    if (queuesToNotify.isEmpty) {
      return result.value;
    }
    final List<String> ordered = queuesToNotify.toList()..sort();
    final List<Future<_NotificationOutcome>> notifications =
        <Future<_NotificationOutcome>>[];
    for (final String name in ordered) {
      // 用 Future.sync 立即启动通知：循环结束时全部通知都已发出，
      // 单个快速失败不会阻止后续通知启动，也不会让本 helper 提前完成。
      notifications.add(
        Future<_NotificationOutcome>.sync(() async {
          try {
            await signal.notify(name);
            return const _NotificationOutcome.succeeded();
          } catch (error, stackTrace) {
            return _NotificationOutcome.failed(error, stackTrace);
          }
        }),
      );
    }
    // 默认非 eager：等待全部通知 settle 后再决定结果。
    final List<_NotificationOutcome> outcomes = await Future.wait(
      notifications,
    );
    for (final _NotificationOutcome outcome in outcomes) {
      final Object? error = outcome.error;
      if (error != null) {
        final StackTrace stackTrace = outcome.stackTrace!;
        Error.throwWithStackTrace(
          QueueNotificationFailure(
            operation: operation,
            committedValue: result.value,
            queuesToNotify: queuesToNotify,
            cause: error,
            stackTrace: stackTrace,
          ),
          stackTrace,
        );
      }
    }
    return result.value;
  }

  /// 登记一个 in-flight 操作。
  ///
  /// 登记的是“吞掉错误的副本”：关闭流程只关心是否 settle，
  /// 原操作异常仍然只交给它的调用方，避免 close 重复接管或掩盖原始错误。
  void _track(Future<void> operation) {
    final Future<void> settled = operation.then<void>(
      (_) {},
      onError: (Object _, StackTrace _) {},
    );
    _inFlight.add(settled);
    settled.whenComplete(() => _inFlight.remove(settled));
  }

  /// 同步标记关闭并启动 drain；返回的 Future 即共享的 close Future。
  Future<void> _closeOnce() {
    _closed = true;
    return _drain();
  }

  /// 等待所有已登记操作（含其全部通知）settle，然后处理资源所有权。
  Future<void> _drain() async {
    // 循环而非单次快照：已开始的操作可能在同一 tick 内继续登记后续阶段。
    while (_inFlight.isNotEmpty) {
      await Future.wait(_inFlight.toList());
    }
    await _closeOwnedResources();
  }

  /// 按 Signal -> Backend 顺序关闭 owned 资源。
  ///
  /// 两个资源都 owned 时，即使第一个关闭失败也继续尝试第二个，
  /// 最终以第一次失败的错误和原始堆栈完成共享 close Future。
  Future<void> _closeOwnedResources() async {
    Object? firstError;
    StackTrace? firstStackTrace;
    if (options.ownsSignal) {
      try {
        await signal.close();
      } catch (error, stackTrace) {
        firstError = error;
        firstStackTrace = stackTrace;
      }
    }
    if (options.ownsBackend) {
      try {
        await backend.close();
      } catch (error, stackTrace) {
        firstError ??= error;
        firstStackTrace ??= stackTrace;
      }
    }
    if (firstError != null) {
      Error.throwWithStackTrace(firstError, firstStackTrace!);
    }
  }
}

/// 单次通知的 settle 结果；成功与失败都表示该通知已经结束。
final class _NotificationOutcome {
  const _NotificationOutcome.succeeded() : error = null, stackTrace = null;

  const _NotificationOutcome.failed(this.error, this.stackTrace);

  final Object? error;

  final StackTrace? stackTrace;
}

/// 结构化比较 QueuePolicy。
///
/// 冻结的 [QueuePolicy] 没有重写 `==`，而“是否需要安装默认策略”是值语义判断，
/// 因此这里逐字段比较，避免把值相同的策略误判为需要写入新 revision。
bool _samePolicy(QueuePolicy left, QueuePolicy right) =>
    left.defaultJobLogRetention == right.defaultJobLogRetention &&
    left.maxEventCount == right.maxEventCount &&
    left.eventRetention == right.eventRetention;
