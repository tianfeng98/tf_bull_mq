/// Worker 的运行配置。
///
/// 默认值面向「本地单进程、Volatile Backend」的最小可用形态：单并发、1 秒轮询、
/// 30 秒优雅关闭宽限期，且不使用租约。
///
/// 该类型是 `const` 可构造的，因此只能声明编译期可求值的 assert：通用参数
/// （[concurrency]）与租约参数成对性。其余与时长相比较的约束（正值、续租间隔
/// 必须短于租约）无法在常量表达式中求值，统一由 [validateWorkerOptions] 在
/// `Worker` 构造时以 `ArgumentError` 拒绝。
final class WorkerOptions {
  const WorkerOptions({
    this.workerId,
    this.concurrency = 1,
    this.pollInterval = const Duration(seconds: 1),
    this.gracefulTimeout = const Duration(seconds: 30),
    this.leaseDuration,
    this.renewInterval,
  }) : assert(concurrency >= 1, 'concurrency must be at least one'),
       assert(workerId != '', 'workerId must not be empty'),
       assert(
         (leaseDuration == null) == (renewInterval == null),
         'leaseDuration and renewInterval must both be set or both be null',
       );

  /// Worker 在 Backend 中的标识。为空表示由注入的 `IdGenerator` 生成一次。
  /// 非空时必须是非空字符串，且在同一次 `start()` 生命周期内稳定。
  final String? workerId;

  /// 单个 Worker 同时执行的 Job 数量上限。必须大于等于 1。
  /// 每次成功 claim 占用一个固定槽位，直到该 execution 完整结束才释放。
  final int concurrency;

  /// 独立周期轮询间隔。必须大于 `Duration.zero`。
  /// 该轮询不依赖 `QueueSignal`：Signal 丢失、关闭或发送失败时仍按此间隔
  /// 从 Backend 领取 Job 并读取控制请求。
  final Duration pollInterval;

  /// 优雅关闭时等待当前 Processor 完成的最长时间。必须大于 `Duration.zero`。
  /// 超时后进入强制关闭路径。没有 active execution 时不会创建该等待。
  final Duration gracefulTimeout;

  /// distributed execution 的租约时长。为空表示 local Profile，绝不续租。
  /// 与 [renewInterval] 必须同时为空或同时非空，且必须大于 `Duration.zero`。
  final Duration? leaseDuration;

  /// distributed execution 的续租间隔。必须大于 `Duration.zero` 且严格小于
  /// [leaseDuration]，否则在租约到期前无法完成续租确认。
  final Duration? renewInterval;
}

/// 校验 [options] 中无法用编译期 assert 表达的运行时约束，并原样返回该配置。
///
/// 供 `Worker` 在构造时调用：即使 `WorkerOptions` 的 const assert 被绕过，
/// 生产路径仍然以 `ArgumentError` 拒绝无效配置。该函数不从
/// `worker/index.dart` 导出，因此不属于模块公开面。
WorkerOptions validateWorkerOptions(WorkerOptions options) {
  final workerId = options.workerId;
  if (workerId != null && workerId.isEmpty) {
    throw ArgumentError.value(workerId, 'workerId', 'must not be empty');
  }
  if (options.concurrency < 1) {
    throw ArgumentError.value(
      options.concurrency,
      'concurrency',
      'must be at least one',
    );
  }
  if (options.pollInterval <= Duration.zero) {
    throw ArgumentError.value(
      options.pollInterval,
      'pollInterval',
      'must be positive',
    );
  }
  if (options.gracefulTimeout <= Duration.zero) {
    throw ArgumentError.value(
      options.gracefulTimeout,
      'gracefulTimeout',
      'must be positive',
    );
  }
  final leaseDuration = options.leaseDuration;
  final renewInterval = options.renewInterval;
  if ((leaseDuration == null) != (renewInterval == null)) {
    throw ArgumentError(
      'leaseDuration and renewInterval must both be set or both be null.',
    );
  }
  if (leaseDuration != null &&
      (leaseDuration <= Duration.zero ||
          renewInterval! <= Duration.zero ||
          renewInterval >= leaseDuration)) {
    throw ArgumentError.value(
      renewInterval,
      'renewInterval',
      'must be positive and shorter than leaseDuration',
    );
  }
  return options;
}
