/// 退避策略。
///
/// - `fixed`：每次重试固定等待 [RetryPolicy.delay]
/// - `exponential`：每次重试等待时间翻倍（上限 [RetryPolicy.maxDelay]）
enum BackoffPolicy { fixed, exponential }

/// Job 级重试策略。
///
/// 由 Processor 抛出业务异常时 Worker 使用：依据 attemptsMade 和本策略计算
/// 下次重试的目标状态（waiting 或 delayed）。
final class RetryPolicy {
  RetryPolicy({
    this.maxAttempts = 1,
    this.backoff = BackoffPolicy.fixed,
    this.delay = Duration.zero,
    this.maxDelay,
  }) {
    if (maxAttempts < 1 || delay.isNegative || maxDelay?.isNegative == true) {
      throw ArgumentError('重试参数范围越界');
    }
  }

  /// 最大尝试次数。必须 >= 1，默认 1（不重试）。
  final int maxAttempts;

  /// 退避策略类型。默认固定。
  final BackoffPolicy backoff;

  /// 退避基准时长。必须 >= Duration.zero。
  final Duration delay;

  /// 指数退避的上限（避免无限增长）。可空。
  final Duration? maxDelay;

  /// 计算第 [attempt] 次重试的等待时长（从 1 开始计数）。
  ///
  /// - fixed：直接返回 [delay]
  /// - exponential：返回 `delay * 2^(attempt-1)`，但不超过 [maxDelay]
  Duration delayForAttempt(int attempt) {
    if (attempt < 1) {
      throw ArgumentError.value(attempt, 'attempt');
    }
    if (backoff == BackoffPolicy.fixed) {
      return delay;
    }
    // 计算乘数，上限为 2^30 防止溢出
    var multiplier = 1;
    for (var index = 1; index < attempt; index++) {
      multiplier = multiplier > 1 << 30 ? 1 << 30 : multiplier * 2;
    }
    final candidate = delay * multiplier;
    return maxDelay != null && candidate > maxDelay! ? maxDelay! : candidate;
  }
}

/// Job 级运行选项。
///
/// 每个 Job 创建时独立指定，Backed 原样保存。构造时校验范围合法性。
final class JobOptions {
  JobOptions({
    this.priority = 0,
    this.delay = Duration.zero,
    RetryPolicy? retry,
    this.maxStalledCount = 1,
    this.logRetention,
    this.stackTraceLimit = 10,
  }) : retry = retry ?? RetryPolicy() {
    if (priority < 0 ||
        delay.isNegative ||
        maxStalledCount < 0 ||
        logRetention != null && logRetention! < 0 ||
        stackTraceLimit < 0) {
      throw ArgumentError('Job 选项参数范围越界');
    }
  }

  /// 优先级，数值越大越优先被领取。必须 >= 0，默认 0。
  final int priority;

  /// 延迟领取时长。必须 >= Duration.zero，默认零（立即领取）。
  final Duration delay;

  /// 重试策略。默认不重试（maxAttempts = 1）。
  final RetryPolicy retry;

  /// 最大允许的 stalled 次数，超过后 Backed 将 Job 标记为 failed。默认 1。
  final int maxStalledCount;

  /// 保留的日志条数上限（可空表示无限）。用于 Backend 自动清理。
  final int? logRetention;

  /// 持久化到 JobFailure 的堆栈条数上限。默认 10，设为 0 时不保存堆栈。
  final int stackTraceLimit;
}
