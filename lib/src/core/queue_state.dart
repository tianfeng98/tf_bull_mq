/// Queue 级策略配置。
///
/// 所有字段可空；非空时 Backend 按策略执行自动清理（日志保留条数、事件数量上限、
/// 事件保留时长）。构造时校验范围合法性。
final class QueuePolicy {
  QueuePolicy({
    this.defaultJobLogRetention,
    this.maxEventCount,
    this.eventRetention,
  }) {
    if (defaultJobLogRetention != null && defaultJobLogRetention! < 0 ||
        maxEventCount != null && maxEventCount! <= 0 ||
        eventRetention != null && eventRetention!.inMicroseconds <= 0) {
      throw ArgumentError('队列策略参数范围越界');
    }
  }

  /// Job 日志默认保留条数（可空表示无限）。
  final int? defaultJobLogRetention;

  /// 队列事件历史的最大条数（可空表示不限制）。超过时 Backend 自动裁剪最早的事件。
  final int? maxEventCount;

  /// 队列事件历史的保留时长（可空表示不限制）。超时事件会被 Backend 自动裁剪。
  final Duration? eventRetention;
}

/// Queue 级持久化状态。
///
/// 包含队列名称、暂停标记、策略配置和乐观并发 revision。
/// Backed 在首次被 Queue 操作访问时自动安装默认策略（revision 0 -> 1）。
final class QueueState {
  const QueueState({
    required this.queueName,
    required this.paused,
    required this.policy,
    required this.revision,
  });

  /// 队列名称。非空。
  final String queueName;

  /// 队列是否被暂停。暂停时 Worker 不能 claim 新 Job，但 active Job 不受影响。
  final bool paused;

  /// 队列级策略配置。
  final QueuePolicy policy;

  /// 乐观并发 revision。每次写入递增。
  final int revision;
}

/// Job 控制操作类型。
enum JobControlAction { pause, cancel }

/// Job 控制请求：由外部提出、Backend 记录、Worker 执行。
///
/// Worker claim 后，Backend 中会保留这个请求直到 Worker 确认（confirmPaused/confirmCanceled）
/// 或 Job 被终止。
final class JobControlRequest {
  const JobControlRequest({required this.action, required this.requestedAt});

  /// 请求类型（暂停或取消）。
  final JobControlAction action;

  /// 请求到达 Backend 的时间点。
  final DateTime requestedAt;
}
