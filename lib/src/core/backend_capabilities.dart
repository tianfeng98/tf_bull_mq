/// Backend 的运行配置档。
///
/// - `local`：单进程内存 Backend，不提供分布式能力
/// - `distributed`：跨进程共享 Backend（如 Redis/PostgreSQL 实现）
enum BackendProfile { local, distributed }

/// Backend 的持久化能力。
///
/// - `volatile`：进程重启后数据丢失（如内存 Backend）
/// - `durable`：进程重启后数据保留（如数据库 Backend）
enum Durability { volatile, durable }

/// Backend 的事件投递能力。
///
/// - `liveOnly`：只支持在线读取，不支持回放
/// - `replayable`：支持游标式回放（distributed Backend 必须具备）
enum EventDelivery { liveOnly, replayable }

/// Backend 能力声明。
///
/// 每个 Backend 实现必须声明自身能力，高层组件据此做 capability preflight：
/// 不满足要求时静默降级会让调用方误以为功能已生效，因此能力不足必须显式抛错。
///
/// 构造约束：`distributed` Backend 必须同时具备 `replayable` 事件投递能力。
final class BackendCapabilities {
  BackendCapabilities({
    required this.profile,
    required this.durability,
    required this.delayedJobs,
    required this.priorities,
    required this.atomicFlows,
    required this.eventDelivery,
  }) {
    if (profile == BackendProfile.distributed &&
        eventDelivery != EventDelivery.replayable) {
      throw ArgumentError('distributed Backend 必须具备可回放事件能力');
    }
  }

  /// 运行配置档（本地或分布式）。
  final BackendProfile profile;

  /// 持久化能力（易失或持久）。
  final Durability durability;

  /// 是否支持延迟领取（Job 的 `delay` 参数）。
  final bool delayedJobs;

  /// 是否支持优先级调度（Job 的 `priority` 参数）。
  final bool priorities;

  /// 是否支持原子 Flow 创建（一次性写入整棵依赖树）。
  final bool atomicFlows;

  /// 事件投递能力（仅在线或可回放）。
  final EventDelivery eventDelivery;
}
