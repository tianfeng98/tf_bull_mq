/// Job 生命周期状态。
///
/// 状态转换由 Backend 的冻结状态机判定：不同状态之间允许的转移路径不同。
/// - `waiting`：等待领取
/// - `delayed`：延迟领取（`availableAt` 未到期）
/// - `waitingChildren`：等待子 Job 全部完成
/// - `active`：正在执行
/// - `paused`：被单独暂停（可通过 `resume()` 恢复，保留进度）
/// - `completed`：已完成（终态，不可逆，不可重试）
/// - `failed`：已失败（当前执行结束，可显式 retry 重新入队）
/// - `canceled`：已取消（当前执行结束，可显式 retry 重新入队，重置进度但保留业务数据）
enum JobStatus {
  /// 等待领取状态。
  waiting,

  /// 延迟领取状态。
  delayed,

  /// 等待子 Job 全部完成状态。
  waitingChildren,

  /// 正在执行状态。
  active,

  /// 被单独暂停状态（通过 resume 恢复，保留 progress）。
  paused,

  /// 已完成状态（终态，不可逆）。
  completed,

  /// 已失败状态（当前执行结束，可显式 retry）。
  failed,

  /// 已取消状态（当前执行结束，可显式 retry，重置 progress 但保留 data/计数/JobLog）。
  canceled,
}
