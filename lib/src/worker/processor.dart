import 'dart:async';

import 'active_job.dart';

/// 单个 Job 的业务处理函数。
///
/// [job] 只在本次 claim 产生的 execution credential 有效期内可用。Worker 调用
/// 本函数最多一次，且不承诺 exactly-once：Backend 不可用或租约丢失时 Job 可能被
/// 重新领取并再次执行，Processor 需要自行保证业务幂等。
///
/// 返回值必须是 JSON-like 值（`null`、`bool`、`String`、有限 `num`、`List` 或
/// `Map<String, ...>` 的嵌套组合）；返回非 JSON-like 值会被归类为 Processor
/// 业务失败。抛出的异常同样按业务失败处理，由 Worker 依据 Job 的 `RetryPolicy`
/// 决定重试、延迟或最终失败。
typedef Processor<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> = FutureOr<TResult?> Function(ActiveJob<TData, TProgress, TResult> job);
