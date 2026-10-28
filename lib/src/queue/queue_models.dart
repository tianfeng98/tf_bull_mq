import '../core/index.dart';
import 'job.dart';

/// Queue 级配置：只承载默认 QueuePolicy 与共享资源所有权。
///
/// 该类型刻意不复制任何 [JobOptions] 字段（priority、delay、retry、
/// logRetention 等）——Job 级配置由每次生产的 `JobOptions` 决定，
/// Queue 只负责"本句柄新建 Queue 时的默认策略"和"是否随本句柄关闭共享资源"。
///
/// `defaultPolicy` 使用可空命名参数而不是 `const QueuePolicy()` 默认值：
/// 冻结的 [QueuePolicy] 构造函数带参数校验体、不是 const 构造，
/// 无法作为 const 默认值出现；省略时按空 [QueuePolicy] 处理，语义等价于"未指定策略"。
final class QueueOptions {
  /// 构造 Queue 级配置。
  ///
  /// [defaultPolicy]：仅在 Queue state revision 为 0 时由 Queue 安装的默认策略；
  /// 为 null 时使用不含任何限制的空 [QueuePolicy]。
  /// [ownsBackend]：是否由 Queue 负责关闭注入的 Backend，默认 false，允许共享。
  /// [ownsSignal]：是否由 Queue 负责关闭注入的 Signal，默认 false，允许共享。
  QueueOptions({
    QueuePolicy? defaultPolicy,
    this.ownsBackend = false,
    this.ownsSignal = false,
  }) : defaultPolicy = defaultPolicy ?? QueuePolicy();

  /// 首次 Queue mutation 前安装的默认策略；不覆盖已存在或已被其他句柄配置的 Queue state。
  final QueuePolicy defaultPolicy;

  /// 是否由本 Queue 关闭注入的 Backend；默认 false，多个高层句柄可安全共享同一 Backend。
  final bool ownsBackend;

  /// 是否由本 Queue 关闭注入的 Signal；默认 false，多个高层句柄可安全共享同一 Signal。
  final bool ownsSignal;
}

/// `Queue.add` 与 `Queue.addBulk` 的输入草稿。
///
/// 只做输入形状校验（名称、显式 ID），不再直接调用 validateJsonLike——
/// 业务载荷的编码和 JSON-like 校验由 Queue 在 Backend 写入前通过 PayloadCodec 统一完成。
final class QueueJobDraft<TData extends Object?> {
  /// 构造 Job 草稿。
  ///
  /// [name]：Job 名称，必须非空白。
  /// [data]：业务载荷，不需要手动是 JSON-like——Queue 会通过 PayloadCodec 编码。
  /// [jobId]：显式 Job ID；为 null 时由 Queue 的 `IdGenerator` 生成；
  /// 非 null 时必须非空白，且原样保留、不做裁剪或归一化。
  /// [options]：Job 级选项；为 null 时使用 `JobOptions()` 默认值。
  ///
  /// 任何校验失败抛 [ArgumentError]；构造过程不访问 Backend。
  QueueJobDraft({
    required this.name,
    required this.data,
    this.jobId,
    JobOptions? options,
  }) : options = options ?? JobOptions() {
    if (name.trim().isEmpty) {
      throw ArgumentError.value(name, 'name', 'must not be blank');
    }
    final explicitJobId = jobId;
    if (explicitJobId != null && explicitJobId.trim().isEmpty) {
      throw ArgumentError.value(explicitJobId, 'jobId', 'must not be blank');
    }
  }

  /// Job 名称，写入 `JobSpec.name`。
  final String name;

  /// 业务载荷，由 Queue 在 Backend 写入前编码为 JSON-like。
  final TData? data;

  /// 显式 Job ID；为 null 表示由 Queue 生成。
  final String? jobId;

  /// Job 级选项，透传给 `JobSpec.options`。
  final JobOptions options;
}

/// 一页 Job 句柄。
///
/// 只承载 Backend `JobPage` 的查询结果投影：items 与 nextCursor 均来自 Backend，
/// 不由 Queue 排序、合并或推导；cursor 的语义完全由 Backend 定义。
final class QueueJobPage<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  /// 构造一页 Job 句柄，防御性复制 [items]，调用方之后修改原列表不影响本页。
  QueueJobPage({required List<Job<TData, TProgress, TResult>> items, required this.nextCursor})
    : items = List<Job<TData, TProgress, TResult>>.unmodifiable(items);

  /// 不可变 Job 句柄列表，顺序与 Backend 返回顺序一致。
  final List<Job<TData, TProgress, TResult>> items;

  /// 下一页游标；为 null 表示没有更多数据，非 null 时原样透传给 `Queue.query` 的 cursor。
  final String? nextCursor;
}

/// Signal 提示失败时抛出的"已提交但未通知"结果。
///
/// 语义极为关键：本次操作的 Backend 事实**已经提交且不会回滚**，
/// 只是部分或全部 queue 提示没有送达。调用方不得把该异常当作"操作失败"而盲目重试 create，
/// 否则可能得到 [DuplicateJob]；正确做法是读取 [committedValue]（add 为 [JobRecord]，
/// bulk 为 `List<JobRecord>`，Queue mutation 为 [QueueState]，Job mutation 为 [JobRecord]，
/// remove 为 `List<JobKey>`，clear logs 为删除数量），并据此重新查询或继续后续流程。
final class QueueNotificationFailure implements Exception {
  /// 构造通知失败结果。
  ///
  /// [operation]：发生失败的高层操作名（如 `add`、`initializeDefaultPolicy`）。
  /// [committedValue]：成功 Backend 写入的 `BackendWriteResult.value`，与调用方将要获得的事实同源。
  /// [queuesToNotify]：本次 Backend 写入返回的完整通知集合，防御性复制为不可变集合。
  /// [cause]：第一个失败通知的原始异常，保持原样不做包装。
  /// [stackTrace]：第一个失败通知的原始堆栈，供调用方定位真实失败点。
  QueueNotificationFailure({
    required this.operation,
    required this.committedValue,
    required Set<String> queuesToNotify,
    required this.cause,
    required this.stackTrace,
  }) : queuesToNotify = Set<String>.unmodifiable(queuesToNotify);

  /// 发生通知失败的高层操作名。
  final String operation;

  /// 已提交的 Backend 事实；类型随操作而变，见类注释。
  final Object? committedValue;

  /// 本次写入声明的全部通知目标（与是否失败无关，始终是完整集合）。
  final Set<String> queuesToNotify;

  /// 第一个失败通知的原始异常。
  final Object cause;

  /// 第一个失败通知的原始堆栈。
  final StackTrace stackTrace;

  @override
  String toString() =>
      'QueueNotificationFailure: $operation committed but failed to notify '
      '${queuesToNotify.join(', ')}: $cause';
}
