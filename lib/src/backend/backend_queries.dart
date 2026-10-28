import '../core/job_event.dart';
import '../core/job_key.dart';
import '../core/job_log.dart';
import '../core/job_record.dart';
import '../core/job_status.dart';

/// Backend 查询请求 DTO 和结果集合。

/// Job 分页查询的请求参数。
///
/// 构造时校验 [limit] 在 `[1, 1000]` 范围内。
final class JobQuery {
  JobQuery({
    required this.queueName,
    Set<JobStatus> statuses = const <JobStatus>{},
    this.cursor,
    this.limit = 100,
  }) : statuses = Set<JobStatus>.unmodifiable(statuses) {
    _validateLimit(limit);
  }

  /// 目标队列名称。
  final String queueName;

  /// 状态过滤集合（空集合表示不过滤）。不可变。
  final Set<JobStatus> statuses;

  /// Backend 定义的下一页游标；为 null 时从头开始。
  final String? cursor;

  /// 每页数量。范围 `[1, 1000]`。
  final int limit;
}

/// Job 分页查询的一页结果（非泛型原始模型）。
final class JobPage {
  const JobPage({required this.items, required this.nextCursor});

  /// 本页的 Job 记录列表。
  final List<JobRecord> items;

  /// 下一页的游标；为 null 表示没有更多数据。
  final String? nextCursor;
}

/// Job 日志分页查询的请求参数。
///
/// 构造时校验 [limit] 在 `[1, 1000]` 范围内。
final class JobLogQuery {
  JobLogQuery({
    required this.key,
    this.attempt,
    Set<JobLogLevel> levels = const <JobLogLevel>{},
    Set<JobLogChannel> channels = const <JobLogChannel>{},
    this.afterSequence,
    this.limit = 100,
  }) : levels = Set<JobLogLevel>.unmodifiable(levels),
       channels = Set<JobLogChannel>.unmodifiable(channels) {
    _validateLimit(limit);
  }

  /// 目标 Job 复合标识。
  final JobKey key;

  /// 按尝试序号过滤（可空表示不过滤）。
  final int? attempt;

  /// 日志级别过滤集合（空集合表示不过滤）。不可变。
  final Set<JobLogLevel> levels;

  /// 日志通道过滤集合（空集合表示不过滤）。不可变。
  final Set<JobLogChannel> channels;

  /// 独占日志游标，只返回 sequence > afterSequence 的日志（可空表示从头开始）。
  final int? afterSequence;

  /// 每页数量。范围 `[1, 1000]`。
  final int limit;
}

/// Job 日志分页查询的一页结果（非泛型原始模型）。
final class JobLogPage {
  const JobLogPage({required this.items, required this.nextSequence});

  /// 本页的日志列表。
  final List<JobLog> items;

  /// 下一页的 sequence 游标；为 null 表示没有更多数据。
  final int? nextSequence;
}

/// 读取队列事件流的请求参数。
///
/// 构造时校验 [limit] 在 `[1, 1000]` 范围内。
final class ReadJobEventsRequest {
  const ReadJobEventsRequest({
    required this.queueName,
    this.cursor,
    this.limit = 100,
  });

  /// 目标队列名称。
  final String queueName;

  /// 起始游标（可空表示从 Backend 认为最早可读的位置开始）。
  final EventCursor? cursor;

  /// 每页数量。范围 `[1, 1000]`。
  final int limit;
}

/// 队列事件流分页读取的一页结果（非泛型原始模型）。
final class JobEventPage {
  const JobEventPage({required this.items, required this.nextCursor});

  /// 本页的带位置事件列表。
  final List<PositionedJobEvent> items;

  /// 下一页的游标；为 null 表示没有更多数据。
  final EventCursor? nextCursor;
}

/// 统一的 limit 校验。[limit] 必须在 `[1, 1000]` 范围内。
void _validateLimit(int limit) {
  if (limit < 1 || limit > 1000) throw ArgumentError.value(limit, 'limit');
}
