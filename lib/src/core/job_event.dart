import 'job_key.dart';
import 'json_like.dart';

/// 事件游标：用于 Backend 事件流中的位置标识。
///
/// 由 [queueName] 和 [position] 组成；position 是 Backend 分配的单调递增整数，
/// 从 1 开始。构造时校验 position >= 1。
class EventCursor {
  EventCursor({required this.queueName, required this.position}) {
    if (position < 1) {
      throw ArgumentError.value(position, 'position');
    }
  }

  /// 游标所属队列名称。
  final String queueName;

  /// Backend 分配的单调递增位置。从 1 开始。
  final int position;

  @override
  bool operator ==(Object other) =>
      other is EventCursor &&
      other.queueName == queueName &&
      other.position == position;

  @override
  int get hashCode => Object.hash(queueName, position);
}

/// Job 事件的类型。
///
/// Backend 在 Job 状态变化或进度更新时产出对应类型的事件。
enum JobEventType {
  added,
  waiting,
  active,
  progress,
  completed,
  failed,
  delayed,
  paused,
  resumed,
  canceled,
  stalled,
  removed,
}

/// Backend 产出的 Job 事件（原始 JSON-like 模型，非泛型）。
///
/// 构造时校验 [eventId] 非空、[revision] 非负，以及 [data] 必须为 JSON-like。
class JobEvent {
  JobEvent({
    required this.eventId,
    required this.jobKey,
    required this.revision,
    required this.type,
    required this.occurredAt,
    Object? data,
  }) : data = freezeJsonLike(data, argumentName: 'data') {
    if (eventId.isEmpty || revision < 0) {
      throw ArgumentError('事件标识和 revision 非法');
    }
  }

  /// Backend 分配的唯一事件标识。非空。
  final String eventId;

  /// 事件关联的 Job 复合标识。
  final JobKey jobKey;

  /// 事件发生时 Job 的 revision。非负。
  final int revision;

  /// 事件类型。
  final JobEventType type;

  /// 事件发生的时间点。
  final DateTime occurredAt;

  /// 事件携带的已冻结 JSON-like 附加数据（如 progress 更新值）。可空。
  final Object? data;
}

/// 带位置信息的 Job 事件（原始 JSON-like 模型，非泛型）。
///
/// Backend 的事件流读取接口返回本类型，配合 [EventCursor] 支持游标式分页消费。
class PositionedJobEvent {
  const PositionedJobEvent({required this.cursor, required this.event});

  /// 事件在 Backend 事件流中的游标位置。
  final EventCursor cursor;

  /// 实际的 Job 事件内容。
  final JobEvent event;
}

/// 游标缺失：请求的位置已被 Backend 事件历史裁剪掉。
///
/// 当调用方传入的游标位置早于 Backend 当前最早可读位置时返回此 gap，
/// 调用方需要从中断处恢复消费或重置游标。
class EventGap {
  const EventGap({required this.requested, required this.earliestAvailable});

  /// 调用方请求的游标。
  final EventCursor requested;

  /// Backend 当前最早可读的游标。
  final EventCursor earliestAvailable;
}
