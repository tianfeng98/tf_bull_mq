import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

/// QueueEvents 测试的能力夹具：按给定 profile 和 delivery 构造稳定的 [BackendCapabilities]。
///
/// 所有 QueueEvents 单元测试共享同一套 capabilities，只在需要特定组合（如分布式 profile）
/// 时通过命名参数覆盖，避免每次手写完整 BackendCapabilities 构造。
BackendCapabilities eventCapabilities({
  BackendProfile profile = BackendProfile.local,
  EventDelivery delivery = EventDelivery.liveOnly,
}) {
  return BackendCapabilities(
    profile: profile,
    durability: Durability.volatile,
    delayedJobs: true,
    priorities: true,
    atomicFlows: true,
    eventDelivery: delivery,
  );
}

/// 按给定队列名和位置构造一条确定性的 [PositionedJobEvent]。
///
/// eventId 格式为 `evt-<position>`，jobId 格式为 `job-<position>`，occurredAt 固定为
/// 2025-01-01。所有字段可预测，便于测试断言。
PositionedJobEvent positionedEvent(String queueName, int position) {
  final cursor = EventCursor(queueName: queueName, position: position);
  final event = JobEvent(
    eventId: 'evt-$position',
    jobKey: JobKey(queueName: queueName, jobId: 'job-$position'),
    revision: 0,
    type: JobEventType.added,
    occurredAt: DateTime.utc(2025, 1, 1),
  );
  return PositionedJobEvent(cursor: cursor, event: event);
}

/// 按给定事件列表和可选的 nextCursor 构造 [JobEventPage]。
///
/// 与 [positionedEvent] 搭配使用，快速拼出 Backend readJobEvents 的返回值。
JobEventPage eventPage(
  List<PositionedJobEvent> items, {
  EventCursor? nextCursor,
}) {
  return JobEventPage(items: items, nextCursor: nextCursor);
}
