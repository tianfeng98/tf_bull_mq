import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

/// 用可提前的 now() 创建 backend，方便控制时间触发 eventRetention 裁剪。
MemoryQueueBackend _backend({DateTime Function()? now}) {
  var counter = 0;
  final b = MemoryQueueBackend(
    now: now ?? () => DateTime.utc(2026, 1, 1),
    idGenerator: () => 'id-${++counter}',
  );
  b.open();
  return b;
}

void main() {
  test(
    '执行保留量裁剪后会更新 trimmedEventPositionByQueue',
    () async {
      final t0 = DateTime.utc(2026, 1, 1);
      var now = t0;
      final backend = _backend(now: () => now);

      // 1. 创建一个 Job（会触发 added 事件）
      const key = JobKey(queueName: 'e', jobId: 'k1');
      await backend.addJob(
        AddJobRequest(
          job: JobSpec(key: key, name: 'k1', data: null),
          createdAt: t0,
          availableAt: t0,
        ),
      );

      // 2. 正常读事件没问题
      final before = await backend.readJobEvents(
        ReadJobEventsRequest(queueName: 'e', cursor: null, limit: 10),
      );
      expect(before.items.length, greaterThan(0));
      final latestPos = before.items.last.cursor.position;

      // 3. 设置 eventRetention=1s，让旧事件可被裁剪
      await backend.setQueuePolicy(
        SetQueuePolicyRequest(
          queueName: 'e',
          expectedRevision: (await backend.getQueueState('e')).revision,
          policy: QueuePolicy(eventRetention: const Duration(seconds: 1)),
          occurredAt: t0,
        ),
      );

      // 4. 让时间前进 10s 触发裁剪
      now = t0.add(const Duration(seconds: 10));
      // 再读一次——会触发 _trimEvents 把 10s 前的 added 事件裁掉
      final afterTrim = await backend.readJobEvents(
        ReadJobEventsRequest(queueName: 'e', cursor: null, limit: 10),
      );
      // 列表为空（唯一的 added 事件被裁剪）
      expect(afterTrim.items, isEmpty);

      // 5. 用旧 cursor position 读取——应该抛 EventCursorGap
      expect(
        () => backend.readJobEvents(
          ReadJobEventsRequest(
            queueName: 'e',
            cursor: EventCursor(queueName: 'e', position: latestPos),
            limit: 10,
          ),
        ),
        throwsA(isA<EventCursorGap>()),
      );
    },
  );

  test('按 maxEventCount 裁剪时会更新 trimmedEventPositionByQueue', () async {
    final backend = _backend();

    // 先创建队列并设置 maxEventCount=2
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(
          key: JobKey(queueName: 'q', jobId: 'setup'),
          name: 'setup',
          data: null,
        ),
        createdAt: DateTime.utc(2026, 1, 1),
        availableAt: DateTime.utc(2026, 1, 1),
      ),
    );
    await backend.setQueuePolicy(
      SetQueuePolicyRequest(
        queueName: 'q',
        expectedRevision: (await backend.getQueueState('q')).revision,
        policy: QueuePolicy(maxEventCount: 2),
        occurredAt: DateTime.utc(2026, 1, 1),
      ),
    );

    // 快速添加 5 个 Job，每次触发裁剪只保留最新 2 条事件
    for (var i = 1; i <= 5; i++) {
      await backend.addJob(
        AddJobRequest(
          job: JobSpec(
            key: JobKey(queueName: 'q', jobId: 'j$i'),
            name: 'j$i',
            data: null,
          ),
          createdAt: DateTime.utc(2026, 1, 1),
          availableAt: DateTime.utc(2026, 1, 1),
        ),
      );
    }

    // 旧 cursor（position=0）必须抛 EventCursorGap
    expect(
      () => backend.readJobEvents(
        ReadJobEventsRequest(
          queueName: 'q',
          cursor: EventCursor(queueName: "q", position: 1),
          limit: 10,
        ),
      ),
      throwsA(isA<EventCursorGap>()),
    );
  });
}
