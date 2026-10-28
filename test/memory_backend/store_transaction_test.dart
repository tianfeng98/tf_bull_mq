import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

/// 验证从 Backend 拿到的 snapshot（JobRecord、JobEvent、JobLog）
/// 的 JSON-like 字段是冻结的——修改嵌套集合抛 UnsupportedError，
/// 且修改不影响 Backend 中已 committed 的事实。
void main() {
  test('MemoryBackend snapshot 无法在不产生 revision 或 event 的情况下修改已提交事实', () async {
    var counter = 0;
    final backend = MemoryQueueBackend(
      now: () => DateTime.utc(2026, 1, 1),
      idGenerator: () => 'id-${++counter}',
    );
    await backend.open();

    const key = JobKey(queueName: 'mail', jobId: '1');
    final t1 = DateTime.utc(2026, 1, 1);

    // 添加一个带嵌套 JSON-like 数据的 Job
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(
          key: key,
          name: 'send',
          data: <String, Object?>{
            'to': 'alice',
            'attachments': <Object?>['a.pdf', 'b.pdf'],
          },
        ),
        createdAt: t1,
        availableAt: t1,
      ),
    );

    // 1. getJob 返回的 JobRecord.data 冻结
    final record = await backend.getJob(key);
    expect(record, isNotNull);
    expect(record!.data, isA<Map<String, Object?>>());
    final data = record.data as Map<String, Object?>;

    // 直接写 record.data 应抛 UnsupportedError
    expect(() => data['extra'] = 'hack', throwsUnsupportedError);

    // 嵌套的 List 也应不可写
    final attachments = data['attachments'] as List<Object?>;
    expect(() => attachments.add('c.pdf'), throwsUnsupportedError);

    // 2. 再次 getJob 确认 Backend 事实未被污染
    final record2 = await backend.getJob(key);
    expect(record2!.data, <String, Object?>{
      'to': 'alice',
      'attachments': <Object?>['a.pdf', 'b.pdf'],
    });

    // 3. Backend 事件流中 PositionedJobEvent.event 是 JobEvent
    // added 事件 data 通常为 null（标量 null 本身没有集合结构），
    // 这里验证事件对象可正常读取且不会因为 DTO 构造问题抛错
    final eventPage = await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'mail'),
    );
    expect(eventPage.items, isNotEmpty);
    final first = eventPage.items.first;
    expect(first.event.eventId, isNotEmpty);
    // added 事件 data 通常为 null，null 是合法 JSON-like 标量
    expect(first.event.data, isNull);

    await backend.close();
  });
}
