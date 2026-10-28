import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

import '../support/memory_backend_harness.dart';

void main() {
  // 验证 MemoryQueueBackend 的基础原子操作：addJob 后 getJob 能正确返回 waiting 状态的记录，
  // 同时 readJobEvents 会产生一条 JobEventType.added 事件。
  test('addJob 成功后可 getJob 并产生 added 事件', () async {
    final harness = MemoryQueueBackendHarness();
    final backend = await harness.openBackend();
    final key = const JobKey(queueName: 'mail', jobId: '1');
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'send', data: null),
        createdAt: harness.now(),
        availableAt: harness.now(),
      ),
    );
    final record = await backend.getJob(key);
    final events = await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'mail'),
    );
    expect(record!.status, JobStatus.waiting);
    expect(events.items.single.event.type, JobEventType.added);
    await harness.dispose();
  });

  // 验证 claimJob 的原子互斥性：同一个 waiting Job 只能被第一个 claim 者拿到，
  // 后续 claim 者返回 null，确保不会出现重复领取。
  test('同一个 waiting Job 只能被 claim 一次，后续 claim 返回 null', () async {
    final harness = MemoryQueueBackendHarness();
    final backend = await harness.openBackend();
    final key = const JobKey(queueName: 'work', jobId: '1');
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'work', data: null),
        createdAt: harness.now(),
        availableAt: harness.now(),
      ),
    );
    // 第一次 claim 由 worker-1 发起，应成功拿到 Job。
    final first = await backend.claimJob(
      ClaimJobRequest(
        queueName: 'work',
        workerId: 'worker-1',
        claimedAt: harness.now(),
      ),
    );
    // 第二次 claim 由 worker-2 发起，应返回 null。
    final second = await backend.claimJob(
      ClaimJobRequest(
        queueName: 'work',
        workerId: 'worker-2',
        claimedAt: harness.now(),
      ),
    );
    expect(first.value, isNotNull);
    expect(second.value, isNull);
    await harness.dispose();
  });
}
