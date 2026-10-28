import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

import 'backend_test_harness.dart';

/// 定义持久化本地 Backend 必须额外满足的契约测试套件。
///
/// 这些测试只有 durable local Backend（如 SQLite/PostgreSQL 实现）需要通过；
/// volatile Memory Backend 不注册这些测试。本期仓库没有 durable adapter，
/// 本文件只需编译通过且注册逻辑结构正确即可。
///
/// 注册的专属行为：
/// - reopen 后数据完整恢复（reopenBackend 返回的 Backend 能读到 reopen 前提交的 Job）
void defineDurableLocalBackendContract(
  String backendName,
  QueueBackendHarnessFactory createHarness,
) {
  // reopen 后数据必须完整恢复：Backed 关闭再打开后，之前 add 的 Job 仍然存在，
  // revision 与关闭前一致，Flow 边结构完整。
  test('$backendName durable: reopenBackend 后数据完整恢复', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();

    // 提交一些事实
    const key = JobKey(queueName: 'persist', jobId: 'j1');
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'persistent', data: null),
        createdAt: harness.now(),
        availableAt: harness.now(),
      ),
    );

    // reopen 后应能读到之前的 Job
    final reopened = await harness.reopenBackend();
    final record = await reopened.getJob(key);
    expect(record, isNotNull);
    expect(record!.revision, 1);
    expect(record.status, JobStatus.waiting);
  });
}
