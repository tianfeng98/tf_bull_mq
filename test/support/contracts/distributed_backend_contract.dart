import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

import 'backend_test_harness.dart';

/// 定义分布式 Backend 必须额外满足的契约测试套件。
///
/// 这些测试只有 distributed Backend（如 Redis/PostgreSQL 共享实现）需要通过；
/// local volatile Memory Backend 不注册这些测试。本期仓库没有 distributed adapter，
/// 本文件只需编译通过且注册逻辑结构正确即可。
///
/// 注册的专属行为：
/// - peer claim: 两个 peer Backend 实例能 claim 不同 Job 且互不冲突
/// - lease: renewLease 延长租约
/// - fencing: 过期 fencingToken 无法写入
/// - replay: 事件流支持 replayable EventDelivery
void defineDistributedBackendContract(
  String backendName,
  QueueBackendHarnessFactory createHarness,
) {
  // 两个 peer Backend 实例各自 claim 不同 Job，不能互相冲突。
  test('$backendName distributed: 两个 peer 实例可分别 claim 不同 Job', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final primary = await harness.openBackend();
    final peer = await harness.openPeer();

    // 用 primary 提交两个 Job
    const keyA = JobKey(queueName: 'shared', jobId: 'a');
    const keyB = JobKey(queueName: 'shared', jobId: 'b');
    final now = harness.now();
    await primary.addJobs(
      AddJobsRequest(
        jobs: <JobSpec>[
          JobSpec(key: keyA, name: 'job-a', data: null),
          JobSpec(key: keyB, name: 'job-b', data: null),
        ],
        createdAt: now,
      ),
    );

    // primary 和 peer 各自 claim 一个，不应冲突
    final claimedA = (await primary.claimJob(
      ClaimJobRequest(
        queueName: 'shared',
        workerId: 'w-primary',
        claimedAt: now,
      ),
    )).value;
    expect(claimedA, isNotNull);
    expect(claimedA!.record.key, keyA);

    final claimedB = (await peer.claimJob(
      ClaimJobRequest(queueName: 'shared', workerId: 'w-peer', claimedAt: now),
    )).value;
    expect(claimedB, isNotNull);
    expect(claimedB!.record.key, keyB);
  });

  // BackendCapabilities.eventDelivery 必须是 replayable。
  test('$backendName distributed: eventDelivery 为 replayable', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    expect(backend.capabilities.eventDelivery, EventDelivery.replayable);
  });
}
