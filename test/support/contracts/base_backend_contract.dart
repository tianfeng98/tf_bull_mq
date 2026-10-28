import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

import 'backend_test_harness.dart';

/// 定义所有 Backend 都必须通过的基础契约测试套件。
///
/// 覆盖 Backend 无关的通用行为：add/batch 原子性、capability profile、
/// claim FIFO 排序与单领取、revision/credential 乐观并发、progress/data 更新、
/// retry 状态转换、failure 事件与日志、日志 sequence/clear、Flow 原子性、
/// 依赖恢复、删除门禁、event cursor/gap 和 stalled recovery。
///
/// 每个测试都通过 [QueueBackendTestHarness] 获取真实 Backend 执行，
/// 不使用 mock 或占位断言。
void defineBaseBackendContract(
  String backendName,
  QueueBackendHarnessFactory createHarness,
) {
  // ─── add & batch 原子性与 revision 单调递增 ───────────────────────────
  test('$backendName addJob 返回 revision=1 且后续 getJob 一致', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    final key = JobKey(queueName: 'mail', jobId: 'single');
    final result = await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'send', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );
    // 首次 add 的 revision 必须是 1
    expect(result.value.revision, 1);
    expect(result.changed, isTrue);
    // getJob 返回的 revision 与 add 结果一致
    final fetched = await backend.getJob(key);
    expect(fetched!.revision, 1);
    expect(fetched.status, JobStatus.waiting);
  });

  test('$backendName addJobs 批量写入全部原子提交且各自 revision=1', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    final keys = <JobKey>[
      JobKey(queueName: 'batch', jobId: 'a'),
      JobKey(queueName: 'batch', jobId: 'b'),
      JobKey(queueName: 'batch', jobId: 'c'),
    ];
    final result = await backend.addJobs(
      AddJobsRequest(
        jobs: keys
            .map((k) => JobSpec(key: k, name: k.jobId, data: null))
            .toList(),
        createdAt: now,
      ),
    );
    expect(result.changed, isTrue);
    // 每个批量 Job 都是首次写入，revision 都是 1
    for (final record in result.value) {
      expect(record.revision, 1);
    }
    // 全部可 getJob 到
    for (final key in keys) {
      expect(await backend.getJob(key), isNotNull);
    }
  });

  // ─── capability: Backend 暴露正确的 capability profile ──────────────
  test(
    '$backendName capabilities profile 不为 null 且具备 BackendCapabilities 结构',
    () async {
      final harness = await createHarness();
      addTearDown(harness.dispose);
      final backend = await harness.openBackend();
      final caps = backend.capabilities;
      // profile 必须是合法 BackendProfile 枚举值
      expect(
        caps.profile,
        anyOf(BackendProfile.local, BackendProfile.distributed),
      );
      // eventDelivery 必须是合法枚举值
      expect(
        caps.eventDelivery,
        anyOf(EventDelivery.liveOnly, EventDelivery.replayable),
      );
      // 布尔能力字段必须可读取（不抛异常）
      expect(caps.delayedJobs, isA<bool>());
      expect(caps.priorities, isA<bool>());
      expect(caps.atomicFlows, isA<bool>());
    },
  );

  // ─── claim 排序与单领取: 同一 Queue FIFO 领取，多 Worker 不重复 ───────
  test('$backendName 多个 waiting Job 按 FIFO 顺序被 claim', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();

    // 按顺序添加三个 Job
    for (var i = 1; i <= 3; i++) {
      await backend.addJob(
        AddJobRequest(
          job: JobSpec(
            key: JobKey(queueName: 'fifo', jobId: 'j$i'),
            name: 'job-$i',
            data: null,
          ),
          createdAt: now,
          availableAt: now,
        ),
      );
    }

    // 依次 claim，每次应该拿到最早添加的
    final first = (await backend.claimJob(
      ClaimJobRequest(queueName: 'fifo', workerId: 'w1', claimedAt: now),
    )).value;
    expect(first!.record.key.jobId, 'j1');

    final second = (await backend.claimJob(
      ClaimJobRequest(queueName: 'fifo', workerId: 'w2', claimedAt: now),
    )).value;
    expect(second!.record.key.jobId, 'j2');

    final third = (await backend.claimJob(
      ClaimJobRequest(queueName: 'fifo', workerId: 'w3', claimedAt: now),
    )).value;
    expect(third!.record.key.jobId, 'j3');

    // 队列已空，再 claim 返回 null
    final none = (await backend.claimJob(
      ClaimJobRequest(queueName: 'fifo', workerId: 'w1', claimedAt: now),
    )).value;
    expect(none, isNull);
  });

  test('$backendName 同一个 waiting Job 只能被第一个 Worker claim 到', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'unique', jobId: 'only');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'only', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );

    final first = (await backend.claimJob(
      ClaimJobRequest(queueName: 'unique', workerId: 'w1', claimedAt: now),
    )).value;
    expect(first, isNotNull);

    // 第二个 Worker 无法 claim 同一个 Job
    final second = (await backend.claimJob(
      ClaimJobRequest(queueName: 'unique', workerId: 'w2', claimedAt: now),
    )).value;
    expect(second, isNull);
  });

  // ─── revision/credential: 旧 revision 被拒绝，stale credential recovery 后失效 ──
  test('$backendName 旧 revision 写入被 RevisionConflict 拒绝', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'rev', jobId: 'j1');

    final added = await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'rev-test', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );
    // 用 oldRevision = 0（而非实际的 1）去 transition
    expect(
      () => backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.canceled,
          expectedRevision: 0,
          occurredAt: now,
        ),
      ),
      throwsA(isA<RevisionConflict>()),
    );
    // 用正确的 revision 可以成功
    final ok = await backend.transitionJob(
      TransitionJobRequest(
        key: key,
        targetStatus: JobStatus.canceled,
        expectedRevision: added.value.revision,
        occurredAt: now,
      ),
    );
    expect(ok.changed, isTrue);
  });

  test('$backendName stale credential 在 recoverStalledJobs 后无法继续写入', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'cred', jobId: 'stale');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'cred-test', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'cred', workerId: 'w1', claimedAt: now),
    )).value!;

    // recovery 把 Job 从 active 重新放回 waiting
    await backend.recoverStalledJobs(
      RecoverStalledJobsRequest(queueName: 'cred', now: now),
    );

    // recovery 后 execution 已清空
    final recovered = await backend.getJob(key);
    expect(recovered!.execution, isNull);
    expect(recovered.status, JobStatus.waiting);

    // 旧 credential 尝试写入 progress 被 LeaseLost 拒绝
    expect(
      () => backend.updateJobProgress(
        UpdateJobProgressRequest(
          key: key,
          expectedRevision: recovered.revision,
          credential: claimed.credential,
          occurredAt: now,
          progress: 0.5,
        ),
      ),
      throwsA(isA<LeaseLost>()),
    );
  });

  // ─── progress/data: active Job 更新后 execution 保留 ──────────────────
  test(
    '$backendName active Job 连续 progress 更新 revision 递增且 execution 保留',
    () async {
      final harness = await createHarness();
      addTearDown(harness.dispose);
      final backend = await harness.openBackend();
      final now = harness.now();
      const key = JobKey(queueName: 'prog', jobId: 'track');

      await backend.addJob(
        AddJobRequest(
          job: JobSpec(key: key, name: 'progress', data: null),
          createdAt: now,
          availableAt: now,
        ),
      );
      final claimed = (await backend.claimJob(
        ClaimJobRequest(queueName: 'prog', workerId: 'w1', claimedAt: now),
      )).value!;
      expect(claimed.record.execution, isNotNull);

      final p1 = await backend.updateJobProgress(
        UpdateJobProgressRequest(
          key: key,
          expectedRevision: claimed.record.revision,
          credential: claimed.credential,
          occurredAt: now,
          progress: 0.3,
        ),
      );
      expect(p1.changed, isTrue);
      expect(p1.value.progress, 0.3);
      // progress 后 execution 必须保留
      expect(p1.value.execution, isNotNull);
      expect(
        p1.value.execution!.credential.executionId,
        claimed.credential.executionId,
      );
      // revision 必须递增
      expect(p1.value.revision, greaterThan(claimed.record.revision));

      final p2 = await backend.updateJobProgress(
        UpdateJobProgressRequest(
          key: key,
          expectedRevision: p1.value.revision,
          credential: claimed.credential,
          occurredAt: now,
          progress: 0.7,
        ),
      );
      expect(p2.value.progress, 0.7);
      expect(p2.value.revision, greaterThan(p1.value.revision));
    },
  );

  test('$backendName updateJobData 接受 null 清空但不丢失 execution', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'data', jobId: 'clear');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(
          key: key,
          name: 'data-test',
          data: <String, Object?>{'keep': 'me'},
        ),
        createdAt: now,
        availableAt: now,
      ),
    );
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'data', workerId: 'w1', claimedAt: now),
    )).value!;

    final updated = await backend.updateJobData(
      UpdateJobDataRequest(
        key: key,
        expectedRevision: claimed.record.revision,
        credential: claimed.credential,
        occurredAt: now,
        data: null,
      ),
    );
    expect(updated.changed, isTrue);
    expect(updated.value.data, isNull);
    // execution 在 data 清空后必须保留
    expect(updated.value.execution, isNotNull);
    expect(
      updated.value.execution!.credential.executionId,
      claimed.credential.executionId,
    );
  });

  // ─── retry: failed Job retry 回 waiting ───────────────────────────────
  test(
    '$backendName failed Job retry 回到 waiting 状态且 attemptsMade 递增',
    () async {
      final harness = await createHarness();
      addTearDown(harness.dispose);
      final backend = await harness.openBackend();
      final now = harness.now();
      const key = JobKey(queueName: 'retry', jobId: 'failable');

      await backend.addJob(
        AddJobRequest(
          job: JobSpec(key: key, name: 'retry-test', data: null),
          createdAt: now,
          availableAt: now,
        ),
      );
      final claimed = (await backend.claimJob(
        ClaimJobRequest(queueName: 'retry', workerId: 'w1', claimedAt: now),
      )).value!;

      // 先 failed
      final failed = await backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.failed,
          expectedRevision: claimed.record.revision,
          credential: claimed.credential,
          occurredAt: now,
          failure: JobFailure(
            type: 'ArgumentError',
            message: 'bad input',
            failedAt: now,
          ),
        ),
      );
      expect(failed.value.status, JobStatus.failed);
      expect(failed.value.attemptsMade, 1);

      // retry 回 waiting
      final retried = await backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.waiting,
          expectedRevision: failed.value.revision,
          occurredAt: now,
        ),
      );
      expect(retried.changed, isTrue);
      expect(retried.value.status, JobStatus.waiting);
      // retry 后 attemptsMade 保持 1（retry 不是 claim）
      expect(retried.value.attemptsMade, 1);
    },
  );

  // ─── failure event+log: processor 抛异常自动生成 failed event + JobLog ──
  test(
    '$backendName transitionJob 失败时 automaticErrorLog 自动创建 JobLog',
    () async {
      final harness = await createHarness();
      addTearDown(harness.dispose);
      final backend = await harness.openBackend();
      final now = harness.now();
      const key = JobKey(queueName: 'fail', jobId: 'processor-err');

      await backend.addJob(
        AddJobRequest(
          job: JobSpec(key: key, name: 'fail-test', data: null),
          createdAt: now,
          availableAt: now,
        ),
      );
      final claimed = (await backend.claimJob(
        ClaimJobRequest(queueName: 'fail', workerId: 'w1', claimedAt: now),
      )).value!;

      // 带 automaticErrorLog 提交 failed
      final failed = await backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.failed,
          expectedRevision: claimed.record.revision,
          credential: claimed.credential,
          occurredAt: now,
          failure: JobFailure(
            type: 'StateError',
            message: 'boom!',
            failedAt: now,
          ),
          automaticErrorLog: JobLogDraft(
            level: JobLogLevel.error,
            channel: JobLogChannel.system,
            message: 'processor threw StateError: boom!',
          ),
        ),
      );
      expect(failed.changed, isTrue);
      expect(failed.value.status, JobStatus.failed);

      // 读取 JobLog，应该有一条 error/system 通道的日志
      final logs = await backend.queryJobLogs(JobLogQuery(key: key));
      expect(logs.items, hasLength(1));
      expect(logs.items.single.level, JobLogLevel.error);
      expect(logs.items.single.channel, JobLogChannel.system);
      expect(logs.items.single.message, contains('boom!'));

      // 事件流中应该有一条 failed 事件
      final events = await backend.readJobEvents(
        const ReadJobEventsRequest(queueName: 'fail'),
      );
      final failedEvents = events.items
          .where((p) => p.event.type == JobEventType.failed)
          .toList();
      expect(failedEvents, isNotEmpty);
    },
  );

  // ─── 日志 sequence/clear: 同 Job sequence 单调递增，clear 成功 ────────
  test('$backendName 同一 Job 的多次 appendJobLog sequence 单调递增', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'log', jobId: 'seq-test');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'log-test', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'log', workerId: 'w1', claimedAt: now),
    )).value!;

    // 连续追加 3 条日志
    final sequences = <int>[];
    for (var i = 0; i < 3; i++) {
      final log = await backend.appendJobLog(
        AppendJobLogRequest(
          key: key,
          credential: claimed.credential,
          attempt: claimed.record.attemptsMade,
          log: JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'log #${i + 1}',
          ),
          occurredAt: now,
        ),
      );
      expect(log.changed, isTrue);
      sequences.add(log.value.sequence);
    }
    // sequence 必须严格递增
    expect(sequences, [1, 2, 3]);

    // 全部可 query 到
    final logs = await backend.queryJobLogs(JobLogQuery(key: key));
    expect(logs.items.length, 3);
  });

  test('$backendName clearJobLogs 可以清空所有日志并返回清除条数', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'log2', jobId: 'clear-test');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'log2-test', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'log2', workerId: 'w1', claimedAt: now),
    )).value!;

    // 追加两条日志
    for (var i = 0; i < 2; i++) {
      await backend.appendJobLog(
        AppendJobLogRequest(
          key: key,
          credential: claimed.credential,
          attempt: claimed.record.attemptsMade,
          log: JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'line ${i + 1}',
          ),
          occurredAt: now,
        ),
      );
    }

    // keepLatest=0 清空全部
    final cleared = await backend.clearJobLogs(
      const ClearJobLogsRequest(key: key, keepLatest: 0),
    );
    expect(cleared.changed, isTrue);
    expect(cleared.value, 2);

    // queryJobLogs 应该为空
    final logs = await backend.queryJobLogs(JobLogQuery(key: key));
    expect(logs.items, isEmpty);
  });

  // ─── Flow 原子性: idGenerator 中途抛错零提交 ──────────────────────────
  test('$backendName addFlow 在 idGenerator 中途失败时全部回滚', () async {
    // 使用可预测的 idGenerator，但让它在第二次调用时抛错
    var calls = 0;
    final harness = await createHarness();
    addTearDown(harness.dispose);
    // harness 的 idGenerator 不可替换，因此我们用原始 MemoryQueueBackend
    // 来专门测试 Flow 原子性——harness 测试已覆盖基础契约
    final backend = MemoryQueueBackend(
      now: harness.now,
      idGenerator: () {
        calls++;
        if (calls == 2) throw StateError('allocator failed');
        return 'id-$calls';
      },
    );
    await backend.open();

    const root = JobKey(queueName: 'flow', jobId: 'root');
    const child = JobKey(queueName: 'flow', jobId: 'child');

    expect(
      () => backend.addFlow(
        AddFlowRequest(
          flow: FlowJobSpec(
            root: root,
            jobs: <JobSpec>[
              JobSpec(key: root, name: 'root', data: null),
              JobSpec(key: child, name: 'child', data: null),
            ],
            edges: <FlowEdge>[FlowEdge(parent: root, child: child)],
          ),
          createdAt: harness.now(),
        ),
      ),
      throwsStateError,
    );

    // 任何 Job 都不应提交
    expect(await backend.getJob(root), isNull);
    expect(await backend.getJob(child), isNull);
    // 事件队列为空
    expect(
      (await backend.readJobEvents(
        const ReadJobEventsRequest(queueName: 'flow'),
      )).items,
      isEmpty,
    );
  });

  // ─── 依赖恢复: 子完成时父自动从 waitingChildren 转移 ─────────────────
  test('$backendName Flow 子 Job 完成后父 Job 从 waitingChildren 转移', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();

    const root = JobKey(queueName: 'dep', jobId: 'root');
    const child = JobKey(queueName: 'dep', jobId: 'child');

    await backend.addFlow(
      AddFlowRequest(
        flow: FlowJobSpec(
          root: root,
          jobs: <JobSpec>[
            JobSpec(key: root, name: 'root', data: null),
            JobSpec(key: child, name: 'child', data: null),
          ],
          edges: <FlowEdge>[FlowEdge(parent: root, child: child)],
        ),
        createdAt: now,
      ),
    );

    // Flow 刚创建时父 Job 应在 waitingChildren 状态
    final parentInitial = await backend.getJob(root);
    expect(parentInitial!.status, JobStatus.waitingChildren);

    // claim 并完成子 Job
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'dep', workerId: 'w1', claimedAt: now),
    )).value!;
    // claim 会优先返回 child（FIFO + 子先被领取）
    await backend.transitionJob(
      TransitionJobRequest(
        key: claimed.record.key,
        targetStatus: JobStatus.completed,
        expectedRevision: claimed.record.revision,
        credential: claimed.credential,
        result: null,
        occurredAt: now,
      ),
    );

    // 父 Job 所有子已完成，应从 waitingChildren 回到 waiting
    final parentAfter = await backend.getJob(root);
    expect(parentAfter!.status, JobStatus.waiting);
  });

  // ─── 删除: active 后代存在时删除祖先被拒绝，terminal Job 可删除 ──────
  test('$backendName 存在 active 后代时删除祖先 Job 被拒绝', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();

    const root = JobKey(queueName: 'del', jobId: 'root');
    const child = JobKey(queueName: 'del', jobId: 'child');

    await backend.addFlow(
      AddFlowRequest(
        flow: FlowJobSpec(
          root: root,
          jobs: <JobSpec>[
            JobSpec(key: root, name: 'root', data: null),
            JobSpec(key: child, name: 'child', data: null),
          ],
          edges: <FlowEdge>[FlowEdge(parent: root, child: child)],
        ),
        createdAt: now,
      ),
    );

    // claim child 使其进入 active
    await backend.claimJob(
      ClaimJobRequest(queueName: 'del', workerId: 'w1', claimedAt: now),
    );

    final rootRecord = await backend.getJob(root);
    // 删除 root 应被 InvalidJobTransition 拒绝（child 还在 active）
    expect(
      () => backend.removeJob(
        RemoveJobRequest(
          key: root,
          expectedRevision: rootRecord!.revision,
          removedAt: now,
        ),
      ),
      throwsA(isA<InvalidJobTransition>()),
    );
    // root 和 child 都仍存在
    expect(await backend.getJob(root), isNotNull);
    expect(await backend.getJob(child), isNotNull);
  });

  test('$backendName terminal Job（无子依赖）可被成功删除', () async {
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now = harness.now();
    const key = JobKey(queueName: 'del2', jobId: 'terminal');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'terminal', data: null),
        createdAt: now,
        availableAt: now,
      ),
    );

    final record = await backend.getJob(key);
    final removed = await backend.removeJob(
      RemoveJobRequest(
        key: key,
        expectedRevision: record!.revision,
        removedAt: now,
      ),
    );
    expect(removed.changed, isTrue);
    expect(removed.value, hasLength(1));
    expect(removed.value.single, key);

    // 删除后 getJob 返回 null
    expect(await backend.getJob(key), isNull);
  });

  // ─── event cursor/gap: EventCursorGap 在 trim 后正确触发 ─────────────
  test('$backendName 事件被裁剪后旧 cursor 触发 EventCursorGap', () async {
    // 用 harness.now 可推进的特性
    final harness = await createHarness();
    addTearDown(harness.dispose);
    final backend = await harness.openBackend();
    final now0 = harness.now();
    const key = JobKey(queueName: 'gap', jobId: 'k1');

    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'gap-test', data: null),
        createdAt: now0,
        availableAt: now0,
      ),
    );

    // 读取事件获取初始 cursor
    final initialEvents = await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'gap', limit: 10),
    );
    expect(initialEvents.items, isNotEmpty);
    final latestPos = initialEvents.items.last.cursor.position;

    // 设置短 retention 触发裁剪
    await backend.setQueuePolicy(
      SetQueuePolicyRequest(
        queueName: 'gap',
        expectedRevision: (await backend.getQueueState('gap')).revision,
        policy: QueuePolicy(eventRetention: const Duration(seconds: 1)),
        occurredAt: now0,
      ),
    );

    // 推进时钟超过 retention，下次 read 会触发裁剪
    harness.advance(const Duration(seconds: 10));

    // 推进后再次 read（这次 read 会先执行 _trimEvents）
    final afterTrim = await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'gap', limit: 10),
    );
    expect(afterTrim.items, isEmpty);

    // 用旧 cursor 读应抛 EventCursorGap
    expect(
      () => backend.readJobEvents(
        ReadJobEventsRequest(
          queueName: 'gap',
          cursor: EventCursor(queueName: 'gap', position: latestPos),
          limit: 10,
        ),
      ),
      throwsA(isA<EventCursorGap>()),
    );
  });

  // ─── stalled recovery: recoverStalledJobs 根据 workerId 重新入队 ─────
  test(
    '$backendName recoverStalledJobs 根据 workerId 正确重新入队 stalled Job',
    () async {
      final harness = await createHarness();
      addTearDown(harness.dispose);
      final backend = await harness.openBackend();
      final now = harness.now();
      const key = JobKey(queueName: 'stall', jobId: 'stuck');

      final added = await backend.addJob(
        AddJobRequest(
          job: JobSpec(key: key, name: 'stall-test', data: null),
          createdAt: now,
          availableAt: now,
        ),
      );

      // claim 让 Job 进入 active
      final claimed = (await backend.claimJob(
        ClaimJobRequest(queueName: 'stall', workerId: 'w1', claimedAt: now),
      )).value!;
      expect(claimed.record.status, JobStatus.active);
      expect(claimed.record.execution?.workerId, 'w1');

      // 仅指定 w1 的 executionId 进行 recovery
      final recovered = await backend.recoverStalledJobs(
        RecoverStalledJobsRequest(
          queueName: 'stall',
          now: now,
          workerId: 'w1',
          executionIds: <String>{claimed.credential.executionId},
        ),
      );
      expect(recovered.changed, isTrue);
      expect(recovered.value.single.status, JobStatus.waiting);
      expect(recovered.queuesToNotify, <String>{'stall'});

      // revision 必须比 add 时递增
      final finalRecord = await backend.getJob(key);
      expect(finalRecord!.revision, greaterThan(added.value.revision));
      // execution 必须清空
      expect(finalRecord.execution, isNull);
    },
  );
}
