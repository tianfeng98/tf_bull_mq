import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

void main() {
  // 验证 addFlow 的原子性：Flow 中包含多个 Job，在 idGenerator 第二次调用时失败，
  // 任何 Job 都不应被提交，保证原子 Flows 要么全部成功要么全部失败。
  test('多事实写入失败后不应留下任何已提交的 Flow 事实', () async {
    var calls = 0;
    final backend = MemoryQueueBackend(
      now: () => DateTime.utc(2026, 1, 1),
      idGenerator: () {
        calls++;
        if (calls == 2) throw StateError('allocator failed');
        return 'id-$calls';
      },
    );
    await backend.open();
    const root = JobKey(queueName: 'flow', jobId: 'root');
    const child = JobKey(queueName: 'flow', jobId: 'child');

    // addFlow 在 idGenerator 第二次调用时抛异常，整体应回滚。
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
          createdAt: DateTime.utc(2026, 1, 1),
        ),
      ),
      throwsStateError,
    );
    // 确认 root 和 child 均未被提交，且事件队列为空。
    expect(await backend.getJob(root), isNull);
    expect(await backend.getJob(child), isNull);
    expect(
      (await backend.readJobEvents(
        const ReadJobEventsRequest(queueName: 'flow'),
      )).items,
      isEmpty,
    );
  });

  // 验证 Flow 依赖失败的传播：子 Job failed 后父 Job 自动 failed，
  // 父 Job 无法单独 retry 来绕过子依赖；只有子 Job 被 retry 回 waiting 后，
  // 父 Job 才会被恢复到 waitingChildren 状态。
  test('子 Job 失败导致父 Job 自动 failed，单独 retry 父 Job 被拒绝', () async {
    final backend = await _openBackend();
    final root = const JobKey(queueName: 'flow', jobId: 'root');
    final child = const JobKey(queueName: 'flow', jobId: 'child');
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
        createdAt: _time,
      ),
    );
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'flow', workerId: 'worker', claimedAt: _time),
    )).value!;
    // 将子 Job 转为 failed，带 failure 信息。
    final failed = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        targetStatus: JobStatus.failed,
        expectedRevision: claimed.record.revision,
        credential: claimed.credential,
        occurredAt: _time,
        failure: JobFailure(
          type: 'ProcessorFailure',
          message: 'boom',
          failedAt: _time,
        ),
      ),
    );
    final parent = await backend.getJob(root);
    // 父 Job 因依赖失败自动变为 failed。
    expect(parent!.status, JobStatus.failed);
    // 单独 retry 父 Job 被拒绝（FlowDependencyFailure）。
    expect(
      () => backend.transitionJob(
        TransitionJobRequest(
          key: root,
          targetStatus: JobStatus.waiting,
          expectedRevision: parent.revision,
          occurredAt: _time,
        ),
      ),
      throwsA(isA<FlowDependencyFailure>()),
    );
    // 子 Job retry 回 waiting 后，父 Job 自动恢复到 waitingChildren。
    final retried = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        targetStatus: JobStatus.waiting,
        expectedRevision: failed.value.revision,
        occurredAt: _time,
      ),
    );
    expect(retried.value.status, JobStatus.waiting);
    expect((await backend.getJob(root))!.status, JobStatus.waitingChildren);
  });

  // 验证活跃后代的删除门禁：当 Flow 的某个后代 Job 处于 active 状态时，
  // 直接 remove 祖先 Job 应被拒绝，防止并发执行中的 Job 被误删。
  test('存在 active 后代时 remove 祖先 Job 被拒绝，子树保持完整', () async {
    final backend = await _openBackend();
    const root = JobKey(queueName: 'flow', jobId: 'root');
    const child = JobKey(queueName: 'flow', jobId: 'child');
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
        createdAt: _time,
      ),
    );
    final claim = (await backend.claimJob(
      ClaimJobRequest(queueName: 'flow', workerId: 'worker', claimedAt: _time),
    )).value!;
    final rootRecord = await backend.getJob(root);
    // 尝试删除 root，但 child 已被 claim 处于 active，应该抛出 InvalidJobTransition。
    expect(
      () => backend.removeJob(
        RemoveJobRequest(
          key: root,
          expectedRevision: rootRecord!.revision,
          removedAt: _time,
        ),
      ),
      throwsA(isA<InvalidJobTransition>()),
    );
    // 确认 root 和 child 都仍在，claim 持有的 Job 仍是 active。
    expect(await backend.getJob(root), isNotNull);
    expect(await backend.getJob(child), isNotNull);
    expect(claim.record.status, JobStatus.active);
  });

  // 验证 cancel 幂等性：同一条 Job 多次 transitionJob 到 canceled，
  // 后续调用 changed=false 且不产生通知；同时验证 JobEvent 游标在 cancel 前后的读取行为。
  test('cancel 幂等：重复 transition 到 canceled 返回 changed=false 且不产生通知', () async {
    final backend = await _openBackend();
    const key = JobKey(queueName: 'mail', jobId: '1');
    final added = await backend.addJob(
      AddJobRequest(
        job: JobSpec(
          key: key,
          name: 'send',
          data: null,
          options: JobOptions(logRetention: 1),
        ),
        createdAt: _time,
        availableAt: _time,
      ),
    );
    final canceled = await backend.transitionJob(
      TransitionJobRequest(
        key: key,
        targetStatus: JobStatus.canceled,
        expectedRevision: added.value.revision,
        occurredAt: _time,
      ),
    );
    // 再次 cancel 同一 Job。
    final again = await backend.transitionJob(
      TransitionJobRequest(
        key: key,
        targetStatus: JobStatus.canceled,
        expectedRevision: canceled.value.revision,
        occurredAt: _time,
      ),
    );
    // changed=false 且 queuesToNotify 为空说明是无效状态变换。
    expect(again.changed, isFalse);
    expect(again.queuesToNotify, isEmpty);
    // 首次 readJobEvents 用 limit=1 只拿到一条事件。
    final events = await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'mail', limit: 1),
    );
    expect(events.items, hasLength(1));
    // 继续用 cursor 读取后续事件仍有数据（cancel 事件）。
    expect(
      (await backend.readJobEvents(
        ReadJobEventsRequest(
          queueName: 'mail',
          cursor: EventCursor(queueName: 'mail', position: 1),
        ),
      )).items,
      isNotEmpty,
    );
  });

  // 验证 canceled Job 可直接 retry 回 waiting：重置 progress 和终态运行字段，
  // 保留 data、attemptsMade、stalledCount 和历史 JobLog，且 completed 仍不可重试。
  test(
    'canceled Job retry 回 waiting：重置 progress 和运行字段但保留 data/计数/日志',
    () async {
      final backend = await _openBackend();
      const key = JobKey(queueName: 'mail', jobId: '1');
      const originalData = <String, int>{'count': 42};
      await backend.addJob(
        AddJobRequest(
          job: JobSpec(
            key: key,
            name: 'send',
            data: originalData,
            options: JobOptions(logRetention: 10),
          ),
          createdAt: _time,
          availableAt: _time,
        ),
      );
      // 先 claim 变成 active，更新 progress，再附加一条 JobLog，最后取消。
      final claimed = (await backend.claimJob(
        ClaimJobRequest(queueName: 'mail', workerId: 'w1', claimedAt: _time),
      )).value!;
      await backend.updateJobProgress(
        UpdateJobProgressRequest(
          key: key,
          expectedRevision: claimed.record.revision,
          credential: claimed.credential,
          occurredAt: _time,
          progress: 0.5,
        ),
      );
      final previousLog = (await backend.appendJobLog(
        AppendJobLogRequest(
          key: key,
          credential: claimed.credential,
          occurredAt: _time,
          attempt: claimed.record.attemptsMade,
          log: JobLogDraft(
            level: JobLogLevel.info,
            channel: JobLogChannel.application,
            message: 'before cancel',
          ),
        ),
      )).value;
      final canceled = await backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.canceled,
          expectedRevision: 3,
          occurredAt: _time,
        ),
      );

      expect(canceled.value.status, JobStatus.canceled);
      expect(canceled.value.finishedAt, _time);
      final originalStartedAt = claimed.record.startedAt;

      // 对 canceled Job 调用 retry，立即回 waiting。
      final retried = await backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.waiting,
          expectedRevision: canceled.value.revision,
          occurredAt: _time,
        ),
      );

      expect(retried.value.status, JobStatus.waiting);
      expect(retried.value.progress, isNull, reason: 'retry 必须重置 progress');
      expect(retried.value.data, originalData);
      expect(retried.value.attemptsMade, canceled.value.attemptsMade);
      expect(retried.value.stalledCount, canceled.value.stalledCount);
      expect(retried.value.finishedAt, isNull);
      expect(retried.value.failure, isNull);
      expect(retried.value.execution, isNull);
      expect(retried.value.controlRequest, isNull);
      // startedAt 表示首次 claim 时间，retry 不应重置。
      expect(retried.value.startedAt, originalStartedAt);
      // 历史 JobLog 保持完整，没有被清空。
      final logs = await backend.queryJobLogs(JobLogQuery(key: key));
      expect(logs.items, contains(previousLog));
    },
  );

  // 验证 completed Job retry 仍被 Backend 拒绝，不产生额外事件。
  test('completed Job retry 仍被 InvalidJobTransition 拒绝', () async {
    final backend = await _openBackend();
    const key = JobKey(queueName: 'mail', jobId: 'done');
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'done', data: null),
        createdAt: _time,
        availableAt: _time,
      ),
    );
    final claimed = (await backend.claimJob(
      ClaimJobRequest(queueName: 'mail', workerId: 'w1', claimedAt: _time),
    )).value!;
    expect(claimed.record.key, key);
    final done = await backend.transitionJob(
      TransitionJobRequest(
        key: key,
        targetStatus: JobStatus.completed,
        expectedRevision: claimed.record.revision,
        credential: claimed.credential,
        result: null,
        occurredAt: _time,
      ),
    );

    final beforeReject = (await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'mail'),
    )).items.length;
    expect(
      () => backend.transitionJob(
        TransitionJobRequest(
          key: key,
          targetStatus: JobStatus.waiting,
          expectedRevision: done.value.revision,
          occurredAt: _time,
        ),
      ),
      throwsA(isA<InvalidJobTransition>()),
    );
    final afterReject = (await backend.readJobEvents(
      const ReadJobEventsRequest(queueName: 'mail'),
    )).items.length;
    expect(afterReject, beforeReject);
  });

  // 验证 canceled Job 可以带正延迟 retry 到 delayed 状态，availableAt = occurredAt + delay。
  test('canceled Job 延迟 retry 进入 delayed 且 availableAt 正确', () async {
    final backend = await _openBackend();
    const key = JobKey(queueName: 'mail', jobId: 'delay-canceled');
    final added = await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'send', data: null),
        createdAt: _time,
        availableAt: _time,
      ),
    );
    final canceled = await backend.transitionJob(
      TransitionJobRequest(
        key: key,
        targetStatus: JobStatus.canceled,
        expectedRevision: added.value.revision,
        occurredAt: _time,
      ),
    );

    const delay = Duration(minutes: 5);
    final delayed = await backend.transitionJob(
      TransitionJobRequest(
        key: key,
        targetStatus: JobStatus.delayed,
        expectedRevision: canceled.value.revision,
        occurredAt: _time,
        availableAt: _time.add(delay),
      ),
    );

    expect(delayed.value.status, JobStatus.delayed);
    expect(delayed.value.availableAt, _time.add(delay));
    expect(delayed.value.finishedAt, isNull);
    expect(delayed.value.failure, isNull);
  });

  // 验证 recoverStalledJobs：后端根据 workerId + executionIds 将 stalled 的 active Job
  // 放回 waiting 并产生 queuesToNotify，revision 递增以反映状态变更。
  test('recoverStalledJobs 根据 workerId 重新入队 stalled Job', () async {
    final backend = await _openBackend();
    const key = JobKey(queueName: 'work', jobId: '1');
    final added = await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: key, name: 'work', data: null),
        createdAt: _time,
        availableAt: _time,
      ),
    );
    final claim = (await backend.claimJob(
      ClaimJobRequest(queueName: 'work', workerId: 'worker', claimedAt: _time),
    )).value!;
    final recovered = await backend.recoverStalledJobs(
      RecoverStalledJobsRequest(
        queueName: 'work',
        now: _time,
        workerId: 'worker',
        executionIds: <String>{claim.credential.executionId},
      ),
    );
    // changed=true 且重新入队为 waiting 状态。
    expect(recovered.changed, isTrue);
    expect(recovered.value.single.status, JobStatus.waiting);
    // backend 产生对应队列的通知，revision 比 add 时递增。
    expect(recovered.queuesToNotify, <String>{'work'});
    expect(
      (await backend.getJob(key))!.revision,
      greaterThan(added.value.revision),
    );
  });
}

final _time = DateTime.utc(2026, 1, 1);

Future<QueueBackend> _openBackend() async {
  final backend = MemoryQueueBackend(now: () => _time, idGenerator: _ids());
  await backend.open();
  return backend;
}

IdGenerator _ids() {
  var serial = 0;
  return () => 'id-${++serial}';
}
