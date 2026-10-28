import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

MemoryQueueBackend _backend() {
  var counter = 0;
  final b = MemoryQueueBackend(
    now: () => DateTime.utc(2026, 1, 1),
    idGenerator: () => 'id-${++counter}',
  );
  b.open();
  return b;
}

final _t = DateTime.utc(2026, 1, 1);

void main() {
  test(
    '移除 active 子代时会拒绝操作，并保持整个子树不变',
    () async {
      final backend = _backend();
      final root = const JobKey(queueName: 'r', jobId: 'root');
      final child = const JobKey(queueName: 'r', jobId: 'child');
      final grand = const JobKey(queueName: 'r', jobId: 'grand');

      await backend.addFlow(
        AddFlowRequest(
          flow: FlowJobSpec(
            root: root,
            jobs: <JobSpec>[
              JobSpec(key: root, name: 'root', data: null),
              JobSpec(key: child, name: 'child', data: null),
              JobSpec(key: grand, name: 'grand', data: null),
            ],
            edges: <FlowEdge>[
              FlowEdge(parent: root, child: child),
              FlowEdge(parent: child, child: grand),
            ],
          ),
          createdAt: _t,
        ),
      );

      final claimed = await backend.claimJob(
        ClaimJobRequest(queueName: 'r', workerId: 'w1', claimedAt: _t),
      );
      expect(claimed.value!.record.key, grand);

      final childRecord = await backend.getJob(child);
      expect(
        () => backend.removeJob(
          RemoveJobRequest(
            key: child,
            expectedRevision: childRecord!.revision,
            removedAt: _t,
          ),
        ),
        throwsA(isA<InvalidJobTransition>()),
      );

      expect((await backend.getJob(root)), isNotNull);
      expect((await backend.getJob(child)), isNotNull);
      final g = await backend.getJob(grand);
      expect(g!.status, JobStatus.active);
    },
  );

  test(
    '重试失败的子 Job 会在父 Job 执行前恢复祖先',
    () async {
      final backend = _backend();
      final root = const JobKey(queueName: 'x', jobId: 'root');
      final child = const JobKey(queueName: 'x', jobId: 'child');

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
          createdAt: _t,
        ),
      );

      final claimed = await backend.claimJob(
        ClaimJobRequest(queueName: 'x', workerId: 'w1', claimedAt: _t),
      );
      final failed = await backend.transitionJob(
        TransitionJobRequest(
          key: child,
          expectedRevision: claimed.value!.record.revision,
          credential: claimed.value!.credential,
          targetStatus: JobStatus.failed,
          failure: JobFailure(
            type: 'ProcessorFailure',
            message: 'boom',
            failedAt: _t,
          ),
          occurredAt: _t,
        ),
      );
      final parentAfterFail = await backend.getJob(root);
      expect(parentAfterFail!.status, JobStatus.failed);
      expect(parentAfterFail.failure?.type, 'FlowDependencyFailure');

      await backend.transitionJob(
        TransitionJobRequest(
          key: child,
          expectedRevision: failed.value.revision,
          targetStatus: JobStatus.waiting,
          occurredAt: _t,
        ),
      );

      final parentAfterRetry = await backend.getJob(root);
      expect(parentAfterRetry!.status, isNot(JobStatus.failed));
      expect(parentAfterRetry.failure, isNull);
    },
  );

  test(
    '移除中间子树会解除其关联，并使父 Job 保持依赖失败状态',
    () async {
      final backend = _backend();
      final root = const JobKey(queueName: 'y', jobId: 'root');
      final child = const JobKey(queueName: 'y', jobId: 'child');
      final grand = const JobKey(queueName: 'y', jobId: 'grand');

      await backend.addFlow(
        AddFlowRequest(
          flow: FlowJobSpec(
            root: root,
            jobs: <JobSpec>[
              JobSpec(key: root, name: 'root', data: null),
              JobSpec(key: child, name: 'child', data: null),
              JobSpec(key: grand, name: 'grand', data: null),
            ],
            edges: <FlowEdge>[
              FlowEdge(parent: root, child: child),
              FlowEdge(parent: child, child: grand),
            ],
          ),
          createdAt: _t,
        ),
      );

      final claimedGrand = await backend.claimJob(
        ClaimJobRequest(queueName: 'y', workerId: 'w1', claimedAt: _t),
      );
      await backend.transitionJob(
        TransitionJobRequest(
          key: grand,
          expectedRevision: claimedGrand.value!.record.revision,
          credential: claimedGrand.value!.credential,
          targetStatus: JobStatus.completed,
          result: null,
          occurredAt: _t,
        ),
      );

      final childBefore = await backend.getJob(child);
      final removed = await backend.removeJob(
        RemoveJobRequest(
          key: child,
          expectedRevision: childBefore!.revision,
          removedAt: _t,
        ),
      );
      expect(removed.changed, isTrue);

      expect((await backend.getJob(child)), isNull);
      expect((await backend.getJob(grand)), isNull);

      final parent = await backend.getJob(root);
      expect(parent!.status, JobStatus.failed);
      expect(parent.failure?.type, 'FlowDependencyFailure');
      expect(parent.failure?.dependencyKeys, contains(child));
    },
  );

  // 验证子 Job 被取消后父 Job 自动进入 FlowDependencyFailure，
  // 子 Job 即时 retry 回 waiting 时父 Job 恢复为 waitingChildren 且 failure 被清除。
  test('canceled 子 Job 即时 retry 触发父 FlowDependencyFailure 恢复', () async {
    final backend = _backend();
    final root = const JobKey(queueName: 'x', jobId: 'root');
    final child = const JobKey(queueName: 'x', jobId: 'child');

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
        createdAt: _t,
      ),
    );

    final claimed = await backend.claimJob(
      ClaimJobRequest(queueName: 'x', workerId: 'w1', claimedAt: _t),
    );
    final canceledChild = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: claimed.value!.record.revision,
        credential: claimed.value!.credential,
        targetStatus: JobStatus.canceled,
        occurredAt: _t,
      ),
    );
    final parentAfterCancel = await backend.getJob(root);
    expect(parentAfterCancel!.status, JobStatus.failed);
    expect(parentAfterCancel.failure?.type, 'FlowDependencyFailure');

    await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: canceledChild.value.revision,
        targetStatus: JobStatus.waiting,
        occurredAt: _t,
      ),
    );

    final parentAfterRetry = await backend.getJob(root);
    expect(parentAfterRetry!.status, JobStatus.waitingChildren);
    expect(parentAfterRetry.failure, isNull);
  });

  // 验证子 Job 延迟 retry（目标 delayed）也能触发父 FlowDependencyFailure 恢复。
  test('canceled 子 Job 延迟 retry 也恢复父 Job', () async {
    final backend = _backend();
    final root = const JobKey(queueName: 'y', jobId: 'root');
    final child = const JobKey(queueName: 'y', jobId: 'child');

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
        createdAt: _t,
      ),
    );

    final claimed = await backend.claimJob(
      ClaimJobRequest(queueName: 'y', workerId: 'w1', claimedAt: _t),
    );
    final canceledChild = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: claimed.value!.record.revision,
        credential: claimed.value!.credential,
        targetStatus: JobStatus.canceled,
        occurredAt: _t,
      ),
    );

    const delay = Duration(minutes: 3);
    await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: canceledChild.value.revision,
        targetStatus: JobStatus.delayed,
        occurredAt: _t,
        availableAt: _t.add(delay),
      ),
    );

    final childAfterRetry = await backend.getJob(child);
    expect(childAfterRetry!.status, JobStatus.delayed);
    final parentAfterRetry = await backend.getJob(root);
    expect(parentAfterRetry!.status, JobStatus.waitingChildren);
    expect(parentAfterRetry.failure, isNull);
  });

  // 验证父 Job 被用户手动 canceled 后，子 Job 再取消并 retry 时
  // 父 Job 保持 canceled（祖先重算不得复活已取消的 Flow 根）。
  test('已取消父 Job 不被子 Job retry 复活', () async {
    final backend = _backend();
    final root = const JobKey(queueName: 'z', jobId: 'root');
    final child = const JobKey(queueName: 'z', jobId: 'child');

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
        createdAt: _t,
      ),
    );

    // 父 Job 先被用户取消。
    final rootBeforeCancel = await backend.getJob(root);
    await backend.transitionJob(
      TransitionJobRequest(
        key: root,
        targetStatus: JobStatus.canceled,
        expectedRevision: rootBeforeCancel!.revision,
        occurredAt: _t,
      ),
    );
    expect((await backend.getJob(root))!.status, JobStatus.canceled);

    // 子 Job claim 后取消再 retry。
    final claimed = await backend.claimJob(
      ClaimJobRequest(queueName: 'z', workerId: 'w1', claimedAt: _t),
    );
    final canceledChild = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: claimed.value!.record.revision,
        credential: claimed.value!.credential,
        targetStatus: JobStatus.canceled,
        occurredAt: _t,
      ),
    );
    await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: canceledChild.value.revision,
        targetStatus: JobStatus.waiting,
        occurredAt: _t,
      ),
    );

    // 父 Job 必须保持 canceled，不得被子 retry 复活。
    expect((await backend.getJob(root))!.status, JobStatus.canceled);
  });

  // 验证父子位于不同 Queue 时，子 Job canceled → waiting retry 的
  // queuesToNotify 同时包含子 Queue 和父 Queue，跨队列通知完整。
  test('跨 Queue 子 Job retry 的 queuesToNotify 同时包含子 Queue 和父 Queue', () async {
    final backend = _backend();
    const root = JobKey(queueName: 'root-q', jobId: 'root');
    const child = JobKey(queueName: 'child-q', jobId: 'child');

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
        createdAt: _t,
      ),
    );

    // 子 Queue 领取并取消子 Job。
    final claimed = await backend.claimJob(
      ClaimJobRequest(queueName: 'child-q', workerId: 'w1', claimedAt: _t),
    );
    final canceledChild = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: claimed.value!.record.revision,
        credential: claimed.value!.credential,
        targetStatus: JobStatus.canceled,
        occurredAt: _t,
      ),
    );

    // 子 Job retry 回 waiting，观察通知集合。
    final retried = await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        expectedRevision: canceledChild.value.revision,
        targetStatus: JobStatus.waiting,
        occurredAt: _t,
      ),
    );

    expect(retried.changed, isTrue);
    expect(retried.queuesToNotify, containsAll(<String>['child-q', 'root-q']));
  });

  // 验证已取消父 Job 存在未完成子任务时，直接 retry 不能绕过依赖：
  // 应先进入 waitingChildren 而非 waiting，等子全部完成后再推进。
  test('已取消父 Job retry 在子未完成时进入 waitingChildren 而非 waiting', () async {
    final backend = _backend();
    const root = JobKey(queueName: 'f', jobId: 'root');
    const child = JobKey(queueName: 'f', jobId: 'child');

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
        createdAt: _t,
      ),
    );

    // 父 Job 先被用户取消；子 Job 仍是 waiting（未完成）。
    final rootBeforeCancel = await backend.getJob(root);
    expect(rootBeforeCancel!.status, JobStatus.waitingChildren);
    await backend.transitionJob(
      TransitionJobRequest(
        key: root,
        targetStatus: JobStatus.canceled,
        expectedRevision: rootBeforeCancel.revision,
        occurredAt: _t,
      ),
    );
    expect((await backend.getJob(root))!.status, JobStatus.canceled);
    expect((await backend.getJob(child))!.status, JobStatus.waiting);

    // 对已取消的父 Job 直接 retry——子还没完成，不能跳过依赖。
    final retry = await backend.transitionJob(
      TransitionJobRequest(
        key: root,
        targetStatus: JobStatus.waiting,
        expectedRevision: (await backend.getJob(root))!.revision,
        occurredAt: _t,
      ),
    );

    // 父应进入 waitingChildren（等待未完成子），不能直接变成 waiting 被 Worker 领取。
    expect(retry.value.status, JobStatus.waitingChildren);
    expect(retry.changed, isTrue);
    // 子 Job 保持 waiting 不变。
    expect((await backend.getJob(child))!.status, JobStatus.waiting);
  });

  // 验证已取消的叶 Job（没有子依赖）retry 仍正常进入 waiting。
  test('已取消叶 Job retry 正常进入 waiting', () async {
    final backend = _backend();
    const leaf = JobKey(queueName: 'f', jobId: 'leaf');
    await backend.addJob(
      AddJobRequest(
        job: JobSpec(key: leaf, name: 'leaf', data: null),
        createdAt: _t,
        availableAt: _t,
      ),
    );
    final added = await backend.getJob(leaf);
    await backend.transitionJob(
      TransitionJobRequest(
        key: leaf,
        targetStatus: JobStatus.canceled,
        expectedRevision: added!.revision,
        occurredAt: _t,
      ),
    );
    final retried = await backend.transitionJob(
      TransitionJobRequest(
        key: leaf,
        targetStatus: JobStatus.waiting,
        expectedRevision: (await backend.getJob(leaf))!.revision,
        occurredAt: _t,
      ),
    );
    expect(retried.value.status, JobStatus.waiting);
  });

  // 验证父 Job 和子 Job 都被 canceled 时，直接 retry 父 Job 应被 FlowDependencyFailure 拒绝：
  // canceled 子依赖代表依赖关系已断裂，必须先 retry 子 Job 才能重新激活父 Job。
  test(
    'canceled 父 Job 存在 canceled 子依赖时 retry 被 FlowDependencyFailure 拒绝',
    () async {
      final backend = _backend();
      const root = JobKey(queueName: 'g', jobId: 'root');
      const child = JobKey(queueName: 'g', jobId: 'child');

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
          createdAt: _t,
        ),
      );

      // 父 Job 和子 Job 都被用户取消（无子被 claim）。
      final rootR = (await backend.getJob(root))!.revision;
      await backend.transitionJob(
        TransitionJobRequest(
          key: root,
          targetStatus: JobStatus.canceled,
          expectedRevision: rootR,
          occurredAt: _t,
        ),
      );
      final childRev = (await backend.getJob(child))!.revision;
      await backend.transitionJob(
        TransitionJobRequest(
          key: child,
          targetStatus: JobStatus.canceled,
          expectedRevision: childRev,
          occurredAt: _t,
        ),
      );
      expect((await backend.getJob(root))!.status, JobStatus.canceled);
      expect((await backend.getJob(child))!.status, JobStatus.canceled);

      // 直接 retry 父 Job——canceled 子依赖尚未恢复，必须由子先 retry。
      final rev = (await backend.getJob(root))!.revision;
      await expectLater(
        () => backend.transitionJob(
          TransitionJobRequest(
            key: root,
            targetStatus: JobStatus.waiting,
            expectedRevision: rev,
            occurredAt: _t,
          ),
        ),
        throwsA(isA<FlowDependencyFailure>()),
      );

      // 父仍保持 canceled 不变。
      expect((await backend.getJob(root))!.status, JobStatus.canceled);

      // 先 retry 子 Job——父是用户 canceled，_recomputeParent 对 canceled 父是 no-op，
      // 所以父不会被自动复活；子本身恢复到 waiting。
      await backend.transitionJob(
        TransitionJobRequest(
          key: child,
          targetStatus: JobStatus.waiting,
          expectedRevision: (await backend.getJob(child))!.revision,
          occurredAt: _t,
        ),
      );
      expect((await backend.getJob(child))!.status, JobStatus.waiting);
      expect((await backend.getJob(root))!.status, JobStatus.canceled);

      // 此时用户显式 retry 父——无子失败依赖（子是 waiting），
      // 但有未完成子（子是 waiting）→ waitingChildren。
      await backend.transitionJob(
        TransitionJobRequest(
          key: root,
          targetStatus: JobStatus.waiting,
          expectedRevision: (await backend.getJob(root))!.revision,
          occurredAt: _t,
        ),
      );
      expect((await backend.getJob(root))!.status, JobStatus.waitingChildren);
    },
  );

  // 验证 3 层 Flow 深度：父 canceled、子 canceled、孙 canceled，
  // 从最底层孙开始逐层 retry，每一层都必须按依赖顺序恢复。
  test('3 层 Flow：祖孙三代全部 canceled 时逐层 retry 必须按依赖顺序', () async {
    final backend = _backend();
    const root = JobKey(queueName: 'h', jobId: 'root');
    const parent = JobKey(queueName: 'h', jobId: 'parent');
    const child = JobKey(queueName: 'h', jobId: 'child');

    await backend.addFlow(
      AddFlowRequest(
        flow: FlowJobSpec(
          root: root,
          jobs: <JobSpec>[
            JobSpec(key: root, name: 'root', data: null),
            JobSpec(key: parent, name: 'parent', data: null),
            JobSpec(key: child, name: 'child', data: null),
          ],
          edges: <FlowEdge>[
            FlowEdge(parent: root, child: parent),
            FlowEdge(parent: parent, child: child),
          ],
        ),
        createdAt: _t,
      ),
    );

    // 全部用户取消。
    await backend.transitionJob(
      TransitionJobRequest(
        key: root,
        targetStatus: JobStatus.canceled,
        expectedRevision: (await backend.getJob(root))!.revision,
        occurredAt: _t,
      ),
    );
    await backend.transitionJob(
      TransitionJobRequest(
        key: parent,
        targetStatus: JobStatus.canceled,
        expectedRevision: (await backend.getJob(parent))!.revision,
        occurredAt: _t,
      ),
    );
    await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        targetStatus: JobStatus.canceled,
        expectedRevision: (await backend.getJob(child))!.revision,
        occurredAt: _t,
      ),
    );

    // root retry → 有 canceled 子 parent → FlowDependencyFailure。
    final rootRev = (await backend.getJob(root))!.revision;
    await expectLater(
      () => backend.transitionJob(
        TransitionJobRequest(
          key: root,
          targetStatus: JobStatus.waiting,
          expectedRevision: rootRev,
          occurredAt: _t,
        ),
      ),
      throwsA(isA<FlowDependencyFailure>()),
    );

    // parent retry → 有 canceled 子 child → FlowDependencyFailure。
    final parentRev = (await backend.getJob(parent))!.revision;
    await expectLater(
      () => backend.transitionJob(
        TransitionJobRequest(
          key: parent,
          targetStatus: JobStatus.waiting,
          expectedRevision: parentRev,
          occurredAt: _t,
        ),
      ),
      throwsA(isA<FlowDependencyFailure>()),
    );

    // child retry → 无子依赖 → waiting。
    await backend.transitionJob(
      TransitionJobRequest(
        key: child,
        targetStatus: JobStatus.waiting,
        expectedRevision: (await backend.getJob(child))!.revision,
        occurredAt: _t,
      ),
    );
    expect((await backend.getJob(child))!.status, JobStatus.waiting);
    // parent/root 是用户 canceled → _recomputeParent no-op，不自动复活。
    expect((await backend.getJob(parent))!.status, JobStatus.canceled);
    expect((await backend.getJob(root))!.status, JobStatus.canceled);

    // parent retry → 有未完成子 child → waitingChildren。
    await backend.transitionJob(
      TransitionJobRequest(
        key: parent,
        targetStatus: JobStatus.waiting,
        expectedRevision: (await backend.getJob(parent))!.revision,
        occurredAt: _t,
      ),
    );
    expect((await backend.getJob(parent))!.status, JobStatus.waitingChildren);

    // root retry → 有未完成子 parent → waitingChildren。
    await backend.transitionJob(
      TransitionJobRequest(
        key: root,
        targetStatus: JobStatus.waiting,
        expectedRevision: (await backend.getJob(root))!.revision,
        occurredAt: _t,
      ),
    );
    expect((await backend.getJob(root))!.status, JobStatus.waitingChildren);
  });
}
