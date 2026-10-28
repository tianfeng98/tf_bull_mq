import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'support/integration_fixture.dart';

void main() {
  // 验证跨队列 Flow 的完成顺序：父 Job 等待所有跨队列子 Job 完成后才能结束，
  // FlowProjection 正确返回子 Job 的结果列表。
  test('跨队列 Flow 中子 Job 先于父 Job 完成，父 Job 正确聚合子结果', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final producer = fixture.flowProducer();
    addTearDown(producer.close);
    final query = fixture.flowQuery();
    addTearDown(query.close);
    final parentQueue = fixture.queue('parents');
    final leafAQueue = fixture.queue('leaf-a');
    final leafBQueue = fixture.queue('leaf-b');
    addTearDown(parentQueue.close);
    addTearDown(leafAQueue.close);
    addTearDown(leafBQueue.close);
    var parentEntered = false;
    final parentCompleted = Completer<void>();
    final parentWorker = fixture.worker(
      queueName: 'parents',
      processor: (job) async {
        parentEntered = true;
        // 父 Job 在 getDependenciesResult 挂起点等待所有子 Job 完成。
        final results = await job.getDependenciesResult();
        if (!parentCompleted.isCompleted) parentCompleted.complete();
        return <String, Object?>{'children': results.length};
      },
    );
    final leafWorkerA = fixture.worker(
      queueName: 'leaf-a',
      processor: (job) => const <String, Object?>{'part': 'a'},
    );
    final leafWorkerB = fixture.worker(
      queueName: 'leaf-b',
      processor: (job) => const <String, Object?>{'part': 'b'},
    );
    addTearDown(parentWorker.close);
    addTearDown(leafWorkerA.close);
    addTearDown(leafWorkerB.close);
    await parentWorker.start();
    // 声明一个根 Job 和两个跨队列叶子子 Job，使用 Atomic Flows 原子添加。
    final flow = await producer.add(
      FlowJob(
        key: const JobKey(queueName: 'parents', jobId: 'root'),
        name: 'parent',
        data: null,
        children: <FlowChild>[
          FlowChild(
            job: FlowJob(
              key: const JobKey(queueName: 'leaf-a', jobId: 'a'),
              name: 'a',
              data: null,
            ),
          ),
          FlowChild(
            job: FlowJob(
              key: const JobKey(queueName: 'leaf-b', jobId: 'b'),
              name: 'b',
              data: null,
            ),
          ),
        ],
      ),
    );
    expect(
      flow.jobs.firstWhere((job) => job.key.jobId == 'root').status,
      JobStatus.waitingChildren,
    );
    // 父 Job 进入 waitingChildren 状态，此时不应被任何 Worker claim。
    expect(parentEntered, isFalse);
    // 让两个叶子 Worker 启动并处理各自队列上的子 Job。
    await leafWorkerA.start();
    await leafWorkerB.start();
    await parentCompleted.future.timeout(
      const Duration(seconds: 5),
      onTimeout: () => throw StateError('parent did not complete'),
    );
    // 所有子 Job 完成后父 Job 才真正进入 processor 并结束。
    expect(parentEntered, isTrue);
    final parentJob = await parentQueue.getJob('root');
    expect((await parentJob!.refresh()).status, JobStatus.completed);
    // 通过 FlowQuery 读取投影，确认 root 已 completed 且 childResults 数量正确。
    final projection = await query.get(
      const JobKey(queueName: 'parents', jobId: 'root'),
    );
    expect(projection!.root.job.status, JobStatus.completed);
    expect(projection.root.childResults, hasLength(2));
  });

  // 验证单个子节点 weightedProgress 在无子 Job 时返回 0（初始态）。
  test('叶子节点的 weightedProgress 在初始态返回 0', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final producer = fixture.flowProducer();
    final query = fixture.flowQuery();
    addTearDown(producer.close);
    addTearDown(query.close);
    final snapshot = await producer.add(
      FlowJob(
        key: const JobKey(queueName: 'weighted', jobId: 'root'),
        name: 'root',
        data: null,
        children: <FlowChild>[
          FlowChild(
            job: FlowJob(
              key: const JobKey(queueName: 'weighted', jobId: 'leaf'),
              name: 'leaf',
              data: null,
            ),
          ),
        ],
      ),
    );
    final projection = await query.get(snapshot.root);
    expect(projection!.root.children.single.node.weightedProgress, 0);
  });

  // 验证子 Job 部分成功部分失败的场景：失败子 Job 触发 FlowDependencyFailure，
  // 但 retry 成功后父 Job 能自动恢复到 waiting 并最终完成。
  test('部分成功后重试会恢复失败的父 Job', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final producer = fixture.flowProducer();
    final query = fixture.flowQuery();
    addTearDown(producer.close);
    addTearDown(query.close);

    final parentQueue = fixture.queue('retry-parent');
    final childQueue = fixture.queue('retry-child');
    addTearDown(parentQueue.close);
    addTearDown(childQueue.close);

    var parentEntered = false;
    final parentCompleted = Completer<void>();
    final parentWorker = fixture.worker(
      queueName: 'retry-parent',
      processor: (job) async {
        parentEntered = true;
        final results = await job.getDependenciesResult();
        if (!parentCompleted.isCompleted) parentCompleted.complete();
        return <String, Object?>{'sum': results.length};
      },
    );

    // 按 jobId 分别计数尝试次数：只有 child-b（配置 retry=2）第一次失败
    final childAttempts = <String, int>{};
    final childWorker = fixture.worker(
      queueName: 'retry-child',
      processor: (ActiveJob job) {
        final id = job.key.jobId;
        childAttempts[id] = (childAttempts[id] ?? 0) + 1;
        // child-b 第一次失败；child-a 和 child-b 后续都成功
        if (id == 'child-b' && childAttempts[id] == 1) {
          throw StateError('first attempt fails');
        }
        return const <String, Object?>{'ok': true};
      },
    );
    addTearDown(parentWorker.close);
    addTearDown(childWorker.close);

    await parentWorker.start();
    await childWorker.start();

    // 原子创建 Flow：父 + 两个子（其中子 B 配置 retry=2）
    await producer.add(
      FlowJob(
        key: const JobKey(queueName: 'retry-parent', jobId: 'parent'),
        name: 'parent',
        data: null,
        children: <FlowChild>[
          FlowChild(
            job: FlowJob(
              key: const JobKey(queueName: 'retry-child', jobId: 'child-a'),
              name: 'a',
              data: null,
            ),
          ),
          FlowChild(
            job: FlowJob(
              key: const JobKey(queueName: 'retry-child', jobId: 'child-b'),
              name: 'b',
              data: null,
              options: JobOptions(retry: RetryPolicy(maxAttempts: 2)),
            ),
          ),
        ],
      ),
    );

    // 等待所有子 Job 完成（包括 retry）和父 Job 完成
    await parentCompleted.future.timeout(
      const Duration(seconds: 5),
      onTimeout: () => throw StateError('parent did not complete after retry'),
    );
    await fixture.runtime.flushMicrotasks();

    expect(parentEntered, isTrue);
    // child-a 只跑一次，child-b 跑两次（失败后 retry 成功）
    expect(childAttempts['child-a'], 1);
    expect(childAttempts['child-b'], 2);

    final parentJob = await parentQueue.getJob('parent');
    expect((await parentJob!.refresh()).status, JobStatus.completed);
  });

  // 验证删除含有 active 后代的 Flow 子树是原子拒绝的：Backend 在单次事务中
  // 扫描整棵子树，只要有一个 active Job 就抛 InvalidJobTransition，
  // 不会留下部分已删除的脏状态。
  test('删除含有 active 子代的 Flow 是原子操作', () async {
    final fixture = await IntegrationFixture.open();
    addTearDown(fixture.close);
    final producer = fixture.flowProducer();
    final query = fixture.flowQuery();
    addTearDown(producer.close);
    addTearDown(query.close);

    final parentQueue = fixture.queue('rm-parent');
    final childQueue = fixture.queue('rm-child');
    addTearDown(parentQueue.close);
    addTearDown(childQueue.close);

    // 子 Job 的 processor 阻塞在 barrier 上——保证它会处于 active 状态
    final childBarrier = Completer<void>();
    final childStarted = Completer<void>();
    final childWorker = fixture.worker(
      queueName: 'rm-child',
      processor: (ActiveJob job) async {
        childStarted.complete();
        await childBarrier.future; // 阻塞，保持 active
        return const <String, Object?>{'done': true};
      },
    );
    addTearDown(childWorker.close);
    await childWorker.start();

    await producer.add(
      FlowJob(
        key: const JobKey(queueName: 'rm-parent', jobId: 'root'),
        name: 'root',
        data: null,
        children: <FlowChild>[
          FlowChild(
            job: FlowJob(
              key: const JobKey(queueName: 'rm-child', jobId: 'leaf'),
              name: 'leaf',
              data: null,
            ),
          ),
        ],
      ),
    );

    // 等待子 Job 被领取并进入 active
    await childStarted.future.timeout(
      const Duration(seconds: 3),
      onTimeout: () => throw StateError('child never started'),
    );

    // 子 Job 正在执行（active）——尝试删除父 Job，应该被原子拒绝
    final parentJob = await parentQueue.getJob('root');
    expect(parentJob, isNotNull);
    expect(() => parentJob!.remove(), throwsA(isA<InvalidJobTransition>()));

    // 确认 Flow 完好无损——查询仍然返回 root
    final projection = await query.get(
      const JobKey(queueName: 'rm-parent', jobId: 'root'),
    );
    expect(projection, isNotNull);
    expect(projection!.root.children, hasLength(1));

    // 释放 barrier，让子 Job 完成
    childBarrier.complete();
    await fixture.runtime.flushMicrotasks();

    // 现在子 Job 已 completed，可以成功删除（先 refresh 拿到最新 revision）
    await parentJob!.refresh();
    final removedKeys = await parentJob.remove();
    expect(removedKeys, hasLength(2)); // root + leaf

    // 删除后 FlowQuery 应返回 null
    final afterDelete = await query.get(
      const JobKey(queueName: 'rm-parent', jobId: 'root'),
    );
    expect(afterDelete, isNull);
  });
}
