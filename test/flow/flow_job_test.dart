import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/flow/index.dart';
import 'package:test/test.dart';

void main() {
  // --- FlowJob 到 FlowJobSpec 的转换与校验 ---

  test('会将嵌套 FlowJob 输入转换为冻结的 FlowJobSpec', () {
    // 验证嵌套的 FlowJob 结构能正确转换为 FlowJobSpec，root、jobs 和 edges 都按预期填充
    final root = FlowJob(
      key: const JobKey(queueName: 'parent', jobId: 'root'),
      name: 'root',
      data: const <String, Object?>{},
      children: <FlowChild>[
        FlowChild(
          weight: 2,
          job: FlowJob(
            key: const JobKey(queueName: 'child', jobId: 'leaf'),
            name: 'leaf',
            data: const <String, Object?>{'part': 1},
          ),
        ),
      ],
    );

    final spec = root.toFlowJobSpec();

    expect(spec.root, root.key);
    expect(spec.jobs.map((job) => job.key), <JobKey>[
      root.key,
      const JobKey(queueName: 'child', jobId: 'leaf'),
    ]);
    expect(spec.edges.single.weight, 2);
  });

  test('会防御性地复制子级集合', () {
    // 验证 children 列表被防御性复制：转换后外部修改原始列表不影响已生成的 spec
    final originalChildren = <FlowChild>[
      FlowChild(
        job: FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'a'),
          name: 'a',
          data: null,
        ),
      ),
    ];
    final job = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'root'),
      name: 'root',
      data: null,
      children: originalChildren,
    );
    originalChildren.add(
      FlowChild(
        job: FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'b'),
          name: 'b',
          data: null,
        ),
      ),
    );
    expect(job.children, hasLength(1));
    final spec = job.toFlowJobSpec();
    expect(spec.jobs, hasLength(2));
  });

  test('会在生成的 JobSpec 记录中保留所有 JobOptions 值', () {
    // 验证 FlowJob 上的 JobOptions（priority、delay 等）完整保留到生成的 JobSpec 中
    final options = JobOptions(priority: 5, delay: Duration(seconds: 3));
    final job = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'a'),
      name: 'a',
      data: null,
      options: options,
    );
    final spec = job.toFlowJobSpec();
    expect(spec.jobs.single.options.priority, 5);
    expect(spec.jobs.single.options.delay, const Duration(seconds: 3));
  });

  test(
    '生成确定性的 Job 和边时会保留子级顺序',
    () {
      // 验证 children 按声明顺序输出到 jobs 和 edges，保证生成结果的确定性
      final job = FlowJob(
        key: const JobKey(queueName: 'q', jobId: 'root'),
        name: 'root',
        data: null,
        children: <FlowChild>[
          FlowChild(
            weight: 1,
            job: FlowJob(
              key: const JobKey(queueName: 'q', jobId: 'child-1'),
              name: 'c1',
              data: null,
            ),
          ),
          FlowChild(
            weight: 2,
            job: FlowJob(
              key: const JobKey(queueName: 'q', jobId: 'child-2'),
              name: 'c2',
              data: null,
            ),
          ),
        ],
      );
      final spec = job.toFlowJobSpec();
      expect(spec.jobs.map((j) => j.key.jobId), <String>[
        'root',
        'child-1',
        'child-2',
      ]);
      expect(spec.edges.map((e) => e.child.jobId), <String>[
        'child-1',
        'child-2',
      ]);
      expect(spec.edges.map((e) => e.weight), <double>[1, 2]);
    },
  );

  test('会拒绝不同分支中的重复 JobKey', () {
    // 验证转换过程中拒绝跨分支重复的 JobKey
    final dupChild = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'dup'),
      name: 'dup',
      data: null,
    );
    final root = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'root'),
      name: 'root',
      data: null,
      children: <FlowChild>[
        FlowChild(job: dupChild),
        FlowChild(job: dupChild),
      ],
    );
    expect(root.toFlowJobSpec, throwsArgumentError);
  });

  test('会拒绝拥有两个直接父级的共享子实例', () {
    // 验证转换过程中拒绝同一个子节点实例被两个不同父节点共享
    final shared = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'shared'),
      name: 'shared',
      data: null,
    );
    final root = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'root'),
      name: 'root',
      data: null,
      children: <FlowChild>[
        FlowChild(
          job: FlowJob(
            key: const JobKey(queueName: 'q', jobId: 'p1'),
            name: 'p1',
            data: null,
            children: <FlowChild>[FlowChild(job: shared)],
          ),
        ),
        FlowChild(
          job: FlowJob(
            key: const JobKey(queueName: 'q', jobId: 'p2'),
            name: 'p2',
            data: null,
            children: <FlowChild>[FlowChild(job: shared)],
          ),
        ),
      ],
    );
    expect(root.toFlowJobSpec, throwsArgumentError);
  });

  test('会拒绝负数、NaN 和无穷大的子级权重', () {
    // 验证 FlowChild.weight 拒绝负数、NaN 和无穷大值
    expect(
      () => FlowChild(
        weight: -1,
        job: FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'x'),
          name: 'x',
          data: null,
        ),
      ),
      throwsArgumentError,
    );
    expect(
      () => FlowChild(
        weight: double.nan,
        job: FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'x'),
          name: 'x',
          data: null,
        ),
      ),
      throwsArgumentError,
    );
    expect(
      () => FlowChild(
        weight: double.infinity,
        job: FlowJob(
          key: const JobKey(queueName: 'q', jobId: 'x'),
          name: 'x',
          data: null,
        ),
      ),
      throwsArgumentError,
    );
  });

  test('允许子级权重为 0', () {
    // 验证 weight = 0 是合法值（表示该子节点不参与加权进度计算）
    final child = FlowChild(
      weight: 0,
      job: FlowJob(
        key: const JobKey(queueName: 'q', jobId: 'zero'),
        name: 'zero',
        data: null,
      ),
    );
    expect(child.weight, 0);
  });

  test('会将 root 可达性和父级校验交由 FlowJobSpec 最终处理', () {
    // FlowJob.toFlowJobSpec 只负责转换，最终的 root 可达性与 parent 合法性校验由 FlowJobSpec 构造器负责
    // 这里只验证转换结果成功传递给 FlowJobSpec，后者自己的合法性在 core 层已有测试覆盖
    final job = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'root'),
      name: 'root',
      data: null,
    );
    final spec = job.toFlowJobSpec();
    expect(spec.root, const JobKey(queueName: 'q', jobId: 'root'));
    expect(spec.jobs, hasLength(1));

    // FlowJobSpec 本身的构造器会校验 root 必须存在于 jobs 中
    expect(
      () => FlowJobSpec(
        root: const JobKey(queueName: 'q', jobId: 'nonexistent'),
        jobs: spec.jobs,
        edges: spec.edges,
      ),
      throwsArgumentError,
    );
  });

  test('转换深层嵌套的 flow 时不会导致调用栈溢出', () {
    // 验证 10000 层深度的嵌套 FlowJob 转换不会导致栈溢出，转换实现必须使用迭代而非递归
    const depth = 10000;
    FlowJob current = FlowJob(
      key: const JobKey(queueName: 'q', jobId: 'leaf-0'),
      name: 'leaf-0',
      data: null,
    );
    for (var i = 1; i <= depth; i++) {
      current = FlowJob(
        key: JobKey(queueName: 'q', jobId: 'node-$i'),
        name: 'node-$i',
        data: null,
        children: <FlowChild>[FlowChild(job: current)],
      );
    }
    final spec = current.toFlowJobSpec();
    expect(spec.jobs, hasLength(depth + 1));
    expect(spec.edges, hasLength(depth));
    expect(spec.root, JobKey(queueName: 'q', jobId: 'node-$depth'));
  });
}
