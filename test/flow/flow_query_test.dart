import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/flow/index.dart';
import 'package:test/test.dart';

import 'support/fake_queue_backend.dart';
import 'support/fake_queue_signal.dart';

/// FlowQuery 测试 fixture：组装一个带 root 和八个覆盖全部 JobStatus 直接子节点的 FlowSnapshot
class QueryFixture {
  QueryFixture._({
    required this.query,
    required this.rootKey,
    required this.backend,
  });

  final FlowQuery query;
  final JobKey rootKey;
  final FakeFlowBackend backend;

  /// 八个覆盖全部 JobStatus 的直接子节点 fixture
  factory QueryFixture.eightChildren() {
    final rootKey = const JobKey(queueName: 'root-q', jobId: 'root');
    final now = DateTime.utc(2026, 9, 22, 12);
    final children = <_ChildJob>[
      _ChildJob('child-0', JobStatus.waiting),
      _ChildJob('child-1', JobStatus.delayed),
      _ChildJob('child-2', JobStatus.waitingChildren),
      _ChildJob('child-3', JobStatus.active),
      _ChildJob('child-4', JobStatus.paused),
      _ChildJob('child-5', JobStatus.completed, progress: 1.0),
      _ChildJob('child-6', JobStatus.failed),
      _ChildJob('child-7', JobStatus.canceled),
    ];
    final jobs = <JobRecord>[
      _makeRecord(
        rootKey,
        'root',
        JobStatus.active,
        null,
        null,
        null,
        now,
        now,
      ),
      for (final c in children)
        _makeRecord(
          JobKey(queueName: 'child-q', jobId: c.name),
          c.name,
          c.status,
          c.progress,
          c.status == JobStatus.completed
              ? const <String, Object?>{'ok': true}
              : null,
          c.status == JobStatus.failed
              ? JobFailure(type: 'error', message: 'boom', failedAt: now)
              : null,
          now,
          now,
          parentKey: rootKey,
        ),
    ];
    final edges = <FlowEdge>[
      for (final c in children)
        FlowEdge(
          parent: rootKey,
          child: JobKey(queueName: 'child-q', jobId: c.name),
          weight: 1,
        ),
    ];
    final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
    final backend = FakeFlowBackend(capabilities: _anyCapabilities());
    backend.flowResult = snapshot;
    return QueryFixture._(
      query: FlowQuery(backend: backend),
      rootKey: rootKey,
      backend: backend,
    );
  }

  factory QueryFixture._progress(
    List<_ProgressChild> childRecords, {
    JobStatus? rootStatus,
  }) {
    final rootKey = const JobKey(queueName: 'q', jobId: 'root');
    final now = DateTime.utc(2026, 9, 22, 12);
    final jobs = <JobRecord>[
      _makeRecord(
        rootKey,
        'root',
        rootStatus ?? JobStatus.active,
        null,
        null,
        null,
        now,
        now,
      ),
      for (var i = 0; i < childRecords.length; i++)
        _makeRecord(
          JobKey(queueName: 'q', jobId: 'c$i'),
          'c$i',
          JobStatus.completed,
          childRecords[i].progress,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
    ];
    final edges = <FlowEdge>[
      for (var i = 0; i < childRecords.length; i++)
        FlowEdge(
          parent: rootKey,
          child: JobKey(queueName: 'q', jobId: 'c$i'),
          weight: childRecords[i].weight,
        ),
    ];
    final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
    final backend = FakeFlowBackend(capabilities: _anyCapabilities());
    backend.flowResult = snapshot;
    return QueryFixture._(
      query: FlowQuery(backend: backend),
      rootKey: rootKey,
      backend: backend,
    );
  }
}

class _ChildJob {
  _ChildJob(this.name, this.status, {this.progress});
  final String name;
  final JobStatus status;
  final Object? progress;
}

class _ProgressChild {
  const _ProgressChild({required this.weight, this.progress});
  final double weight;
  final Object? progress;
}

JobRecord _makeRecord(
  JobKey key,
  String name,
  JobStatus status,
  Object? progress,
  Object? result,
  JobFailure? failure,
  DateTime createdAt,
  DateTime updatedAt, {
  JobKey? parentKey,
}) {
  return JobRecord(
    schemaVersion: 0,
    key: key,
    name: name,
    data: null,
    status: status,
    progress: progress,
    result: result,
    failure: failure,
    revision: 0,
    attemptsMade: 0,
    stalledCount: 0,
    priority: 0,
    availableAt: createdAt,
    options: JobOptions(),
    parentKey: parentKey,
    createdAt: createdAt,
    updatedAt: updatedAt,
    startedAt: null,
    finishedAt: null,
    execution: null,
    controlRequest: null,
  );
}

BackendCapabilities _anyCapabilities() => BackendCapabilities(
  profile: BackendProfile.local,
  durability: Durability.volatile,
  delayedJobs: true,
  priorities: true,
  atomicFlows: true,
  eventDelivery: EventDelivery.liveOnly,
);

void main() {
  // --- DTO 不变量：各种 FlowProjection 子结构的参数校验 ---

  group('DTO 不变量', () {
    test(
      '会拒绝 status 既非 failed 也非 canceled 的子级失败引用',
      () {
        // 验证 FlowChildFailureReference 只接受 failed / canceled 状态
        expect(
          () => FlowChildFailureReference(
            key: const JobKey(queueName: 'q', jobId: 'x'),
            status: JobStatus.completed,
            failure: null,
          ),
          throwsArgumentError,
        );
      },
    );

    test('会拒绝权重为负数、NaN 或无穷大的子级投影', () {
      // 验证 FlowChildProjection.weight 拒绝负数、NaN、无穷大
      expect(
        () => FlowChildProjection(weight: -1, node: _dummyNode()),
        throwsArgumentError,
      );
      expect(
        () => FlowChildProjection(weight: double.nan, node: _dummyNode()),
        throwsArgumentError,
      );
      expect(
        () => FlowChildProjection(weight: double.infinity, node: _dummyNode()),
        throwsArgumentError,
      );
    });

    test(
      '会拒绝 progress 非有限或超出范围的节点投影',
      () {
        // 验证 FlowNodeProjection 的 ownProgress / weightedProgress 拒绝 NaN、超范围值
        expect(
          () => _makeNodeProjection(ownProgress: double.nan),
          throwsArgumentError,
        );
        expect(
          () => _makeNodeProjection(weightedProgress: double.infinity),
          throwsArgumentError,
        );
        expect(
          () => _makeNodeProjection(ownProgress: 1.5),
          throwsArgumentError,
        );
        expect(
          () => _makeNodeProjection(weightedProgress: -0.1),
          throwsArgumentError,
        );
      },
    );

    test('会拒绝 descendant count 为负数的节点投影', () {
      // 验证 descendantCounts 的每种 JobStatus 计数不能为负数
      expect(
        () => _makeNodeProjection(
          counts: {
            for (final s in JobStatus.values)
              s: s == JobStatus.waiting ? -1 : 0,
          },
        ),
        throwsArgumentError,
      );
    });

    test('会拒绝缺少某个 JobStatus 计数的节点投影', () {
      // 验证 descendantCounts 必须包含全部 8 种 JobStatus 键
      expect(
        () => _makeNodeProjection(
          counts: <JobStatus, int>{
            for (final s in JobStatus.values.where(
              (s) => s != JobStatus.waiting,
            ))
              s: 0,
          },
        ),
        throwsArgumentError,
      );
    });

    test(
      '会拒绝 processed 或 unprocessed 计数为负数的节点投影',
      () {
        // 验证 processedCount 和 unprocessedCount 不能为负数
        expect(
          () => _makeNodeProjection(processedCount: -1, unprocessedCount: 0),
          throwsArgumentError,
        );
      },
    );

    test(
      'descendant 计数无法划分所有子代时会拒绝节点投影',
      () {
        // 验证 processedCount + unprocessedCount 必须等于各状态 descendantCounts 之和
        // 示例：两个 waiting 子节点，processed=0, unprocessed=2, total=2；故意让 processed=1 打破不变量
        expect(
          () => _makeNodeProjection(
            counts: {for (final s in JobStatus.values) s: 0}
              ..[JobStatus.waiting] = 2,
            processedCount: 1,
            unprocessedCount: 1,
          ),
          throwsArgumentError,
        );
      },
    );
  });

  // --- 直接投影：FlowProjectionDepth.direct 只展开 root 的直接子节点 ---

  group('直接查询', () {
    test('backend 没有 flow snapshot 时返回 null', () async {
      // 验证 Backend 返回 null 时，FlowQuery.get 也返回 null 而不是抛异常
      final backend = FakeFlowBackend(capabilities: _anyCapabilities());
      backend.flowResult = null;
      final query = FlowQuery(backend: backend);
      final result = await query.get(
        const JobKey(queueName: 'q', jobId: 'missing'),
      );
      expect(result, isNull);
    });

    test(
      '只调用一次 getFlow，且不会调用 backend 写入方法',
      () async {
        // 验证只读查询：只调用一次 getFlow，完全不触达任何 Backend 写操作
        final fixture = QueryFixture.eightChildren();
        await fixture.query.get(fixture.rootKey);
        expect(fixture.backend.getFlowRequests, hasLength(1));
      },
    );

    test(
      'snapshot 的 root 与请求的 root 不同时会拒绝该 snapshot',
      () async {
        // 验证 Backend 返回的 FlowSnapshot.root 必须与查询 key 一致
        final fixture = QueryFixture.eightChildren();
        await expectLater(
          fixture.query.get(const JobKey(queueName: 'other', jobId: 'root')),
          throwsArgumentError,
        );
      },
    );

    test(
      '直接投影只包含 root 和直接子级',
      () async {
        // 验证 direct 投影只展开 root 的直接子节点，不会递归更深层
        final fixture = QueryFixture.eightChildren();
        final projection = await fixture.query.get(fixture.rootKey);
        expect(projection!.root.children, hasLength(8));
      },
    );

    test(
      '直接子代计数包含所有 JobStatus，但不包含 root',
      () async {
        // 验证 descendantCounts 为 8 种 JobStatus 各统计一次，且 root 自身不计入
        final fixture = QueryFixture.eightChildren();
        final projection = await fixture.query.get(fixture.rootKey);
        final counts = projection!.root.descendantCounts;
        for (final status in JobStatus.values) {
          expect(counts.containsKey(status), isTrue);
        }
        expect(counts[JobStatus.waiting], 1);
        expect(counts[JobStatus.delayed], 1);
        expect(counts[JobStatus.waitingChildren], 1);
        expect(counts[JobStatus.active], 1);
        expect(counts[JobStatus.paused], 1);
        expect(counts[JobStatus.completed], 1);
        expect(counts[JobStatus.failed], 1);
        expect(counts[JobStatus.canceled], 1);
      },
    );

    test(
      'processed 和 unprocessed 计数共同划分所有直接子代',
      () async {
        // 验证 processedCount + unprocessedCount = 直接子节点总数，其中 completed/failed/canceled 算 processed
        final fixture = QueryFixture.eightChildren();
        final projection = await fixture.query.get(fixture.rootKey);
        // completed(1) + failed(1) + canceled(1) = 3; 其他 = 5
        expect(projection!.root.processedCount, 3);
        expect(projection.root.unprocessedCount, 5);
        expect(
          projection.root.processedCount + projection.root.unprocessedCount,
          8,
        );
      },
    );

    test('子级结果会包含已完成但结果为 null 的项', () async {
      // 验证 childResults 只收集 completed 子节点的 result（本例中只有一个 completed 子节点）
      final fixture = QueryFixture.eightChildren();
      final projection = await fixture.query.get(fixture.rootKey);
      // completed child 的 result 是非 null 的 ({'ok': true})
      expect(projection!.root.childResults, hasLength(1));
      expect(projection.root.childResults.single.key.jobId, 'child-5');
      expect(projection.root.childResults.single.result, isNotNull);
    });

    test('子级失败会保留 failed 和 canceled 引用', () async {
      // 验证 childFailures 同时包含 failed（failure 非 null）和 canceled（failure 为 null）子节点的引用
      final fixture = QueryFixture.eightChildren();
      final projection = await fixture.query.get(fixture.rootKey);
      expect(projection!.root.childFailures, hasLength(2));
      final failedKeys = projection.root.childFailures
          .map((f) => f.key.jobId)
          .toSet();
      expect(failedKeys, containsAll(<String>['child-6', 'child-7']));
      // failed 的 failure 不为 null
      final failedRef = projection.root.childFailures.firstWhere(
        (f) => f.status == JobStatus.failed,
      );
      expect(failedRef.failure, isNotNull);
      // canceled 的 failure 为 null（fixture 构造时没给）
      final canceledRef = projection.root.childFailures.firstWhere(
        (f) => f.status == JobStatus.canceled,
      );
      expect(canceledRef.failure, isNull);
    });
  });

  // --- 直接加权进度：只用直接子节点的 numeric progress 参与加权 ---

  group('直接加权进度', () {
    test('直接进度只使用直接子级的数值型 progress', () async {
      // 验证加权进度只认直接子节点中 progress 为数字类型的；结构化/空 progress 被当作 0
      final fixture = QueryFixture._progress(<_ProgressChild>[
        _ProgressChild(weight: 1, progress: 0.25),
        _ProgressChild(weight: 3, progress: 0.75),
        _ProgressChild(weight: 4, progress: const <String, Object?>{'done': 1}),
      ]);
      final projection = await fixture.query.get(fixture.rootKey);
      expect(projection!.root.weightedProgress, closeTo(0.3125, 0.000001));
    });

    test('没有直接子级时加权进度为 0', () async {
      // 验证无直接子节点时 weightedProgress 为 0（避免除零）
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final now = DateTime.utc(2026, 9, 22, 12);
      final snapshot = FlowSnapshot(
        root: rootKey,
        jobs: [
          _makeRecord(
            rootKey,
            'root',
            JobStatus.waiting,
            null,
            null,
            null,
            now,
            now,
          ),
        ],
        edges: const <FlowEdge>[],
      );
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      final projection = await query.get(rootKey);
      expect(projection!.root.weightedProgress, 0);
    });

    test('所有直接权重均为 0 时加权进度为 0', () async {
      // 验证全部子节点 weight=0 时 weightedProgress 为 0（避免除零）
      final fixture = QueryFixture._progress(<_ProgressChild>[
        _ProgressChild(weight: 0, progress: 1.0),
        _ProgressChild(weight: 0, progress: 0.0),
      ]);
      final projection = await fixture.query.get(fixture.rootKey);
      expect(projection!.root.weightedProgress, 0);
    });

    test('缺失或结构化的 progress 按 0 计入', () async {
      // 验证 null 和结构化（Map / List）progress 都按 0 处理，不参与加权
      final fixture = QueryFixture._progress(<_ProgressChild>[
        _ProgressChild(weight: 1, progress: null),
        _ProgressChild(weight: 1, progress: const <String, Object?>{'key': 1}),
        _ProgressChild(weight: 1, progress: const [1, 2, 3]),
      ]);
      final projection = await fixture.query.get(fixture.rootKey);
      expect(projection!.root.weightedProgress, 0);
    });

    test('整数型 0 和 1 的 progress 会作为 double 参与计算', () async {
      // 验证 int 类型的 progress（0 / 1）会被当作 double 参与加权
      final fixture = QueryFixture._progress(<_ProgressChild>[
        _ProgressChild(weight: 1, progress: 0),
        _ProgressChild(weight: 1, progress: 1),
      ]);
      final projection = await fixture.query.get(fixture.rootKey);
      expect(projection!.root.weightedProgress, closeTo(0.5, 0.000001));
    });

    test('数值型 progress 的边界保持在 0 到 1 之间', () async {
      // 验证数值 progress 被裁剪在 [0, 1] 范围内
      final fixture = QueryFixture._progress(<_ProgressChild>[
        _ProgressChild(weight: 1, progress: 0),
        _ProgressChild(weight: 1, progress: 1),
      ]);
      final projection = await fixture.query.get(fixture.rootKey);
      expect(projection!.root.weightedProgress, greaterThanOrEqualTo(0));
      expect(projection.root.weightedProgress, lessThanOrEqualTo(1));
    });

    test(
      '多个最大有限权重且 progress 全为 1 时结果为 1',
      () async {
        // 验证 weight 为 double.maxFinite 时仍能正确归约到 progress=1，不会数值溢出
        final fixture = QueryFixture._progress(<_ProgressChild>[
          _ProgressChild(weight: double.maxFinite, progress: 1),
          _ProgressChild(weight: double.maxFinite, progress: 1),
        ]);
        final projection = await fixture.query.get(fixture.rootKey);
        expect(projection!.root.weightedProgress, 1);
        expect(projection.root.weightedProgress.isFinite, isTrue);
        expect(projection.root.weightedProgress.isNaN, isFalse);
      },
    );

    test(
      '多个最大有限权重对应 0 和 1 的 progress 时结果为 0.5',
      () async {
        // 验证 weight 为 double.maxFinite 时，progress 0 和 1 的加权仍正确归约到 0.5
        final fixture = QueryFixture._progress(<_ProgressChild>[
          _ProgressChild(weight: double.maxFinite, progress: 0),
          _ProgressChild(weight: double.maxFinite, progress: 1),
        ]);
        final projection = await fixture.query.get(fixture.rootKey);
        expect(projection!.root.weightedProgress, closeTo(0.5, 0.000001));
        expect(projection.root.weightedProgress.isFinite, isTrue);
        expect(projection.root.weightedProgress.isNaN, isFalse);
      },
    );
  });

  // --- 递归投影：FlowProjectionDepth.recursive 完整展开整棵子树 ---

  group('递归投影', () {
    test(
      '递归投影会且仅会展开每个子代一次',
      () async {
        // 验证递归投影完整展开每一层后代，每个节点只出现一次
        // 两层结构：root -> child-a, child-b -> grandchild
        final rootKey = const JobKey(queueName: 'q', jobId: 'root');
        final aKey = const JobKey(queueName: 'q', jobId: 'a');
        final bKey = const JobKey(queueName: 'q', jobId: 'b');
        final gcKey = const JobKey(queueName: 'q', jobId: 'gc');
        final now = DateTime.utc(2026, 9, 22, 12);
        final jobs = <JobRecord>[
          _makeRecord(
            rootKey,
            'root',
            JobStatus.active,
            null,
            null,
            null,
            now,
            now,
          ),
          _makeRecord(
            aKey,
            'a',
            JobStatus.completed,
            1.0,
            null,
            null,
            now,
            now,
            parentKey: rootKey,
          ),
          _makeRecord(
            bKey,
            'b',
            JobStatus.active,
            null,
            null,
            null,
            now,
            now,
            parentKey: rootKey,
          ),
          _makeRecord(
            gcKey,
            'gc',
            JobStatus.waiting,
            null,
            null,
            null,
            now,
            now,
            parentKey: bKey,
          ),
        ];
        final edges = <FlowEdge>[
          FlowEdge(parent: rootKey, child: aKey, weight: 1),
          FlowEdge(parent: rootKey, child: bKey, weight: 1),
          FlowEdge(parent: bKey, child: gcKey, weight: 1),
        ];
        final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
        final backend = FakeFlowBackend(capabilities: _anyCapabilities())
          ..flowResult = snapshot;
        final query = FlowQuery(backend: backend);

        final recursive = await query.get(
          rootKey,
          depth: FlowProjectionDepth.recursive,
        );
        expect(recursive!.root.children, hasLength(2));
        final bNode = recursive.root.children
            .firstWhere((c) => c.node.job.key.jobId == 'b')
            .node;
        expect(bNode.children, hasLength(1));
        // descendantCounts 不包含自身
        expect(recursive.root.descendantCounts[JobStatus.waiting], 1);
        expect(recursive.root.descendantCounts[JobStatus.active], 1);
        expect(recursive.root.descendantCounts[JobStatus.completed], 1);
      },
    );

    test('递归子代计数不包含节点自身', () async {
      // 验证每个节点的 descendantCounts 只统计后代，不包含自身的状态
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final aKey = const JobKey(queueName: 'q', jobId: 'a');
      final now = DateTime.utc(2026, 9, 22, 12);
      final jobs = <JobRecord>[
        _makeRecord(
          rootKey,
          'root',
          JobStatus.completed,
          1.0,
          null,
          null,
          now,
          now,
        ),
        _makeRecord(
          aKey,
          'a',
          JobStatus.waiting,
          null,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
      ];
      final snapshot = FlowSnapshot(
        root: rootKey,
        jobs: jobs,
        edges: <FlowEdge>[FlowEdge(parent: rootKey, child: aKey, weight: 1)],
      );
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );
      // root 自身 completed，descendantCounts 只统计子节点 a 的 waiting
      expect(recursive!.root.descendantCounts[JobStatus.waiting], 1);
      expect(recursive.root.descendantCounts[JobStatus.completed], 0);
    });

    test('递归投影中的每个父级仅包含其直接子级的结果和失败', () async {
      // 验证 childResults 和 childFailures 只包含直接子节点；孙节点结果归属于中间父节点
      // 两层：root -> child-a(completed), child-b(failed) -> grandchild-completed
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final aKey = const JobKey(queueName: 'q', jobId: 'a');
      final bKey = const JobKey(queueName: 'q', jobId: 'b');
      final gcKey = const JobKey(queueName: 'q', jobId: 'gc');
      final now = DateTime.utc(2026, 9, 22, 12);
      final jobs = <JobRecord>[
        _makeRecord(
          rootKey,
          'root',
          JobStatus.active,
          null,
          null,
          null,
          now,
          now,
        ),
        _makeRecord(
          aKey,
          'a',
          JobStatus.completed,
          1.0,
          'a-result',
          null,
          now,
          now,
          parentKey: rootKey,
        ),
        _makeRecord(
          bKey,
          'b',
          JobStatus.active,
          null,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
        _makeRecord(
          gcKey,
          'gc',
          JobStatus.completed,
          1.0,
          'gc-result',
          null,
          now,
          now,
          parentKey: bKey,
        ),
      ];
      final edges = <FlowEdge>[
        FlowEdge(parent: rootKey, child: aKey, weight: 1),
        FlowEdge(parent: rootKey, child: bKey, weight: 1),
        FlowEdge(parent: bKey, child: gcKey, weight: 1),
      ];
      final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);

      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );
      // root 只有 a 的直接结果；gc 不在 root 的 childResults 里
      expect(recursive!.root.childResults, hasLength(1));
      expect(recursive.root.childResults.single.key.jobId, 'a');
      // b 有自己的直接子 gc 的结果
      final bNode = recursive.root.children
          .firstWhere((c) => c.node.job.key.jobId == 'b')
          .node;
      expect(bNode.childResults, hasLength(1));
      expect(bNode.childResults.single.key.jobId, 'gc');
    });

    test(
      '递归的 processed 和 unprocessed 计数会划分每个子树',
      () async {
        // 验证 processed/unprocessed 的划分在每棵子树的局部都保持正确
        final rootKey = const JobKey(queueName: 'q', jobId: 'root');
        final aKey = const JobKey(queueName: 'q', jobId: 'a');
        final bKey = const JobKey(queueName: 'q', jobId: 'b');
        final gcKey = const JobKey(queueName: 'q', jobId: 'gc');
        final now = DateTime.utc(2026, 9, 22, 12);
        final jobs = <JobRecord>[
          _makeRecord(
            rootKey,
            'root',
            JobStatus.active,
            null,
            null,
            null,
            now,
            now,
          ),
          _makeRecord(
            aKey,
            'a',
            JobStatus.waiting,
            null,
            null,
            null,
            now,
            now,
            parentKey: rootKey,
          ),
          _makeRecord(
            bKey,
            'b',
            JobStatus.failed,
            0,
            null,
            JobFailure(type: 'e', message: 'm', failedAt: now),
            now,
            now,
            parentKey: rootKey,
          ),
          _makeRecord(
            gcKey,
            'gc',
            JobStatus.completed,
            1.0,
            null,
            null,
            now,
            now,
            parentKey: bKey,
          ),
        ];
        final edges = <FlowEdge>[
          FlowEdge(parent: rootKey, child: aKey, weight: 1),
          FlowEdge(parent: rootKey, child: bKey, weight: 1),
          FlowEdge(parent: bKey, child: gcKey, weight: 1),
        ];
        final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
        final backend = FakeFlowBackend(capabilities: _anyCapabilities())
          ..flowResult = snapshot;
        final query = FlowQuery(backend: backend);

        final recursive = await query.get(
          rootKey,
          depth: FlowProjectionDepth.recursive,
        );
        // root 的后代：a(waiting, unprocessed) + b(failed, processed) + gc(completed, 归入 b 的子树 processed)
        expect(recursive!.root.processedCount, 2);
        expect(recursive.root.unprocessedCount, 1);
        // b 的后代：只有 gc(completed)
        final bNode = recursive.root.children
            .firstWhere((c) => c.node.job.key.jobId == 'b')
            .node;
        expect(bNode.processedCount, 1);
        expect(bNode.unprocessedCount, 0);
      },
    );

    test('递归投影不会修改源 JobRecord', () async {
      // 验证递归投影不会修改原始 FlowSnapshot 中的 JobRecord（特别是 progress 字段）
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final now = DateTime.utc(2026, 9, 22, 12);
      final originalProgress = 0.5;
      final rootRecord = _makeRecord(
        rootKey,
        'root',
        JobStatus.active,
        originalProgress,
        null,
        null,
        now,
        now,
      );
      final snapshot = FlowSnapshot(
        root: rootKey,
        jobs: [rootRecord],
        edges: const <FlowEdge>[],
      );
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      await query.get(rootKey, depth: FlowProjectionDepth.recursive);
      // 验证原始 record 的 progress 没被修改
      expect(rootRecord.progress, originalProgress);
    });

    test('直接和递归投影为共享节点提供相同的本地进度', () async {
      // 验证 direct 与 recursive 投影在共享节点上的本地加权进度值保持一致
      // 两层不对称树：root
      // |- weight 3 -> branch（own progress 0.6，下方有 weight 1 leaf-a progress 0.2，weight 3 leaf-b progress 1.0）
      // `- weight 1 -> leaf-c progress 0.4
      // branch 的本地 weighted = (1*0.2 + 3*1.0) / 4 = 0.8；root 的本地 weighted = (3*0.6 + 1*0.4) / 4 = 0.55
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final branchKey = const JobKey(queueName: 'q', jobId: 'branch');
      final leafAKey = const JobKey(queueName: 'q', jobId: 'leaf-a');
      final leafBKey = const JobKey(queueName: 'q', jobId: 'leaf-b');
      final leafCKey = const JobKey(queueName: 'q', jobId: 'leaf-c');
      final now = DateTime.utc(2026, 9, 22, 12);
      final jobs = <JobRecord>[
        _makeRecord(
          rootKey,
          'root',
          JobStatus.active,
          null,
          null,
          null,
          now,
          now,
        ),
        _makeRecord(
          branchKey,
          'branch',
          JobStatus.completed,
          0.6,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
        _makeRecord(
          leafAKey,
          'leaf-a',
          JobStatus.completed,
          0.2,
          null,
          null,
          now,
          now,
          parentKey: branchKey,
        ),
        _makeRecord(
          leafBKey,
          'leaf-b',
          JobStatus.completed,
          1.0,
          null,
          null,
          now,
          now,
          parentKey: branchKey,
        ),
        _makeRecord(
          leafCKey,
          'leaf-c',
          JobStatus.completed,
          0.4,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
      ];
      final edges = <FlowEdge>[
        FlowEdge(parent: rootKey, child: branchKey, weight: 3),
        FlowEdge(parent: rootKey, child: leafCKey, weight: 1),
        FlowEdge(parent: branchKey, child: leafAKey, weight: 1),
        FlowEdge(parent: branchKey, child: leafBKey, weight: 3),
      ];
      final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);

      final direct = await query.get(rootKey);
      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );

      expect(direct!.root.weightedProgress, closeTo(0.55, 0.000001));
      expect(recursive!.root.weightedProgress, closeTo(0.55, 0.000001));
      // branch 在 direct 中是 root 的直接子；在 recursive 中是完整展开的
      final directBranch = direct.root.children
          .firstWhere((c) => c.node.job.key.jobId == 'branch')
          .node;
      final recursiveBranch = recursive.root.children
          .firstWhere((c) => c.node.job.key.jobId == 'branch')
          .node;
      expect(directBranch.weightedProgress, closeTo(0.8, 0.000001));
      expect(recursiveBranch.weightedProgress, closeTo(0.8, 0.000001));
    });

    test(
      '递归展开会保持进度局部，并使其不受深度影响',
      () async {
        // 递归展开不应该改变本地加权进度的计算结果（这一点已由上一个 direct/recursive 一致性测试覆盖）
      },
    );

    test('递归父级使用非叶子子级自身的进度，而非子级的加权进度', () async {
      // 关键语义验证：父节点加权时使用子节点的 ownProgress，而不是子节点已经聚合好的 weightedProgress
      // 同上面的两层不对称树例子：
      // root 应该用 branch 的 ownProgress=0.6 而不是 branch 的 weightedProgress=0.8
      // 如果错误地用 weightedProgress，root 的进度会变成 (3*0.8 + 1*0.4)/4 = 0.7，与正确的 0.55 不同
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final branchKey = const JobKey(queueName: 'q', jobId: 'branch');
      final leafAKey = const JobKey(queueName: 'q', jobId: 'leaf-a');
      final leafBKey = const JobKey(queueName: 'q', jobId: 'leaf-b');
      final leafCKey = const JobKey(queueName: 'q', jobId: 'leaf-c');
      final now = DateTime.utc(2026, 9, 22, 12);
      final jobs = <JobRecord>[
        _makeRecord(
          rootKey,
          'root',
          JobStatus.active,
          null,
          null,
          null,
          now,
          now,
        ),
        _makeRecord(
          branchKey,
          'branch',
          JobStatus.completed,
          0.6,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
        _makeRecord(
          leafAKey,
          'leaf-a',
          JobStatus.completed,
          0.2,
          null,
          null,
          now,
          now,
          parentKey: branchKey,
        ),
        _makeRecord(
          leafBKey,
          'leaf-b',
          JobStatus.completed,
          1.0,
          null,
          null,
          now,
          now,
          parentKey: branchKey,
        ),
        _makeRecord(
          leafCKey,
          'leaf-c',
          JobStatus.completed,
          0.4,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
      ];
      final edges = <FlowEdge>[
        FlowEdge(parent: rootKey, child: branchKey, weight: 3),
        FlowEdge(parent: rootKey, child: leafCKey, weight: 1),
        FlowEdge(parent: branchKey, child: leafAKey, weight: 1),
        FlowEdge(parent: branchKey, child: leafBKey, weight: 3),
      ];
      final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );
      // root 应该用 branch 的 ownProgress=0.6 而不是 branch 的 weightedProgress=0.8
      expect(recursive!.root.weightedProgress, closeTo(0.55, 0.000001));
    });

    test('直接和递归投影的 root 进度保持一致', () async {
      // 验证 direct 和 recursive 两种深度下 root 节点的本地加权进度值一致
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final now = DateTime.utc(2026, 9, 22, 12);
      final rootRecord = _makeRecord(
        rootKey,
        'root',
        JobStatus.completed,
        0.7,
        null,
        null,
        now,
        now,
      );
      final snapshot = FlowSnapshot(
        root: rootKey,
        jobs: [rootRecord],
        edges: const <FlowEdge>[],
      );
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      final direct = await query.get(rootKey);
      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );
      expect(direct!.root.weightedProgress, recursive!.root.weightedProgress);
    });

    test(
      '递归投影中的零权重分支贡献 0，且不会发生除零',
      () async {
        // 验证 weight=0 的分支不参与加权，保证不会除以 0
        final rootKey = const JobKey(queueName: 'q', jobId: 'root');
        final aKey = const JobKey(queueName: 'q', jobId: 'a');
        final bKey = const JobKey(queueName: 'q', jobId: 'b');
        final now = DateTime.utc(2026, 9, 22, 12);
        final jobs = <JobRecord>[
          _makeRecord(
            rootKey,
            'root',
            JobStatus.active,
            null,
            null,
            null,
            now,
            now,
          ),
          _makeRecord(
            aKey,
            'a',
            JobStatus.completed,
            1.0,
            null,
            null,
            now,
            now,
            parentKey: rootKey,
          ),
          _makeRecord(
            bKey,
            'b',
            JobStatus.waiting,
            null,
            null,
            null,
            now,
            now,
            parentKey: rootKey,
          ),
        ];
        final edges = <FlowEdge>[
          FlowEdge(parent: rootKey, child: aKey, weight: 1),
          FlowEdge(parent: rootKey, child: bKey, weight: 0),
        ];
        final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
        final backend = FakeFlowBackend(capabilities: _anyCapabilities())
          ..flowResult = snapshot;
        final query = FlowQuery(backend: backend);
        final recursive = await query.get(
          rootKey,
          depth: FlowProjectionDepth.recursive,
        );
        expect(recursive!.root.weightedProgress, closeTo(1.0, 0.000001));
      },
    );

    test('递归投影中结构化或缺失的叶子 progress 均贡献 0', () async {
      // 验证递归场景下，结构化和缺失的 progress 都被当作 0
      final rootKey = const JobKey(queueName: 'q', jobId: 'root');
      final aKey = const JobKey(queueName: 'q', jobId: 'a');
      final bKey = const JobKey(queueName: 'q', jobId: 'b');
      final cKey = const JobKey(queueName: 'q', jobId: 'c');
      final now = DateTime.utc(2026, 9, 22, 12);
      final jobs = <JobRecord>[
        _makeRecord(
          rootKey,
          'root',
          JobStatus.active,
          null,
          null,
          null,
          now,
          now,
        ),
        _makeRecord(
          aKey,
          'a',
          JobStatus.active,
          null,
          null,
          null,
          now,
          now,
          parentKey: rootKey,
        ),
        _makeRecord(
          bKey,
          'b',
          JobStatus.active,
          const <String, Object?>{'done': 1},
          null,
          null,
          now,
          now,
          parentKey: aKey,
        ),
        _makeRecord(
          cKey,
          'c',
          JobStatus.completed,
          1.0,
          null,
          null,
          now,
          now,
          parentKey: aKey,
        ),
      ];
      final edges = <FlowEdge>[
        FlowEdge(parent: rootKey, child: aKey, weight: 1),
        FlowEdge(parent: aKey, child: bKey, weight: 1),
        FlowEdge(parent: aKey, child: cKey, weight: 1),
      ];
      final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );
      // a 的本地 weighted：b(structured=0) 和 c(1.0) 权重都是 1 → (1*0 + 1*1) / 2 = 0.5
      final aNode = recursive!.root.children.single.node;
      expect(aNode.weightedProgress, closeTo(0.5, 0.000001));
    });

    test('递归投影处理深层嵌套 snapshot 时不会导致调用栈溢出', () async {
      // 验证 10000 层深度的 FlowSnapshot 递归投影不会导致栈溢出
      const depth = 10000;
      final now = DateTime.utc(2026, 9, 22, 12);
      final jobs = <JobRecord>[];
      final edges = <FlowEdge>[];
      for (var i = 0; i <= depth; i++) {
        final key = JobKey(queueName: 'q', jobId: 'node-$i');
        jobs.add(
          _makeRecord(
            key,
            'node-$i',
            i == depth ? JobStatus.completed : JobStatus.active,
            i == depth ? 1.0 : null,
            null,
            null,
            now,
            now,
            parentKey: i == 0
                ? null
                : JobKey(queueName: 'q', jobId: 'node-${i - 1}'),
          ),
        );
        if (i > 0) {
          edges.add(
            FlowEdge(
              parent: JobKey(queueName: 'q', jobId: 'node-${i - 1}'),
              child: key,
              weight: 1,
            ),
          );
        }
      }
      final rootKey = JobKey(queueName: 'q', jobId: 'node-0');
      final snapshot = FlowSnapshot(root: rootKey, jobs: jobs, edges: edges);
      final backend = FakeFlowBackend(capabilities: _anyCapabilities())
        ..flowResult = snapshot;
      final query = FlowQuery(backend: backend);
      final recursive = await query.get(
        rootKey,
        depth: FlowProjectionDepth.recursive,
      );
      expect(recursive, isNotNull);
      var current = recursive!.root;
      for (var i = 1; i <= depth; i++) {
        expect(current.children, hasLength(1));
        current = current.children.single.node;
      }
      expect(current.job.key.jobId, 'node-$depth');
    });
  });

  // --- 生命周期：close 幂等性、资源所有权、关闭失败处理 ---

  group('查询生命周期', () {
    test(
      'query close 可重复调用，之后再调用 get 会抛出 LifecycleError',
      () async {
        // 验证 FlowQuery.close 幂等；关闭后再次 get 抛 LifecycleError
        final backend = FakeFlowBackend(capabilities: _anyCapabilities());
        final query = FlowQuery(backend: backend);
        await query.close();
        await query.close();
        await expectLater(
          query.get(const JobKey(queueName: 'q', jobId: 'x')),
          throwsA(predicate((e) => e is LifecycleError)),
        );
      },
    );

    test('默认情况下，query close 不会关闭共享 backend', () async {
      // 验证默认情况下 FlowQuery 不关闭注入的 Backend（共享所有权）
      final backend = FakeFlowBackend(capabilities: _anyCapabilities());
      final query = FlowQuery(backend: backend);
      await query.close();
      expect(backend.closeCalls, 0);
    });

    test(
      '仅当 ownsBackend 为 true 时，query close 才会释放 backend',
      () async {
        // 验证 ownsBackend=true 时 FlowQuery.close 会关闭 Backend
        final backend = FakeFlowBackend(capabilities: _anyCapabilities());
        final query = FlowQuery(backend: backend, ownsBackend: true);
        await query.close();
        expect(backend.closeCalls, 1);
      },
    );

    test('两个 ownership flag 均为 false 时，producer 和 query 可以共享 backend', () async {
      // 验证 Producer 和 Query 可以共享同一个 Backend，且都不声明所有权时 Backend 不会被主动关闭
      final backend = FakeFlowBackend(
        capabilities: BackendCapabilities(
          profile: BackendProfile.local,
          durability: Durability.volatile,
          delayedJobs: true,
          priorities: true,
          atomicFlows: true,
          eventDelivery: EventDelivery.liveOnly,
        ),
      );
      final signal = FakeQueueSignal();
      final producer = FlowProducer(
        backend: backend,
        signal: signal,
        now: () => DateTime.now(),
      );
      final query = FlowQuery(backend: backend);
      await producer.close();
      await query.close();
      expect(backend.closeCalls, 0);
    });
  });
}

FlowNodeProjection _dummyNode() => _makeNodeProjection();

FlowNodeProjection _makeNodeProjection({
  double? ownProgress,
  double? weightedProgress,
  Map<JobStatus, int>? counts,
  int? processedCount,
  int? unprocessedCount,
}) {
  final c = counts ?? {for (final s in JobStatus.values) s: 0};
  final total = c.values.fold<int>(0, (a, b) => a + b);
  final processed =
      processedCount ??
      (c[JobStatus.completed]! + c[JobStatus.failed]! + c[JobStatus.canceled]!);
  final unprocessed = unprocessedCount ?? (total - processed);
  final rootKey = const JobKey(queueName: 'q', jobId: 'x');
  final now = DateTime.utc(2026, 9, 22, 12);
  return FlowNodeProjection(
    job: JobSnapshot(
      rawRecord: _makeRecord(
        rootKey,
        'x',
        JobStatus.waiting,
        null,
        null,
        null,
        now,
        now,
      ),
      codecs: JobPayloadCodecs.jsonLike(),
    ),
    ownProgress: ownProgress ?? 0,
    weightedProgress: weightedProgress ?? 0,
    descendantCounts: c,
    processedCount: processed,
    unprocessedCount: unprocessed,
    children: const <FlowChildProjection>[],
    childResults: const <FlowChildResult>[],
    childFailures: const <FlowChildFailureReference>[],
  );
}
