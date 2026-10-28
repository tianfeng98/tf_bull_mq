// ignore_for_file: prefer_initializing_formals

import '../backend/index.dart';
import '../core/index.dart';
import 'flow_projection.dart';

/// 只读的 Flow 查询入口。
///
/// [FlowQuery] 只调用 `backend.getFlow`，不依赖或通知 Signal，不调用
/// 任何 Backend 写方法。Backend 返回 `null` 时查询返回 `null`。
///
/// 每次 [get] 最多触发一次 Backend 读；direct/recursive 只改变展开
/// 范围，所有共用节点的本地 `weightedProgress` 使用同一份
/// `localWeightedProgressByKey` 计算，保证 depth 切换不改变数值。
///
/// [codecs] 用于把 Backend 原始 JSON-like JobRecord 解码为业务类型化的
/// JobSnapshot。省略时使用默认 JSON-like Codec（标量和 Map/List 透传）。
final class FlowQuery<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  FlowQuery({
    required QueueBackend backend,
    bool ownsBackend = false,
    JobPayloadCodecs<TData, TProgress, TResult>? codecs,
  }) : _backend = backend,
       _ownsBackend = ownsBackend,
       _codecs = codecs ?? JobPayloadCodecs<TData, TProgress, TResult>.jsonLike();

  final QueueBackend _backend;
  final bool _ownsBackend;
  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;
  var _closed = false;

  /// 读取指定 root 的 Flow 并构建类型化投影。
  ///
  /// 默认 [depth] 为 [FlowProjectionDepth.direct]，只返回根节点和直接
  /// 子节点的本地投影；`recursive` 会展开完整后代树。Backend 返回
  /// `null` 时也返回 `null`，不抛异常。
  Future<FlowProjection<TData, TProgress, TResult>?> get(
    JobKey root, {
    FlowProjectionDepth depth = FlowProjectionDepth.direct,
  }) async {
    _ensureOpen('get');
    final snapshot = await _backend.getFlow(root);
    if (snapshot == null) return null;
    if (snapshot.root != root) {
      throw ArgumentError.value(
        snapshot.root,
        'snapshot.root',
        'Flow snapshot root must match the requested root.',
      );
    }

    // 用 Codec 把原始 JobRecord 解码为类型化 JobSnapshot
    final jobsByKey = <JobKey, JobSnapshot<TData, TProgress, TResult>>{
      for (final record in snapshot.jobs)
        record.key: JobSnapshot<TData, TProgress, TResult>(
          rawRecord: record,
          codecs: _codecs,
        ),
    };
    final edgesByParent = <JobKey, List<FlowEdge>>{};
    for (final edge in snapshot.edges) {
      (edgesByParent[edge.parent] ??= <FlowEdge>[]).add(edge);
    }

    // 预先为每个 Job 计算 ownProgress（数值归一化，非数值按 0）
    final ownProgressByKey = <JobKey, double>{
      for (final job in snapshot.jobs) job.key: _ownProgress(job.progress),
    };

    // 预先为每个 Job 计算本地 weightedProgress（只看直接子的 ownProgress）
    final localWeightedProgressByKey = <JobKey, double>{};
    for (final job in snapshot.jobs) {
      final edges = edgesByParent[job.key] ?? const <FlowEdge>[];
      final contributions = <(double, double)>[
        for (final edge in edges) (edge.weight, ownProgressByKey[edge.child]!),
      ];
      localWeightedProgressByKey[job.key] = _weightedProgress(contributions);
    }

    final rootNode = _buildRootNodeProjection(
      root: root,
      depth: depth,
      jobsByKey: jobsByKey,
      edgesByParent: edgesByParent,
      ownProgressByKey: ownProgressByKey,
      localWeightedProgressByKey: localWeightedProgressByKey,
    );

    return FlowProjection<TData, TProgress, TResult>(depth: depth, root: rootNode);
  }

  /// 按 depth 构建根节点投影。direct 只展开一层子节点；recursive 用
  /// 显式 post-order stack 完整展开。
  FlowNodeProjection<TData, TProgress, TResult> _buildRootNodeProjection({
    required JobKey root,
    required FlowProjectionDepth depth,
    required Map<JobKey, JobSnapshot<TData, TProgress, TResult>> jobsByKey,
    required Map<JobKey, List<FlowEdge>> edgesByParent,
    required Map<JobKey, double> ownProgressByKey,
    required Map<JobKey, double> localWeightedProgressByKey,
  }) {
    if (depth == FlowProjectionDepth.direct) {
      return _buildDirectNode(
        root,
        jobsByKey: jobsByKey,
        edgesByParent: edgesByParent,
        ownProgressByKey: ownProgressByKey,
        localWeightedProgressByKey: localWeightedProgressByKey,
      );
    }

    // recursive: 显式 post-order DFS 构建整棵树
    final projections = <JobKey, FlowNodeProjection<TData, TProgress, TResult>>{};
    final stack = <(JobKey, bool)>[(root, false)];
    while (stack.isNotEmpty) {
      final (key, expanded) = stack.removeLast();
      final edges = edgesByParent[key] ?? const <FlowEdge>[];
      if (!expanded) {
        stack.add((key, true));
        for (var index = edges.length - 1; index >= 0; index--) {
          stack.add((edges[index].child, false));
        }
        continue;
      }
      projections[key] = _buildRecursiveNode(
        key,
        projections: projections,
        edgesByParent: edgesByParent,
        jobsByKey: jobsByKey,
        ownProgressByKey: ownProgressByKey,
        localWeightedProgressByKey: localWeightedProgressByKey,
      );
    }
    return projections[root]!;
  }

  /// direct 模式下构建单层子节点投影。
  ///
  /// root 含其直接 children projection；每个 child projection 的 `children`
  /// 为空、`descendantCounts` 为 0，但 child 自身的 `weightedProgress` 仍按
  /// snapshot 中其直接依赖计算，保持与 recursive 模式数值一致。
  FlowNodeProjection<TData, TProgress, TResult> _buildDirectNode(
    JobKey key, {
    required Map<JobKey, JobSnapshot<TData, TProgress, TResult>> jobsByKey,
    required Map<JobKey, List<FlowEdge>> edgesByParent,
    required Map<JobKey, double> ownProgressByKey,
    required Map<JobKey, double> localWeightedProgressByKey,
  }) {
    final edges = edgesByParent[key] ?? const <FlowEdge>[];
    final children = <FlowChildProjection<TData, TProgress, TResult>>[
      for (final edge in edges)
        FlowChildProjection(
          weight: edge.weight,
          node: _buildDirectChildNode(
            edge.child,
            jobsByKey: jobsByKey,
            ownProgressByKey: ownProgressByKey,
            localWeightedProgressByKey: localWeightedProgressByKey,
          ),
        ),
    ];
    return buildNodeProjection(
      key: key,
      children: children,
      weightedProgress: localWeightedProgressByKey[key]!,
      jobsByKey: jobsByKey,
      ownProgressByKey: ownProgressByKey,
    );
  }

  /// direct 模式下叶子节点：children 为空、descendant counts 全 0。
  FlowNodeProjection<TData, TProgress, TResult> _buildDirectChildNode(
    JobKey key, {
    required Map<JobKey, JobSnapshot<TData, TProgress, TResult>> jobsByKey,
    required Map<JobKey, double> ownProgressByKey,
    required Map<JobKey, double> localWeightedProgressByKey,
  }) {
    return buildNodeProjection(
      key: key,
      children: const [],
      weightedProgress: localWeightedProgressByKey[key]!,
      jobsByKey: jobsByKey,
      ownProgressByKey: ownProgressByKey,
    );
  }

  /// recursive 模式下从已完成的子投影构建父节点
  FlowNodeProjection<TData, TProgress, TResult> _buildRecursiveNode(
    JobKey key, {
    required Map<JobKey, FlowNodeProjection<TData, TProgress, TResult>> projections,
    required Map<JobKey, List<FlowEdge>> edgesByParent,
    required Map<JobKey, JobSnapshot<TData, TProgress, TResult>> jobsByKey,
    required Map<JobKey, double> ownProgressByKey,
    required Map<JobKey, double> localWeightedProgressByKey,
  }) {
    final edges = edgesByParent[key] ?? const <FlowEdge>[];
    final children = <FlowChildProjection<TData, TProgress, TResult>>[
      for (final edge in edges)
        FlowChildProjection(
          weight: edge.weight,
          node: projections[edge.child]!,
        ),
    ];
    return buildNodeProjection(
      key: key,
      children: children,
      weightedProgress: localWeightedProgressByKey[key]!,
      jobsByKey: jobsByKey,
      ownProgressByKey: ownProgressByKey,
    );
  }

  /// 从 snapshot 直接 child 边构建完整 FlowNodeProjection，包括计数、
  /// 结果和失败引用。
  FlowNodeProjection<TData, TProgress, TResult> buildNodeProjection({
    required JobKey key,
    required List<FlowChildProjection<TData, TProgress, TResult>> children,
    required double weightedProgress,
    required Map<JobKey, JobSnapshot<TData, TProgress, TResult>> jobsByKey,
    required Map<JobKey, double> ownProgressByKey,
  }) {
    final job = jobsByKey[key]!;
    final counts = <JobStatus, int>{for (final s in JobStatus.values) s: 0};
    final childResults = <FlowChildResult<TResult>>[];
    final childFailures = <FlowChildFailureReference>[];
    for (final child in children) {
      final childNode = child.node;
      final childStatus = childNode.job.status;
      counts[childStatus] = (counts[childStatus] ?? 0) + 1;
      // 累加子节点后代计数
      for (final status in JobStatus.values) {
        counts[status] =
            (counts[status] ?? 0) + (childNode.descendantCounts[status] ?? 0);
      }
      if (childStatus == JobStatus.completed) {
        childResults.add(
          FlowChildResult<TResult>(key: childNode.job.key, result: childNode.job.result),
        );
      } else if (childStatus == JobStatus.failed ||
          childStatus == JobStatus.canceled) {
        childFailures.add(
          FlowChildFailureReference(
            key: childNode.job.key,
            status: childStatus,
            failure: childNode.job.failure,
          ),
        );
      }
    }
    final processed =
        counts[JobStatus.completed]! +
        counts[JobStatus.failed]! +
        counts[JobStatus.canceled]!;
    final total = counts.values.fold<int>(0, (a, b) => a + b);
    final unprocessed = total - processed;

    return FlowNodeProjection<TData, TProgress, TResult>(
      job: job,
      ownProgress: ownProgressByKey[key]!,
      weightedProgress: weightedProgress,
      descendantCounts: counts,
      processedCount: processed,
      unprocessedCount: unprocessed,
      children: children,
      childResults: childResults,
      childFailures: childFailures,
    );
  }

  /// 将 [JobRecord.progress] 归一化到 [0, 1] 有限 [double]；非数值返回 0。
  static double _ownProgress(Object? value) {
    if (value is! int && value is! double) return 0;
    final number = (value as num).toDouble();
    if (!number.isFinite) {
      throw ArgumentError.value(value, 'progress', 'Progress must be finite.');
    }
    return number.clamp(0, 1).toDouble();
  }

  /// 基于 [maxWeight] 归一化后累加子节点进度的加权算法。
  ///
  /// 先找出 [contributions] 中最大的 weight，以它归一化每条边的权重，
  /// 再分别累加加权后的 progress 和归一化权重；最终 `weighted / totalWeight`
  /// 得到结果。空集、全零权重或零 totalWeight 都返回 0；结果防御性 clamp 到 [0, 1]。
  static double _weightedProgress(
    Iterable<(double weight, double progress)> contributions,
  ) {
    final values = contributions.toList(growable: false);
    if (values.isEmpty) return 0;

    var maxWeight = 0.0;
    for (final (weight, _) in values) {
      if (weight > maxWeight) maxWeight = weight;
    }
    if (maxWeight == 0) return 0;

    var weighted = 0.0;
    var totalWeight = 0.0;
    for (final (weight, progress) in values) {
      final normalizedWeight = weight / maxWeight;
      weighted += normalizedWeight * progress;
      totalWeight += normalizedWeight;
    }
    if (totalWeight == 0) return 0;

    final result = weighted / totalWeight;
    if (!result.isFinite) {
      throw StateError('Weighted progress must be finite.');
    }
    return result.clamp(0, 1).toDouble();
  }

  /// 幂等关闭 Query，只在 [ownsBackend] 为 `true` 时释放 Backend。
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    if (_ownsBackend) {
      await _backend.close();
    }
  }

  void _ensureOpen(String operation) {
    if (_closed) {
      throw LifecycleError(
        'FlowQuery',
        operation,
        'Cannot query a Flow after FlowQuery.close().',
      );
    }
  }
}
