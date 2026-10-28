import '../core/index.dart';

/// FlowQuery 展开的深度。
///
/// [direct] 只返回根节点和其直接子节点的本地投影；
/// [recursive] 展开完整后代树，每个父节点独立聚合自己的直接子节点。
enum FlowProjectionDepth { direct, recursive }

/// 已完成直接子节点的结果引用（类型化，使用 TResult 业务类型）。
final class FlowChildResult<TResult extends Object?> {
  const FlowChildResult({required this.key, required this.result});

  final JobKey key;
  final TResult? result;
}

/// 失败或取消的直接子节点引用。
///
/// [status] 只允许 `failed` 或 `canceled`；[failure] 可以为 `null`
/// （例如被父依赖级联取消但未产生 [JobFailure]）。
final class FlowChildFailureReference {
  FlowChildFailureReference({
    required this.key,
    required this.status,
    required this.failure,
  }) {
    if (status != JobStatus.failed && status != JobStatus.canceled) {
      throw ArgumentError.value(
        status,
        'status',
        'FlowChildFailureReference.status must be failed or canceled.',
      );
    }
  }

  final JobKey key;
  final JobStatus status;
  final JobFailure? failure;
}

/// 带权边到子节点投影的组合。
final class FlowChildProjection<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  FlowChildProjection({required this.weight, required this.node}) {
    if (!weight.isFinite || weight < 0) {
      throw ArgumentError.value(weight, 'weight');
    }
  }

  final double weight;
  final FlowNodeProjection<TData, TProgress, TResult> node;
}

/// 单个 Job 的本地投影视图。
///
/// [ownProgress] 来自 [JobSnapshot.progress] 的数值归一化结果（非数值按 0）；
/// [weightedProgress] 只由本节点的直接子节点的 [ownProgress] 加权得到，
/// 不递归读取子节点的 `weightedProgress`；[descendantCounts] 不包含自身。
/// [job] 使用类型化的 JobSnapshot 视图。
final class FlowNodeProjection<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  FlowNodeProjection({
    required this.job,
    required this.ownProgress,
    required this.weightedProgress,
    required Map<JobStatus, int> descendantCounts,
    required this.processedCount,
    required this.unprocessedCount,
    required Iterable<FlowChildProjection<TData, TProgress, TResult>> children,
    required Iterable<FlowChildResult<TResult>> childResults,
    required Iterable<FlowChildFailureReference> childFailures,
  }) : descendantCounts = Map<JobStatus, int>.unmodifiable(descendantCounts),
       children =
           List<FlowChildProjection<TData, TProgress, TResult>>.unmodifiable(
             children,
           ),
       childResults = List<FlowChildResult<TResult>>.unmodifiable(childResults),
       childFailures = List<FlowChildFailureReference>.unmodifiable(
         childFailures,
       ) {
    _validateProgress('ownProgress', ownProgress);
    _validateProgress('weightedProgress', weightedProgress);
    _validateDescendantCounts(
      this.descendantCounts,
      processedCount,
      unprocessedCount,
    );
  }

  final JobSnapshot<TData, TProgress, TResult> job;
  final double ownProgress;
  final double weightedProgress;
  final Map<JobStatus, int> descendantCounts;
  final int processedCount;
  final int unprocessedCount;
  final List<FlowChildProjection<TData, TProgress, TResult>> children;
  final List<FlowChildResult<TResult>> childResults;
  final List<FlowChildFailureReference> childFailures;

  static void _validateProgress(String name, double value) {
    if (!value.isFinite || value < 0 || value > 1) {
      throw ArgumentError.value(
        value,
        name,
        'Progress must be finite and within [0, 1].',
      );
    }
  }

  static void _validateDescendantCounts(
    Map<JobStatus, int> counts,
    int processed,
    int unprocessed,
  ) {
    // 所有 JobStatus 都必须出现且非负
    for (final status in JobStatus.values) {
      final count = counts[status];
      if (count == null) {
        throw ArgumentError(
          'FlowNodeProjection.descendantCounts missing entry for $status.',
        );
      }
      if (count < 0) {
        throw ArgumentError.value(
          count,
          'descendantCounts[$status]',
          'Count must be non-negative.',
        );
      }
    }
    if (processed < 0) {
      throw ArgumentError.value(
        processed,
        'processedCount',
        'Must be non-negative.',
      );
    }
    if (unprocessed < 0) {
      throw ArgumentError.value(
        unprocessed,
        'unprocessedCount',
        'Must be non-negative.',
      );
    }
    final total = counts.values.fold<int>(0, (a, b) => a + b);
    if (processed + unprocessed != total) {
      throw ArgumentError(
        'FlowNodeProjection processed ($processed) + unprocessed ($unprocessed) '
        'must equal descendant total ($total).',
      );
    }
    // processed = completed + failed + canceled; unprocessed = remaining
    final expectedProcessed =
        counts[JobStatus.completed]! +
        counts[JobStatus.failed]! +
        counts[JobStatus.canceled]!;
    final expectedUnprocessed = total - expectedProcessed;
    if (processed != expectedProcessed || unprocessed != expectedUnprocessed) {
      throw ArgumentError(
        'FlowNodeProjection processed/unprocessed must partition descendants correctly.',
      );
    }
  }
}

/// FlowQuery 的完整结果（类型化）。
final class FlowProjection<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  const FlowProjection({required this.depth, required this.root});

  final FlowProjectionDepth depth;
  final FlowNodeProjection<TData, TProgress, TResult> root;
}
