import 'job_key.dart';
import 'job_options.dart';
import 'job_record.dart';
import 'json_like.dart';

/// Flow 中单个 Job 的输入规格（非泛型原始 JSON-like 模型）。
///
/// 构造时校验 [data] 为 JSON-like。由 FlowJobSpec 或 FlowSnapshot 中的 jobs 列表引用。
class JobSpec {
  JobSpec({
    required this.key,
    required this.name,
    Object? data,
    JobOptions? options,
  }) : data = freezeJsonLike(data, argumentName: 'data'),
       options = options ?? JobOptions();

  /// Job 复合标识。
  final JobKey key;

  /// Job 名称。
  final String name;

  /// Job 业务输入数据（已冻结的 JSON-like，可空）。
  final Object? data;

  /// Job 级运行选项。默认使用空的 JobOptions。
  final JobOptions options;
}

/// Flow 中连接父节点到子节点的边。
///
/// [weight] 是父节点的加权进度计算时对该子节点分配的权重；
/// 构造时拒绝非有限或负值。
class FlowEdge {
  FlowEdge({required this.parent, required this.child, this.weight = 1}) {
    if (!weight.isFinite || weight < 0) {
      throw ArgumentError.value(weight, 'weight');
    }
  }

  /// 父节点 Job 复合标识。
  final JobKey parent;

  /// 子节点 Job 复合标识。
  final JobKey child;

  /// 边的非负权重，用于父节点加权进度计算。默认 1。
  final double weight;
}

/// Flow 的输入规格（不可变、已校验，非泛型原始 JSON-like 模型）。
///
/// 构造时会执行完整的 Flow 树校验：JobKey 唯一性、root 必须存在、
/// 子节点只能有一个父、无环、所有节点从 root 可达。
class FlowJobSpec {
  FlowJobSpec({
    required this.root,
    required List<JobSpec> jobs,
    required List<FlowEdge> edges,
  }) : jobs = List<JobSpec>.unmodifiable(jobs),
       edges = List<FlowEdge>.unmodifiable(edges) {
    _validate(
      root: root,
      keys: this.jobs.map((job) => job.key),
      edges: this.edges,
    );
  }

  /// 根 Job 的复合标识。
  final JobKey root;

  /// Flow 中所有 Job 的规格列表（不可变）。
  final List<JobSpec> jobs;

  /// Flow 中所有边的列表（不可变）。
  final List<FlowEdge> edges;
}

/// Flow 的 Backend 持久化快照（非泛型原始 JSON-like 模型）。
///
/// 与 [FlowJobSpec] 类似，但 jobs 列表包含的是已持久化的 [JobRecord]。
/// 构造时同样执行完整的 Flow 树校验。
class FlowSnapshot {
  FlowSnapshot({
    required this.root,
    required List<JobRecord> jobs,
    required List<FlowEdge> edges,
  }) : jobs = List<JobRecord>.unmodifiable(jobs),
       edges = List<FlowEdge>.unmodifiable(edges) {
    _validate(
      root: root,
      keys: this.jobs.map((job) => job.key),
      edges: this.edges,
    );
  }

  /// 根 Job 的复合标识。
  final JobKey root;

  /// Flow 中所有 Job 的持久化记录列表（不可变）。
  final List<JobRecord> jobs;

  /// Flow 中所有边的列表（不可变）。
  final List<FlowEdge> edges;
}

/// Flow 树校验逻辑。
///
/// 依次检查：
/// 1. JobKey 唯一性
/// 2. root 在 jobs 中
/// 3. 每条边引用的 parent/child 都在 jobs 中
/// 4. 子节点只能有一个父（通过 [edges] 中的 child 计数）
/// 5. 无环（从 root DFS，visited 集合避免回环）
/// 6. 所有 Job 从 root 可达（visited 覆盖全部 keys）
void _validate({
  required JobKey root,
  required Iterable<JobKey> keys,
  required List<FlowEdge> edges,
}) {
  // 1. 收集并校验 JobKey 唯一性
  final keySet = <JobKey>{};
  for (final key in keys) {
    if (!keySet.add(key)) {
      throw ArgumentError('Flow 包含重复的 JobKey $key');
    }
  }
  // 2. 校验 root 存在
  if (!keySet.contains(root)) {
    throw ArgumentError('Flow root 缺失');
  }

  // 3-4. 校验边引用合法性和子节点单父
  final parentCount = <JobKey, int>{};
  final children = <JobKey, List<JobKey>>{};
  for (final edge in edges) {
    if (!keySet.contains(edge.parent) || !keySet.contains(edge.child)) {
      throw ArgumentError('Flow 边引用了未知的 JobKey');
    }
    if (!edge.weight.isFinite || edge.weight < 0) {
      throw ArgumentError.value(edge.weight, 'weight');
    }
    // 统计每个 child 的父节点数量，保证单父
    final count = (parentCount[edge.child] ?? 0) + 1;
    if (count > 1) {
      throw ArgumentError('Flow 子节点有多个父节点');
    }
    parentCount[edge.child] = count;
    // 构建父 -> 直接子的邻接表
    (children[edge.parent] ??= <JobKey>[]).add(edge.child);
  }

  // 5. 从 root DFS，检测环
  final visited = <JobKey>{root};
  final pending = <JobKey>[root];
  for (var index = 0; index < pending.length; index++) {
    final parent = pending[index];
    for (final child in children[parent] ?? const <JobKey>[]) {
      if (!visited.add(child)) {
        throw ArgumentError('Flow 包含环');
      }
      pending.add(child);
    }
  }

  // 6. 校验所有 Job 从 root 可达
  if (visited.length != keySet.length) {
    throw ArgumentError('Flow 包含从 root 不可达的 Job');
  }
}
