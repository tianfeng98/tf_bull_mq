import '../core/index.dart';

/// 嵌套 Flow 中连接父节点到子节点的边定义。
///
/// [job] 是子节点的 [FlowJob] 定义；[weight] 是父-子边的非负权重，
/// 用于父节点加权进度计算。构造时拒绝非有限或负权重。
final class FlowChild<TData extends Object?> {
  FlowChild({required this.job, this.weight = 1}) {
    if (!weight.isFinite || weight < 0) {
      throw ArgumentError.value(
        weight,
        'weight',
        'FlowChild weight must be finite and non-negative.',
      );
    }
  }

  final FlowJob<TData> job;
  final double weight;
}

/// 易用的嵌套 Flow Job 输入。
///
/// 该类型是用户构造 Flow 的高层入口：[children] 以嵌套 [FlowChild] 表示
/// 子节点和边权重。不可变字段构造时即做防御性复制。
///
/// [toFlowJobSpec] 使用显式 stack 将嵌套输入转换为阶段 01 冻结的
/// [FlowJobSpec]（非泛型原始 JSON-like 模型），拒绝环、共享子节点和重复
/// [JobKey]，最终交由冻结构造器完成整树校验。
final class FlowJob<TData extends Object?> {
  FlowJob({
    required this.key,
    required this.name,
    required this.data,
    JobOptions? options,
    Iterable<FlowChild<TData>> children = const [],
  }) : options = options ?? JobOptions(),
       children = List<FlowChild<TData>>.unmodifiable(children);

  final JobKey key;
  final String name;
  final TData? data;
  final JobOptions options;
  final List<FlowChild<TData>> children;

  /// 将嵌套输入转换为冻结的 [FlowJobSpec]（非泛型原始 JSON-like 模型）。
  ///
  /// 转换使用显式 DFS stack，父节点先于子节点，同层保持输入顺序；
  /// 拒绝共享 child 实例（多父）、重复 [JobKey]；[FlowJobSpec] 构造
  /// 进一步保证 root 存在、单父、无环和可达性。
  ///
  /// [encode] 回调把业务 TData 转成 JSON-like Object?；省略时使用
  /// `PayloadCodec<TData>.jsonLike()` 默认透传。
  FlowJobSpec toFlowJobSpec({Object? Function(TData value)? encode}) {
    final encoder = encode ?? PayloadCodec<TData>.jsonLike().encodeNullable;
    final jobs = <JobSpec>[];
    final edges = <FlowEdge>[];
    final keys = <JobKey>{};
    final visited = Set<FlowJob>.identity();

    final stack = <(FlowJob, JobKey?, double?)>[(this, null, null)];
    while (stack.isNotEmpty) {
      final (node, parent, weight) = stack.removeLast();
      if (!visited.add(node)) {
        throw ArgumentError('FlowJob nodes must form one ownership tree.');
      }
      if (!keys.add(node.key)) {
        throw ArgumentError.value(node.key, 'key', 'Duplicate Flow JobKey.');
      }
      if (parent != null) {
        edges.add(FlowEdge(parent: parent, child: node.key, weight: weight!));
      }
      // 兼容 encodeNullable 签名（接受 TData?）与用户 encode 签名
      // （接受非空 TData）——null data 直接透传。
      final encoded = node.data == null
          ? null
          : encoder(node.data as TData);
      jobs.add(
        JobSpec(
          key: node.key,
          name: node.name,
          data: encoded,
          options: node.options,
        ),
      );
      for (var index = node.children.length - 1; index >= 0; index--) {
        final child = node.children[index];
        stack.add((child.job, node.key, child.weight));
      }
    }

    return FlowJobSpec(root: key, jobs: jobs, edges: edges);
  }
}
