import '../core/flow.dart';
import '../core/job_event.dart';
import '../core/job_key.dart';
import '../core/job_log.dart';
import '../core/job_record.dart';
import '../core/queue_state.dart';

/// 内存 Backend 的完整状态快照。
///
/// 此类是 [MemoryQueueBackend] 的唯一持久化容器，所有队列、Job、Flow、
/// 日志和事件历史的运行时数据都以 Map 形式存放在此。Backend 采用
/// copy-on-write 事务模型（见 [MemoryTransaction]），每次写入都会先
/// 对 committed store 做深拷贝得到 draft，在 draft 上执行 mutation，
/// 成功后将 draft 替换为新的 committed store。因此 [copy] 方法必须保证
/// 生成一个完全独立的深拷贝，两个 store 之间不存在任何共享的可变引用。
///
/// 数据生命周期：
/// - 进程内有效，进程重启全部丢失（Durability.volatile）
/// - Job 通过 addJob/addJobs/addFlow 进入 store，通过 removeJob 删除
/// - QueueState 在首次访问队列时自动创建（revision=0 表示未初始化）
/// - 事件历史按 QueuePolicy 自动裁剪；日志按 Job 级或 Queue 级策略裁剪
///
/// 字段分组：
/// 1. Job/Queue 主索引：[jobs]、[queues]
/// 2. Flow 依赖索引：[parentByChild]、[childrenByParent]
/// 3. 日志相关：[logsByJob]、[nextLogSequenceByJob]
/// 4. 事件流相关：[eventsByQueue]、[nextEventPositionByQueue]、
///    [trimmedEventPositionByQueue]
final class MemoryStore {
  /// 构造一个可选带初始值的 store。
  ///
  /// 所有参数均可选且默认为空集合，便于测试场景下预先注入状态。
  /// 生产环境中 [MemoryQueueBackend] 始终调用无参构造，得到空白 store。
  MemoryStore({
    Map<JobKey, JobRecord>? jobs,
    Map<String, QueueState>? queues,
    Map<JobKey, JobKey>? parentByChild,
    Map<JobKey, List<FlowEdge>>? childrenByParent,
    Map<JobKey, List<JobLog>>? logsByJob,
    Map<JobKey, int>? nextLogSequenceByJob,
    Map<String, List<PositionedJobEvent>>? eventsByQueue,
    Map<String, int>? nextEventPositionByQueue,
    Map<String, int>? trimmedEventPositionByQueue,
  }) : jobs = jobs ?? <JobKey, JobRecord>{},
       queues = queues ?? <String, QueueState>{},
       parentByChild = parentByChild ?? <JobKey, JobKey>{},
       childrenByParent = childrenByParent ?? <JobKey, List<FlowEdge>>{},
       logsByJob = logsByJob ?? <JobKey, List<JobLog>>{},
       nextLogSequenceByJob = nextLogSequenceByJob ?? <JobKey, int>{},
       eventsByQueue = eventsByQueue ?? <String, List<PositionedJobEvent>>{},
       nextEventPositionByQueue = nextEventPositionByQueue ?? <String, int>{},
       trimmedEventPositionByQueue =
           trimmedEventPositionByQueue ?? <String, int>{};

  // ==================== Job / Queue 主索引 ====================

  /// 全量 Job 记录的主索引，以 [JobKey] 为键。
  ///
  /// 数据来源：addJob/addJobs/addFlow 写入，removeJob 删除。
  /// 键的唯一性由 Backend 在写入前做冲突检查（DuplicateJob）保证。
  /// 值为不可变的 [JobRecord]，状态流转时通过替换整条 record 实现。
  final Map<JobKey, JobRecord> jobs;

  /// 已访问队列的持久化状态，以队列名称为键。
  ///
  /// 数据来源：首次访问某队列时由 [MemoryQueueBackend._queue] 懒创建
  /// （初始 revision=0 表示未初始化），setQueuePaused/setQueuePolicy 更新。
  /// 暂停标记影响 claimJob 的领取行为；策略配置驱动日志和事件的自动裁剪。
  final Map<String, QueueState> queues;

  // ==================== Flow 依赖索引 ====================

  /// Flow 中「子 Job → 父 Job」的反向索引。
  ///
  /// 仅对 Flow 中的非根节点存在对应条目（根节点无父）。
  /// 数据来源：addFlow 时从 edges 构建；removeJob 时同步清理被删节点的入边。
  /// 用于从子节点向上遍历祖先树（_recomputeAncestors）以及判断某 Job 是否属于 Flow。
  final Map<JobKey, JobKey> parentByChild;

  /// Flow 中「父 Job → 直接子边列表」的正向索引。
  ///
  /// 仅对有子节点的父 Job 存在对应条目；叶子节点和独立 Job 不出现在此 Map 中。
  /// 数据来源：addFlow 时从 edges 构建；removeJob 时同步清理被删节点的出边和自身条目。
  /// 用于 BFS 收集 Flow 子树（getFlow、removeJob 的级联删除）以及父节点状态重算时
  /// 判断是否还有未完成子节点。
  final Map<JobKey, List<FlowEdge>> childrenByParent;

  // ==================== 日志相关 ====================

  /// 每个 Job 的日志记录列表，以 [JobKey] 为键。
  ///
  /// 数据来源：appendJobLog 追加写入；clearJobLogs 和 removeJob 清理。
  /// 列表按 sequence 升序排列；Backend 按 Job.options.logRetention 或
  /// QueuePolicy.defaultJobLogRetention 自动裁剪最早的日志，超出条数时
  /// 从头部删除。可空语义：键不存在等价于该 Job 暂无日志。
  final Map<JobKey, List<JobLog>> logsByJob;

  /// 每个 Job 下一条日志将使用的 sequence 计数器。
  ///
  /// 数据来源：首次 appendJobLog 时初始化（从 1 开始），之后每次追加递增。
  /// 用于保证同一 Job 下日志 sequence 单调递增、不重复、不回退。
  /// 与 [logsByJob] 配合实现日志的游标分页读取。
  final Map<JobKey, int> nextLogSequenceByJob;

  // ==================== 事件流相关 ====================

  /// 每个队列的事件历史列表，以队列名称为键。
  ///
  /// 数据来源：addJob/addFlow/claimJob/transitionJob 等写入操作统一经
  /// [MemoryQueueBackend._event] 追加。列表按 cursor.position 升序排列。
  /// 裁剪由 QueuePolicy.maxEventCount（条数上限）和
  /// QueuePolicy.eventRetention（时间上限）共同驱动，见 _trimEvents。
  final Map<String, List<PositionedJobEvent>> eventsByQueue;

  /// 每个队列下一条事件将使用的 cursor.position 计数器。
  ///
  /// 从 1 开始，每次产出事件递增。与 [eventsByQueue] 配合保证事件流的
  /// position 单调递增、不回退。Backend 的 EventCursor 基于此值实现
  /// 跨进程可恢复的游标定位。
  final Map<String, int> nextEventPositionByQueue;

  /// 每个队列已被裁剪掉的最大事件 position。
  ///
  /// 设计意图：当事件历史按策略裁剪后，[eventsByQueue] 中的列表可能变空，
  /// 但调用方持有的旧游标指向的 position 已不可读。此下界用于在列表为空时
  /// 仍能正确检测游标 gap——只要 cursor.position <= 下界，就说明它指向的
  /// 事件已被裁剪，必须抛 EventCursorGap。更新逻辑见
  /// [MemoryQueueBackend._updateTrimmed]。
  final Map<String, int> trimmedEventPositionByQueue;

  // ==================== copy-on-write 支持 ====================

  /// 生成当前 store 的独立深拷贝。
  ///
  /// 拷贝语义：
  /// - Map 容器自身新建（非同一引用）
  /// - List 容器新建（childrenByParent、logsByJob、eventsByQueue 的值列表）
  /// - 列表中的元素（FlowEdge、JobLog、PositionedJobEvent）为不可变对象，直接共享引用即可
  /// - Map 值类型（JobRecord、QueueState、int、JobKey）均为不可变，同样可以共享引用
  ///
  /// 此方法是 copy-on-write 事务的核心前提——保证 mutation 只操作 draft，
  /// committed store 完全不受影响。
  MemoryStore copy() => MemoryStore(
    jobs: Map<JobKey, JobRecord>.of(jobs),
    queues: Map<String, QueueState>.of(queues),
    parentByChild: Map<JobKey, JobKey>.of(parentByChild),
    childrenByParent: {
      for (final entry in childrenByParent.entries)
        entry.key: List<FlowEdge>.of(entry.value),
    },
    logsByJob: {
      for (final entry in logsByJob.entries)
        entry.key: List<JobLog>.of(entry.value),
    },
    nextLogSequenceByJob: Map<JobKey, int>.of(nextLogSequenceByJob),
    eventsByQueue: {
      for (final entry in eventsByQueue.entries)
        entry.key: List<PositionedJobEvent>.of(entry.value),
    },
    nextEventPositionByQueue: Map<String, int>.of(nextEventPositionByQueue),
    trimmedEventPositionByQueue: Map<String, int>.of(
      trimmedEventPositionByQueue,
    ),
  );
}
