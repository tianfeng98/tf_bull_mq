import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'manual_runtime.dart';

/// 集成测试共享的测试夹具。
///
/// 负责组装一套开箱即用的后端组件：手动时钟 runtime、基于内存的 backend、
/// 进程内 signal，以及依赖它们构建的 Queue / Worker / QueueEvents / FlowProducer /
/// FlowQuery 工厂方法。所有构造都在同一个 fixture 实例内共享 backend 和 signal，
/// 便于跨组件验证同一套事实源上的端到端行为。
final class IntegrationFixture {
  IntegrationFixture._({required this.runtime})
    : backend = MemoryQueueBackend(
        now: runtime.now,
        idGenerator: runtime.nextId,
      ),
      signal = InProcessQueueSignal();

  /// 手动时钟，控制 now / id / delay 的时间推进。
  final ManualRuntime runtime;

  /// 共享的内存后端实例，所有工厂方法产生的 Queue / Worker 都复用它。
  final MemoryQueueBackend backend;

  /// 进程内提示通道，跨组件共享。
  final InProcessQueueSignal signal;

  /// 打开 fixture：构造 runtime、初始化 backend 并完成 open()。
  static Future<IntegrationFixture> open() async {
    final fixture = IntegrationFixture._(runtime: ManualRuntime());
    await fixture.backend.open();
    return fixture;
  }

  /// 为指定队列名构建 Queue 句柄，注入共享的 backend / signal / runtime。
  Queue queue(String queueName) => Queue(
    queueName: queueName,
    backend: backend,
    signal: signal,
    idGenerator: runtime.nextId,
    now: runtime.now,
  );

  /// 为指定队列名构建 Worker 实例。
  Worker worker({
    required String queueName,
    required Processor processor,
    WorkerOptions options = const WorkerOptions(),
    WorkerLogSink logSink = const SilentWorkerLogSink(),
  }) => Worker(
    queueName: queueName,
    backend: backend,
    signal: signal,
    processor: processor,
    options: options,
    logSink: logSink,
    now: runtime.now,
    idGenerator: runtime.nextId,
    delay: runtime.delay,
  );

  /// 为指定队列名构建 QueueEvents 实例，可选覆盖 delay。
  QueueEvents queueEvents(String queueName, {Delay? delay}) => QueueEvents(
    queueName: queueName,
    backend: backend,
    signal: signal,
    delay: delay ?? runtime.delay,
  );

  /// 构建 FlowProducer，共享 backend 和 signal。
  FlowProducer flowProducer() =>
      FlowProducer(backend: backend, signal: signal, now: runtime.now);

  /// 构建 FlowQuery，用于查询 Flow 快照。
  FlowQuery flowQuery() => FlowQuery(backend: backend);

  /// 关闭 fixture：释放 runtime 中所有未完成的 delay，然后关闭 signal 和 backend。
  Future<void> close() async {
    runtime.completeAll();
    await signal.close();
    await backend.close();
  }
}

/// 静默 WorkerLogSink 实现：丢弃所有日志写入。
///
/// 用于测试场景下不想让 Worker 日志产生副作用，只需要传入一个合规的 Sink。
final class SilentWorkerLogSink implements WorkerLogSink {
  const SilentWorkerLogSink();

  @override
  Future<void> write(WorkerLogEntry entry) async {}
}
