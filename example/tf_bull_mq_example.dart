import 'dart:async';

import 'package:tf_bull_mq/tf_bull_mq.dart';

/// 使用 tf_bull_mq 完整运行链路的示例程序。
///
/// 演示了组件装配、普通 Job（progress/data/JobLog）生产、Flow 创建
/// （非默认 weight）、direct 与 recursive 投影查询，以及 QueueEvents
/// 等待 Job completed 的典型使用方式。
Future<void> main() async {
  // 闭包形式的 ID 生成器，每次调用返回自增的 example-<序号>。
  var nextId = 0;
  String idGenerator() => 'example-${nextId++}';
  // 用 Future.delayed 实现的手工延迟，便于示例在任何环境运行。
  Future<void> delay(Duration duration) => Future<void>.delayed(duration);

  // MemoryQueueBackend 与 InProcessQueueSignal 共同构成本地单进程运行时。
  final backend = MemoryQueueBackend(
    now: DateTime.now,
    idGenerator: idGenerator,
  );
  final signal = InProcessQueueSignal();

  // example 队列：承载普通 Job 和 Flow 的顶层父节点。
  final queue = Queue(
    queueName: 'example',
    backend: backend,
    signal: signal,
    idGenerator: idGenerator,
    now: DateTime.now,
  );
  // example-leaf 队列：承载 Flow 的叶子子节点。
  final leafQueue = Queue(
    queueName: 'example-leaf',
    backend: backend,
    signal: signal,
    idGenerator: idGenerator,
    now: DateTime.now,
  );

  // QueueEvents 订阅 example 队列的事件流，用于等待 Job completed。
  final events = QueueEvents(
    queueName: 'example',
    backend: backend,
    signal: signal,
    delay: delay,
  );

  // ordinaryWorker：消费 example 队列的普通 Job，在 Processor 里
  // 演示 updateProgress / updateData / JobLog，最后返回 result 完成。
  final ordinaryCompleted = Completer<JobKey>();
  final ordinaryWorker = Worker(
    queueName: 'example',
    backend: backend,
    signal: signal,
    processor: (job) async {
      await job.updateProgress(0.5);
      await job.updateData(<String, Object?>{
        ...(job.data as Map<String, Object?>),
        'processed': true,
      });
      await job.log(
        JobLogDraft(
          level: JobLogLevel.info,
          channel: JobLogChannel.application,
          message: 'ordinary job half-done',
          data: const <String, Object?>{'stage': 'mid'},
        ),
      );
      // 不返回终态 result——Worker 用返回值作为 completed result
      // 或者直接返回 null 也可以（Backend 存 null result）。
      if (!ordinaryCompleted.isCompleted) {
        ordinaryCompleted.complete(job.key);
      }
      return <String, Object?>{'final': 'done'};
    },
    logSink: const _SilentWorkerLogSink(),
    now: DateTime.now,
    idGenerator: idGenerator,
    delay: delay,
  );

  // leafWorker：消费 Flow 的子节点。
  final leafWorker = Worker(
    queueName: 'example-leaf',
    backend: backend,
    signal: signal,
    processor: (job) async => <String, Object?>{'leaf': job.key.jobId},
    logSink: const _SilentWorkerLogSink(),
    now: DateTime.now,
    idGenerator: idGenerator,
    delay: delay,
  );

  // parentWorker：消费 Flow 的顶层父节点，在 Processor 中读取子节点结果。
  final parentCompleted = Completer<void>();
  final parentWorker = Worker(
    queueName: 'example',
    backend: backend,
    signal: signal,
    processor: (job) async {
      final results = await job.getDependenciesResult();
      if (!parentCompleted.isCompleted) parentCompleted.complete();
      return <String, Object?>{'children': results.length};
    },
    logSink: const _SilentWorkerLogSink(),
    now: DateTime.now,
    idGenerator: idGenerator,
    delay: delay,
  );

  // FlowProducer 原子创建嵌套 Flow；FlowQuery 只读地查询 Flow 投影。
  final producer = FlowProducer(
    backend: backend,
    signal: signal,
    now: DateTime.now,
  );
  final query = FlowQuery(backend: backend);
  StreamSubscription<PositionedJobEvent>? subscription;
  Completer<void>? ordinaryByEvents;

  try {
    // Backend 必须先 open，之后所有高层组件才能正常工作。
    await backend.open();

    // 订阅 Job 事件流，打印事件类型和对应的 JobKey，
    // 并为普通 Job 单独监听 completed 事件。
    // Events 和 Worker 都有独立的 start 生命周期。
    await events.start();
    await ordinaryWorker.start();
    await parentWorker.start();
    await leafWorker.start();

    // 先生产普通 Job，拿到其 key，再启动事件监听以精确匹配 completed。
    final ordinaryKey = await queue.add(
      'ordinary-job',
      data: const <String, Object?>{'value': 42},
    );
    print('Ordinary job key: $ordinaryKey');

    // 订阅 Job 事件流，打印事件类型和对应的 JobKey，
    // 并精确等待普通 Job 的 completed 事件。
    ordinaryByEvents = Completer<void>();
    subscription = events.events.listen((item) {
      print('${item.event.type.name}: ${item.event.jobKey}');
      if (item.event.type == JobEventType.completed &&
          item.event.jobKey.jobId == ordinaryKey.jobId) {
        ordinaryByEvents?.complete();
      }
    });

    // 创建一个嵌套 Flow，使用非默认 weight（3.0 / 2.0）：
    // 父节点 weight=3 的子节点完成对父进度的贡献更大。
    final flow = await producer.add(
      FlowJob(
        key: const JobKey(queueName: 'example', jobId: 'flow-parent'),
        name: 'parent',
        data: null,
        children: <FlowChild>[
          FlowChild(
            job: FlowJob(
              key: const JobKey(
                queueName: 'example-leaf',
                jobId: 'flow-leaf-a',
              ),
              name: 'leaf-a',
              data: const <String, Object?>{'value': 1},
            ),
            weight: 3.0,
          ),
          FlowChild(
            job: FlowJob(
              key: const JobKey(
                queueName: 'example-leaf',
                jobId: 'flow-leaf-b',
              ),
              name: 'leaf-b',
              data: const <String, Object?>{'value': 2},
            ),
            weight: 2.0,
          ),
        ],
      ),
    );
    print('Flow root: ${flow.root}');

    // 等待普通 Job 完成（QueueEvents 事件）和 Flow 父节点完成
    // （其 Processor 读取了子节点结果）。
    await ordinaryByEvents.future.timeout(const Duration(seconds: 5));
    await parentCompleted.future.timeout(const Duration(seconds: 5));
    // 让出若干事件循环 tick，确保终态提交和事件发射都完成。
    for (var index = 0; index < 10; index++) {
      await Future<void>.value();
    }

    // ---- 查询 direct 投影：只展开一层，子节点 children 为空 ----
    final direct = (await query.get(
      flow.root,
      depth: FlowProjectionDepth.direct,
    ))!;
    print('=== Flow direct projection ===');
    print('root status: ${direct.root.job.status.name}');
    print('root children count: ${direct.root.children.length}');
    // direct 模式下子节点的 children 应该为空（只展开一层）
    for (final child in direct.root.children) {
      print(
        '  child ${child.node.job.key.jobId} (weight=${child.weight}): '
        'ownProgress=${child.node.ownProgress}, '
        'children=${child.node.children.length}',
      );
    }

    // ---- 查询 recursive 投影：展开完整后代树 ----
    final recursive = (await query.get(
      flow.root,
      depth: FlowProjectionDepth.recursive,
    ))!;
    print('=== Flow recursive projection ===');
    print('root status: ${recursive.root.job.status.name}');
    print('root weightedProgress: ${recursive.root.weightedProgress}');
    for (final child in recursive.root.children) {
      print(
        '  child ${child.node.job.key.jobId}: '
        'weightedProgress=${child.node.weightedProgress}, '
        'descendantCounts=${child.node.descendantCounts}',
      );
    }
  } finally {
    // 反向关闭：Flow 组件 → QueueEvents → Workers → Queue → Signal → Backend。
    // ownership 默认为 false，由创建者按依赖反向顺序关闭。
    await query.close();
    await producer.close();
    await subscription?.cancel();
    await events.close();
    await ordinaryWorker.close();
    await parentWorker.close();
    await leafWorker.close();
    await queue.close();
    await leafQueue.close();
    await signal.close();
    await backend.close();
  }
}

/// 静默的 WorkerLogSink 实现，丢弃所有运行日志。
///
/// 示例中不需要消费 Worker 的运行日志，因此提供这个最简单的实现。
final class _SilentWorkerLogSink implements WorkerLogSink {
  const _SilentWorkerLogSink();

  @override
  Future<void> write(WorkerLogEntry entry) async {}
}
