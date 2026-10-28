import 'dart:async';

import 'package:tf_bull_mq/tf_bull_mq.dart';

/// 演示 tf_bull_mq 各类查询 API 的独立示例。
///
/// 覆盖范围：
/// - `Queue.getJob`：按 jobId 查单个 Job 句柄（含不存在时返回 null）
/// - `Queue.query`：按状态分页查询 Job，支持 cursor 翻页
/// - `Queue.getState`：读取 Queue 的暂停状态和策略
/// - `Job.refresh`：从 Backend 重新读取最新 Job 记录
/// - `Job.queryLogs`：按级别/通道/尝试次数/游标过滤执行日志
///
/// 示例先通过 Worker 让部分 Job 进入不同终态（completed / failed），
/// 再演示各查询 API 的输入和输出形态。所有组件使用 MemoryQueueBackend
/// 和 InProcessQueueSignal，在单进程内运行无需外部依赖。
Future<void> main() async {
  var nextId = 0;
  String idGenerator() => 'qexample-${nextId++}';
  Future<void> delay(Duration d) => Future<void>.delayed(d);

  // --- 组件装配 ---
  final backend = MemoryQueueBackend(
    now: DateTime.now,
    idGenerator: idGenerator,
  );
  final signal = InProcessQueueSignal();

  final queue = Queue(
    queueName: 'query-demo',
    backend: backend,
    signal: signal,
    idGenerator: idGenerator,
    now: DateTime.now,
  );

  // 单一 Worker：同时处理正常和失败的 Job。
  // - fail- 前缀的 jobId：抛异常，让 Backend 记为 failed 并记录 failure 信息。
  // - 其余 Job：写几条 JobLog 后正常完成。
  final worker = Worker(
    queueName: 'query-demo',
    backend: backend,
    signal: signal,
    processor: (job) async {
      await job.log(
        JobLogDraft(
          level: JobLogLevel.info,
          channel: JobLogChannel.application,
          message: 'start processing ${job.key.jobId}',
          data: <String, Object?>{'jobId': job.key.jobId},
        ),
      );
      if (job.key.jobId.startsWith('fail-')) {
        // 故意抛异常——Worker 会把 Job 标记为 failed 并持久化 failure 信息。
        throw StateError('intentional failure for ${job.key.jobId}');
      }
      await Future<void>.delayed(const Duration(milliseconds: 3));
      await job.log(
        JobLogDraft(
          level: JobLogLevel.debug,
          channel: JobLogChannel.application,
          message: 'processing done',
          data: null,
        ),
      );
      await job.log(
        JobLogDraft(
          level: JobLogLevel.warning,
          channel: JobLogChannel.system,
          message: 'finalizing result',
          data: <String, Object?>{'resultSize': 1},
        ),
      );
      return <String, Object?>{'ok': true};
    },
    logSink: const _SilentSink(),
    now: DateTime.now,
    idGenerator: idGenerator,
    delay: delay,
  );

  await backend.open();
  await worker.start();

  try {
    // ========== 种子化 ==========
    // 先创建一个明确 jobId 的 Job，稍后用 getJob 精确查询。
    // final specificKey = const JobKey(
    //   queueName: 'query-demo',
    //   jobId: 'specific-job',
    // );
    final specificJob = await queue.add(
      'specific',
      jobId: 'specific-job',
      data: <String, Object?>{'label': 'lookup-me'},
    );

    // 创建会被 Worker 消费完成的普通 Job。
    final completed = await queue.add(
      'normal-1',
      data: <String, Object?>{'v': 1},
    );
    final _ = await queue.add('normal-2', data: <String, Object?>{'v': 2});
    final _ = await queue.add('normal-3', data: <String, Object?>{'v': 3});

    // 创建会失败的 Job。
    // final failed = await queue.add(
    //   'fail-me',
    //   jobId: 'fail-1',
    //   data: <String, Object?>{'intentional': true},
    // );

    // 创建一个保持 waiting 状态的 Job（不被任何 Worker 特别处理，但 Worker
    // 会把它也 claim 掉，所以我们这里不做等待，先查询看看部分完成时的状态）。

    // 等待 Worker 把已投递的 Job 处理完。
    // 让出若干 tick 确保终态提交完成。
    for (var i = 0; i < 40; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 2));
    }

    // ========== Queue.getJob ==========
    print('--- Queue.getJob: 按 jobId 查单个 Job ---');

    // 精确查询刚才创建的 specific-job，应该存在。
    final found = await queue.getJob('specific-job');
    if (found != null) {
      print(
        '  found jobId=${found.key.jobId}, '
        'status=${found.snapshot.status.name}, '
        'revision=${found.snapshot.revision}, '
        'data=${found.snapshot.data}',
      );
    } else {
      print('  specific-job NOT FOUND');
    }

    // 查询不存在的 jobId——Backend 返回 null，Queue 也返回 null，不抛异常。
    final missing = await queue.getJob('nonexistent-job');
    print('  missing job is null: ${missing == null}');

    // ========== Job.refresh ==========
    print('\n--- Job.refresh: 从 Backend 重新读取最新状态 ---');

    // specificJob 句柄是 add 返回的初始 snapshot（当时可能还是 waiting），
    // refresh 之后应当反映 Worker 处理后的真实状态。
    print(
      '  before refresh: status=${specificJob.snapshot.status.name}, '
      'revision=${specificJob.snapshot.revision}',
    );
    await specificJob.refresh();
    print(
      '  after refresh:  status=${specificJob.snapshot.status.name}, '
      'revision=${specificJob.snapshot.revision}, '
      'result=${specificJob.snapshot.result}',
    );

    // ========== Queue.query ==========
    print('\n--- Queue.query: 分页查询 + 状态过滤 + cursor 翻页 ---');

    // 第一页：只取前 2 个，不限状态。
    var page = await queue.query(limit: 2);
    print('  first page (limit=2): ${page.items.length} items');
    for (final job in page.items) {
      print('    ${job.key.jobId}: status=${job.snapshot.status.name}');
    }
    print('    nextCursor: ${page.nextCursor ?? "(none)"}');

    // 第二页：用上一页的 nextCursor 继续取。
    if (page.nextCursor != null) {
      page = await queue.query(limit: 2, cursor: page.nextCursor);
      print('  second page (limit=2): ${page.items.length} items');
      for (final job in page.items) {
        print('    ${job.key.jobId}: status=${job.snapshot.status.name}');
      }
      print('    nextCursor: ${page.nextCursor ?? "(none)"}');
    }

    // 按状态过滤：只查 completed 的 Job。
    final completedPage = await queue.query(
      statuses: <JobStatus>{JobStatus.completed},
    );
    print('  status filter (completed): ${completedPage.items.length} items');
    for (final job in completedPage.items) {
      print('    ${job.key.jobId}: result=${job.snapshot.result}');
    }

    // 按状态过滤：只查 failed 的 Job。
    final failedPage = await queue.query(
      statuses: <JobStatus>{JobStatus.failed},
    );
    print('  status filter (failed): ${failedPage.items.length} items');
    for (final job in failedPage.items) {
      print(
        '    ${job.key.jobId}: failure=${job.snapshot.failure?.message ?? "(none)"}',
      );
    }

    // ========== Queue.getState ==========
    print('\n--- Queue.getState: 读取 Queue 持久化状态 ---');

    final state = await queue.getState();
    print(
      '  queueName=${state.queueName}, paused=${state.paused}, '
      'revision=${state.revision}, '
      'policy=${state.policy.defaultJobLogRetention ?? "unlimited logs"}, '
      'maxEventCount=${state.policy.maxEventCount ?? "unlimited events"}',
    );

    // ========== Job.queryLogs ==========
    print('\n--- Job.queryLogs: 查询 Job 执行日志 ---');

    // 先对一个已知完成的 Job 刷新，然后查它的日志。
    await completed.refresh();
    final logs = await completed.queryLogs();
    print('  ${completed.key.jobId} log count: ${logs.items.length}');
    for (final log in logs.items) {
      print(
        '    [seq=${log.sequence} attempt=${log.attempt}] '
        '${log.level.name}/${log.channel.name}: ${log.message}',
      );
    }

    // 按级别过滤：只查 info 及以上。
    final infoLogs = await completed.queryLogs(
      levels: <JobLogLevel>{
        JobLogLevel.info,
        JobLogLevel.warning,
        JobLogLevel.error,
      },
    );
    print('  filtered by info+: ${infoLogs.items.length} log(s)');

    // 按通道过滤：只查 application 通道。
    final appLogs = await completed.queryLogs(
      channels: <JobLogChannel>{JobLogChannel.application},
    );
    print('  filtered by application channel: ${appLogs.items.length} log(s)');

    // 按游标翻页：模拟只取 sequence > 1 的日志（如果有多条的话）。
    if (logs.items.length > 1) {
      final tailLogs = await completed.queryLogs(afterSequence: 1, limit: 5);
      print(
        '  afterSequence=1: ${tailLogs.items.length} log(s), '
        'nextSequence=${tailLogs.nextSequence ?? "(none)"}',
      );
    }
  } finally {
    // 反向关闭：Worker → Queue → Signal → Backend。
    await worker.close();
    await queue.close();
    await signal.close();
    await backend.close();
  }
}

/// 静默的 WorkerLogSink，丢弃 Worker 运行时的诊断日志。
final class _SilentSink implements WorkerLogSink {
  const _SilentSink();

  @override
  Future<void> write(WorkerLogEntry entry) async {}
}
