import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

import 'support/manual_runtime.dart';
import 'support/switchable_queue_signal.dart';

void main() {
  // 验证 Worker 独立周期 poll 能力：Signal 完全被丢弃后，Worker 仍能通过 poll claim 新 Job。
  // 这确保 Signal 不是唯一事实来源，即使提示全部丢失也不会阻塞消费。
  test('Worker 在 Signal 通知被完全丢弃后仍能通过 poll claim Job', () async {
    final runtime = ManualRuntime();
    final backend = MemoryQueueBackend(
      now: runtime.now,
      idGenerator: runtime.nextId,
    );
    final signal = SwitchableQueueSignal(InProcessQueueSignal());
    await backend.open();
    addTearDown(signal.close);
    addTearDown(backend.close);
    final queue = Queue(
      queueName: 'poll-worker',
      backend: backend,
      signal: signal,
      idGenerator: runtime.nextId,
      now: runtime.now,
    );
    addTearDown(queue.close);
    final entered = Completer<void>();
    final worker = Worker(
      queueName: 'poll-worker',
      backend: backend,
      signal: signal,
      processor: (job) {
        if (!entered.isCompleted) entered.complete();
        return null;
      },
      // 设置 1s poll interval，手动驱动这个 delay 来触发 claim。
      options: const WorkerOptions(pollInterval: Duration(seconds: 1)),
      logSink: const _NoopLogSink(),
      now: runtime.now,
      idGenerator: runtime.nextId,
      delay: runtime.delay,
    );
    addTearDown(worker.close);
    await worker.start();
    // 立即切换为丢弃通知模式，从这一刻起所有 notify 都被吞掉。
    signal.dropNotifications = true;
    await queue.add('poll', jobId: 'job-1', data: null);
    // 还没到 poll 周期，Processor 不应被触发。
    expect(entered.isCompleted, isFalse);
    // 手动释放 Worker 等待的 poll delay，触发一次 poll 循环。
    runtime.completeNext(const Duration(seconds: 1));
    await withWatchdog(
      'worker poll processor',
      entered.future,
      phase: () => worker.state.name,
      snapshot: () => runtime.pendingDelays,
    );
  });
}

/// 测试专用无操作 WorkerLogSink，避免 Worker 日志写入干扰测试。
final class _NoopLogSink implements WorkerLogSink {
  const _NoopLogSink();

  @override
  Future<void> write(WorkerLogEntry entry) async {}
}
