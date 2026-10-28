import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/observability/index.dart';

void main() {
  // 验证 WorkerLogEntry 可以正确承载基础设施级错误日志，
  // 包括 WorkerLogContext 中的 workerId / queueName / operation / jobKey 字段。
  test('WorkerLogEntry 可承载基础设施级错误上下文', () {
    final entry = WorkerLogEntry(
      level: WorkerLogLevel.error,
      message: 'backend unavailable',
      occurredAt: DateTime.utc(2026),
      context: const WorkerLogContext(
        workerId: 'worker-1',
        queueName: 'mail',
        operation: 'claim',
        jobKey: JobKey(queueName: 'mail', jobId: '1'),
      ),
      error: StateError('down'),
    );
    expect(entry.context.workerId, 'worker-1');
  });

  // 验证 WorkerLogEntry 在 data 字段上严格校验 JSON-like：
  // 传入 DateTime 等非 JSON-like 值时应抛 ArgumentError。
  test('WorkerLogEntry 的 data 字段拒绝非 JSON-like 值', () {
    expect(
      () => WorkerLogEntry(
        level: WorkerLogLevel.info,
        message: 'bad',
        occurredAt: DateTime.utc(2026),
        context: const WorkerLogContext(
          workerId: 'worker-1',
          queueName: 'mail',
          operation: 'claim',
        ),
        data: DateTime.utc(2026),
      ),
      throwsArgumentError,
    );
  });
}
