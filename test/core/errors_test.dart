import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';

void main() {
  /// 验证 [RevisionConflict] 和 [QueueRevisionConflict] 能正确保留各自的
  /// 身份载荷：前者携带完整 [JobKey]，后者携带 queueName 字符串。
  test('Job 与 Queue 冲突会保留各自不同的身份信息', () {
    const key = JobKey(queueName: 'mail', jobId: 'job-1');
    const jobError = RevisionConflict(key, 3, 4, 'stale job revision');
    const queueError = QueueRevisionConflict(
      'mail',
      7,
      8,
      'stale queue revision',
    );

    expect(jobError.key, key);
    expect(jobError.expected, 3);
    expect(jobError.actual, 4);
    expect(queueError.queueName, 'mail');
    expect(queueError.expected, 7);
    expect(queueError.actual, 8);
  });

  /// 验证 [EventCursorGap] 能完整保留请求游标与最早可用游标两个边界，
  /// 使调用方在事件被裁剪时仍可据此决定回溯策略。
  test('event cursor 间隙会保留两端 cursor 边界', () {
    final requested = EventCursor(queueName: 'mail', position: 2);
    final earliest = EventCursor(queueName: 'mail', position: 5);
    final gap = EventGap(requested: requested, earliestAvailable: earliest);
    final error = EventCursorGap(gap, 'events were trimmed');

    expect(error.gap.requested, requested);
    expect(error.gap.earliestAvailable, earliest);
  });

  /// 覆盖所有架构层定义的异常类型，断言它们均继承自 [TfBullMqException]
  /// 且 message 字段非空，确保异常携带足够的诊断上下文。
  test('所有架构错误都会保留其上下文', () {
    const key = JobKey(queueName: 'mail', jobId: 'job-1');
    final failure = JobFailure(
      type: 'error',
      message: 'failed',
      failedAt: DateTime.utc(2026),
    );
    final errors = <TfBullMqException>[
      InvalidJobTransition(key, JobStatus.waiting, JobStatus.active, 'bad'),
      const LeaseLost(key, 'execution-1', 'lost'),
      const BackendUnavailable(null, 'down'),
      const UnsupportedCapability('leases', 'unsupported'),
      const JobLogWriteFailure(key, null, 'log failed'),
      ProcessorFailure(failure, 'processor failed'),
      FlowDependencyFailure(key, <JobKey>[key], 'dependency failed'),
      const LifecycleError('Queue', 'close', 'closed'),
      const JobNotFound(key, 'missing'),
      const DuplicateJob(key, 'duplicate'),
    ];

    expect(errors, everyElement(isA<TfBullMqException>()));
    expect(errors.map((error) => error.message), everyElement(isNotEmpty));
  });
}
