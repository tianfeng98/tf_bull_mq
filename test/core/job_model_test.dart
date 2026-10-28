import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';

void main() {
  /// 验证 [JobKey] 的身份同时由 queueName 和 jobId 共同决定，
  /// 两者相同才视为同一把 key。
  test('JobKey 的身份由 queueName 和 jobId 共同确定', () {
    const first = JobKey(queueName: 'mail', jobId: '1');
    const second = JobKey(queueName: 'other', jobId: '1');
    expect(first, isNot(second));
    expect(first.queueName, 'mail');
  });

  /// 验证 [JobKey] 的排序先按 queueName 字典序、再按 jobId 字典序，
  /// 保证同一 queue 内的 job 会连续排列。
  test('JobKey 先按 queueName、再按 jobId 稳定排序', () {
    final keys = <JobKey>[
      const JobKey(queueName: 'b', jobId: '1'),
      const JobKey(queueName: 'a', jobId: '2'),
      const JobKey(queueName: 'a', jobId: '1'),
    ]..sort();
    expect(keys.map((key) => key.jobId), <String>['1', '2', '1']);
    expect(keys.map((key) => key.queueName), <String>['a', 'a', 'b']);
  });

  /// 验证 [JobOptions.priority] 必须 >= 0，负数被拒绝。
  test('JobOptions 会拒绝负 priority', () {
    expect(() => JobOptions(priority: -1), throwsArgumentError);
  });

  /// 验证默认优先级为 0，更大优先级在调度时排在更小之前。
  test('较大的 priority 会排在较小值之前', () {
    expect(
      JobOptions(priority: 10).priority,
      greaterThan(JobOptions().priority),
    );
  });

  /// 验证 [RetryPolicy] 的两种退避策略：固定延迟直接返回配置值，
  /// 指数退避按尝试次数翻倍但受 maxDelay 封顶。
  test('RetryPolicy 会计算固定延迟和带上限的指数延迟', () {
    final fixed = RetryPolicy(delay: Duration(seconds: 2));
    final exponential = RetryPolicy(
      backoff: BackoffPolicy.exponential,
      delay: Duration(seconds: 2),
      maxDelay: Duration(seconds: 5),
    );
    expect(fixed.delayForAttempt(1), const Duration(seconds: 2));
    expect(exponential.delayForAttempt(2), const Duration(seconds: 4));
    expect(exponential.delayForAttempt(3), const Duration(seconds: 5));
  });

  /// 验证 [RetryPolicy] 的入参校验：maxAttempts 必须 >= 1，
  /// delay 不能为负，delayForAttempt 的 attempt 参数也必须 >= 1。
  test('RetryPolicy 会拒绝小于 1 的 attempts 和负延迟', () {
    expect(() => RetryPolicy(maxAttempts: 0), throwsArgumentError);
    expect(
      () => RetryPolicy(delay: const Duration(seconds: -1)),
      throwsArgumentError,
    );
    expect(() => RetryPolicy().delayForAttempt(0), throwsArgumentError);
  });

  /// 验证 distributed 级别的 Backend 必须支持可回放事件，
  /// liveOnly 交付模式无法满足分布式消费者的回放需求。
  test('distributed profile 要求 events 可重放', () {
    expect(
      () => BackendCapabilities(
        profile: BackendProfile.distributed,
        durability: Durability.durable,
        delayedJobs: true,
        priorities: true,
        atomicFlows: true,
        eventDelivery: EventDelivery.liveOnly,
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [QueuePolicy] 能区分 null（沿用默认行为）与 0（显式保留零条日志），
  /// 两者语义不同，不能简单用空值表示所有默认。
  test('QueuePolicy 会区分 null 默认值和 0 日志保留量', () {
    final policy = QueuePolicy(defaultJobLogRetention: 0);
    expect(policy.defaultJobLogRetention, 0);
    expect(QueuePolicy().defaultJobLogRetention, isNull);
  });

  /// 验证 [QueuePolicy.eventRetention] 必须为正的 Duration，零时长被拒绝。
  test('QueuePolicy 会拒绝非正数的 event 保留量', () {
    expect(
      () => QueuePolicy(eventRetention: Duration.zero),
      throwsArgumentError,
    );
  });

  /// 验证 [JobOptions.stackTraceLimit] 必须 >= 0，负数被拒绝。
  test('JobOptions 会拒绝负 stack trace 限制', () {
    expect(() => JobOptions(stackTraceLimit: -1), throwsArgumentError);
  });

  /// 验证 [JobRecord.data] 必须为 JSON-like 可序列化值，
  /// DateTime 等原始 Dart 对象被构造时拒绝。
  test('JobRecord 会验证 data、progress 和 result 是否为 JSON-like', () {
    final record = JobRecord(
      schemaVersion: 1,
      key: const JobKey(queueName: 'mail', jobId: '1'),
      name: 'send',
      data: const <String, Object?>{'to': 'a'},
      status: JobStatus.waiting,
      progress: 0.5,
      result: null,
      failure: null,
      revision: 1,
      attemptsMade: 0,
      stalledCount: 0,
      priority: 0,
      availableAt: DateTime.utc(2026),
      options: JobOptions(),
      parentKey: null,
      createdAt: DateTime.utc(2026),
      updatedAt: DateTime.utc(2026),
      startedAt: null,
      finishedAt: null,
      execution: null,
      controlRequest: null,
    );
    expect(record.key.jobId, '1');
    expect(
      () => JobRecord(
        schemaVersion: 1,
        key: const JobKey(queueName: 'mail', jobId: '2'),
        name: 'bad',
        data: DateTime.utc(2026),
        status: JobStatus.waiting,
        progress: 0,
        result: null,
        failure: null,
        revision: 1,
        attemptsMade: 0,
        stalledCount: 0,
        priority: 0,
        availableAt: DateTime.utc(2026),
        options: JobOptions(),
        parentKey: null,
        createdAt: DateTime.utc(2026),
        updatedAt: DateTime.utc(2026),
        startedAt: null,
        finishedAt: null,
        execution: null,
        controlRequest: null,
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [JobRecord.progress] 必须在 [0, 1] 闭区间内，超过 1.0 的值被拒绝。
  test('数值型 progress 必须介于 0 和 1 之间', () {
    expect(
      () => JobRecord(
        schemaVersion: 1,
        key: const JobKey(queueName: 'mail', jobId: '1'),
        name: 'bad',
        data: null,
        status: JobStatus.waiting,
        progress: 1.1,
        result: null,
        failure: null,
        revision: 1,
        attemptsMade: 0,
        stalledCount: 0,
        priority: 0,
        availableAt: DateTime.utc(2026),
        options: JobOptions(),
        parentKey: null,
        createdAt: DateTime.utc(2026),
        updatedAt: DateTime.utc(2026),
        startedAt: null,
        finishedAt: null,
        execution: null,
        controlRequest: null,
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [JobRecord] 在构造时递归冻结 [data]、[progress] 和 [result]：
  /// 修改原始输入集合不影响已构造对象，直接写对象内部集合抛 UnsupportedError。
  test('JobRecord 会冻结嵌套的 data、progress 和 result', () {
    final originalData = <String, Object?>{
      'nested': <Object?>[1, 2],
    };
    final originalProgress = <String, Object?>{'percent': 0.5};
    final originalResult = <String, Object?>{
      'items': <Object?>[3],
    };

    final record = JobRecord(
      schemaVersion: 1,
      key: const JobKey(queueName: 'mail', jobId: 'frozen'),
      name: 'task',
      data: originalData,
      status: JobStatus.waiting,
      progress: originalProgress,
      result: originalResult,
      failure: null,
      revision: 1,
      attemptsMade: 0,
      stalledCount: 0,
      priority: 0,
      availableAt: DateTime.utc(2026),
      options: JobOptions(),
      parentKey: null,
      createdAt: DateTime.utc(2026),
      updatedAt: DateTime.utc(2026),
      startedAt: null,
      finishedAt: null,
      execution: null,
      controlRequest: null,
    );

    // 修改原始输入不应影响构造结果
    (originalData['nested'] as List<Object?>).add(99);
    originalData['newKey'] = 'hack';
    expect(record.data, <String, Object?>{
      'nested': <Object?>[1, 2],
    });

    originalProgress['percent'] = 1.0;
    expect(record.progress, <String, Object?>{'percent': 0.5});

    (originalResult['items'] as List<Object?>).add(99);
    expect(record.result, <String, Object?>{
      'items': <Object?>[3],
    });

    // 直接修改 record 内部集合应抛 UnsupportedError
    final recordData = record.data as Map<String, Object?>;
    expect(() => recordData['extra'] = 'boom', throwsUnsupportedError);

    final recordProgress = record.progress as Map<String, Object?>;
    expect(() => recordProgress['x'] = 0, throwsUnsupportedError);

    final recordResult = record.result as Map<String, Object?>;
    expect(() => recordResult['y'] = 0, throwsUnsupportedError);
  });
}
