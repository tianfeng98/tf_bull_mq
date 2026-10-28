import 'dart:io';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';

import 'support/queue_fixtures.dart';

/// 编译期固定 Queue 的公开签名；任何参数、返回值或可空性变化都会让本文件无法编译。
List<Object> _verifyQueueApi(Queue queue, Job job) {
  final Future<Job> Function(
    String, {
    required Object? data,
    String? jobId,
    JobOptions options,
  })
  add = queue.add;
  final Future<List<Job>> Function(List<QueueJobDraft>) addBulk = queue.addBulk;
  final Future<Job?> Function(String) getJob = queue.getJob;
  final Future<QueueJobPage> Function({
    Set<JobStatus> statuses,
    String? cursor,
    int limit,
  })
  query = queue.query;
  final Future<QueueState> Function() getState = queue.getState;
  final Future<QueueState> Function({int? expectedRevision}) pause =
      queue.pause;
  final Future<QueueState> Function({int? expectedRevision}) resume =
      queue.resume;
  final Future<void> Function() close = queue.close;

  return <Object>[add, addBulk, getJob, query, getState, pause, resume, close];
}

/// 编译期固定 Job 的公开签名。
List<Object> _verifyJobApi(Job job) {
  final Future<JobSnapshot<Object?, Object?, Object?>> Function() refresh = job.refresh;
  final Future<JobSnapshot<Object?, Object?, Object?>> Function({Duration delay, int? expectedRevision})
  retry = job.retry;
  final Future<JobSnapshot<Object?, Object?, Object?>> Function({int? expectedRevision}) cancel = job.cancel;
  final Future<JobSnapshot<Object?, Object?, Object?>> Function({int? expectedRevision}) pause = job.pause;
  final Future<JobSnapshot<Object?, Object?, Object?>> Function({int? expectedRevision}) resume = job.resume;
  final Future<List<JobKey>> Function({int? expectedRevision}) remove =
      job.remove;
  final Future<JobLogPage> Function({
    int? attempt,
    Set<JobLogLevel> levels,
    Set<JobLogChannel> channels,
    int? afterSequence,
    int limit,
  })
  queryLogs = job.queryLogs;
  final Future<int> Function({int keepLatest}) clearLogs = job.clearLogs;

  return <Object>[
    refresh,
    retry,
    cancel,
    pause,
    resume,
    remove,
    queryLogs,
    clearLogs,
  ];
}

/// 编译期固定四个公共值对象的构造签名。
List<Object> _verifyValueObjectApi() {
  final QueueOptions Function({
    QueuePolicy? defaultPolicy,
    bool ownsBackend,
    bool ownsSignal,
  })
  buildOptions = QueueOptions.new;
  final QueueJobDraft Function({
    required String name,
    required Object? data,
    String? jobId,
    JobOptions? options,
  })
  buildDraft = QueueJobDraft.new;
  final QueueJobPage Function({
    required List<Job> items,
    required String? nextCursor,
  })
  buildPage = QueueJobPage.new;
  final QueueNotificationFailure Function({
    required String operation,
    required Object? committedValue,
    required Set<String> queuesToNotify,
    required Object cause,
    required StackTrace stackTrace,
  })
  buildFailure = QueueNotificationFailure.new;
  final Queue Function({
    required String queueName,
    required QueueBackend backend,
    required QueueSignal signal,
    required IdGenerator idGenerator,
    required Now now,
    QueueOptions options,
  })
  buildQueue = Queue.new;

  return <Object>[
    buildOptions,
    buildDraft,
    buildPage,
    buildFailure,
    buildQueue,
  ];
}

void main() {
  /// 编译期烟雾测试：确保 Queue/Job/值对象的公开签名函数指针非空，
  /// 任何参数/返回值/可空性变化都会让本文件无法编译。
  test('queue 模块入口仅暴露规划的公共 API', () {
    expect(_verifyQueueApi, isNotNull);
    expect(_verifyJobApi, isNotNull);
    expect(_verifyValueObjectApi, isNotNull);
  });

  /// 运行期验证：Queue 和 Job 句柄的实例满足所有编译期 API 签名，
  /// 并暴露 queueName、snapshot 等关键只读属性。
  test('Queue 和 Job 句柄符合编译期 API 接口', () async {
    final QueueFixture fixture = QueueFixture();
    final Job job = await fixture.attachJob();

    expect(_verifyQueueApi(fixture.queue, job), hasLength(8));
    expect(_verifyJobApi(job), hasLength(8));
    expect(_verifyValueObjectApi(), hasLength(5));

    expect(fixture.queue.queueName, 'work');
    expect(fixture.queue.options, isA<QueueOptions>());
    expect(fixture.queue.isClosed, isFalse);
    expect(job.key, isA<JobKey>());
    expect(job.queueName, 'work');
    expect(job.jobId, 'job-1');
    expect(job.snapshot.rawRecord, isA<JobRecord>());
  });

  /// 验证 `src/queue/index.dart` 的模块出口只导出规划中的公共 API，
  /// 内部实现（QueueContext、句柄工厂）不得泄露到模块外部。
  test('queue index 仅导出规划的模块 API', () {
    final String source = File('lib/src/queue/index.dart').readAsStringSync();
    final String normalized = source.replaceAll(RegExp(r'\s+'), ' ');

    expect(normalized, contains("export 'job.dart' show Job;"));
    expect(normalized, contains("export 'queue.dart' show Queue;"));
    expect(
      normalized,
      contains(
        "export 'queue_models.dart' show QueueJobDraft, QueueJobPage, "
        'QueueNotificationFailure, QueueOptions;',
      ),
    );
    // 内部实现不得出现在模块出口：context、句柄工厂都不属于公共 API。
    expect(normalized, isNot(contains('queue_context')));
    expect(normalized, isNot(contains('createJobHandle')));
    expect(normalized, isNot(contains('QueueContext')));
  });
}
