import 'dart:mirrors';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';

import 'support/queue_fixtures.dart';

void main() {
  group('Job.queryLogs', () {
    /// 验证 [Job.queryLogs] 把调用参数（attempt、levels、channels、cursor、limit）
    /// 无改动地透传给 Backend，并返回 Backend 的原始分页结果。
    test(
      'queryLogs 会转发 job key、attempt、levels、channels、cursor 和 limit',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await fixture.attachJob();
        final JobLogPage page = fixture.logPage(
          items: <JobLog>[fixture.log()],
          nextSequence: 5,
        );
        fixture.backend.respond('queryJobLogs', page);

        final JobLogPage returned = await job.queryLogs(
          attempt: 2,
          levels: <JobLogLevel>{JobLogLevel.error},
          channels: <JobLogChannel>{JobLogChannel.errorOutput},
          afterSequence: 7,
          limit: 10,
        );

        final JobLogQuery forwarded =
            fixture.backend.lastArgument('queryJobLogs') as JobLogQuery;
        expect(forwarded.key, job.key);
        expect(forwarded.attempt, 2);
        expect(forwarded.levels, <JobLogLevel>{JobLogLevel.error});
        expect(forwarded.channels, <JobLogChannel>{JobLogChannel.errorOutput});
        expect(forwarded.afterSequence, 7);
        expect(forwarded.limit, 10);
        expect(returned, same(page));
      },
    );

    /// 验证 [queryLogs] 是只读操作：不修改 Job 的 snapshot，也不触发 Signal 通知。
    test(
      'queryLogs 会返回 backend 分页，且不会修改 Job snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await fixture.attachJob(revision: 6);
        final JobSnapshot prior = job.snapshot;
        final JobLogPage page = fixture.logPage(items: <JobLog>[fixture.log()]);
        fixture.backend.respond('queryJobLogs', page);

        final JobLogPage returned = await job.queryLogs();

        expect(returned, same(page));
        expect(job.snapshot, same(prior));
        expect(job.snapshot.revision, 6);
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证非法 limit 值（0 或 >1000）在 DTO 构造阶段就被拒绝，
    /// 不会对 Backend 产生任何调用。
    test('queryLogs 会通过冻结的 DTO 拒绝无效 limit', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await fixture.attachJob();
      final int callsBefore = fixture.backend.calls.length;

      expect(() => job.queryLogs(limit: 0), throwsA(isA<ArgumentError>()));
      expect(() => job.queryLogs(limit: 1001), throwsA(isA<ArgumentError>()));

      expect(fixture.backend.calls.length, callsBefore);
      expect(fixture.backend.called('queryJobLogs'), isFalse);
    });
  });

  group('Job.clearLogs', () {
    /// 验证 [clearLogs] 不传 keepLatest 时默认为 0，
    /// 即清空所有日志（除非 Backend 另有保留策略）。
    test('clearLogs 默认将 keepLatest 设为 0', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await fixture.attachJob();
      fixture.backend.respond(
        'clearJobLogs',
        fixture.writeResult(12, queuesToNotify: <String>{'work'}),
      );

      final int deleted = await job.clearLogs();

      final ClearJobLogsRequest request =
          fixture.backend.lastArgument('clearJobLogs') as ClearJobLogsRequest;
      expect(request.keepLatest, 0);
      expect(request.key, job.key);
      expect(deleted, 12);
    });

    /// 验证显式传入 keepLatest 时被原样透传给 Backend。
    test('clearLogs 会原样转发正数 keepLatest', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await fixture.attachJob();
      fixture.backend.respond(
        'clearJobLogs',
        fixture.writeResult(0, queuesToNotify: <String>{'work'}),
      );

      await job.clearLogs(keepLatest: 5);

      final ClearJobLogsRequest request =
          fixture.backend.lastArgument('clearJobLogs') as ClearJobLogsRequest;
      expect(request.keepLatest, 5);
    });

    /// 验证 [clearLogs] 对应的 Backend 请求 DTO 不包含 expectedRevision 字段，
    /// 因为清空日志是 append-only 操作，不依赖乐观锁。
    test(
      'backend contract 未定义 expectedRevision，因此 clearLogs 不提供该参数',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await fixture.attachJob(revision: 4);
        final JobSnapshot prior = job.snapshot;
        fixture.backend.respond(
          'clearJobLogs',
          fixture.writeResult(1, queuesToNotify: <String>{'work'}),
        );

        final Future<int> Function({int keepLatest}) clearLogs = job.clearLogs;
        expect(clearLogs, isNotNull);

        final List<String> fields =
            reflectClass(ClearJobLogsRequest).declarations.values
                .whereType<VariableMirror>()
                .map(
                  (VariableMirror field) =>
                      MirrorSystem.getName(field.simpleName),
                )
                .toList()
              ..sort();
        expect(fields, <String>['keepLatest', 'key']);

        await job.clearLogs();

        expect(job.snapshot, same(prior));
        expect(job.snapshot.revision, 4);
      },
    );

    /// 验证 [clearLogs] 返回 Backend 声明的删除条数，
    /// 且 Signal 只通知 Backend 指定的 queue 集合。
    test(
      'clearLogs 只通知 backend queues，并返回删除数量',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await fixture.attachJob();
        fixture.backend.respond(
          'clearJobLogs',
          fixture.writeResult(
            3,
            queuesToNotify: <String>{'other-queue', 'work'},
          ),
        );

        final int deleted = await job.clearLogs();

        expect(deleted, 3);
        expect(fixture.signal.notifiedQueues, <String>['other-queue', 'work']);
      },
    );

    /// 验证 Backend 返回空通知集时，[clearLogs] 完全不调用 Signal。
    test('通知列表为空时，clearLogs 不会调用 Signal', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await fixture.attachJob();
      fixture.backend.respond('clearJobLogs', fixture.writeResult(0));

      final int deleted = await job.clearLogs();

      expect(deleted, 0);
      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证 Signal 通知失败时，[clearLogs] 抛出 [QueueNotificationFailure]，
    /// 其中 committedValue 携带已提交的删除数量，使调用方知道操作已完成但通知失败。
    test(
      'clearLogs 的 signal 通知失败时仍会暴露已提交的删除数量',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await fixture.attachJob();
        fixture.backend.respond(
          'clearJobLogs',
          fixture.writeResult(4, queuesToNotify: <String>{'work'}),
        );
        fixture.signal.failingQueues.add('work');

        await expectLater(
          job.clearLogs(),
          throwsA(
            isA<QueueNotificationFailure>()
                .having(
                  (QueueNotificationFailure error) => error.operation,
                  'operation',
                  'job.clearLogs',
                )
                .having(
                  (QueueNotificationFailure error) => error.committedValue,
                  'committedValue',
                  4,
                ),
          ),
        );
      },
    );
  });
}
