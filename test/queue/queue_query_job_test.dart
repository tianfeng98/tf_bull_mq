import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';

import 'support/queue_fixtures.dart';

void main() {
  group('Queue.getJob', () {
    /// 验证 getJob 把传入的 jobId 与 queueName 组合成完整 JobKey 再查询 Backend，
    /// 调用方只需要关心 jobId，Queue 自动做作用域限定。
    test('getJob 会将 jobId 限定在 Queue 的 queueName 内', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond('getJob', fixture.record(jobId: 'job-1'));

      final Job? job = await fixture.queue.getJob('job-1');

      final JobKey requested = fixture.backend.lastArgument('getJob') as JobKey;
      expect(requested, JobKey(queueName: 'work', jobId: 'job-1'));
      expect(job, isNotNull);
      expect(job!.key, JobKey(queueName: 'work', jobId: 'job-1'));
      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证 Backend 返回 null（Job 不存在）时 getJob 返回 null，
    /// 不抛任何异常——缺失是正常业务状态。
    test('backend 记录不存在时，getJob 返回 null', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond('getJob', null);

      final Job? job = await fixture.queue.getJob('missing');

      expect(job, isNull);
      expect(fixture.backend.countOf('getJob'), 1);
    });

    /// 验证同一个 jobId 多次调用 getJob 返回的是不同的 Job 句柄实例，
    /// 各自独立持有当时 Backend 返回的 snapshot，互不影响。
    test('每次调用 getJob 都会返回独立句柄', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 2),
      );
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 5),
      );

      final Job? first = await fixture.queue.getJob('job-1');
      final Job? second = await fixture.queue.getJob('job-1');

      expect(identical(first, second), isFalse);
      expect(first!.snapshot.revision, 2);
      expect(second!.snapshot.revision, 5);
      expect(first.key, second.key);
    });
  });

  group('Queue.query', () {
    /// 验证 query 把 statuses/cursor/limit 透传给 Backend，
    /// queueName 由 Queue 自动填为自身 queueName，调用方无需关心。
    test(
      'query 会使用固定的 queueName 转发 statuses、cursor 和 limit',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.backend.respond(
          'queryJobs',
          const JobPage(items: <JobRecord>[], nextCursor: null),
        );

        await fixture.queue.query(
          statuses: <JobStatus>{JobStatus.waiting, JobStatus.failed},
          cursor: 'cursor-7',
          limit: 25,
        );

        final JobQuery forwarded =
            fixture.backend.lastArgument('queryJobs') as JobQuery;
        expect(forwarded.queueName, 'work');
        expect(forwarded.statuses, <JobStatus>{
          JobStatus.waiting,
          JobStatus.failed,
        });
        expect(forwarded.cursor, 'cursor-7');
        expect(forwarded.limit, 25);
      },
    );

    /// 验证 query 把 Backend 返回的每个 JobRecord 包装成独立的 Job 句柄，
    /// 且顺序与 Backend 返回一致，snapshot 直接指向 Backend 原对象。
    test(
      'query 会将每条记录转换为 Job，且不改变顺序',
      () async {
        final QueueFixture fixture = QueueFixture();
        final List<JobRecord> records = <JobRecord>[
          fixture.record(jobId: 'job-b', revision: 3),
          fixture.record(jobId: 'job-a', revision: 9),
        ];
        fixture.backend.respond(
          'queryJobs',
          JobPage(items: records, nextCursor: null),
        );

        final QueueJobPage page = await fixture.queue.query();

        expect(page.items.map((Job job) => job.snapshot.rawRecord).toList(), records);
        expect(identical(page.items.first.snapshot.rawRecord, records.first), isTrue);
        expect(page.items.first.snapshot.revision, 3);
        expect(page.items.last.snapshot.revision, 9);
      },
    );

    /// 验证 query 不修改 Backend 返回的 nextCursor，原样透传给 QueueJobPage。
    test('query 会保留 backend 的 nextCursor', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'queryJobs',
        JobPage(
          items: <JobRecord>[fixture.record(jobId: 'job-1')],
          nextCursor: 'next-1',
        ),
      );

      final QueueJobPage page = await fixture.queue.query();

      expect(page.nextCursor, 'next-1');
      expect(page.items, hasLength(1));
    });

    /// 验证非法 limit 值（0 或 >1000）在 JobQuery DTO 构造阶段就被拒绝，
    /// 不会触碰 Backend。
    test('query 会通过冻结的 JobQuery DTO 拒绝无效 limit', () {
      final QueueFixture fixture = QueueFixture();

      expect(
        () => fixture.queue.query(limit: 0),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => fixture.queue.query(limit: 1001),
        throwsA(isA<ArgumentError>()),
      );
      expect(fixture.backend.calls, isEmpty);
    });

    /// 验证只读操作 query 不会触发 defaultPolicy 初始化写入——
    /// 只有 add/addBulk 等写操作才会走 policy 初始化流程。
    test('只读 query 不会安装默认 queue policy', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      fixture.queueState = fixture.state(revision: 0);
      fixture.backend.respond(
        'queryJobs',
        const JobPage(items: <JobRecord>[], nextCursor: null),
      );

      await fixture.queue.query();

      expect(fixture.backend.called('setQueuePolicy'), isFalse);
      expect(fixture.backend.called('getQueueState'), isFalse);
      expect(fixture.backend.calls, <String>['queryJobs']);
    });
  });

  group('Job.refresh', () {
    /// 验证 [Job.refresh] 每次都用 composite JobKey（带 queueName）查询 Backend，
    /// 保证即使 Backend 内部做了跨 queue 路由也能正确定位。
    test(
      '每次 refresh 都会从 backend 读取组合 JobKey',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.backend.respond(
          'getJob',
          fixture.record(jobId: 'job-1', revision: 1),
        );
        final Job job = (await fixture.queue.getJob('job-1'))!;
        fixture.backend.respond(
          'getJob',
          fixture.record(jobId: 'job-1', revision: 4),
        );
        fixture.backend.respond(
          'getJob',
          fixture.record(jobId: 'job-1', revision: 6),
        );

        await job.refresh();
        await job.refresh();

        expect(fixture.backend.countOf('getJob'), 3);
        expect(
          fixture.backend
              .argumentsOf('getJob')
              .cast<JobKey>()
              .every(
                (JobKey key) =>
                    key == JobKey(queueName: 'work', jobId: 'job-1'),
              ),
          isTrue,
        );
      },
    );

    /// 验证 refresh 只更新当前 Job 句柄的 snapshot，返回值与 Backend 返回
    /// 是同一个对象引用——不做复制、不做转换。
    test('refresh 只会替换当前句柄的最新 snapshot', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 1),
      );
      final Job job = (await fixture.queue.getJob('job-1'))!;
      final JobRecord refreshed = fixture.record(jobId: 'job-1', revision: 4);
      fixture.backend.respond('getJob', refreshed);

      final JobSnapshot returned = await job.refresh();

      expect(returned.rawRecord, same(refreshed));
      expect(job.snapshot.rawRecord, same(refreshed));
      expect(job.snapshot.revision, 4);
    });

    /// 验证 Backend 返回 null（Job 已被删除）时，refresh 抛 JobNotFound，
    /// 但 Job 句柄的 snapshot 保持上一次的值不变——调用方仍可读取删除前的状态。
    test(
      '记录不存在时 refresh 会抛出 JobNotFound 并保留之前的 snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final JobRecord prior = fixture.record(jobId: 'job-1', revision: 3);
        fixture.backend.respond('getJob', prior);
        final Job job = (await fixture.queue.getJob('job-1'))!;
        fixture.backend.respond('getJob', null);

        await expectLater(
          job.refresh(),
          throwsA(
            isA<JobNotFound>().having(
              (JobNotFound error) => error.key,
              'key',
              JobKey(queueName: 'work', jobId: 'job-1'),
            ),
          ),
        );

        expect(job.snapshot.rawRecord, same(prior));
        expect(job.snapshot.revision, 3);
      },
    );

    /// 验证同一个 JobKey 的两个独立句柄，各自 refresh 只更新自己，
    /// 互不影响——这与 getJob 返回独立句柄的设计一致。
    test('同一个 key 的两个句柄会分别保留独立 snapshot', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 1),
      );
      final Job first = (await fixture.queue.getJob('job-1'))!;
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 2),
      );
      final Job second = (await fixture.queue.getJob('job-1'))!;
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 9),
      );

      await first.refresh();

      expect(identical(first, second), isFalse);
      expect(first.snapshot.revision, 9);
      expect(second.snapshot.revision, 2);
    });

    /// 验证 refresh 是只读 Backend 调用，不触发任何 Signal 通知——
    /// 读操作不影响 queue 调度状态。
    test('refresh 不会发出 Signal 通知', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 1),
      );
      final Job job = (await fixture.queue.getJob('job-1'))!;
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 2),
      );

      await job.refresh();

      expect(fixture.signal.notifiedQueues, isEmpty);
      expect(fixture.backend.called('transitionJob'), isFalse);
    });
  });
}
