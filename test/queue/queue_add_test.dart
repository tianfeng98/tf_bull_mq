import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';

import 'support/queue_fixtures.dart';

/// 可识别堆栈：用于证明通知失败时保留了**原始**堆栈而不是重新生成。
final class _MarkerStackTrace implements StackTrace {
  const _MarkerStackTrace();

  @override
  String toString() => 'marker-stack-trace';
}

BackendCapabilities _capabilities({
  bool delayedJobs = true,
  bool priorities = true,
}) => BackendCapabilities(
  profile: BackendProfile.local,
  durability: Durability.volatile,
  delayedJobs: delayedJobs,
  priorities: priorities,
  atomicFlows: true,
  eventDelivery: EventDelivery.liveOnly,
);

void main() {
  group('Queue.add', () {
    /// 验证不显式传 jobId 时，add 调用 IdGenerator 生成 ID，
    /// 创建时间和可用时间都取自 Now，且 Now 只读取一次。
    test('add 会生成缺失的 jobId，并且只读取一次 Now', () async {
      final QueueFixture fixture = QueueFixture();
      final JobRecord committed = fixture.record(jobId: 'generated-1');
      fixture.backend.respond('addJob', fixture.writeResult(committed));

      final Job job = await fixture.queue.add(
        'task',
        data: const <String, Object?>{'id': 1},
      );

      final AddJobRequest request =
          fixture.backend.lastArgument('addJob') as AddJobRequest;
      expect(request.job.key.queueName, 'work');
      expect(request.job.key.jobId, 'generated-1');
      expect(request.createdAt, fixture.time);
      expect(request.availableAt, fixture.time);
      expect(fixture.nowReads, 1);
      expect(job.jobId, 'generated-1');
    });

    /// 验证显式传入 jobId 时 IdGenerator 完全不被调用，
    /// addJob 请求使用调用方提供的 ID。
    test(
      'add 会保留显式 jobId，且不会调用 IdGenerator',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.idGenerator = () =>
            throw StateError('IdGenerator must not run');
        fixture.backend.respond(
          'addJob',
          fixture.writeResult(fixture.record(jobId: 'explicit-1')),
        );

        await fixture.queue.add('task', data: null, jobId: 'explicit-1');

        final AddJobRequest request =
            fixture.backend.lastArgument('addJob') as AddJobRequest;
        expect(request.job.key.jobId, 'explicit-1');
      },
    );

    /// 验证 JobOptions 中的 delay 被映射到 addJobRequest.availableAt，
    /// 而 JobOptions 其他字段（priority、maxStalledCount 等）原样保留。
    test('add 会将 delay 映射为 availableAt，且不会修改 JobOptions', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(fixture.record(jobId: 'generated-1')),
      );
      final JobOptions options = JobOptions(
        delay: const Duration(minutes: 5),
        priority: 3,
        maxStalledCount: 4,
      );

      await fixture.queue.add('task', data: null, options: options);

      final AddJobRequest request =
          fixture.backend.lastArgument('addJob') as AddJobRequest;
      expect(
        request.availableAt.difference(request.createdAt),
        const Duration(minutes: 5),
      );
      expect(request.job.options, same(options));
      expect(request.job.options.priority, 3);
      expect(request.job.options.maxStalledCount, 4);
    });

    /// 验证 add 返回的 Job 句柄 snapshot 就是 Backend 提交的结果，
    /// revision 等字段均来自 Backend，不由 Queue 层二次构造。
    test('add 会返回带有已提交 snapshot 的 Job 句柄', () async {
      final QueueFixture fixture = QueueFixture();
      final JobRecord committed = fixture.record(
        jobId: 'generated-1',
        revision: 7,
        status: JobStatus.waiting,
      );
      fixture.backend.respond('addJob', fixture.writeResult(committed));

      final Job job = await fixture.queue.add('task', data: null);

      expect(job.snapshot.rawRecord, same(committed));
      expect(job.snapshot.revision, 7);
      expect(job.key, JobKey(queueName: 'work', jobId: 'generated-1'));
      expect(job.queueName, 'work');
      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证带 delay 的 add 在任何 Backend 写入前先检查 Backend 是否支持 delayedJobs，
    /// 即使 Queue 配置了 defaultPolicy 也不会触发 setQueuePolicy。
    test('add 会在写入 policy 或新增 Job 前预检 delayed 能力', () {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
        capabilities: _capabilities(delayedJobs: false),
      );

      expect(
        () => fixture.queue.add(
          'task',
          data: null,
          options: JobOptions(delay: const Duration(seconds: 30)),
        ),
        throwsA(
          isA<UnsupportedCapability>().having(
            (UnsupportedCapability error) => error.capability,
            'capability',
            'delayedJobs',
          ),
        ),
      );
      expect(fixture.backend.calls, isEmpty);
      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证带 priority 的 add 在任何 Backend 写入前先检查 Backend 是否支持 priorities。
    test('add 会在写入 policy 或新增 Job 前预检 priority 能力', () {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
        capabilities: _capabilities(priorities: false),
      );

      expect(
        () => fixture.queue.add(
          'task',
          data: null,
          options: JobOptions(priority: 5),
        ),
        throwsA(
          isA<UnsupportedCapability>().having(
            (UnsupportedCapability error) => error.capability,
            'capability',
            'priorities',
          ),
        ),
      );
      expect(fixture.backend.calls, isEmpty);
      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证 IdGenerator 生成的空字符串或抛错时，add 在 Backend 写入之前就失败，
    /// 队列不产生任何副作用。
    test('add 会在每次 backend 写入前验证生成的 ID', () {
      final QueueFixture blank = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      blank.idGenerator = () => '   ';
      expect(
        () => blank.queue.add('task', data: null),
        throwsA(isA<ArgumentError>()),
      );
      expect(blank.backend.calls, isEmpty);

      final QueueFixture throwing = QueueFixture();
      throwing.idGenerator = () => throw StateError('id generator failed');
      expect(
        () => throwing.queue.add('task', data: null),
        throwsA(isA<StateError>()),
      );
      expect(throwing.backend.calls, isEmpty);
    });
  });

  group('Queue.addBulk', () {
    /// 验证 addBulk 单次调用 Backend 的 addJobs，不会循环调用 addJob，
    /// 返回的 Job 句柄列表顺序与输入一致。
    test(
      'addBulk 只发送一个 AddJobsRequest，不会循环调用 addJob',
      () async {
        final QueueFixture fixture = QueueFixture();
        final List<JobRecord> committed = <JobRecord>[
          fixture.record(jobId: 'generated-1'),
          fixture.record(jobId: 'generated-2'),
        ];
        fixture.backend.respond('addJobs', fixture.writeResult(committed));

        final List<Job> jobs = await fixture.queue.addBulk(<QueueJobDraft>[
          QueueJobDraft(name: 'a', data: null),
          QueueJobDraft(name: 'b', data: null),
        ]);

        expect(fixture.backend.countOf('addJobs'), 1);
        expect(fixture.backend.called('addJob'), isFalse);
        final AddJobsRequest request =
            fixture.backend.lastArgument('addJobs') as AddJobsRequest;
        expect(request.jobs.map((JobSpec spec) => spec.key.jobId), <String>[
          'generated-1',
          'generated-2',
        ]);
        expect(jobs, hasLength(2));
        expect(jobs.map((Job job) => job.snapshot.rawRecord).toList(), committed);
      },
    );

    /// 验证 addBulk 为所有 draft 使用同一个 createdAt（只读一次 Now），
    /// 输入顺序在请求和返回中都被保留。
    test(
      'addBulk 为所有 draft 使用同一个 createdAt，并保留输入顺序',
      () async {
        final QueueFixture fixture = QueueFixture();
        final List<JobRecord> committed = <JobRecord>[
          fixture.record(jobId: 'generated-1'),
          fixture.record(jobId: 'generated-2'),
          fixture.record(jobId: 'generated-3'),
        ];
        fixture.backend.respond('addJobs', fixture.writeResult(committed));

        final List<Job> jobs = await fixture.queue.addBulk(<QueueJobDraft>[
          QueueJobDraft(name: 'a', data: null),
          QueueJobDraft(
            name: 'b',
            data: null,
            options: JobOptions(delay: const Duration(minutes: 1)),
          ),
          QueueJobDraft(name: 'c', data: null),
        ]);

        final AddJobsRequest request =
            fixture.backend.lastArgument('addJobs') as AddJobsRequest;
        expect(request.createdAt, fixture.time);
        expect(fixture.nowReads, 1);
        expect(request.jobs.map((JobSpec spec) => spec.name), <String>[
          'a',
          'b',
          'c',
        ]);
        expect(request.jobs[1].options.delay, const Duration(minutes: 1));
        expect(jobs.map((Job job) => job.jobId), <String>[
          'generated-1',
          'generated-2',
          'generated-3',
        ]);
      },
    );

    /// 验证 addBulk 只为没有显式 jobId 的 draft 调用 IdGenerator，
    /// 生成顺序与输入一致，已存在的显式 ID 被跳过。
    test('addBulk 仅按输入顺序为缺失项生成 ID', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'addJobs',
        fixture.writeResult(<JobRecord>[
          fixture.record(jobId: 'explicit-1'),
          fixture.record(jobId: 'generated-1'),
          fixture.record(jobId: 'explicit-2'),
          fixture.record(jobId: 'generated-2'),
        ]),
      );

      await fixture.queue.addBulk(<QueueJobDraft>[
        QueueJobDraft(name: 'a', data: null, jobId: 'explicit-1'),
        QueueJobDraft(name: 'b', data: null),
        QueueJobDraft(name: 'c', data: null, jobId: 'explicit-2'),
        QueueJobDraft(name: 'd', data: null),
      ]);

      final AddJobsRequest request =
          fixture.backend.lastArgument('addJobs') as AddJobsRequest;
      expect(request.jobs.map((JobSpec spec) => spec.key.jobId), <String>[
        'explicit-1',
        'generated-1',
        'explicit-2',
        'generated-2',
      ]);
    });

    /// 验证 addBulk 在任何 Backend 写入前检查显式重复 key、
    /// IdGenerator 生成的重复 key、以及显式 ID 与生成 ID 之间的冲突。
    test('addBulk 会在写入 policy 前拒绝重复的显式 key 或生成的 key', () {
      final QueueFixture explicit = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      expect(
        () => explicit.queue.addBulk(<QueueJobDraft>[
          QueueJobDraft(name: 'a', data: null, jobId: 'dup'),
          QueueJobDraft(name: 'b', data: null, jobId: 'dup'),
        ]),
        throwsA(isA<ArgumentError>()),
      );
      expect(explicit.backend.calls, isEmpty);

      final QueueFixture generated = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      generated.idGenerator = () => 'dup';
      expect(
        () => generated.queue.addBulk(<QueueJobDraft>[
          QueueJobDraft(name: 'a', data: null),
          QueueJobDraft(name: 'b', data: null),
        ]),
        throwsA(isA<ArgumentError>()),
      );
      expect(generated.backend.calls, isEmpty);

      final QueueFixture mixed = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      mixed.idGenerator = () => 'same';
      expect(
        () => mixed.queue.addBulk(<QueueJobDraft>[
          QueueJobDraft(name: 'a', data: null),
          QueueJobDraft(name: 'b', data: null, jobId: 'same'),
        ]),
        throwsA(isA<ArgumentError>()),
      );
      expect(mixed.backend.calls, isEmpty);
    });

    /// 验证 addBulk 在任何 Backend 写入前对所有 draft 做能力预检：
    /// 只要有一个 draft 带 delay/delay 但 Backend 不支持，整批操作就拒绝。
    test(
      'addBulk 会在 backend 写入前预检所有 delay 和 priority',
      () {
        final QueueFixture delayed = QueueFixture(
          options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
          capabilities: _capabilities(delayedJobs: false),
        );
        expect(
          () => delayed.queue.addBulk(<QueueJobDraft>[
            QueueJobDraft(name: 'a', data: null),
            QueueJobDraft(
              name: 'b',
              data: null,
              options: JobOptions(delay: const Duration(seconds: 5)),
            ),
          ]),
          throwsA(
            isA<UnsupportedCapability>().having(
              (UnsupportedCapability error) => error.capability,
              'capability',
              'delayedJobs',
            ),
          ),
        );
        expect(delayed.backend.calls, isEmpty);

        final QueueFixture prioritized = QueueFixture(
          options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
          capabilities: _capabilities(priorities: false),
        );
        expect(
          () => prioritized.queue.addBulk(<QueueJobDraft>[
            QueueJobDraft(name: 'a', data: null),
            QueueJobDraft(
              name: 'b',
              data: null,
              options: JobOptions(priority: 2),
            ),
          ]),
          throwsA(
            isA<UnsupportedCapability>().having(
              (UnsupportedCapability error) => error.capability,
              'capability',
              'priorities',
            ),
          ),
        );
        expect(prioritized.backend.calls, isEmpty);
      },
    );

    /// 验证 IdGenerator 在批量生成中抛出异常时，整批操作失败，
    /// Backend 完全不被调用——已成功生成的前几个也不会被回滚。
    test('后续生成 ID 时抛出错误会导致 addBulk 整批失败', () {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      var generated = 0;
      fixture.idGenerator = () {
        generated += 1;
        if (generated == 3) {
          throw StateError('id generator failed on the third draft');
        }
        return 'generated-$generated';
      };

      expect(
        () => fixture.queue.addBulk(<QueueJobDraft>[
          QueueJobDraft(name: 'a', data: null),
          QueueJobDraft(name: 'b', data: null),
          QueueJobDraft(name: 'c', data: null),
        ]),
        throwsA(isA<StateError>()),
      );
      expect(fixture.backend.calls, isEmpty);
    });

    /// 验证 Backend 的 addJobs 返回错误时，整批操作失败，
    /// 不会返回部分成功的 Job 句柄，Signal 也不被调用。
    test(
      'addBulk 会传递 backend 错误，且不会返回部分句柄',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.backend.respondWithFailure(
          'addJobs',
          DuplicateJob(fixture.key('generated-2'), 'job already exists'),
        );

        await expectLater(
          fixture.queue.addBulk(<QueueJobDraft>[
            QueueJobDraft(name: 'a', data: null),
            QueueJobDraft(name: 'b', data: null),
          ]),
          throwsA(isA<DuplicateJob>()),
        );

        expect(fixture.backend.countOf('addJobs'), 1);
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );
  });

  group('默认 policy 初始化', () {
    /// 验证 Queue 配置了非默认 defaultPolicy 时，首次 add 在 queue revision=0
    /// 的前提下先写入 setQueuePolicy（预期 revision=0），然后再 addJob。
    test(
      '仅当 queue revision 为 0 时才会安装非默认 policy',
      () async {
        final QueuePolicy policy = QueuePolicy(maxEventCount: 10);
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(defaultPolicy: policy),
        );
        fixture.backend.respond(
          'setQueuePolicy',
          fixture.writeResult(
            fixture.state(policy: policy, revision: 1),
            queuesToNotify: <String>{'work'},
          ),
        );
        fixture.backend.respond(
          'addJob',
          fixture.writeResult(fixture.record(jobId: 'generated-1')),
        );

        await fixture.queue.add('task', data: null);

        expect(fixture.backend.calls, <String>[
          'getQueueState',
          'setQueuePolicy',
          'addJob',
        ]);
        final SetQueuePolicyRequest request = fixture.backend.lastArgument(
          'setQueuePolicy',
        ) as SetQueuePolicyRequest;
        expect(request.queueName, 'work');
        expect(request.policy, same(policy));
        expect(request.expectedRevision, 0);
        expect(request.occurredAt, fixture.time);
        expect(fixture.signal.notifiedQueues, <String>['work']);
      },
    );

    /// 验证 queue 已有非零 revision 且存在非默认 policy 时，Queue 不会
    /// 用自己的 defaultPolicy 去覆盖 Backend 已有策略。
    test('不会覆盖已存在的 queue policy', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      fixture.queueState = fixture.state(
        policy: QueuePolicy(maxEventCount: 99),
        revision: 4,
      );
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(fixture.record(jobId: 'generated-1')),
      );

      await fixture.queue.add('task', data: null);

      expect(fixture.backend.called('setQueuePolicy'), isFalse);
      expect(fixture.backend.calls, <String>['getQueueState', 'addJob']);
    });

    /// 验证 policy 初始化遇到 QueueRevisionConflict（并发写入）时，
    /// 只要 Backend 最终已经写入该 policy，Queue 可以接受并发胜利者的结果继续 add。
    test('未指定 revision 且发生 QueueRevisionConflict 后，会接受并发 policy 初始化的胜出结果', () async {
      final QueuePolicy policy = QueuePolicy(maxEventCount: 10);
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: policy),
      );
      fixture.backend.respond('getQueueState', fixture.state(revision: 0));
      fixture.queueState = fixture.state(policy: policy, revision: 1);
      fixture.backend.respondWithFailure(
        'setQueuePolicy',
        QueueRevisionConflict('work', 0, 1, 'stale queue revision'),
      );
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(fixture.record(jobId: 'generated-1')),
      );

      final Job job = await fixture.queue.add('task', data: null);

      expect(fixture.backend.calls, <String>[
        'getQueueState',
        'setQueuePolicy',
        'getQueueState',
        'addJob',
      ]);
      expect(job.snapshot, isNotNull);
    });
  });

  group('通知语义', () {
    /// 验证 Signal 通知失败时，Backend 已提交的 add 不会被回滚，
    /// QueueNotificationFailure.committedValue 携带完整提交结果。
    test('signal 通知失败时会暴露已提交的新增结果，且不会回滚', () async {
      final QueueFixture fixture = QueueFixture();
      final JobRecord committed = fixture.record(
        queueName: 'work',
        jobId: 'generated-1',
        revision: 1,
      );
      fixture.backend.respond(
        'addJob',
        BackendWriteResult<JobRecord>(
          value: committed,
          changed: true,
          queuesToNotify: <String>{'work', 'parent'},
        ),
      );
      fixture.signal.failingQueues.add('parent');

      await expectLater(
        fixture.queue.add('task', data: const <String, Object?>{}),
        throwsA(
          isA<QueueNotificationFailure>()
              .having(
                (QueueNotificationFailure error) => error.operation,
                'operation',
                'add',
              )
              .having(
                (QueueNotificationFailure error) => error.committedValue,
                'committedValue',
                same(committed),
              )
              .having(
                (QueueNotificationFailure error) => error.queuesToNotify,
                'queuesToNotify',
                <String>{'work', 'parent'},
              ),
        ),
      );

      expect(
        fixture.backend.calls.where((String call) => call == 'addJob'),
        hasLength(1),
      );
      expect(fixture.backend.calls, isNot(contains('removeJob')));
    });

    /// 验证 Signal 通知的 queue 集合完全取自 BackendWriteResult.queuesToNotify，
    /// 不由 Queue 层决定——Backend 返回什么就通知什么。
    test(
      '只通知 BackendWriteResult 中的 queues，而非输入的 queue',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.backend.respond(
          'addJob',
          fixture.writeResult(
            fixture.record(jobId: 'generated-1'),
            queuesToNotify: <String>{'parent-queue', 'other-queue'},
          ),
        );

        await fixture.queue.add('task', data: null);

        expect(fixture.signal.notifiedQueues, <String>[
          'other-queue',
          'parent-queue',
        ]);
      },
    );

    /// 验证 Backend 返回空 queuesToNotify 时，add 完全不调用 Signal。
    test('通知集合为空时不会调用 Signal', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(fixture.record(jobId: 'generated-1')),
      );

      await fixture.queue.add('task', data: null);

      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证 Backend 写入失败（addJob 返回错误）时，Signal 完全不被调用——
    /// 通知只在写入成功后才发出。
    test('backend add 失败时绝不会调用 Signal', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respondWithFailure(
        'addJob',
        BackendUnavailable(null, 'backend is down'),
      );

      await expectLater(
        fixture.queue.add('task', data: null),
        throwsA(isA<BackendUnavailable>()),
      );

      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证 addBulk 通知失败时，QueueNotificationFailure 的 committedValue
    /// 是完整的提交记录列表，而非部分结果。
    test(
      '批量通知失败时会暴露完整的已提交记录列表',
      () async {
        final QueueFixture fixture = QueueFixture();
        final List<JobRecord> committed = <JobRecord>[
          fixture.record(jobId: 'generated-1'),
          fixture.record(jobId: 'generated-2'),
        ];
        fixture.backend.respond(
          'addJobs',
          fixture.writeResult(
            committed,
            queuesToNotify: <String>{'work', 'other'},
          ),
        );
        fixture.signal.failingQueues.add('other');

        await expectLater(
          fixture.queue.addBulk(<QueueJobDraft>[
            QueueJobDraft(name: 'a', data: null),
            QueueJobDraft(name: 'b', data: null),
          ]),
          throwsA(
            isA<QueueNotificationFailure>()
                .having(
                  (QueueNotificationFailure error) => error.operation,
                  'operation',
                  'addBulk',
                )
                .having(
                  (QueueNotificationFailure error) => error.committedValue,
                  'committedValue',
                  same(committed),
                )
                .having(
                  (QueueNotificationFailure error) => error.queuesToNotify,
                  'queuesToNotify',
                  <String>{'work', 'other'},
                ),
          ),
        );

        expect(fixture.signal.notifiedQueues, <String>['other', 'work']);
      },
    );

    /// 验证 add 在任意通知失败后，仍会等待所有其他通知完成后才返回（或抛错），
    /// 不会让慢通知阻塞在失败之后。
    test('即使通知快速失败，也会启动并完成对每个 queue 的通知', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(
          fixture.record(jobId: 'generated-1'),
          queuesToNotify: <String>{'alpha', 'beta'},
        ),
      );
      final Completer<void> betaGate = Completer<void>();
      fixture.signal.notifyResponder = (String queueName) {
        if (queueName == 'alpha') {
          throw StateError('alpha is down');
        }
        return betaGate.future;
      };

      final Future<Job> pending = fixture.queue.add('task', data: null);
      var settled = false;
      pending.then<void>(
        (_) => settled = true,
        onError: (Object _) => settled = true,
      );
      await Future<void>.delayed(Duration.zero);

      expect(fixture.signal.notifiedQueues, <String>['alpha', 'beta']);
      expect(betaGate.isCompleted, isFalse);
      expect(
        settled,
        isFalse,
        reason: 'mutation must wait for slow notifications',
      );

      betaGate.complete();
      await expectLater(pending, throwsA(isA<QueueNotificationFailure>()));
      expect(fixture.signal.notifiedQueues, <String>['alpha', 'beta']);
    });

    /// 验证通知失败后，最终抛给调用方的是最早失败的原始 error 和原始 stack trace，
    /// 不被 Queue 层重新包装或替换。
    test('所有通知完成后，通知失败仍会保留最先发生的原始错误和堆栈', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(
          fixture.record(jobId: 'generated-1'),
          queuesToNotify: <String>{'alpha', 'beta', 'gamma'},
        ),
      );
      const StackTrace marker = _MarkerStackTrace();
      final StateError original = StateError('alpha is down');
      fixture.signal.notifyResponder = (String queueName) {
        if (queueName == 'alpha') {
          Error.throwWithStackTrace(original, marker);
        }
      };

      await expectLater(
        fixture.queue.add('task', data: null),
        throwsA(
          isA<QueueNotificationFailure>()
              .having(
                (QueueNotificationFailure error) => error.cause,
                'cause',
                same(original),
              )
              .having(
                (QueueNotificationFailure error) =>
                    identical(error.stackTrace, marker),
                'stackTrace is the original one',
                isTrue,
              ),
        ),
      );

      expect(fixture.signal.notifiedQueues, <String>['alpha', 'beta', 'gamma']);
    });

    /// 验证多个通知同时失败时，最终报告的是按字母序（通知启动序）
    /// 第一个失败的 queue 的错误。
    test('通知失败时会按排序后的启动顺序报告第一个失败的 queue', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'addJob',
        fixture.writeResult(
          fixture.record(jobId: 'generated-1'),
          queuesToNotify: <String>{'zeta', 'alpha', 'middle'},
        ),
      );
      final StateError alphaError = StateError('alpha failed');
      final StateError zetaError = StateError('zeta failed');
      fixture.signal.notifyResponder = (String queueName) {
        if (queueName == 'alpha') {
          throw alphaError;
        }
        if (queueName == 'zeta') {
          throw zetaError;
        }
      };

      await expectLater(
        fixture.queue.add('task', data: null),
        throwsA(
          isA<QueueNotificationFailure>().having(
            (QueueNotificationFailure error) => error.cause,
            'cause',
            same(alphaError),
          ),
        ),
      );

      expect(fixture.signal.notifiedQueues, <String>[
        'alpha',
        'middle',
        'zeta',
      ]);
    });
  });
}
