import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';

import 'support/queue_fixtures.dart';

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

/// 通过 getJob 取得一个 status/revision 受控的 Job 句柄。
Future<Job> _job(
  QueueFixture fixture, {
  JobStatus status = JobStatus.waiting,
  int revision = 1,
  String jobId = 'job-1',
  JobControlRequest? controlRequest,
}) async {
  fixture.backend.respond(
    'getJob',
    fixture.record(
      jobId: jobId,
      status: status,
      revision: revision,
      controlRequest: controlRequest,
    ),
  );
  return (await fixture.queue.getJob(jobId))!;
}

void main() {
  group('Job.retry', () {
    /// 验证无参 [retry] 时 expectedRevision 默认取自当前 snapshot.revision，
    /// 并透传给 Backend 的 transitionJob 请求。
    test(
      'retry 默认使用最新 snapshot revision 作为 expectedRevision',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.failed,
          revision: 4,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(
            fixture.record(
              jobId: 'job-1',
              status: JobStatus.waiting,
              revision: 5,
            ),
            queuesToNotify: <String>{'work'},
          ),
        );

        await job.retry();

        final TransitionJobRequest request =
            fixture.backend.lastArgument('transitionJob')
                as TransitionJobRequest;
        expect(request.expectedRevision, 4);
        expect(request.key, job.key);
      },
    );

    /// 验证显式传入 expectedRevision 时原样透传，不会被 snapshot.revision 覆盖。
    test('retry 会原样转发显式传入的 expectedRevision', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(
        fixture,
        status: JobStatus.failed,
        revision: 4,
      );
      fixture.backend.respond(
        'transitionJob',
        fixture.writeResult(
          fixture.record(
            jobId: 'job-1',
            status: JobStatus.waiting,
            revision: 9,
          ),
          queuesToNotify: <String>{'work'},
        ),
      );

      await job.retry(expectedRevision: 9);

      final TransitionJobRequest request =
          fixture.backend.lastArgument('transitionJob') as TransitionJobRequest;
      expect(request.expectedRevision, 9);
    });

    /// 验证 delay 为负数时在能力检查之前就被 ArgumentError 拒绝，
    /// Backend 和 Now 都不会被触碰——即使 Backend 不支持 delayedJobs。
    test(
      'retry 会在检查 backend 能力、调用 backend 或读取 Now 前拒绝负 delay',
      () async {
        // 使用不支持 delayedJobs 的 Backend：若能力检查先于负延迟校验，
        // 这里会抛 UnsupportedCapability 而不是 ArgumentError。
        final QueueFixture fixture = QueueFixture(
          capabilities: _capabilities(delayedJobs: false),
        );
        final Job job = await _job(fixture, status: JobStatus.failed);
        final int callsBefore = fixture.backend.calls.length;
        final int nowBefore = fixture.nowReads;

        expect(
          () => job.retry(delay: const Duration(seconds: -1)),
          throwsA(isA<ArgumentError>()),
        );

        expect(fixture.backend.calls.length, callsBefore);
        expect(fixture.nowReads, nowBefore);
        expect(fixture.backend.called('transitionJob'), isFalse);
      },
    );

    /// 验证无 delay 的 [retry] 将目标状态设为 waiting，
    /// occurredAt 和 availableAt 都等于 Now，且只读取 Now 一次。
    test('不带 delay 的 retry 会在 Now 将状态设为 waiting', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(
        fixture,
        status: JobStatus.failed,
        revision: 2,
      );
      final JobRecord committed = fixture.record(
        jobId: 'job-1',
        status: JobStatus.waiting,
        revision: 3,
      );
      fixture.backend.respond(
        'transitionJob',
        fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
      );

      await job.retry();

      final TransitionJobRequest request =
          fixture.backend.lastArgument('transitionJob') as TransitionJobRequest;
      expect(request.targetStatus, JobStatus.waiting);
      expect(request.occurredAt, fixture.time);
      expect(request.availableAt, fixture.time);
      expect(fixture.nowReads, 1);
      expect(job.snapshot.rawRecord, same(committed));
    });

    /// 验证 canceled Job 调用 [retry] 与 failed Job 走相同契约：
    /// 目标状态 waiting、revision 正确、提交后 snapshot 更新、Signal 只在成功后通知。
    test(
      'retry canceled Job 会将状态设为 waiting，并在提交后更新 snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.canceled,
          revision: 7,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.waiting,
          revision: 8,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
        );
        JobSnapshot? observedDuringNotify;
        fixture.signal.notifyResponder = (String queueName) {
          observedDuringNotify = job.snapshot;
        };

        await job.retry();

        final TransitionJobRequest request =
            fixture.backend.lastArgument('transitionJob')
                as TransitionJobRequest;
        expect(request.targetStatus, JobStatus.waiting);
        expect(request.expectedRevision, 7);
        expect(observedDuringNotify!.rawRecord, same(committed));
        expect(job.snapshot.rawRecord, same(committed));
        expect(fixture.backend.calls, <String>['getJob', 'transitionJob']);
      },
    );

    /// 验证带 delay 的 [retry] 先检查 Backend 是否支持 delayedJobs，
    /// 通过后将目标状态设为 delayed，availableAt = Now + delay。
    test(
      '带 delay 的 retry 会先预检能力，再将状态设为 delayed',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.failed,
          revision: 2,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(
            fixture.record(
              jobId: 'job-1',
              status: JobStatus.delayed,
              revision: 3,
            ),
            queuesToNotify: <String>{'work'},
          ),
        );

        await job.retry(delay: const Duration(seconds: 30));

        final TransitionJobRequest request =
            fixture.backend.lastArgument('transitionJob')
                as TransitionJobRequest;
        expect(request.targetStatus, JobStatus.delayed);
        expect(
          request.availableAt,
          fixture.time.add(const Duration(seconds: 30)),
        );
        expect(fixture.backend.calls, <String>['getJob', 'transitionJob']);
      },
    );

    /// 验证 Backend 不支持 delayedJobs 时，带 delay 的 retry 直接抛
    /// UnsupportedCapability，不做任何 Backend 写入；对 failed 和 canceled 来源状态均适用。
    test(
      '不支持延迟任务时，failed 和 canceled 状态的 retry 不会写入 backend',
      () async {
        for (final JobStatus source in <JobStatus>[
          JobStatus.failed,
          JobStatus.canceled,
        ]) {
          final QueueFixture fixture = QueueFixture(
            capabilities: _capabilities(delayedJobs: false),
          );
          final Job job = await _job(fixture, status: source);
          final List<String> callsBefore = List<String>.of(
            fixture.backend.calls,
          );

          expect(
            () => job.retry(delay: const Duration(seconds: 5)),
            throwsA(
              isA<UnsupportedCapability>().having(
                (UnsupportedCapability error) => error.capability,
                'capability',
                'delayedJobs',
              ),
            ),
          );

          expect(
            fixture.backend.calls,
            callsBefore,
            reason: 'source=$source 不应有 Backend 写入',
          );
          expect(fixture.signal.notifiedQueues, isEmpty);
        }
      },
    );

    /// 验证 retry 成功提交后，Signal 通知阶段观测到的已经是最新 snapshot，
    /// 即 snapshot 更新先于通知发出。
    test(
      'retry 成功后会在发出通知前替换 Job snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.failed,
          revision: 2,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.waiting,
          revision: 3,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
        );
        JobSnapshot? observedDuringNotify;
        fixture.signal.notifyResponder = (String queueName) {
          observedDuringNotify = job.snapshot;
        };

        await job.retry();

        expect(observedDuringNotify!.rawRecord, same(committed));
        expect(job.snapshot.rawRecord, same(committed));
      },
    );

    /// 验证 Signal 通知失败时，已提交的 snapshot 仍保留在 Job 句柄上，
    /// 调用方在捕获 QueueNotificationFailure 后仍可读取最新状态。
    test(
      'retry 的 signal 通知失败时仍会保留已提交的 snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.failed,
          revision: 2,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.waiting,
          revision: 3,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
        );
        fixture.signal.failingQueues.add('work');

        await expectLater(
          job.retry(),
          throwsA(
            isA<QueueNotificationFailure>()
                .having(
                  (QueueNotificationFailure error) => error.operation,
                  'operation',
                  'job.retry',
                )
                .having(
                  (QueueNotificationFailure error) => error.committedValue,
                  'committedValue',
                  same(committed),
                ),
          ),
        );

        expect(job.snapshot.rawRecord, same(committed));
      },
    );

    /// 验证 Backend 写失败时，Job 句柄的 snapshot 不被替换，
    /// Signal 也完全不会被调用——失败在写入阶段就终止了。
    test(
      'retry 的 backend 调用失败时会保留之前的 snapshot，并跳过 Signal',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.failed,
          revision: 2,
        );
        final JobSnapshot prior = job.snapshot;
        fixture.backend.respondWithFailure(
          'transitionJob',
          InvalidJobTransition(
            job.key,
            JobStatus.completed,
            JobStatus.waiting,
            'cannot retry a completed job',
          ),
        );

        await expectLater(job.retry(), throwsA(isA<InvalidJobTransition>()));

        expect(job.snapshot, same(prior));
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 Flow 依赖失败的父节点在 retry 时，Backend 返回的
    /// FlowDependencyFailure 被原样抛出，携带失败的子节点 key 列表。
    test(
      'retry 会向依赖失败的父 Job 传递 FlowDependencyFailure',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job parent = await _job(
          fixture,
          status: JobStatus.waitingChildren,
          revision: 3,
          jobId: 'parent',
        );
        final JobKey failedChildKey = fixture.key('child');
        fixture.backend.respondWithFailure(
          'transitionJob',
          FlowDependencyFailure(parent.key, <JobKey>[
            failedChildKey,
          ], 'retry the failed dependency before retrying its parent'),
        );
        final JobSnapshot parentBefore = parent.snapshot;

        await expectLater(
          parent.retry(),
          throwsA(
            isA<FlowDependencyFailure>()
                .having(
                  (FlowDependencyFailure error) => error.key,
                  'key',
                  parent.key,
                )
                .having(
                  (FlowDependencyFailure error) => error.dependencies,
                  'dependencies',
                  <JobKey>[failedChildKey],
                ),
          ),
        );

        expect(parent.snapshot, same(parentBefore));
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证子节点 retry 成功后，父节点仍保持 waitingChildren 状态不变，
    /// 只有显式 refresh 才会从 Backend 读取最新状态（可能仍是 waitingChildren
    /// 也可能变成 waiting，取决于其他子节点是否完成）。
    test(
      '重试失败的子 Job 后，父 Job 可从 backend 刷新并恢复为 waitingChildren 状态',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job child = await _job(
          fixture,
          status: JobStatus.failed,
          revision: 2,
          jobId: 'child',
        );
        final Job parent = await _job(
          fixture,
          status: JobStatus.waitingChildren,
          revision: 3,
          jobId: 'parent',
        );
        final JobRecord childCommitted = fixture.record(
          jobId: 'child',
          status: JobStatus.waiting,
          revision: 3,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(
            childCommitted,
            queuesToNotify: <String>{'work', 'parent'},
          ),
        );

        await child.retry();

        expect(fixture.backend.countOf('transitionJob'), 1);
        expect(fixture.signal.notifiedQueues, <String>['parent', 'work']);
        expect(child.snapshot.rawRecord, same(childCommitted));

        final JobRecord restoredParent = fixture.record(
          jobId: 'parent',
          status: JobStatus.waitingChildren,
          revision: 4,
        );
        fixture.backend.respond('getJob', restoredParent);

        final JobSnapshot refreshed = await parent.refresh();

        expect(refreshed.rawRecord, same(restoredParent));
        expect(parent.snapshot.status, JobStatus.waitingChildren);
        expect(fixture.backend.countOf('transitionJob'), 1);
      },
    );
  });

  group('Job.cancel', () {
    /// 验证 active 状态的 Job 调用 cancel 时，发的是 requestJobControl（请求控制信号），
    /// 而非 transitionJob，因为 Worker 可能正在执行中，需要协作式取消。
    test(
      '对 active Job 调用 cancel 会请求 cancel control，并保持 active 状态',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.active,
          revision: 6,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.active,
          revision: 7,
          controlRequest: JobControlRequest(
            action: JobControlAction.cancel,
            requestedAt: fixture.time,
          ),
        );
        fixture.backend.respond(
          'requestJobControl',
          fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
        );

        final JobSnapshot returned = await job.cancel();

        expect(fixture.backend.calls, <String>['getJob', 'requestJobControl']);
        expect(fixture.backend.called('transitionJob'), isFalse);
        final RequestJobControlRequest request =
            fixture.backend.lastArgument('requestJobControl')
                as RequestJobControlRequest;
        expect(request.key, job.key);
        expect(request.action, JobControlAction.cancel);
        expect(request.expectedRevision, 6);
        expect(request.requestedAt, fixture.time);
        expect(returned.rawRecord, same(committed));
      },
    );

    /// 验证 Backend 返回的 snapshot 是 active + cancel controlRequest 时，
    /// cancel 不会额外伪造一个 canceled 状态——完全尊重 Backend 的返回。
    test(
      'cancel 不会仅因 active control 请求就伪造 canceled snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.active,
          revision: 2,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.active,
          revision: 3,
          controlRequest: JobControlRequest(
            action: JobControlAction.cancel,
            requestedAt: fixture.time,
          ),
        );
        fixture.backend.respond(
          'requestJobControl',
          fixture.writeResult(committed),
        );

        await job.cancel();

        expect(job.snapshot.rawRecord, same(committed));
        expect(job.snapshot.status, JobStatus.active);
        expect(job.snapshot.controlRequest?.action, JobControlAction.cancel);
      },
    );

    Future<void> expectCancelTransition(JobStatus status) async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(fixture, status: status, revision: 3);
      final JobRecord committed = fixture.record(
        jobId: 'job-1',
        status: JobStatus.canceled,
        revision: 4,
      );
      fixture.backend.respond(
        'transitionJob',
        fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
      );

      await job.cancel();

      // 非 active 状态直接走 transitionJob 转为 canceled，不发控制请求。
      final TransitionJobRequest request =
          fixture.backend.lastArgument('transitionJob') as TransitionJobRequest;
      expect(request.targetStatus, JobStatus.canceled);
      expect(request.expectedRevision, 3);
      expect(request.occurredAt, fixture.time);
      expect(fixture.backend.called('requestJobControl'), isFalse);
      expect(job.snapshot.rawRecord, same(committed));
    }

    /// 验证 waiting 状态调用 cancel 时直接 transition 为 canceled。
    test('waiting 状态的 Job 调用 cancel 后会转为 canceled', () async {
      await expectCancelTransition(JobStatus.waiting);
    });

    /// 验证 delayed 状态调用 cancel 时直接 transition 为 canceled。
    test('delayed 状态的 Job 调用 cancel 后会转为 canceled', () async {
      await expectCancelTransition(JobStatus.delayed);
    });

    /// 验证 waitingChildren 状态调用 cancel 时直接 transition 为 canceled。
    test('waitingChildren 状态的 Job 调用 cancel 后会转为 canceled', () async {
      await expectCancelTransition(JobStatus.waitingChildren);
    });

    /// 验证 paused 状态调用 cancel 时直接 transition 为 canceled。
    test('paused 状态的 Job 调用 cancel 后会转为 canceled', () async {
      await expectCancelTransition(JobStatus.paused);
    });

    /// 验证已 canceled 的 Job 再次 cancel 时，Backend 返回 same 对象 + changed=false，
    /// 不触发 Signal 通知，revision 也保持不变。
    test(
      '对 canceled Job 调用 cancel 会委托未变更的转移，且不发出通知',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.canceled,
          revision: 4,
        );
        final JobSnapshot prior = job.snapshot;
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(prior.rawRecord, changed: false),
        );

        final JobSnapshot returned = await job.cancel();

        final TransitionJobRequest request =
            fixture.backend.lastArgument('transitionJob')
                as TransitionJobRequest;
        expect(request.targetStatus, JobStatus.canceled);
        expect(returned.rawRecord, same(prior.rawRecord));
        expect(job.snapshot.revision, 4);
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 completed 或 failed 状态的 Job 调用 cancel 时，
    /// Backend 返回 InvalidJobTransition 并原样向上抛出，Signal 不被调用。
    test(
      '对 completed 或 failed Job 调用 cancel 时由 backend 拒绝',
      () async {
        for (final JobStatus status in <JobStatus>[
          JobStatus.completed,
          JobStatus.failed,
        ]) {
          final QueueFixture fixture = QueueFixture();
          final Job job = await _job(fixture, status: status, revision: 2);
          fixture.backend.respondWithFailure(
            'transitionJob',
            InvalidJobTransition(
              job.key,
              status,
              JobStatus.canceled,
              'cannot cancel a finished job',
            ),
          );

          await expectLater(
            job.cancel(),
            throwsA(isA<InvalidJobTransition>()),
            reason: '$status must be rejected by the backend',
          );

          expect(
            fixture.backend.lastArgument('transitionJob'),
            isA<TransitionJobRequest>(),
          );
          expect(fixture.signal.notifiedQueues, isEmpty);
        }
      },
    );

    /// 验证 cancel 不传 expectedRevision 时默认取 snapshot.revision，
    /// 显式传入则原样透传——两种路径共存。
    test(
      'cancel 默认从 snapshot 读取 revision，并转发显式传入的 revision',
      () async {
        final QueueFixture defaulted = QueueFixture();
        final Job first = await _job(
          defaulted,
          status: JobStatus.waiting,
          revision: 8,
        );
        defaulted.backend.respond(
          'transitionJob',
          defaulted.writeResult(
            defaulted.record(
              jobId: 'job-1',
              status: JobStatus.canceled,
              revision: 9,
            ),
          ),
        );
        await first.cancel();
        expect(
          (defaulted.backend.lastArgument('transitionJob')
                  as TransitionJobRequest)
              .expectedRevision,
          8,
        );

        final QueueFixture explicit = QueueFixture();
        final Job second = await _job(
          explicit,
          status: JobStatus.waiting,
          revision: 8,
        );
        explicit.backend.respond(
          'transitionJob',
          explicit.writeResult(
            explicit.record(
              jobId: 'job-1',
              status: JobStatus.canceled,
              revision: 9,
            ),
          ),
        );
        await second.cancel(expectedRevision: 3);
        expect(
          (explicit.backend.lastArgument('transitionJob')
                  as TransitionJobRequest)
              .expectedRevision,
          3,
        );
      },
    );
  });

  group('Job.resume', () {
    /// 验证 paused 状态调用 resume 时 transition 为 waiting，
    /// occurredAt 和 availableAt 都等于 Now。
    test('resume 会在 Now 请求将状态从 paused 改为 waiting', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(
        fixture,
        status: JobStatus.paused,
        revision: 2,
      );
      final JobRecord committed = fixture.record(
        jobId: 'job-1',
        status: JobStatus.waiting,
        revision: 3,
      );
      fixture.backend.respond(
        'transitionJob',
        fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
      );

      await job.resume();

      final TransitionJobRequest request =
          fixture.backend.lastArgument('transitionJob') as TransitionJobRequest;
      expect(request.targetStatus, JobStatus.waiting);
      expect(request.occurredAt, fixture.time);
      expect(request.availableAt, fixture.time);
      expect(job.snapshot.rawRecord, same(committed));
      expect(fixture.nowReads, 1);
    });

    /// 验证非 paused 状态调用 resume 时，Backend 返回 InvalidJobTransition，
    /// Job 句柄的 snapshot 保持不变，Signal 不被调用。
    test('resume 会让 backend 拒绝非 paused 状态的 snapshot', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(
        fixture,
        status: JobStatus.waiting,
        revision: 2,
      );
      final JobSnapshot prior = job.snapshot;
      fixture.backend.respondWithFailure(
        'transitionJob',
        InvalidJobTransition(
          job.key,
          JobStatus.waiting,
          JobStatus.waiting,
          'job is not paused',
        ),
      );

      await expectLater(job.resume(), throwsA(isA<InvalidJobTransition>()));

      expect(job.snapshot, same(prior));
      expect(fixture.signal.notifiedQueues, isEmpty);
    });

    /// 验证 resume 成功提交后，即使 Signal 通知失败，snapshot 也已更新为 Backend 提交值。
    test(
      'resume 会在抛出通知错误前更新 snapshot',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.paused,
          revision: 2,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.waiting,
          revision: 3,
        );
        fixture.backend.respond(
          'transitionJob',
          fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
        );
        fixture.signal.failingQueues.add('work');

        await expectLater(
          job.resume(),
          throwsA(
            isA<QueueNotificationFailure>().having(
              (QueueNotificationFailure error) => error.operation,
              'operation',
              'job.resume',
            ),
          ),
        );

        expect(job.snapshot.rawRecord, same(committed));
      },
    );
  });

  group('Job.pause', () {
    /// 验证 pause 只允许 active 状态：调用方 snapshot 非 active 时
    /// 抛 [LifecycleError]，Backend 不被访问，Signal 也不被调用。
    test(
      '对非 active snapshot 调用 pause 会在任何 backend 写入前抛出 LifecycleError',
      () async {
        for (final JobStatus nonActive in <JobStatus>[
          JobStatus.waiting,
          JobStatus.delayed,
          JobStatus.failed,
          JobStatus.completed,
          JobStatus.canceled,
          JobStatus.paused,
        ]) {
          final QueueFixture fixture = QueueFixture();
          final Job job = await _job(fixture, status: nonActive, revision: 3);

          await expectLater(job.pause(), throwsA(isA<LifecycleError>()));

          // 没有任何 Backend 写入（没有 requestJobControl 或 transitionJob）。
          expect(fixture.backend.called('requestJobControl'), isFalse);
          expect(fixture.backend.called('transitionJob'), isFalse);
          // Signal 也不会被调用。
          expect(fixture.signal.notifiedQueues, isEmpty);
        }
      },
    );

    /// 验证 active 状态调用 pause 时，发的是 requestJobControl(pause)，
    /// 且 Backend 返回的 snapshot 仍是 active（协作式暂停不自动转换状态）。
    test(
      '对 active Job 调用 pause 会请求 pause control，并保持 active 状态',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(
          fixture,
          status: JobStatus.active,
          revision: 5,
        );
        final JobRecord committed = fixture.record(
          jobId: 'job-1',
          status: JobStatus.active,
          revision: 6,
          controlRequest: JobControlRequest(
            action: JobControlAction.pause,
            requestedAt: fixture.time,
          ),
        );
        fixture.backend.respond(
          'requestJobControl',
          fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
        );

        final JobSnapshot returned = await job.pause();

        expect(fixture.backend.calls, <String>['getJob', 'requestJobControl']);
        expect(fixture.backend.called('transitionJob'), isFalse);
        final RequestJobControlRequest request =
            fixture.backend.lastArgument('requestJobControl')
                as RequestJobControlRequest;
        expect(request.key, job.key);
        expect(request.action, JobControlAction.pause);
        expect(request.expectedRevision, 5);
        expect(request.requestedAt, fixture.time);
        expect(returned.rawRecord, same(committed));
        expect(returned.status, JobStatus.active);
        expect(returned.controlRequest?.action, JobControlAction.pause);
        expect(job.snapshot.rawRecord, same(committed));
      },
    );

    /// 验证 pause 显式传入 expectedRevision 时原样透传给 Backend，
    /// 不会被当前 snapshot.revision 覆盖。
    test('pause 会原样转发显式传入的 expectedRevision', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(
        fixture,
        status: JobStatus.active,
        revision: 5,
      );
      fixture.backend.respond(
        'requestJobControl',
        fixture.writeResult(
          fixture.record(jobId: 'job-1', status: JobStatus.active, revision: 6),
        ),
      );

      await job.pause(expectedRevision: 3);

      final RequestJobControlRequest request =
          fixture.backend.lastArgument('requestJobControl')
              as RequestJobControlRequest;
      expect(request.expectedRevision, 3);
    });
  });

  group('Job.remove', () {
    /// 验证 [remove] 不传 expectedRevision 时默认取 snapshot.revision，
    /// 请求的 removedAt 等于 Now。
    test('remove 默认使用最新 snapshot 的 expectedRevision', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(fixture, revision: 5);
      fixture.backend.respond(
        'removeJob',
        fixture.writeResult(
          <JobKey>[job.key],
          queuesToNotify: <String>{'work'},
        ),
      );

      await job.remove();

      final RemoveJobRequest request =
          fixture.backend.lastArgument('removeJob') as RemoveJobRequest;
      expect(request.expectedRevision, 5);
      expect(request.removedAt, fixture.time);
      expect(request.key, job.key);
    });

    /// 验证 remove 显式传入 expectedRevision 时原样透传，
    /// 返回值是 Backend 声明的完整删除子树（可能包含子节点和孙节点）。
    test(
      'remove 会转发显式 revision，并返回完整的已删除子树',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(fixture, revision: 5);
        final List<JobKey> removed = <JobKey>[
          job.key,
          fixture.key('child'),
          fixture.key('grandchild'),
        ];
        fixture.backend.respond(
          'removeJob',
          fixture.writeResult(
            removed,
            queuesToNotify: <String>{'work', 'parent'},
          ),
        );

        final List<JobKey> returned = await job.remove(expectedRevision: 2);

        final RemoveJobRequest request =
            fixture.backend.lastArgument('removeJob') as RemoveJobRequest;
        expect(request.expectedRevision, 2);
        expect(returned, removed);
        expect(fixture.signal.notifiedQueues, <String>['parent', 'work']);
      },
    );

    /// 验证 remove 只调用 Backend 的 removeJob，不会额外读 Flow 或做 transition。
    test('remove 只调用 removeJob，且不会读取 Flow', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await _job(fixture, revision: 1);
      fixture.backend.respond(
        'removeJob',
        fixture.writeResult(<JobKey>[job.key]),
      );

      await job.remove();

      expect(fixture.backend.calls, <String>['getJob', 'removeJob']);
      expect(fixture.backend.called('getFlow'), isFalse);
      expect(fixture.backend.called('transitionJob'), isFalse);
    });

    /// 验证子节点仍处于 active 时，Backend 会拒绝删除其父节点（InvalidJobTransition），
    /// 该错误原样上抛，Job snapshot 不更新，Signal 不被调用。
    test(
      'active 子代导致的删除拒绝会向上传递，且不发出通知',
      () async {
        final QueueFixture fixture = QueueFixture();
        final Job job = await _job(fixture, revision: 1);
        final JobSnapshot prior = job.snapshot;
        fixture.backend.respondWithFailure(
          'removeJob',
          InvalidJobTransition(
            fixture.key('child'),
            JobStatus.active,
            JobStatus.canceled,
            'active descendant must be canceled first',
          ),
        );

        await expectLater(job.remove(), throwsA(isA<InvalidJobTransition>()));

        expect(job.snapshot, same(prior));
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );
  });
}
