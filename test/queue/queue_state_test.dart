import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';

import 'support/queue_fixtures.dart';

void main() {
  group('Queue.getState', () {
    /// 验证 getState 把 queueName 透传给 Backend 的 getQueueState，
    /// 不做任何写入——包括 defaultPolicy 初始化也不会触发。
    test('getState 会读取固定的 queueName，且不会写入', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
      );
      fixture.queueState = fixture.state(
        revision: 2,
        policy: QueuePolicy(maxEventCount: 10),
      );

      final QueueState state = await fixture.queue.getState();

      expect(fixture.backend.lastArgument('getQueueState'), 'work');
      expect(state.revision, 2);
      expect(fixture.backend.calls, <String>['getQueueState']);
      expect(fixture.signal.notifiedQueues, isEmpty);
    });
  });

  group('pause 和 resume 的乐观 revision', () {
    /// 验证 pause 不传 expectedRevision 时先 getQueueState 拿到最新 revision，
    /// 再用它作为 expectedRevision 调 setQueuePaused。
    test(
      '未提供 expectedRevision 时，pause 会读取最新 revision',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.queueState = fixture.state(revision: 5);
        fixture.backend.respond(
          'setQueuePaused',
          fixture.writeResult(
            fixture.state(paused: true, revision: 6),
            queuesToNotify: <String>{'work'},
          ),
        );

        final QueueState state = await fixture.queue.pause();

        final SetQueuePausedRequest request = fixture.backend.lastArgument(
          'setQueuePaused',
        ) as SetQueuePausedRequest;
        expect(request.expectedRevision, 5);
        expect(request.paused, isTrue);
        expect(state.revision, 6);
        expect(fixture.backend.calls, <String>[
          'getQueueState',
          'setQueuePaused',
        ]);
      },
    );

    /// 验证 resume 不传 expectedRevision 时同样先 getQueueState，
    /// 流程与 pause 对称。
    test(
      '未提供 expectedRevision 时，resume 会读取最新 revision',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.queueState = fixture.state(paused: true, revision: 5);
        fixture.backend.respond(
          'setQueuePaused',
          fixture.writeResult(
            fixture.state(paused: false, revision: 6),
            queuesToNotify: <String>{'work'},
          ),
        );

        await fixture.queue.resume();

        final SetQueuePausedRequest request = fixture.backend.lastArgument(
          'setQueuePaused',
        ) as SetQueuePausedRequest;
        expect(request.expectedRevision, 5);
        expect(request.paused, isFalse);
      },
    );

    /// 验证 pause 显式传入 expectedRevision 时原样透传，
    /// Queue 不做任何 revision 替换或校验。
    test('pause 会转发显式 revision，而不替换该值', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.queueState = fixture.state(revision: 2);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          fixture.state(paused: true, revision: 3),
          queuesToNotify: <String>{'work'},
        ),
      );

      await fixture.queue.pause(expectedRevision: 2);

      final SetQueuePausedRequest request = fixture.backend.lastArgument(
        'setQueuePaused',
      ) as SetQueuePausedRequest;
      expect(request.expectedRevision, 2);
    });

    /// 验证 resume 显式传入 expectedRevision 时原样透传，对称于 pause。
    test('resume 会转发显式 revision，而不替换该值', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.queueState = fixture.state(paused: true, revision: 2);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          fixture.state(paused: false, revision: 3),
          queuesToNotify: <String>{'work'},
        ),
      );

      await fixture.queue.resume(expectedRevision: 2);

      final SetQueuePausedRequest request = fixture.backend.lastArgument(
        'setQueuePaused',
      ) as SetQueuePausedRequest;
      expect(request.expectedRevision, 2);
    });

    /// 验证 pause 显式传入过期 revision 时，Queue 先 getQueueState 拿到最新值
    /// 做预检，发现不匹配就抛 QueueRevisionConflict，不会再做 policy 或 pause 写入。
    test(
      'pause 会在写入任何 policy 前预检过期的显式 revision',
      () async {
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
        );
        fixture.queueState = fixture.state(revision: 3);

        await expectLater(
          fixture.queue.pause(expectedRevision: 1),
          throwsA(
            isA<QueueRevisionConflict>()
                .having(
                  (QueueRevisionConflict error) => error.queueName,
                  'queueName',
                  'work',
                )
                .having(
                  (QueueRevisionConflict error) => error.expected,
                  'expected',
                  1,
                )
                .having(
                  (QueueRevisionConflict error) => error.actual,
                  'actual',
                  3,
                ),
          ),
        );

        expect(fixture.backend.calls, <String>['getQueueState']);
        expect(fixture.backend.called('setQueuePolicy'), isFalse);
        expect(fixture.backend.called('setQueuePaused'), isFalse);
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 resume 显式传入过期 revision 时同样做预检，与 pause 对称。
    test(
      'resume 会在写入任何 policy 前预检过期的显式 revision',
      () async {
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(defaultPolicy: QueuePolicy(maxEventCount: 10)),
        );
        fixture.queueState = fixture.state(paused: true, revision: 7);

        await expectLater(
          fixture.queue.resume(expectedRevision: 2),
          throwsA(
            isA<QueueRevisionConflict>()
                .having(
                  (QueueRevisionConflict error) => error.expected,
                  'expected',
                  2,
                )
                .having(
                  (QueueRevisionConflict error) => error.actual,
                  'actual',
                  7,
                ),
          ),
        );

        expect(fixture.backend.calls, <String>['getQueueState']);
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 pause 带非默认 policy 时的完整流程：queue revision=0 →
    /// setQueuePolicy（expectedRevision=0）成功 → setQueuePaused 使用 policy 写入后的 revision 作为 expectedRevision。
    test('使用非默认 policy 调用 pause 时，会采用 policy 写入返回的 revision', () async {
      final QueuePolicy policy = QueuePolicy(maxEventCount: 10);
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: policy),
      );
      fixture.backend.respond('getQueueState', fixture.state(revision: 0));
      fixture.queueState = fixture.state(policy: policy, revision: 1);
      fixture.backend.respond(
        'setQueuePolicy',
        fixture.writeResult(
          fixture.state(policy: policy, revision: 1),
          queuesToNotify: <String>{'work'},
        ),
      );
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          fixture.state(paused: true, policy: policy, revision: 2),
          queuesToNotify: <String>{'work'},
        ),
      );

      final QueueState state = await fixture.queue.pause();

      final SetQueuePausedRequest request = fixture.backend.lastArgument(
        'setQueuePaused',
      ) as SetQueuePausedRequest;
      expect(request.expectedRevision, 1);
      expect(state.revision, 2);
      expect(fixture.backend.calls, <String>[
        'getQueueState',
        'setQueuePolicy',
        'setQueuePaused',
      ]);
      expect(fixture.signal.notifiedQueues, <String>['work', 'work']);
    });

    /// 验证 resume 带非默认 policy 时的流程与 pause 对称——policy 初始化
    /// 在 pause/resume 之前完成，且 pause/resume 使用 policy 返回的 revision。
    test('使用非默认 policy 调用 resume 时，会采用 policy 写入返回的 revision', () async {
      final QueuePolicy policy = QueuePolicy(maxEventCount: 10);
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(defaultPolicy: policy),
      );
      fixture.backend.respond('getQueueState', fixture.state(revision: 0));
      fixture.queueState = fixture.state(policy: policy, revision: 1);
      fixture.backend.respond(
        'setQueuePolicy',
        fixture.writeResult(
          fixture.state(policy: policy, revision: 1),
          queuesToNotify: <String>{'work'},
        ),
      );
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          fixture.state(paused: false, policy: policy, revision: 2),
          queuesToNotify: <String>{'work'},
        ),
      );

      await fixture.queue.resume();

      final SetQueuePausedRequest request = fixture.backend.lastArgument(
        'setQueuePaused',
      ) as SetQueuePausedRequest;
      expect(request.expectedRevision, 1);
      expect(fixture.backend.calls, <String>[
        'getQueueState',
        'setQueuePolicy',
        'setQueuePaused',
      ]);
    });

    /// 验证 pause 和 resume 都把 Now 作为 occurredAt 传给 Backend，
    /// 各只读一次 Now（pause 一次、resume 一次）。
    test('pause 和 resume 会使用 Now 作为 occurredAt', () async {
      final QueueFixture fixture = QueueFixture();
      final DateTime frozen = fixture.time;
      fixture.queueState = fixture.state(revision: 4);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(fixture.state(paused: true, revision: 5)),
      );
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(fixture.state(paused: false, revision: 6)),
      );

      await fixture.queue.pause();
      final SetQueuePausedRequest pauseRequest = fixture.backend.lastArgument(
        'setQueuePaused',
      ) as SetQueuePausedRequest;
      expect(pauseRequest.occurredAt, frozen);
      expect(fixture.nowReads, 1);

      await fixture.queue.resume();
      final SetQueuePausedRequest resumeRequest = fixture.backend.lastArgument(
        'setQueuePaused',
      ) as SetQueuePausedRequest;
      expect(resumeRequest.occurredAt, frozen);
      expect(resumeRequest.paused, isFalse);
      expect(fixture.nowReads, 2);
    });

    /// 验证 Backend 返回 QueueRevisionConflict 时，Queue 直接向上抛出，
    /// 不调用 Signal——错误发生在写入阶段，通知逻辑不会被触发。
    test(
      'QueueRevisionConflict 会向上传递，且不会发出 Signal 通知',
      () async {
        final QueueFixture fixture = QueueFixture();
        fixture.queueState = fixture.state(revision: 1);
        fixture.backend.respondWithFailure(
          'setQueuePaused',
          QueueRevisionConflict('work', 1, 2, 'stale queue revision'),
        );

        await expectLater(
          fixture.queue.pause(),
          throwsA(isA<QueueRevisionConflict>()),
        );

        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );
  });

  group('pause 和 resume 的结果', () {
    /// 验证 Backend 返回 changed=false（幂等 pause，queue 已经是 paused）时，
    /// pause 返回 Backend 的原状态对象，不触发 Signal 通知。
    test(
      '幂等的 pause 会返回当前状态，且不会发出通知',
      () async {
        final QueueFixture fixture = QueueFixture();
        final QueueState current = fixture.state(paused: true, revision: 3);
        fixture.queueState = current;
        fixture.backend.respond(
          'setQueuePaused',
          fixture.writeResult(current, changed: false),
        );

        final QueueState state = await fixture.queue.pause();

        expect(state, same(current));
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 Backend 返回 changed=false（幂等 resume，queue 已经是 unpaused）时，
    /// resume 同样返回原状态、不触发通知，与 pause 对称。
    test(
      '幂等的 resume 会返回当前状态，且不会发出通知',
      () async {
        final QueueFixture fixture = QueueFixture();
        final QueueState current = fixture.state(paused: false, revision: 3);
        fixture.queueState = current;
        fixture.backend.respond(
          'setQueuePaused',
          fixture.writeResult(current, changed: false),
        );

        final QueueState state = await fixture.queue.resume();

        expect(state, same(current));
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 pause 通知的 queue 集合完全取自 BackendWriteResult.queuesToNotify，
    /// 不由 Queue 层决定——Backend 返回什么就通知什么。
    test('pause 只会通知 backend 指定的通知集合', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.queueState = fixture.state(revision: 1);
      final QueueState committed = fixture.state(paused: true, revision: 2);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          committed,
          queuesToNotify: <String>{'other-queue', 'work'},
        ),
      );

      final QueueState state = await fixture.queue.pause();

      expect(state, same(committed));
      expect(fixture.signal.notifiedQueues, <String>['other-queue', 'work']);
    });

    /// 验证 pause 的 Signal 通知失败时，QueueNotificationFailure.committedValue
    /// 携带 Backend 已提交的 QueueState——pause 本身已经生效，只是通知没发出去。
    test('pause 的 signal 通知失败时会报告已提交的 QueueState', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.queueState = fixture.state(revision: 1);
      final QueueState committed = fixture.state(paused: true, revision: 2);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(committed, queuesToNotify: <String>{'work'}),
      );
      fixture.signal.failingQueues.add('work');

      await expectLater(
        fixture.queue.pause(),
        throwsA(
          isA<QueueNotificationFailure>()
              .having(
                (QueueNotificationFailure error) => error.operation,
                'operation',
                'pause',
              )
              .having(
                (QueueNotificationFailure error) => error.committedValue,
                'committedValue',
                same(committed),
              ),
        ),
      );
    });

    /// 验证 queue 级 pause 不会级联修改任何 Job——
    /// 不会调用 transitionJob、不会调 requestJobControl，单个 Job 的状态保持不变。
    test('暂停 queue 不会改变任何 Job snapshot', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', revision: 1),
      );
      final Job job = (await fixture.queue.getJob('job-1'))!;
      fixture.queueState = fixture.state(revision: 0);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          fixture.state(paused: true, revision: 1),
          queuesToNotify: <String>{'work'},
        ),
      );

      await fixture.queue.pause();

      expect(fixture.backend.called('transitionJob'), isFalse);
      expect(fixture.backend.called('requestJobControl'), isFalse);
      expect(job.snapshot.revision, 1);
      expect(job.snapshot.status, JobStatus.waiting);
    });

    /// 验证 queue 级 resume 不会级联恢复处于 paused 状态的单个 Job——
    /// queue.resume 和 job.resume 是完全独立的两个操作。
    test('恢复 queue 不会恢复单独暂停的 Job', () async {
      final QueueFixture fixture = QueueFixture();
      fixture.backend.respond(
        'getJob',
        fixture.record(jobId: 'job-1', status: JobStatus.paused, revision: 2),
      );
      final Job job = (await fixture.queue.getJob('job-1'))!;
      fixture.queueState = fixture.state(paused: true, revision: 1);
      fixture.backend.respond(
        'setQueuePaused',
        fixture.writeResult(
          fixture.state(paused: false, revision: 2),
          queuesToNotify: <String>{'work'},
        ),
      );

      await fixture.queue.resume();

      expect(fixture.backend.called('transitionJob'), isFalse);
      expect(fixture.backend.called('requestJobControl'), isFalse);
      expect(job.snapshot.status, JobStatus.paused);
      expect(job.snapshot.revision, 2);
    });
  });
}
