import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/worker/active_job.dart';

import 'support/recording_backend.dart';
import 'support/worker_fixture.dart';

/// 断言 [future] 以模块内部 marker 失败，并返回该 marker 以便检查原始错误。
Future<ActiveJobInfrastructureFailure> expectInfrastructureFailure(
  Future<Object?> future,
) async {
  try {
    await future;
  } on ActiveJobInfrastructureFailure catch (failure) {
    return failure;
  }
  fail('Expected an ActiveJobInfrastructureFailure.');
}

/// 构造一个与当前 credential 匹配的刷新结果。
///
/// 默认继承当前 claim 的 name 与 attemptsMade：同一次执行内它们不会变化。
JobRecord refreshed(
  WorkerFixture fixture,
  ClaimedJob claimed, {
  int? revision,
  int? attemptsMade,
  Object? data,
  Object? progress,
  Object? result,
  JobStatus status = JobStatus.active,
  JobControlRequest? controlRequest,
}) => fixture.record(
  queueName: claimed.record.key.queueName,
  jobId: claimed.record.key.jobId,
  name: claimed.record.name,
  status: status,
  revision: revision,
  attemptsMade: attemptsMade ?? claimed.record.attemptsMade,
  data: data,
  progress: progress,
  result: result,
  execution: claimed.record.execution,
  controlRequest: controlRequest,
);

JobLogDraft infoLog(String message) => JobLogDraft(
  level: JobLogLevel.info,
  channel: JobLogChannel.application,
  message: message,
);

void main() {
  // --- snapshot 与 mutation：验证 ActiveJob 暴露的身份标识与业务读写 ---

  test(
    '会从最新 snapshot 暴露 identity、data、progress 和 attempt',
    () async {
      // 验证 ActiveJob 暴露 Job 的 key、name、data、progress、attempt，且 refreshControl 后从 Backend 拿到新快照

      final fixture = WorkerFixture();
      final claimed = fixture.claim(
        jobId: 'j1',
        name: 'send-email',
        data: <String, Object?>{'to': 'a@example.com'},
        progress: 0.25,
        attemptsMade: 3,
      );
      final controller = fixture.controllerFor(claimed);
      final job = controller.job;

      expect(job.rawRecord, same(claimed.record));
      expect(job.key, claimed.record.key);
      expect(job.name, 'send-email');
      expect(job.data, <String, Object?>{'to': 'a@example.com'});
      expect(job.progress, 0.25);
      expect(job.attempt, 3);

      fixture.backend.onGetJob = (JobKey key) async => refreshed(
        fixture,
        claimed,
        revision: 9,
        data: <String, Object?>{'to': 'b@example.com'},
        progress: 0.75,
      );
      await controller.refreshControl();

      expect(job.data, <String, Object?>{'to': 'b@example.com'});
      expect(job.progress, 0.75);
      expect(job.rawRecord.revision, 9);
      expect(job.attempt, 3);
    },
  );

  test('会按 revision 串行处理并发 progress 和 data 更新', () async {
    // 验证并发的 updateProgress 和 updateData 通过共享 revision 串行化：后者等待前者完成后再用新 revision 提交
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final barrier = Completer<void>();
    var revision = claimed.record.revision;

    fixture.backend.onUpdateJobProgress =
        (UpdateJobProgressRequest request) async {
          await barrier.future;
          revision = request.expectedRevision + 1;
          return changedWrite(
            refreshed(
              fixture,
              claimed,
              revision: revision,
              progress: request.progress,
            ),
          );
        };
    fixture.backend.onUpdateJobData = (UpdateJobDataRequest request) async {
      revision = request.expectedRevision + 1;
      return changedWrite(
        refreshed(fixture, claimed, revision: revision, data: request.data),
      );
    };

    final first = controller.updateProgress(0.5);
    final second = controller.updateData(<String, Object?>{'step': 1});
    await fixture.runtime.flushMicrotasks();

    // 第一个写入仍被 barrier 阻塞，第二个请求必须还没有发出。
    expect(fixture.backend.countOf('updateJobProgress'), 1);
    expect(fixture.backend.countOf('updateJobData'), 0);

    barrier.complete();
    await first;
    await second;

    // 第二个请求必须使用第一个响应带来的新 revision。
    expect(
      fixture.backend.updateJobProgressRequests.single.expectedRevision,
      1,
    );
    expect(fixture.backend.updateJobDataRequests.single.expectedRevision, 2);
    expect(controller.record.data, <String, Object?>{'step': 1});
  });

  test('会通过同一个 controller 串行处理 refresh、变更、续约、终态操作和 leaseLost', () async {
    // 验证 refresh、mutation、renew、terminal、leaseLost 全部通过同一个串行 tail 执行，且 leaseLost 后业务写入被拒绝
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final barrier = Completer<void>();
    final epochBefore = controller.epoch;

    fixture.backend.onUpdateJobProgress =
        (UpdateJobProgressRequest request) async {
          await barrier.future;
          return changedWrite(
            refreshed(
              fixture,
              claimed,
              revision: request.expectedRevision + 1,
              progress: request.progress,
            ),
          );
        };
    fixture.backend.onGetJob = (JobKey key) async =>
        refreshed(fixture, claimed, revision: controller.record.revision);

    final mutation = controller.updateProgress(0.4);
    final refresh = controller.refreshControl();
    final leaseLost = controller.markLeaseLost(
      LeaseLost(controller.key, controller.executionId, 'lease lost'),
      StackTrace.current,
    );
    await fixture.runtime.flushMicrotasks();

    // 只有第一个操作进入了 Backend，refresh 与 leaseLost 仍排在同一个 tail 之后。
    expect(fixture.backend.countOf('updateJobProgress'), 1);
    expect(fixture.backend.countOf('getJob'), 0);

    barrier.complete();
    await mutation;
    await refresh;
    await leaseLost;

    expect(controller.isLeaseLost, isTrue);
    expect(controller.controlState, ActiveJobControlState.leaseLost);
    expect(controller.epoch, greaterThan(epochBefore));

    // leaseLost 之后业务写入立即失败且不再触碰 Backend。
    final failure = await expectInfrastructureFailure(
      controller.updateData(<String, Object?>{'step': 2}),
    );
    expect(failure.original, isA<LeaseLost>());
    expect(fixture.backend.countOf('updateJobData'), 0);
  });

  test(
    '每次写入 progress、data 或日志后都会刷新 snapshot',
    () async {
      // 验证 updateProgress、updateData、log 每次写入后都通过 getJob 刷新快照，保持 revision 与 Backend 一致
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'j1');
      final controller = fixture.controllerFor(claimed);
      var revision = claimed.record.revision;
      Object? data;
      Object? progress;

      fixture.backend.onUpdateJobProgress =
          (UpdateJobProgressRequest request) async {
            revision = request.expectedRevision + 1;
            progress = request.progress;
            return changedWrite(
              refreshed(
                fixture,
                claimed,
                revision: revision,
                progress: progress,
                data: data,
              ),
            );
          };
      fixture.backend.onUpdateJobData = (UpdateJobDataRequest request) async {
        revision = request.expectedRevision + 1;
        data = request.data;
        return changedWrite(
          refreshed(
            fixture,
            claimed,
            revision: revision,
            data: data,
            progress: progress,
          ),
        );
      };
      fixture.backend.onAppendJobLog = (AppendJobLogRequest request) async =>
          changedWrite(
            JobLog(
              logId: 'log-1',
              jobKey: request.key,
              sequence: 1,
              attempt: request.attempt,
              executionId: request.credential.executionId,
              level: request.log.level,
              channel: request.log.channel,
              message: request.log.message,
              data: request.log.data,
              occurredAt: request.occurredAt,
            ),
          );
      fixture.backend.onGetJob = (JobKey key) async => refreshed(
        fixture,
        claimed,
        revision: revision,
        data: data,
        progress: progress,
      );

      await controller.updateProgress(0.6);
      expect(controller.record.progress, 0.6);
      expect(controller.record.revision, revision);

      await controller.updateData(<String, Object?>{'step': 2});
      expect(controller.record.data, <String, Object?>{'step': 2});
      expect(controller.record.revision, revision);

      final revisionBeforeLog = controller.record.revision;
      await controller.log(infoLog('done'));

      // appendJobLog 不递增 revision，但成功后仍通过 getJob 刷新 snapshot。
      expect(fixture.backend.countOf('getJob'), 1);
      expect(controller.record.revision, revisionBeforeLog);
      expect(controller.record.progress, 0.6);
      expect(controller.record.data, <String, Object?>{'step': 2});
    },
  );

  test('每次变更成功后只通知 queuesToNotify', () async {
    // 验证每次成功 mutation 后只通知 Backend 声明的 queuesToNotify，不会额外通知当前 Job 所在的队列
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);

    fixture.backend.onUpdateJobProgress =
        (UpdateJobProgressRequest request) async => changedWrite(
          refreshed(
            fixture,
            claimed,
            revision: request.expectedRevision + 1,
            progress: request.progress,
          ),
          queuesToNotify: const <String>{'other-queue'},
        );
    fixture.backend.onUpdateJobData = (UpdateJobDataRequest request) async =>
        changedWrite(
          refreshed(
            fixture,
            claimed,
            revision: request.expectedRevision + 1,
            data: request.data,
          ),
          queuesToNotify: const <String>{'reports', 'files'},
        );

    await controller.updateProgress(0.1);
    expect(fixture.signal.notifyCalls, <String>['other-queue']);

    await controller.updateData('payload');
    expect(fixture.signal.notifyCalls, <String>[
      'other-queue',
      'reports',
      'files',
    ]);
    // 未声明的队列不会被通知，例如当前 Job 所在的 emails。
    expect(fixture.signal.notifyCalls, isNot(contains('emails')));
  });

  test(
    'signal notify 失败时，仍会将已提交的变更视为成功',
    () async {
      // 验证 Signal 通知失败不影响已提交的 Backend 事实：mutation 仍然成功，Job 仍 active
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'j1');
      final controller = fixture.controllerFor(claimed);
      fixture.signal.notifyFails = true;

      fixture.backend.onUpdateJobProgress =
          (UpdateJobProgressRequest request) async => changedWrite(
            refreshed(
              fixture,
              claimed,
              revision: request.expectedRevision + 1,
              progress: request.progress,
            ),
            queuesToNotify: const <String>{'emails'},
          );

      final record = await controller.updateProgress(0.2);
      expect(record.progress, 0.2);
      expect(controller.record.progress, 0.2);
      expect(controller.isLeaseLost, isFalse);
      expect(fixture.backend.countOf('updateJobProgress'), 1);
      expect(fixture.signal.notifyCalls, <String>['emails']);
    },
  );

  test(
    '刷新同一 execution 后会重试一次 revision 冲突',
    () async {
      // 验证同一 execution 下的 RevisionConflict 会触发一次 refresh 与重试，刷新后用新 revision 成功
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'j1', revision: 5);
      final controller = fixture.controllerFor(claimed);
      var attempts = 0;

      fixture
          .backend
          .onUpdateJobProgress = (UpdateJobProgressRequest request) async {
        attempts++;
        if (attempts == 1) {
          expect(request.expectedRevision, 5);
          throw RevisionConflict(request.key, 5, 6, 'stale revision');
        }
        expect(request.expectedRevision, 7);
        return changedWrite(
          refreshed(fixture, claimed, revision: 8, progress: request.progress),
        );
      };
      fixture.backend.onGetJob = (JobKey key) async =>
          refreshed(fixture, claimed, revision: 7);

      final record = await controller.updateProgress(0.3);
      expect(record.progress, 0.3);
      expect(attempts, 2);
      expect(fixture.backend.countOf('getJob'), 1);
    },
  );

  test(
    'execution 不同时，会将 revision 冲突转换为 LeaseLost',
    () async {
      // 验证 refresh 后发现 execution 已被他人替换时，直接判定 LeaseLost 而非再重试
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'j1', revision: 5);
      final controller = fixture.controllerFor(claimed);

      fixture.backend.onUpdateJobProgress = (
        UpdateJobProgressRequest request,
      ) async => throw RevisionConflict(request.key, 5, 9, 'stale revision');
      fixture.backend.onGetJob = (JobKey key) async => fixture.record(
        jobId: 'j1',
        revision: 9,
        execution: ExecutionOwner(
          workerId: 'other-worker',
          credential: const ExecutionCredential(executionId: 'other-exec'),
          startedAt: fixture.runtime.now(),
        ),
      );

      final failure = await expectInfrastructureFailure(
        controller.updateProgress(0.3),
      );
      expect(failure.original, isA<LeaseLost>());
      expect(controller.isLeaseLost, isTrue);
      expect(controller.controlState, ActiveJobControlState.leaseLost);
      // 不同 execution 时不允许第二次写入。
      expect(fixture.backend.countOf('updateJobProgress'), 1);
    },
  );

  test('LeaseLost 后会拒绝所有操作', () async {
    // 验证 LeaseLost 后所有 ActiveJob 操作（mutation、log、confirm、refresh、dependency 读取）都以基础设施失败终止
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    await controller.markLeaseLost(
      LeaseLost(controller.key, controller.executionId, 'lease lost'),
      StackTrace.current,
    );

    final operations = <String, Future<Object?>>{
      'updateProgress': controller.updateProgress(0.5),
      'updateData': controller.updateData('x'),
      'log': controller.log(infoLog('x')),
      'confirmPaused': controller.confirmPaused(),
      'confirmCanceled': controller.confirmCanceled(),
      'getDependenciesResult': controller.getDependenciesResult(),
      'refreshControl': controller.refreshControl(),
    };

    for (final entry in operations.entries) {
      final failure = await expectInfrastructureFailure(entry.value);
      expect(failure.original, isA<LeaseLost>(), reason: entry.key);
    }
    // 失去租约后不产生任何 Backend 写，也不读依赖。
    expect(fixture.backend.countOf('updateJobProgress'), 0);
    expect(fixture.backend.countOf('updateJobData'), 0);
    expect(fixture.backend.countOf('appendJobLog'), 0);
    expect(fixture.backend.countOf('transitionJob'), 0);
    expect(fixture.backend.countOf('getFlow'), 0);
    expect(fixture.backend.countOf('getJob'), 0);
  });

  test('会将 ActiveJob 的 backend、日志和依赖错误标记为基础设施故障', () async {
    // 验证 appendJobLog 和 getFlow 的 Backend 错误被包装为 ActiveJobInfrastructureFailure，且不等于 LeaseLost
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final backendError = BackendUnavailable(null, 'backend is down');

    fixture.backend.onAppendJobLog = (AppendJobLogRequest request) async =>
        throw backendError;
    final logFailure = await expectInfrastructureFailure(
      controller.log(infoLog('x')),
    );
    expect(logFailure.original, same(backendError));
    expect(logFailure.operation, 'appendJobLog');

    fixture.backend.onGetFlow = (JobKey key) async => throw backendError;
    final flowFailure = await expectInfrastructureFailure(
      controller.getDependenciesResult(),
    );
    expect(flowFailure.original, same(backendError));
    expect(flowFailure.operation, 'getFlow');
    // 基础设施错误不等于失去租约。
    expect(controller.isLeaseLost, isFalse);
  });

  // --- 控制状态：pause / cancel 请求的发现、升级与确认 ---

  test('每个 pause 或 cancel control 状态只会发布一次', () async {
    // 验证 pause / cancel 控制请求被 refresh 后只发布一次 controlStream 事件，重复 refresh 不重复发布
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final events = <ActiveJobControlState>[];
    controller.controlStream.listen(events.add);

    fixture.backend.onGetJob = (JobKey key) async => refreshed(
      fixture,
      claimed,
      revision: controller.record.revision,
      controlRequest: JobControlRequest(
        action: JobControlAction.pause,
        requestedAt: fixture.runtime.now(),
      ),
    );
    await controller.refreshControl();
    await controller.refreshControl();

    expect(controller.controlState, ActiveJobControlState.pauseRequested);
    expect(events, <ActiveJobControlState>[
      ActiveJobControlState.pauseRequested,
    ]);
  });

  test('pauseRequested 会且仅会升级为 cancelRequested 一次', () async {
    // 验证 pause 可以被 cancel 升级，且只发布一次 cancelRequested 事件
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final events = <ActiveJobControlState>[];
    controller.controlStream.listen(events.add);

    var action = JobControlAction.pause;
    fixture.backend.onGetJob = (JobKey key) async => refreshed(
      fixture,
      claimed,
      revision: controller.record.revision,
      controlRequest: JobControlRequest(
        action: action,
        requestedAt: fixture.runtime.now(),
      ),
    );

    await controller.refreshControl();
    action = JobControlAction.cancel;
    await controller.refreshControl();
    await controller.refreshControl();

    expect(events, <ActiveJobControlState>[
      ActiveJobControlState.pauseRequested,
      ActiveJobControlState.cancelRequested,
    ]);
    expect(controller.controlState, ActiveJobControlState.cancelRequested);
  });

  test('绝不会将 cancelRequested 降级回 pauseRequested', () async {
    // 验证 cancel 是最高优先级：控制请求不会从 cancelRequested 降级回 pauseRequested
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final events = <ActiveJobControlState>[];
    controller.controlStream.listen(events.add);

    var action = JobControlAction.cancel;
    fixture.backend.onGetJob = (JobKey key) async => refreshed(
      fixture,
      claimed,
      revision: controller.record.revision,
      controlRequest: JobControlRequest(
        action: action,
        requestedAt: fixture.runtime.now(),
      ),
    );

    await controller.refreshControl();
    action = JobControlAction.pause;
    await controller.refreshControl();

    expect(controller.controlState, ActiveJobControlState.cancelRequested);
    expect(events, <ActiveJobControlState>[
      ActiveJobControlState.cancelRequested,
    ]);
  });

  test('不会向较晚加入的 listener 重放 control 事件', () async {
    // 验证 controlStream 不重放历史事件：晚订阅的监听者只会收到之后的新事件
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);

    fixture.backend.onGetJob = (JobKey key) async => refreshed(
      fixture,
      claimed,
      revision: controller.record.revision,
      controlRequest: JobControlRequest(
        action: JobControlAction.pause,
        requestedAt: fixture.runtime.now(),
      ),
    );
    await controller.refreshControl();

    final late = <ActiveJobControlState>[];
    controller.controlStream.listen(late.add);
    await fixture.runtime.flushMicrotasks();

    expect(late, isEmpty);
    // 调用方必须先读 controlState 再监听 stream。
    expect(controller.controlState, ActiveJobControlState.pauseRequested);
  });

  test('confirmPaused 要求存在 pause 请求', () async {
    // 验证 confirmPaused 必须在 pause 请求存在时调用；cancel 请求或无请求都抛 LifecycleError
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);

    await expectLater(
      controller.confirmPaused(),
      throwsA(isA<LifecycleError>()),
    );

    fixture.backend.onGetJob = (JobKey key) async => refreshed(
      fixture,
      claimed,
      revision: controller.record.revision,
      controlRequest: JobControlRequest(
        action: JobControlAction.cancel,
        requestedAt: fixture.runtime.now(),
      ),
    );
    await controller.refreshControl();
    // cancel 请求不能用于确认 paused。
    await expectLater(
      controller.confirmPaused(),
      throwsA(isA<LifecycleError>()),
    );

    expect(fixture.backend.countOf('transitionJob'), 0);
    expect(controller.isTerminal, isFalse);
  });

  test('confirmCanceled 要求存在 cancel 请求', () async {
    // 验证 confirmCanceled 必须在 cancel 请求存在时调用；pause 请求或无请求都抛 LifecycleError
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);

    await expectLater(
      controller.confirmCanceled(),
      throwsA(isA<LifecycleError>()),
    );

    fixture.backend.onGetJob = (JobKey key) async => refreshed(
      fixture,
      claimed,
      revision: controller.record.revision,
      controlRequest: JobControlRequest(
        action: JobControlAction.pause,
        requestedAt: fixture.runtime.now(),
      ),
    );
    await controller.refreshControl();
    await expectLater(
      controller.confirmCanceled(),
      throwsA(isA<LifecycleError>()),
    );

    expect(fixture.backend.countOf('transitionJob'), 0);
    expect(controller.isTerminal, isFalse);
  });

  test(
    '确认操作会刷新 snapshot 并结束 active execution',
    () async {
      // 验证 confirmPaused / confirmCanceled 会 transition Job 到终态并结束 active 执行，后续业务操作以 LeaseLost 终止
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'j1');
      final controller = fixture.controllerFor(claimed);

      fixture.backend.onGetJob = (JobKey key) async => refreshed(
        fixture,
        claimed,
        revision: controller.record.revision,
        controlRequest: JobControlRequest(
          action: JobControlAction.pause,
          requestedAt: fixture.runtime.now(),
        ),
      );
      await controller.refreshControl();

      fixture.backend.onTransitionJob = (TransitionJobRequest request) async =>
          changedWrite(
            refreshed(
              fixture,
              claimed,
              status: JobStatus.paused,
              revision: request.expectedRevision + 1,
            ),
          );

      final record = await controller.confirmPaused();
      expect(record.status, JobStatus.paused);
      expect(controller.record.status, JobStatus.paused);
      expect(controller.isTerminal, isTrue);
      expect(
        fixture.backend.transitionJobRequests.single.targetStatus,
        JobStatus.paused,
      );
      expect(
        fixture.backend.transitionJobRequests.single.credential,
        claimed.credential,
      );

      // 确认之后所有业务写入与再次确认都以 LeaseLost 语义失败。
      final failure = await expectInfrastructureFailure(
        controller.updateProgress(0.5),
      );
      expect(failure.original, isA<LeaseLost>());
      await expectLater(
        controller.confirmPaused(),
        throwsA(isA<ActiveJobInfrastructureFailure>()),
      );
      expect(fixture.backend.countOf('updateJobProgress'), 0);
      expect(fixture.backend.countOf('transitionJob'), 1);
    },
  );

  // --- 直接依赖结果：getDependenciesResult 只返回根节点直接子节点的结果 ---

  test(
    '仅按稳定的 JobKey 顺序返回直接依赖结果',
    () async {
      // 验证 getDependenciesResult 只返回 root 的直接子节点中 completed 的结果，按稳定 JobKey 顺序输出
      final fixture = WorkerFixture();
      final claimed = fixture.claim(queueName: 'reports', jobId: 'root');
      final controller = fixture.controllerFor(claimed);
      final rootKey = claimed.record.key;
      final childB = const JobKey(queueName: 'reports', jobId: 'b');
      final childA = const JobKey(queueName: 'files', jobId: 'a');
      final grandchild = const JobKey(queueName: 'files', jobId: 'g');

      fixture.backend.onGetFlow = (JobKey key) async => FlowSnapshot(
        root: rootKey,
        jobs: <JobRecord>[
          fixture.record(queueName: 'reports', jobId: 'root', revision: 3),
          fixture.record(
            queueName: 'reports',
            jobId: 'b',
            status: JobStatus.completed,
            revision: 4,
            result: 2,
          ),
          fixture.record(
            queueName: 'files',
            jobId: 'a',
            status: JobStatus.completed,
            revision: 5,
            result: 1,
          ),
          fixture.record(
            queueName: 'files',
            jobId: 'g',
            status: JobStatus.completed,
            revision: 6,
            result: 99,
          ),
        ],
        edges: <FlowEdge>[
          FlowEdge(parent: rootKey, child: childB),
          FlowEdge(parent: rootKey, child: childA),
          FlowEdge(parent: childA, child: grandchild),
        ],
      );

      final results = await controller.getDependenciesResult();
      expect(results.keys.toList(), <JobKey>[childA, childB]);
      expect(results[childA], 1);
      expect(results[childB], 2);
      // 只调用一次 getFlow，且没有任何写操作。
      expect(fixture.backend.calls, <String>['getFlow']);
      expect(fixture.backend.getFlowRequests.single, rootKey);
    },
  );

  test(
    '保留值为 null 的直接子级结果，并忽略嵌套的孙级结果',
    () async {
      // 验证 getDependenciesResult 保留 null 结果的子节点，同时忽略孙节点的结果
      final fixture = WorkerFixture();
      final claimed = fixture.claim(queueName: 'reports', jobId: 'root');
      final controller = fixture.controllerFor(claimed);
      final rootKey = claimed.record.key;
      final childA = const JobKey(queueName: 'files', jobId: 'a');
      final childB = const JobKey(queueName: 'reports', jobId: 'b');
      final grandchild = const JobKey(queueName: 'files', jobId: 'g');

      fixture.backend.onGetFlow = (JobKey key) async => FlowSnapshot(
        root: rootKey,
        jobs: <JobRecord>[
          fixture.record(queueName: 'reports', jobId: 'root', revision: 3),
          fixture.record(
            queueName: 'files',
            jobId: 'a',
            status: JobStatus.completed,
            revision: 5,
          ),
          fixture.record(
            queueName: 'reports',
            jobId: 'b',
            status: JobStatus.completed,
            revision: 4,
            result: 'ok',
          ),
          fixture.record(
            queueName: 'files',
            jobId: 'g',
            status: JobStatus.completed,
            revision: 6,
            result: 99,
          ),
        ],
        edges: <FlowEdge>[
          FlowEdge(parent: rootKey, child: childA),
          FlowEdge(parent: rootKey, child: childB),
          FlowEdge(parent: childB, child: grandchild),
        ],
      );

      final results = await controller.getDependenciesResult();
      expect(results.keys.toList(), <JobKey>[childA, childB]);
      expect(results.containsKey(childA), isTrue);
      expect(results[childA], isNull);
      expect(results[childB], 'ok');
      expect(results.containsKey(grandchild), isFalse);
    },
  );

  test('getFlow 返回 null 时会返回不可变空 Map', () async {
    // 验证 Backend 返回 null FlowSnapshot 时，getDependenciesResult 返回不可修改的空 Map
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);

    final results = await controller.getDependenciesResult();
    expect(results, isEmpty);
    expect(fixture.backend.countOf('getFlow'), 1);
    expect(
      () => results[const JobKey(queueName: 'files', jobId: 'a')] = 1,
      throwsUnsupportedError,
    );
  });

  test('LeaseLost 后读取依赖会失败', () async {
    // 验证 LeaseLost 后 getDependenciesResult 以基础设施失败终止，且不触达 Backend
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    await controller.markLeaseLost(
      LeaseLost(controller.key, controller.executionId, 'lease lost'),
      StackTrace.current,
    );

    final failure = await expectInfrastructureFailure(
      controller.getDependenciesResult(),
    );
    expect(failure.original, isA<LeaseLost>());
    expect(fixture.backend.countOf('getFlow'), 0);
  });

  test(
    'controller epoch 变化时会丢弃依赖读取结果',
    () async {
      // 验证 getDependenciesResult 进行中如果 controller epoch 改变（如 leaseLost），会丢弃本次结果并重试一次
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'j1');
      final controller = fixture.controllerFor(claimed);
      final barrier = Completer<void>();
      var flowReads = 0;

      fixture.backend.onGetFlow = (JobKey key) async {
        flowReads++;
        if (flowReads == 1) {
          await barrier.future;
          return FlowSnapshot(
            root: key,
            jobs: <JobRecord>[fixture.record(jobId: 'j1', revision: 1)],
            edges: const <FlowEdge>[],
          );
        }
        return null;
      };
      fixture.backend.onGetJob = (JobKey key) async =>
          refreshed(fixture, claimed, revision: controller.record.revision);

      final pending = controller.getDependenciesResult();
      await fixture.runtime.flushMicrotasks();
      // 读取进行中刷新控制状态，使 controller epoch 改变。
      await controller.refreshControl();
      barrier.complete();

      final results = await pending;
      // 第一次读取结果被丢弃，重试一次后返回第二次的结果。
      expect(flowReads, 2);
      expect(results, isEmpty);
    },
  );
}
