import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/worker/index.dart';

import 'support/recording_backend.dart';
import 'support/worker_fixture.dart';

/// `WorkerOptions.pollInterval` 的默认值。
const Duration poll = Duration(seconds: 1);

/// 构造一个仍属于当前 execution、状态仍为 active 的刷新结果。
///
/// 控制 refresh 只有在 Job 仍 active 且 `executionId` / `fencingToken` 与当前
/// credential 一致时才接受新 snapshot；因此所有 refresh 断言都必须沿用 claim
/// 时写入的 `execution`。
JobRecord refreshedRecord(
  WorkerFixture fixture,
  ClaimedJob claimed, {
  int? revision,
  Object? data,
  Object? progress,
  JobControlRequest? controlRequest,
}) => fixture.record(
  queueName: claimed.record.key.queueName,
  jobId: claimed.record.key.jobId,
  name: claimed.record.name,
  status: JobStatus.active,
  revision: revision,
  attemptsMade: claimed.record.attemptsMade,
  data: data ?? claimed.record.data,
  progress: progress ?? claimed.record.progress,
  result: claimed.record.result,
  execution: claimed.record.execution,
  controlRequest: controlRequest,
);

/// 构造一个 pause/cancel 的 `JobControlRequest`。
JobControlRequest controlRequest(
  WorkerFixture fixture,
  JobControlAction action,
) => JobControlRequest(action: action, requestedAt: fixture.runtime.now());

/// 让 `transitionJob` 原子成功，并在首次调用时完成 [firstCall]。
///
/// 用一个只在首轮完成的 barrier，测试可以精确等待「终态已经提交」而不依赖
/// 固定次数的微任务冲洗。
void installTransitionBarrier(
  WorkerFixture fixture,
  Completer<TransitionJobRequest> firstCall,
) {
  fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
    if (!firstCall.isCompleted) firstCall.complete(request);
    return changedWrite(
      fixture.record(
        queueName: request.key.queueName,
        jobId: request.key.jobId,
        status: request.targetStatus,
        revision: request.expectedRevision + 1,
        finishedAt: fixture.runtime.now(),
      ),
    );
  };
}

void main() {
  // --- 控制发现与非伪造：Worker 如何发现 pause/cancel 请求，且不会在 Processor 未确认时伪造终态 ---

  test(
    'signal 唤醒后会在 claim 前刷新每个 active Job 的 control',
    () async {
      // 验证 Signal 唤醒后 Worker 先逐个刷新 active Job 的控制请求，再领取新 Job
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 3);
      fixture.installSuccessfulTransitions();
      // 第三轮 fill 返回「无 Job」，让 Worker 停在「有 active execution 且槽位未满」
      // 的状态：这样下一次 Signal 必须在同一次 pump 中既 refresh 又 claim。
      var claimCalls = 0;
      fixture.backend.onClaimJob = (ClaimJobRequest request) {
        claimCalls++;
        if (claimCalls == 3) {
          return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
        }
        return Future<BackendWriteResult<ClaimedJob?>>.value(pool.take());
      };
      final release = Completer<void>();
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 3),
        processor: (ActiveJob job) async {
          await release.future;
          return null;
        },
      );

      await worker.start();
      expect(worker.activeCount, 2);
      expect(pool.served, 2);

      final mark = fixture.backend.calls.length;
      fixture.signal.emit('emails');
      await fixture.runtime.flushMicrotasks(turns: 24);

      // 唤醒后同一轮 pump：先逐个刷新 active execution 的控制请求，再领取新 Job。
      final afterWake = fixture.backend.calls.sublist(mark);
      expect(afterWake.where((String call) => call == 'getJob'), hasLength(2));
      expect(
        afterWake.indexOf('getJob'),
        lessThan(afterWake.indexOf('claimJob')),
      );
      expect(pool.served, 3);
      expect(worker.activeCount, 3);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test(
    '所有 signal 都丢失后，定期 poll 仍会发现 control 请求',
    () async {
      // 验证所有 Signal 提示丢失时，周期 poll 仍能发现 pause 请求并更新控制状态
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.single;
      fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, JobControlAction.pause),
      );

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;
      expect(observed.controlState, ActiveJobControlState.running);

      // 不发出任何 Signal，只推进一个 pollInterval。
      fixture.runtime.completeNext(poll);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.pauseRequested);
      expect(fixture.backend.countOf('getJob'), 1);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test(
    '暂停的 Worker 会通过 signal 和 poll 发现 control，但不会 claim Job',
    () async {
      // 验证 paused Worker 通过 Signal 和 poll 刷新 active 控制请求，但绝不领取新 Job
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 2);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.first;
      fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, JobControlAction.cancel),
      );

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;
      expect(pool.served, 1);

      await worker.pause();
      final claimsBefore = fixture.backend.countOf('claimJob');
      final refreshesBefore = fixture.backend.countOf('getJob');

      // Signal 与周期 poll 两条发现路径都只在 paused 下刷新控制，不领取新 Job。
      fixture.signal.emit('emails');
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(observed.controlState, ActiveJobControlState.cancelRequested);

      fixture.runtime.completeNext(poll);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.backend.countOf('claimJob'), claimsBefore);
      expect(fixture.backend.countOf('getJob'), greaterThan(refreshesBefore));
      expect(pool.served, 1);
      expect(worker.activeCount, 1);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('pause 请求只会到达匹配的 ActiveJob', () async {
    // 验证 pause 请求只触达匹配的 ActiveJob，不影响同 Worker 中其他 Job
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 2);
    fixture.installSuccessfulTransitions();
    fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
      fixture,
      pool.byKey[key]!,
      revision: pool.byKey[key]!.record.revision + 1,
      controlRequest: key.jobId == 'job-1'
          ? controlRequest(fixture, JobControlAction.pause)
          : null,
    );

    final jobs = <String, ActiveJob>{};
    final ready = Completer<void>();
    final release = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 2),
      processor: (ActiveJob job) async {
        jobs[job.key.jobId] = job;
        if (jobs.length == 2) ready.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await ready.future;

    fixture.signal.emit('emails');
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(jobs['job-1']!.controlState, ActiveJobControlState.pauseRequested);
    expect(jobs['job-2']!.controlState, ActiveJobControlState.running);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test('cancel 请求只会到达匹配的 ActiveJob', () async {
    // 验证 cancel 请求只触达匹配的 ActiveJob，不影响同 Worker 中其他 Job
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 2);
    fixture.installSuccessfulTransitions();
    fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
      fixture,
      pool.byKey[key]!,
      revision: pool.byKey[key]!.record.revision + 1,
      controlRequest: key.jobId == 'job-2'
          ? controlRequest(fixture, JobControlAction.cancel)
          : null,
    );

    final jobs = <String, ActiveJob>{};
    final ready = Completer<void>();
    final release = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 2),
      processor: (ActiveJob job) async {
        jobs[job.key.jobId] = job;
        if (jobs.length == 2) ready.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await ready.future;

    fixture.signal.emit('emails');
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(jobs['job-1']!.controlState, ActiveJobControlState.running);
    expect(jobs['job-2']!.controlState, ActiveJobControlState.cancelRequested);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test(
    '确认前，cancel 请求会升级先前的 pause 请求',
    () async {
      // 验证 cancel 请求在 Processor 确认前会覆盖已有的 pause 请求
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.single;
      var action = JobControlAction.pause;
      fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, action),
      );

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final events = <ActiveJobControlState>[];
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          observed = job;
          job.controlStream.listen(events.add);
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;

      fixture.signal.emit('emails');
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(observed.controlState, ActiveJobControlState.pauseRequested);

      action = JobControlAction.cancel;
      fixture.signal.emit('emails');
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.cancelRequested);
      expect(events, <ActiveJobControlState>[
        ActiveJobControlState.pauseRequested,
        ActiveJobControlState.cancelRequested,
      ]);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test(
    '重复刷新 control 不会重复发出 stream 事件',
    () async {
      // 验证同一控制状态被反复刷新时，controlStream 只发布一次事件
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.single;
      fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, JobControlAction.pause),
      );

      final entered = Completer<void>();
      final release = Completer<void>();
      final events = <ActiveJobControlState>[];
      late ActiveJob observed;
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          observed = job;
          job.controlStream.listen(events.add);
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;

      // 同一状态被反复刷新：只应发布一次事件。
      fixture.signal.emit('emails');
      fixture.signal.emit('emails');
      await fixture.runtime.flushMicrotasks(turns: 24);
      fixture.runtime.completeNext(poll);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.backend.countOf('getJob'), greaterThanOrEqualTo(3));
      expect(observed.controlState, ActiveJobControlState.pauseRequested);
      expect(events, <ActiveJobControlState>[
        ActiveJobControlState.pauseRequested,
      ]);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('Processor 未确认时，Worker 绝不会确认 pause', () async {
    // 验证 Worker 不会在 Processor 未调用 confirmPaused 时伪造 paused 终态
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    final claimed = pool.jobs.single;
    final refreshed = Completer<void>();
    fixture.backend.onGetJob = (JobKey key) async {
      if (!refreshed.isCompleted) refreshed.complete();
      return refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, JobControlAction.pause),
      );
    };

    final entered = Completer<void>();
    final release = Completer<void>();
    late ActiveJob observed;
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        observed = job;
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;

    fixture.signal.emit('emails');
    await refreshed.future;
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(observed.controlState, ActiveJobControlState.pauseRequested);
    // Processor 仍在运行且没有确认：Worker 不得伪造 paused。
    expect(fixture.backend.countOf('transitionJob'), 0);
    expect(worker.activeCount, 1);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test(
    'Processor 未确认时，Worker 绝不会确认 cancel',
    () async {
      // 验证 Worker 不会在 Processor 未调用 confirmCanceled 时伪造 canceled 终态
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.single;
      final refreshed = Completer<void>();
      fixture.backend.onGetJob = (JobKey key) async {
        if (!refreshed.isCompleted) refreshed.complete();
        return refreshedRecord(
          fixture,
          claimed,
          revision: claimed.record.revision + 1,
          controlRequest: controlRequest(fixture, JobControlAction.cancel),
        );
      };

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;

      fixture.signal.emit('emails');
      await refreshed.future;
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.cancelRequested);
      expect(fixture.backend.countOf('transitionJob'), 0);
      expect(worker.activeCount, 1);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('请求未确认后，Processor 仍可继续运行', () async {
    // 验证 cancel 请求本身不强制终止 Processor：Processor 自行结束后仍可正常提交 completed
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    final claimed = pool.jobs.single;
    final firstTransition = Completer<TransitionJobRequest>();
    installTransitionBarrier(fixture, firstTransition);
    final refreshed = Completer<void>();
    fixture.backend.onGetJob = (JobKey key) async {
      if (!refreshed.isCompleted) refreshed.complete();
      return refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, JobControlAction.cancel),
      );
    };

    final entered = Completer<void>();
    final release = Completer<void>();
    late ActiveJob observed;
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        observed = job;
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;

    fixture.signal.emit('emails');
    await refreshed.future;
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(observed.controlState, ActiveJobControlState.cancelRequested);
    expect(fixture.backend.countOf('transitionJob'), 0);

    // 请求本身不是伪造终态的命令：Processor 自行结束后仍可正常提交 completed。
    release.complete();
    final request = await firstTransition.future;
    expect(request.targetStatus, JobStatus.completed);
    await fixture.runtime.flushMicrotasks(turns: 24);

    await worker.close();
  });

  // --- 确认与 revision 竞争：confirmPaused/confirmCanceled 使用最新刷新的 revision ---

  test(
    'confirmPaused 会使用最新刷新的 revision 提交 paused 状态',
    () async {
      // 验证 confirmPaused 用 refresh 后的最新 revision 提交 JobStatus.paused
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      final claimed = pool.jobs.single;
      final refreshedRevision = claimed.record.revision + 5;
      final firstTransition = Completer<TransitionJobRequest>();
      installTransitionBarrier(fixture, firstTransition);
      final refreshed = Completer<void>();
      fixture.backend.onGetJob = (JobKey key) async {
        if (!refreshed.isCompleted) refreshed.complete();
        return refreshedRecord(
          fixture,
          claimed,
          revision: refreshedRevision,
          controlRequest: controlRequest(fixture, JobControlAction.pause),
        );
      };

      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await refreshed.future;
          return job.confirmPaused();
        },
      );

      await worker.start();
      fixture.signal.emit('emails');
      final request = await firstTransition.future;

      expect(request.targetStatus, JobStatus.paused);
      expect(request.expectedRevision, refreshedRevision);
      expect(request.credential, isNotNull);
      expect(fixture.backend.transitionJobRequests, hasLength(1));

      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test(
    'confirmCanceled 会使用最新刷新的 revision 提交 canceled 状态',
    () async {
      // 验证 confirmCanceled 用 refresh 后的最新 revision 提交 JobStatus.canceled
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      final claimed = pool.jobs.single;
      final refreshedRevision = claimed.record.revision + 5;
      final firstTransition = Completer<TransitionJobRequest>();
      installTransitionBarrier(fixture, firstTransition);
      final refreshed = Completer<void>();
      fixture.backend.onGetJob = (JobKey key) async {
        if (!refreshed.isCompleted) refreshed.complete();
        return refreshedRecord(
          fixture,
          claimed,
          revision: refreshedRevision,
          controlRequest: controlRequest(fixture, JobControlAction.cancel),
        );
      };

      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await refreshed.future;
          return job.confirmCanceled();
        },
      );

      await worker.start();
      fixture.signal.emit('emails');
      final request = await firstTransition.future;

      expect(request.targetStatus, JobStatus.canceled);
      expect(request.expectedRevision, refreshedRevision);
      expect(fixture.backend.transitionJobRequests, hasLength(1));

      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('确认后 Processor 返回时不会提交 completed 状态', () async {
    // 验证 confirmPaused / confirmCanceled 后 Processor 的迟到返回只结束自身，不再提交 completed
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    final claimed = pool.jobs.single;
    final firstTransition = Completer<TransitionJobRequest>();
    installTransitionBarrier(fixture, firstTransition);
    final refreshed = Completer<void>();
    fixture.backend.onGetJob = (JobKey key) async {
      if (!refreshed.isCompleted) refreshed.complete();
      return refreshedRecord(
        fixture,
        claimed,
        revision: claimed.record.revision + 1,
        controlRequest: controlRequest(fixture, JobControlAction.pause),
      );
    };

    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        await refreshed.future;
        await job.confirmPaused();
        return null;
      },
    );

    await worker.start();
    fixture.signal.emit('emails');
    final request = await firstTransition.future;
    expect(request.targetStatus, JobStatus.paused);

    // 确认之后 Processor 的迟到返回只结束自身 Future，不得产生 completed。
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(fixture.backend.transitionJobRequests, hasLength(1));
    expect(worker.activeCount, 0);

    await worker.close();
  });

  test(
    'control revision 与 updateProgress 竞争时会刷新并重试一次',
    () async {
      // 验证控制请求推进 revision 导致 updateProgress 冲突时，会 refresh 并仅重试一次
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.single;
      final baseRevision = claimed.record.revision;
      final refreshedRevision = baseRevision + 1;
      // 控制请求把 record 推进到 N+1，因此第一次写入必然撞上旧 revision。
      fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
        fixture,
        claimed,
        revision: refreshedRevision,
        controlRequest: controlRequest(fixture, JobControlAction.cancel),
      );
      fixture.backend.onUpdateJobProgress =
          (UpdateJobProgressRequest request) async {
            if (request.expectedRevision != refreshedRevision) {
              throw RevisionConflict(
                request.key,
                request.expectedRevision,
                refreshedRevision,
                'stale revision',
              );
            }
            return changedWrite(
              refreshedRecord(
                fixture,
                claimed,
                revision: refreshedRevision + 1,
                progress: request.progress,
              ),
            );
          };

      final processorDone = Completer<void>();
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await job.updateProgress(0.5);
          processorDone.complete();
          return null;
        },
      );

      await worker.start();
      await processorDone.future;

      final requests = fixture.backend.updateJobProgressRequests;
      expect(requests, hasLength(2));
      expect(requests[0].expectedRevision, baseRevision);
      // 受控 refresh 之后必须用刷新到的新 revision 重试，且只重试一次。
      expect(requests[1].expectedRevision, refreshedRevision);
      expect(fixture.backend.countOf('getJob'), 1);

      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test(
    'control revision 与完成操作竞争时会刷新并仅提交一次',
    () async {
      // 验证控制请求推进 revision 导致 Processor 完成时 transitionJob 冲突，会 refresh 后重试一次提交 completed
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      final claimed = pool.jobs.single;
      final baseRevision = claimed.record.revision;
      final refreshedRevision = baseRevision + 1;
      var transitionCalls = 0;
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        transitionCalls++;
        if (request.expectedRevision != refreshedRevision) {
          throw RevisionConflict(
            request.key,
            request.expectedRevision,
            refreshedRevision,
            'stale revision',
          );
        }
        return changedWrite(
          fixture.record(
            queueName: request.key.queueName,
            jobId: request.key.jobId,
            status: request.targetStatus,
            revision: request.expectedRevision + 1,
            finishedAt: fixture.runtime.now(),
          ),
        );
      };
      // 控制请求把 record 推到 N+1：refresh 后同一 execution 仍然 active。
      fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
        fixture,
        claimed,
        revision: refreshedRevision,
        controlRequest: controlRequest(fixture, JobControlAction.pause),
      );

      final worker = fixture.worker();
      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      final requests = fixture.backend.transitionJobRequests;
      expect(requests, hasLength(2));
      expect(requests[0].expectedRevision, baseRevision);
      expect(requests[1].expectedRevision, refreshedRevision);
      expect(requests.last.targetStatus, JobStatus.completed);
      expect(transitionCalls, 2);
      expect(worker.activeCount, 0);

      await worker.close();
    },
  );

  test(
    'control 刷新期间 credential 被替换时会发布 LeaseLost',
    () async {
      // 验证控制刷新时发现 credential 已被替换，立即判定 LeaseLost，后续 Processor 返回不得提交终态
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final claimed = pool.jobs.single;
      final refreshed = Completer<void>();
      // refresh 返回的执行身份不属于本次 claim：Worker 必须立即判定 lease lost。
      fixture.backend.onGetJob = (JobKey key) async {
        if (!refreshed.isCompleted) refreshed.complete();
        return fixture.record(
          queueName: key.queueName,
          jobId: key.jobId,
          name: claimed.record.name,
          status: JobStatus.active,
          revision: claimed.record.revision + 1,
          attemptsMade: claimed.record.attemptsMade,
          execution: ExecutionOwner(
            workerId: fixture.workerId,
            credential: const ExecutionCredential(executionId: 'other-exec'),
            startedAt: fixture.runtime.now(),
          ),
        );
      };

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;

      fixture.signal.emit('emails');
      await refreshed.future;
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.leaseLost);
      expect(fixture.logSink.forOperation('getJob'), isNotEmpty);

      // 失去所有权后 Processor 的迟到返回不得提交任何终态。
      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('transitionJob'), 0);
      expect(worker.activeCount, 0);

      await worker.close();
    },
  );

  test('getJob 遇到 BackendUnavailable 时会记录基础设施错误，并在下一轮 poll 重试', () async {
    // 验证 getJob 临时失败只记录基础设施 warning，holder 与槽位保留，下一次 poll 继续刷新
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    final claimed = pool.jobs.single;
    var getJobCalls = 0;
    fixture.backend.onGetJob = (JobKey key) {
      getJobCalls++;
      if (getJobCalls == 1) {
        return Future<JobRecord?>.error(
          BackendUnavailable(null, 'backend is down'),
        );
      }
      return Future<JobRecord?>.value(
        refreshedRecord(
          fixture,
          claimed,
          revision: claimed.record.revision + 1,
          controlRequest: controlRequest(fixture, JobControlAction.pause),
        ),
      );
    };

    final entered = Completer<void>();
    final release = Completer<void>();
    late ActiveJob observed;
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        observed = job;
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;

    // 第一次 poll：refresh 失败只记录基础设施 warning，holder 与槽位保留。
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(getJobCalls, 1);
    expect(fixture.logSink.forOperation('getJob'), isNotEmpty);
    expect(observed.controlState, ActiveJobControlState.running);
    expect(worker.activeCount, 1);
    expect(fixture.backend.countOf('claimJob'), 1);

    // 第二次 poll：同一 holder 重新 refresh 并发现控制请求。
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(getJobCalls, 2);
    expect(observed.controlState, ActiveJobControlState.pauseRequested);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test('refresh 与变更竞争时会由 ActiveJob controller 串行处理', () async {
    // 验证业务 mutation 与 control refresh 同时排入时，ActiveJob 内部串行 tail 保证 refresh 不插队
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'j1');
    final controller = fixture.controllerFor(claimed);
    final mutationBarrier = Completer<void>();
    final baseRevision = claimed.record.revision;

    fixture.backend.onUpdateJobProgress =
        (UpdateJobProgressRequest request) async {
          await mutationBarrier.future;
          return changedWrite(
            refreshedRecord(
              fixture,
              claimed,
              revision: request.expectedRevision + 1,
              progress: request.progress,
            ),
          );
        };
    fixture.backend.onGetJob = (JobKey key) async => refreshedRecord(
      fixture,
      claimed,
      revision: baseRevision + 1,
      controlRequest: controlRequest(fixture, JobControlAction.pause),
    );

    // 同时排入业务 mutation 与 control refresh：唯一的串行 tail 决定执行顺序。
    final mutation = controller.updateProgress(0.5);
    final refresh = controller.refreshControl();
    await fixture.runtime.flushMicrotasks();

    // 第一个操作仍在 Backend 中，refresh 不得插队触碰 Backend。
    expect(fixture.backend.countOf('updateJobProgress'), 1);
    expect(fixture.backend.countOf('getJob'), 0);

    mutationBarrier.complete();
    await mutation;
    await refresh;

    expect(fixture.backend.countOf('getJob'), 1);
    expect(
      fixture.backend.calls.indexOf('updateJobProgress'),
      lessThan(fixture.backend.calls.indexOf('getJob')),
    );
    expect(controller.controlState, ActiveJobControlState.pauseRequested);
  });
}
