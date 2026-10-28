import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';
import 'package:tf_bull_mq/src/worker/index.dart';

import 'support/manual_runtime.dart';
import 'support/recording_backend.dart';
import 'support/recording_signal.dart';
import 'support/worker_fixture.dart';

/// `WorkerOptions.pollInterval` 的默认值。
const Duration poll = Duration(seconds: 1);

/// `WorkerOptions.gracefulTimeout` 的默认值。
const Duration gracefulTimeout = Duration(seconds: 30);

/// 测试使用的租约时长。
const Duration leaseDuration = Duration(seconds: 10);

/// 测试使用的续租间隔。
const Duration renewInterval = Duration(seconds: 3);

/// 一个声明 distributed Profile 的能力集合。
BackendCapabilities distributedCapabilities() => BackendCapabilities(
  profile: BackendProfile.distributed,
  durability: Durability.durable,
  delayedJobs: true,
  priorities: true,
  atomicFlows: true,
  eventDelivery: EventDelivery.replayable,
);

/// 当前仍处于 pending 的指定时长的延迟数量。
int pendingDelayCount(WorkerFixture fixture, Duration duration) => fixture
    .runtime
    .pendingDelays
    .where((ManualDelayEntry entry) => entry.duration == duration)
    .length;

/// 安装 recovery handler：记录请求，并可按需阻塞或声明通知队列。
void installRecovery(
  WorkerFixture fixture, {
  Set<String> queuesToNotify = const <String>{},
  Completer<void>? barrier,
  void Function(RecoverStalledJobsRequest request)? onInvoke,
}) {
  fixture.backend.onRecoverStalledJobs =
      (RecoverStalledJobsRequest request) async {
        onInvoke?.call(request);
        if (barrier != null) await barrier.future;
        return changedWrite(<JobRecord>[], queuesToNotify: queuesToNotify);
      };
}

void main() {
  // --- pause / resume 与 graceful close：Worker 的暂停、恢复和优雅关闭行为 ---

  test('pause 只会停止当前 Worker claim 新 Job', () async {
    // 验证 pause 只阻止当前 Worker 领取新 Job，不影响同 Backend 下其他 Worker
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 4);
    fixture.installSuccessfulTransitions();
    final signalA = RecordingSignal();
    final signalB = RecordingSignal();
    // 让两个 Worker 共享同一个 Backend，但各自持有独立的 Signal。
    final release = Completer<void>();
    final workerA = fixture.worker(
      signal: signalA,
      options: const WorkerOptions(workerId: 'worker-a', concurrency: 2),
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );
    var bClaims = 0;
    // worker-b 的领取预算：启动阶段只允许 1 个，从而留出一个空槽；
    // 暂停 worker-a 之后再放开 1 个，用于观察未暂停的 Worker 仍会领取。
    var bClaimBudget = 1;
    fixture.backend.onClaimJob = (ClaimJobRequest request) {
      if (request.workerId == 'worker-b') {
        if (bClaimBudget <= 0) {
          return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
        }
        bClaimBudget--;
        bClaims++;
      }
      return Future<BackendWriteResult<ClaimedJob?>>.value(pool.take());
    };
    final workerB = fixture.worker(
      signal: signalB,
      options: const WorkerOptions(workerId: 'worker-b', concurrency: 2),
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    await workerA.start();
    await workerB.start();
    expect(pool.served, 3);

    final claimsBefore = fixture.backend.claimJobRequests
        .where((ClaimJobRequest request) => request.workerId == 'worker-a')
        .length;
    await workerA.pause();

    // 放开 worker-b 的第二个领取预算，然后同时唤醒两个 Worker。
    bClaimBudget = 1;
    signalA.emit('emails');
    signalB.emit('emails');
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(
      fixture.backend.claimJobRequests
          .where((ClaimJobRequest request) => request.workerId == 'worker-a')
          .length,
      claimsBefore,
    );
    expect(bClaims, 2);
    expect(pool.served, 4);
    expect(workerA.state, WorkerState.paused);
    expect(workerB.state, WorkerState.running);
    // 本地暂停绝不持久化 Queue 状态。
    expect(fixture.backend.setQueuePausedRequests, isEmpty);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await workerA.close();
    await workerB.close();
  });

  test(
    'pause 不会调用 setQueuePaused，也不会停止 active Processor',
    () async {
      // 说明 Worker.pause 是 Worker 本地行为：不调用 Backend.setQueuePaused，也不中断正在运行的 Processor
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 2);
      fixture.installSuccessfulTransitions();
      var processorFinished = false;
      final release = Completer<void>();
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await release.future;
          processorFinished = true;
          return null;
        },
      );

      await worker.start();
      final transitionsBefore = fixture.backend.countOf('transitionJob');
      await worker.pause();
      await fixture.runtime.flushMicrotasks(turns: 24);

      // 暂停不终止已经在运行的 Processor，也不提交任何终态。
      expect(processorFinished, isFalse);
      expect(fixture.backend.countOf('transitionJob'), transitionsBefore);
      expect(fixture.backend.setQueuePausedRequests, isEmpty);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(processorFinished, isTrue);
      expect(fixture.backend.countOf('transitionJob'), 1);

      await worker.close();
    },
  );

  test('resume 会立即启动处理，不会等待 pollInterval', () async {
    // 说明 resume 立即触发一次 claim pump，不等待下一个 pollInterval
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 2);
    fixture.installSuccessfulTransitions();
    final release = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    await worker.start();
    await worker.pause();
    // 释放 Processor 让槽位空出来；paused 状态只 refresh，不领取新 Job。
    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(fixture.backend.countOf('claimJob'), 1);

    final claimsBefore = fixture.backend.countOf('claimJob');
    await worker.resume();

    // 不推进任何 poll 延迟，resume 自身就要触发一次 pump。
    expect(fixture.backend.countOf('claimJob'), claimsBefore + 1);
    expect(fixture.runtime.completedCount(poll), 0);

    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test('pause 和 resume 在各自有效状态下均可重复调用', () async {
    // 说明 pause 和 resume 在有效状态下都是幂等操作：重复调用不产生额外副作用
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    final release = Completer<void>();
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    // created 状态下两个操作都非法。
    await expectLater(worker.pause(), throwsA(isA<LifecycleError>()));
    await expectLater(worker.resume(), throwsA(isA<LifecycleError>()));

    await worker.start();
    await worker.pause();
    await worker.pause();
    expect(worker.state, WorkerState.paused);
    await worker.resume();
    await worker.resume();
    expect(worker.state, WorkerState.running);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();

    // closed 之后同样非法。
    await expectLater(worker.pause(), throwsA(isA<LifecycleError>()));
    await expectLater(worker.resume(), throwsA(isA<LifecycleError>()));
  });

  test(
    'start 前调用 close 默认会关闭 Worker，但不会触碰注入的资源',
    () async {
      // 说明未 start 的 Worker 调用 close 后状态为 closed，默认不关闭注入的 Backend 和 Signal
      final fixture = WorkerFixture();
      final worker = fixture.worker();

      await worker.close();

      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
      expect(fixture.backend.countOf('close'), 0);
      expect(fixture.signal.closeCount, 0);
    },
  );

  test('graceful close 会停止 claim 并等待 active Processor 完成', () async {
    // 说明 graceful close 停止 claim 并等待所有 active Processor 完成后再真正关闭
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 2);
    fixture.installSuccessfulTransitions();
    final release = Completer<void>();
    final worker = fixture.worker(
      options: const WorkerOptions(concurrency: 1),
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    await worker.start();
    expect(pool.served, 1);

    final closing = worker.close();
    await fixture.runtime.flushMicrotasks(turns: 24);

    // 停止领取但仍在等待 Processor。
    expect(worker.state, WorkerState.closing);
    expect(pool.served, 1);
    expect(worker.activeCount, 1);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await closing;

    expect(worker.state, WorkerState.closed);
    expect(worker.activeCount, 0);
    expect(pool.served, 1);
  });

  test(
    'distributed graceful close 会持续续约，直到 Processor 完成',
    () async {
      // 说明 distributed Worker 在 graceful close 期间继续续租直到 Processor 完成
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = RecordingSignal(scope: SignalScope.distributed);
      final claimed = fixture.claim(
        jobId: 'job-1',
        leaseDuration: leaseDuration,
      );
      var served = false;
      fixture.backend.onClaimJob = (ClaimJobRequest request) {
        if (served) {
          return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
        }
        served = true;
        return Future<BackendWriteResult<ClaimedJob?>>.value(
          claimedWrite(claimed),
        );
      };
      fixture.backend.onGetJob = (JobKey key) async => claimed.record;
      var renewCalls = 0;
      fixture.backend.onRenewLease = (RenewLeaseRequest request) async {
        renewCalls++;
        return changedWrite(
          ExecutionCredential(
            executionId: request.credential.executionId,
            leaseExpiresAt: fixture.runtime.now().add(leaseDuration),
          ),
        );
      };
      fixture.installSuccessfulTransitions();

      final release = Completer<void>();
      final worker = fixture.worker(
        signal: signal,
        options: const WorkerOptions(
          leaseDuration: leaseDuration,
          renewInterval: renewInterval,
        ),
        processor: (ActiveJob job) async {
          await release.future;
          return null;
        },
      );

      await worker.start();
      expect(renewCalls, 0);

      final closing = worker.close();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(worker.state, WorkerState.closing);

      // 宽限期内仍在续租。
      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(renewCalls, 1);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await closing;

      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
    },
  );

  test(
    '没有 active Job 时，graceful close 不会安排 gracefulTimeout',
    () async {
      // 说明 graceful close 无 active Job 时不登记 gracefulTimeout delay，立即关闭
      final fixture = WorkerFixture();
      final worker = fixture.worker();

      await worker.start();
      await worker.close();

      expect(worker.state, WorkerState.closed);
      expect(pendingDelayCount(fixture, gracefulTimeout), 0);
      expect(fixture.runtime.completedCount(gracefulTimeout), 0);
    },
  );

  test('graceful timeout 的 Delay 失败时只会升级为 force 一次', () async {
    // 说明 gracefulTimeout delay 失败只记一次日志并升级为 forceClose，不会重入
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    installRecovery(fixture);
    final release = Completer<void>();
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    await worker.start();
    final closing = worker.close();
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(pendingDelayCount(fixture, gracefulTimeout), 1);

    fixture.runtime.failNext(gracefulTimeout);
    await fixture.runtime.flushMicrotasks(turns: 24);

    // 宽限 Delay 失败只记录一次并直接升级 force，不重新登记新的 timeout。
    expect(fixture.logSink.forOperation('close'), hasLength(1));
    expect(pendingDelayCount(fixture, gracefulTimeout), 0);
    expect(fixture.runtime.completedCount(gracefulTimeout), 0);
    expect(fixture.backend.countOf('recoverStalledJobs'), 1);

    await closing;
    expect(worker.state, WorkerState.closed);
    expect(worker.activeCount, 0);
  });

  test(
    'close 可重复调用，并向并发调用方返回相同结果',
    () async {
      // 说明 close 是幂等的：并发调用者收到同一个 Future，重复 close 返回相同结果
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final release = Completer<void>();
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await release.future;
          return null;
        },
      );

      await worker.start();
      final first = worker.close();
      final second = worker.close();
      final third = worker.forceClose();

      expect(identical(first, second), isTrue);
      expect(identical(first, third), isTrue);

      release.complete();
      await first;
      await second;
      await third;

      expect(worker.state, WorkerState.closed);
      expect(identical(worker.close(), first), isTrue);
    },
  );

  // --- timeout / force、所有权与关闭竞态：forceClose 行为、所有权关闭、Recovery 通知 ---

  test(
    'local graceful timeout 会升级为一次针对该 Worker 的恢复',
    () async {
      // 说明 graceful close 超时后升级为 force close，local Profile 调用一次 recoverWorkers 包含所有 active executionId
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      installRecovery(fixture);
      final release = Completer<void>();
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await release.future;
          return null;
        },
      );

      await worker.start();
      final closing = worker.close();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(pendingDelayCount(fixture, gracefulTimeout), 1);

      fixture.runtime.completeNext(gracefulTimeout);
      await fixture.runtime.flushMicrotasks(turns: 24);

      final request = fixture.backend.recoverStalledJobsRequests.single;
      expect(request.queueName, 'emails');
      expect(request.workerId, fixture.workerId);
      expect(request.executionIds, isNotNull);
      expect(request.executionIds, hasLength(1));

      await closing;
      expect(worker.state, WorkerState.closed);
    },
  );

  test(
    'forceClose 会中断 graceful 等待，且不会等待 Processor',
    () async {
      // 说明 forceClose 立即中断 graceful 等待，不等 Processor 完成
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      installRecovery(fixture);
      var processorFinished = false;
      final release = Completer<void>();
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await release.future;
          processorFinished = true;
          return null;
        },
      );

      await worker.start();
      final closing = worker.close();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(worker.state, WorkerState.closing);

      await worker.forceClose();
      await closing;

      // 不等待 Processor 结束即可关闭。
      expect(processorFinished, isFalse);
      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
      // Processor 仍在运行：迟到结果不得提交。
      expect(fixture.backend.countOf('transitionJob'), 0);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('transitionJob'), 0);
    },
  );

  test(
    'forceClose 会阻止新的 pump，并等待正在进行的 claim 完成登记',
    () async {
      // 说明 forceClose 阻塞新的 claim pump，但会等待已 in-flight 的 claim 完成注册
      final fixture = WorkerFixture();
      final claimed = fixture.claim(jobId: 'job-1');
      final claimBarrier = Completer<void>();
      fixture.backend.onClaimJob = (ClaimJobRequest request) =>
          claimBarrier.future.then((void _) => claimedWrite(claimed));
      installRecovery(fixture);

      final worker = fixture.worker();
      final starting = worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('claimJob'), 1);

      fixture.signal.emit('emails');
      final closing = worker.forceClose();
      await fixture.runtime.flushMicrotasks(turns: 24);

      claimBarrier.complete();
      await starting;
      await closing;

      // in-flight claim 的返回先被登记，再由精确 recovery 覆盖。
      expect(fixture.backend.countOf('claimJob'), 1);
      expect(
        fixture.backend.recoverStalledJobsRequests.single.executionIds,
        <String>{claimed.credential.executionId},
      );
      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
    },
  );

  test(
    'forceClose 会在恢复前向每个 ActiveJob 发布 LeaseLost',
    () async {
      // 说明 forceClose 在调用 recoverStalledJobs 之前，先向每个 ActiveJob 发布 LeaseLost
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 2);
      fixture.installSuccessfulTransitions();
      final jobs = <ActiveJob>[];
      final ready = Completer<void>();
      final release = Completer<void>();
      var leaseLostBeforeRecovery = false;
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 2),
        processor: (ActiveJob job) async {
          jobs.add(job);
          if (jobs.length == 2) ready.complete();
          await release.future;
          return null;
        },
      );
      installRecovery(
        fixture,
        onInvoke: (RecoverStalledJobsRequest request) {
          leaseLostBeforeRecovery = jobs.every(
            (ActiveJob job) =>
                job.controlState == ActiveJobControlState.leaseLost,
          );
        },
      );

      await worker.start();
      await ready.future;

      await worker.forceClose();

      expect(jobs, hasLength(2));
      expect(
        jobs.every(
          (ActiveJob job) =>
              job.controlState == ActiveJobControlState.leaseLost,
        ),
        isTrue,
      );
      // recovery 被调用时，每个 ActiveJob 都已经观察到 leaseLost。
      expect(leaseLostBeforeRecovery, isTrue);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
    },
  );

  test(
    'local forceClose 只恢复当前 holder 的 executionIds',
    () async {
      // 说明 local forceClose 只 recover 当前 holder 的 executionId，不包含已释放的
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 2);
      fixture.installSuccessfulTransitions();
      installRecovery(fixture);
      final release = Completer<void>();
      final ready = Completer<void>();
      final started = <JobKey>[];
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 2),
        processor: (ActiveJob job) async {
          started.add(job.key);
          if (started.length == 2) ready.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await ready.future;

      await worker.forceClose();

      final expected = pool.jobs
          .map((ClaimedJob job) => job.credential.executionId)
          .toSet();
      expect(expected, hasLength(2));
      expect(
        fixture.backend.recoverStalledJobsRequests.single.executionIds,
        expected,
      );

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
    },
  );

  test(
    'local forceClose 绝不会恢复已释放或无关的 executionId',
    () async {
      // 说明 local forceClose 不会 recover 已释放或其他 Worker 的 executionId
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 2);
      fixture.installSuccessfulTransitions();
      installRecovery(fixture);
      final secondStarted = Completer<void>();
      final release = Completer<void>();
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 1),
        processor: (ActiveJob job) async {
          if (job.key == pool.jobs.last.record.key) {
            secondStarted.complete();
            await release.future;
          }
          return null;
        },
      );

      await worker.start();
      // 第一个 Job 立即完成并释放槽位，Worker 接着领取第二个 Job。
      await secondStarted.future;
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(pool.served, 2);
      expect(worker.activeCount, 1);

      await worker.forceClose();

      final recovered =
          fixture.backend.recoverStalledJobsRequests.single.executionIds!;
      // 已释放的 execution 不得进入 recovery 集合。
      expect(recovered, <String>{pool.jobs.last.credential.executionId});
      expect(
        recovered,
        isNot(contains(pool.jobs.first.credential.executionId)),
      );

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
    },
  );

  test(
    'local forceClose 没有 holder 时绝不会调用 recoverStalledJobs',
    () async {
      // 说明 local forceClose 无 holder 时不调用 recoverStalledJobs
      final fixture = WorkerFixture();
      installRecovery(fixture);
      final worker = fixture.worker();

      await worker.start();
      await worker.forceClose();

      expect(fixture.backend.countOf('recoverStalledJobs'), 0);
      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
    },
  );

  test(
    'distributed forceClose 会停止续约，且绝不会调用 recoverStalledJobs',
    () async {
      // 说明 distributed forceClose 停止续租，不调用 recoverStalledJobs
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = RecordingSignal(scope: SignalScope.distributed);
      final claimed = fixture.claim(
        jobId: 'job-1',
        leaseDuration: leaseDuration,
      );
      var served = false;
      fixture.backend.onClaimJob = (ClaimJobRequest request) {
        if (served) {
          return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
        }
        served = true;
        return Future<BackendWriteResult<ClaimedJob?>>.value(
          claimedWrite(claimed),
        );
      };
      fixture.backend.onGetJob = (JobKey key) async => claimed.record;
      fixture.backend.onRenewLease = (RenewLeaseRequest request) async =>
          changedWrite(
            ExecutionCredential(
              executionId: request.credential.executionId,
              leaseExpiresAt: fixture.runtime.now().add(leaseDuration),
            ),
          );
      fixture.installSuccessfulTransitions();
      installRecovery(fixture);

      final release = Completer<void>();
      final worker = fixture.worker(
        signal: signal,
        options: const WorkerOptions(
          leaseDuration: leaseDuration,
          renewInterval: renewInterval,
        ),
        processor: (ActiveJob job) async {
          await release.future;
          return null;
        },
      );

      await worker.start();
      final renewBefore = fixture.backend.countOf('renewLease');

      await worker.forceClose();
      expect(worker.state, WorkerState.closed);
      // distributed 从不主动恢复：只停止续租并等待 lease 到期后由 Backend 接管。
      expect(fixture.backend.countOf('recoverStalledJobs'), 0);

      // 残留的续租等待即使被人为完成，也不得再发起续租。
      if (pendingDelayCount(fixture, renewInterval) > 0) {
        fixture.runtime.completeNext(renewInterval);
      }
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('renewLease'), renewBefore);
      expect(fixture.backend.countOf('recoverStalledJobs'), 0);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
    },
  );

  test('强制恢复只会通知 queuesToNotify', () async {
    // 说明 force close 的 recovery 只通知 recoverWorkers 返回结果中的 queuesToNotify
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    installRecovery(
      fixture,
      queuesToNotify: const <String>{'emails', 'reports'},
    );
    final release = Completer<void>();
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    await worker.start();
    expect(fixture.signal.notifyCalls, isEmpty);

    await worker.forceClose();

    expect(fixture.signal.notifyCalls, <String>['emails', 'reports']);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
  });

  test('恢复后迟到的 Processor 成功不会重复提交', () async {
    // 说明 force recovery 之后 Processor 的迟到成功不会产生第二次 transition
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    final recoveryBarrier = Completer<void>();
    installRecovery(fixture, barrier: recoveryBarrier);
    final release = Completer<void>();
    final worker = fixture.worker(
      processor: (ActiveJob job) async {
        await release.future;
        return null;
      },
    );

    await worker.start();
    final closing = worker.forceClose();
    await fixture.runtime.flushMicrotasks(turns: 24);
    // recovery handler 已经进入并阻塞。
    expect(fixture.backend.countOf('recoverStalledJobs'), 1);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    recoveryBarrier.complete();
    await closing;

    // 迟到成功只结束自身 Future：不得产生第二次提交。
    expect(fixture.backend.countOf('transitionJob'), 0);
    expect(fixture.backend.countOf('recoverStalledJobs'), 1);
    expect(worker.state, WorkerState.closed);
  });

  test(
    '恢复后迟到的 Processor 错误不会创建失败状态转移',
    () async {
      // 说明 force recovery 之后 Processor 的迟到失败不会产生 failure transition
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      installRecovery(fixture);
      final release = Completer<void>();
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await release.future;
          throw StateError('processor failed late');
        },
      );

      await worker.start();
      await worker.forceClose();
      expect(worker.state, WorkerState.closed);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.backend.countOf('transitionJob'), 0);
    },
  );

  test(
    '强制恢复前已完成的任务不会再次恢复',
    () async {
      // 说明 Processor 完成先于 force recovery 时，同一个 execution 不会被重复 recover
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      final transitionBarrier = Completer<void>();
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        await transitionBarrier.future;
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
      installRecovery(fixture);
      final worker = fixture.worker();

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('transitionJob'), 1);

      final closing = worker.forceClose();
      await fixture.runtime.flushMicrotasks(turns: 24);
      transitionBarrier.complete();
      await closing;

      // completed 只提交一次；已经终态的 holder 不再进入 recovery 集合。
      expect(fixture.backend.countOf('transitionJob'), 1);
      expect(fixture.backend.countOf('recoverStalledJobs'), 0);
      expect(worker.state, WorkerState.closed);
    },
  );

  // --- 资源所有权：ownsBackend / ownsSignal 控制 close 时是否关闭注入资源 ---

  test('默认情况下，close 不会关闭 Backend 和 Signal', () async {
    // 说明默认 close 不关闭注入的 Backend 和 Signal
    final fixture = WorkerFixture();
    final worker = fixture.worker();

    await worker.start();
    await worker.close();

    expect(worker.state, WorkerState.closed);
    expect(fixture.backend.countOf('close'), 0);
    expect(fixture.signal.closeCount, 0);
  });

  test('关闭自有资源时会先关闭 Signal，再关闭 Backend', () async {
    // 说明 ownsBackend 和 ownsSignal 都为 true 时，close 先关闭 Signal 再关闭 Backend
    final fixture = WorkerFixture();
    final order = <String>[];
    fixture.signal.onClose = () async => order.add('signal');
    fixture.backend.onClose = () async => order.add('backend');
    final worker = fixture.worker(ownsSignal: true, ownsBackend: true);

    await worker.start();
    await worker.close();

    expect(order, <String>['signal', 'backend']);
    expect(fixture.signal.closeCount, 1);
    expect(fixture.backend.countOf('close'), 1);
  });

  test('ownsBackend 和 ownsSignal 相互独立', () async {
    // 说明 ownsBackend 和 ownsSignal 可以独立设置：只关闭其中一个而不影响另一个
    final signalOnly = WorkerFixture();
    signalOnly.signal.onClose = () async {};
    signalOnly.backend.onClose = () async {};
    final signalOnlyWorker = signalOnly.worker(ownsSignal: true);

    await signalOnlyWorker.start();
    await signalOnlyWorker.close();

    expect(signalOnly.signal.closeCount, 1);
    expect(signalOnly.backend.countOf('close'), 0);

    final backendOnly = WorkerFixture();
    final backendOnlyWorker = backendOnly.worker(ownsBackend: true);

    await backendOnlyWorker.start();
    await backendOnlyWorker.close();

    expect(backendOnly.signal.closeCount, 0);
    expect(backendOnly.backend.countOf('close'), 1);
  });

  test(
    '资源关闭失败会记入日志，且仍会尝试关闭两个资源',
    () async {
      // 说明 Signal 或 Backend close 失败时，Worker 记录日志后仍尝试关闭另一个资源
      final fixture = WorkerFixture();
      fixture.signal.closeError = StateError('signal close failed');
      fixture.backend.onClose = () async =>
          throw StateError('backend close failed');
      final worker = fixture.worker(ownsSignal: true, ownsBackend: true);

      await worker.start();
      await worker.close();

      // 第一项失败不跳过第二项，且错误只写运行日志。
      expect(fixture.signal.closeCount, 1);
      expect(fixture.backend.countOf('close'), 1);
      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
      expect(fixture.logSink.forOperation('close'), hasLength(2));
    },
  );

  test(
    '未提供 delay 时会回退到默认计时器，轮询仍会继续',
    () async {
      // 说明 delay 是可选参数：省略时 Worker 使用内部默认等待实现（真实计时器），
      // 周期轮询兜底仍然生效。用极短的 pollInterval 避免真实等待 1 秒。
      final fixture = WorkerFixture();
      var claims = 0;
      fixture.backend.onClaimJob = (ClaimJobRequest request) {
        claims++;
        return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
      };
      final worker = fixture.worker(
        omitDelay: true,
        options: const WorkerOptions(
          workerId: 'worker-default-delay',
          pollInterval: Duration(milliseconds: 10),
          gracefulTimeout: Duration(milliseconds: 50),
        ),
      );

      await worker.start();
      // 首次 pump 立即 claim，不经过计时路径。
      expect(claims, 1);
      // 留出远大于一个轮询周期的裕度，默认计时器必须完成并触发新的 claim。
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(claims, greaterThan(1));

      // 没有 active execution 时不会创建宽限期等待，close 立即完成。
      await worker.close();
      expect(worker.state, WorkerState.closed);
      expect(worker.activeCount, 0);
    },
  );
}
