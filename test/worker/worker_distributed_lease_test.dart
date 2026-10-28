import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';
import 'package:tf_bull_mq/src/worker/active_job.dart';
import 'package:tf_bull_mq/src/worker/index.dart';

import 'support/manual_runtime.dart';
import 'support/recording_backend.dart';
import 'support/recording_signal.dart';
import 'support/worker_fixture.dart';

/// `WorkerOptions.pollInterval` 的默认值。
const Duration poll = Duration(seconds: 1);

/// 测试使用的租约时长。claim 的 `leaseExpiresAt` 固定为 `now() + leaseDuration`。
const Duration leaseDuration = Duration(seconds: 10);

/// 测试使用的续租间隔，必须严格小于 [leaseDuration]。
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

/// distributed Profile 要求的 Signal 作用域替身。
RecordingSignal distributedSignal() =>
    RecordingSignal(scope: SignalScope.distributed);

/// 构造一个带完整租约配置的 distributed Worker。
Worker distributedWorker(
  WorkerFixture fixture, {
  required RecordingSignal signal,
  required Processor processor,
}) => fixture.worker(
  signal: signal,
  options: const WorkerOptions(
    leaseDuration: leaseDuration,
    renewInterval: renewInterval,
  ),
  processor: processor,
);

/// 记录一次 distributed 执行的最新 credential，供 claim / 续租 / getJob 保持事实一致。
///
/// 续租只替换 credential，不修改 `JobRecord`；因此测试必须显式维护「当前
/// credential」，否则 refresh 会因 fencing token 不匹配而误判为失去所有权。
final class DistributedLease {
  DistributedLease(this._fixture, this._claimed)
    : _credential = _claimed.credential;

  final WorkerFixture _fixture;
  final ClaimedJob _claimed;
  ExecutionCredential _credential;

  ClaimedJob get claimed => _claimed;
  ExecutionCredential get credential => _credential;

  /// 用新期限替换 credential，可选地推进 fencing token / leaseId。
  ExecutionCredential replace({int? fencingToken, String? leaseId}) {
    _credential = ExecutionCredential(
      executionId: _credential.executionId,
      leaseId: leaseId ?? _credential.leaseId,
      fencingToken: fencingToken ?? _credential.fencingToken,
      leaseExpiresAt: _fixture.runtime.now().add(leaseDuration),
    );
    return _credential;
  }

  /// 构造一个仍属于当前 execution、状态 active 的 `JobRecord`。
  JobRecord activeRecord({
    int? revision,
    Object? progress,
    JobControlRequest? controlRequest,
  }) => _fixture.record(
    queueName: _claimed.record.key.queueName,
    jobId: _claimed.record.key.jobId,
    name: _claimed.record.name,
    status: JobStatus.active,
    revision: revision ?? _claimed.record.revision,
    attemptsMade: _claimed.record.attemptsMade,
    data: _claimed.record.data,
    progress: progress ?? _claimed.record.progress,
    execution: ExecutionOwner(
      workerId: _fixture.workerId,
      credential: _credential,
      startedAt: _fixture.runtime.now(),
    ),
    controlRequest: controlRequest,
  );
}

/// 默认的 distributed 后端接线：claim 一次、getJob 返回当前 credential、续租延长租约。
///
/// 各测试可在调用后再覆盖单个 handler 以表达失败路径。
void installDistributedDefaults(
  WorkerFixture fixture,
  DistributedLease lease, {
  Set<String> renewQueuesToNotify = const <String>{},
}) {
  var served = false;
  fixture.backend.onClaimJob = (ClaimJobRequest request) {
    if (served) {
      return Future<BackendWriteResult<ClaimedJob?>>.value(noClaim());
    }
    served = true;
    return Future<BackendWriteResult<ClaimedJob?>>.value(
      claimedWrite(lease.claimed),
    );
  };
  fixture.backend.onGetJob = (JobKey key) async => lease.activeRecord();
  fixture.backend.onRenewLease = (RenewLeaseRequest request) async =>
      changedWrite(lease.replace(), queuesToNotify: renewQueuesToNotify);
  fixture.installSuccessfulTransitions();
}

/// 当前仍处于 pending 的指定时长的延迟数量。
int pendingDelayCount(WorkerFixture fixture, Duration duration) => fixture
    .runtime
    .pendingDelays
    .where((ManualDelayEntry entry) => entry.duration == duration)
    .length;

void main() {
  // --- 能力与租约配置：验证 distributed Profile 的能力校验、claim 参数与续租行为 ---

  test('local Worker 会在订阅前拒绝 lease 选项', () async {
    // 验证 local Profile 的 Backend 即使传入 leaseDuration/renewInterval 也会被拒绝，且不产生任何副作用
    final fixture = WorkerFixture();
    final worker = fixture.worker(
      options: const WorkerOptions(
        leaseDuration: leaseDuration,
        renewInterval: renewInterval,
      ),
    );

    await expectLater(worker.start(), throwsA(isA<UnsupportedCapability>()));

    expect(worker.state, WorkerState.created);
    // 能力不足必须在任何副作用之前拒绝。
    expect(fixture.signal.subscribedQueues, isEmpty);
    expect(fixture.backend.calls, isEmpty);
    expect(fixture.runtime.pendingDelays, isEmpty);
  });

  test('local Worker 绝不会调用 renewLease', () async {
    // 验证 local Worker 即使设置了 renewInterval 也不会调用 renewLease，且不会登记续租 delay
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
    await fixture.runtime.flushMicrotasks();

    // local 不创建续租循环，也不登记 renewInterval 延迟。
    expect(pendingDelayCount(fixture, renewInterval), 0);

    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(fixture.backend.countOf('renewLease'), 0);
    expect(worker.activeCount, 1);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
    expect(fixture.backend.countOf('renewLease'), 0);
  });

  test('distributed Worker 要求使用 distributed Signal', () async {
    // 验证 distributed Profile 的 Backend 与 process scope Signal 组合被拒绝，必须用 distributed scope
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    // fixture.signal 默认是 process scope。
    final worker = fixture.worker(
      options: const WorkerOptions(
        leaseDuration: leaseDuration,
        renewInterval: renewInterval,
      ),
    );

    await expectLater(worker.start(), throwsA(isA<UnsupportedCapability>()));

    expect(worker.state, WorkerState.created);
    expect(fixture.signal.subscribedQueues, isEmpty);
    expect(fixture.backend.calls, isEmpty);
    expect(fixture.runtime.pendingDelays, isEmpty);
  });

  test('distributed Worker 要求设置 leaseDuration 和 renewInterval', () async {
    // 验证 distributed Worker 必须同时提供 leaseDuration 和 renewInterval，否则抛 UnsupportedCapability
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    final signal = distributedSignal();
    final worker = fixture.worker(signal: signal);

    await expectLater(worker.start(), throwsA(isA<UnsupportedCapability>()));

    expect(worker.state, WorkerState.created);
    expect(signal.subscribedQueues, isEmpty);
    expect(fixture.backend.calls, isEmpty);
    expect(fixture.runtime.pendingDelays, isEmpty);
  });

  test('distributed claim 会包含 leaseDuration', () async {
    // 验证 distributed Worker claimJob 请求携带 leaseDuration，返回的 leaseExpiresAt 等于 now + leaseDuration
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    final signal = distributedSignal();
    final worker = fixture.worker(
      signal: signal,
      options: const WorkerOptions(
        leaseDuration: leaseDuration,
        renewInterval: renewInterval,
      ),
    );

    await worker.start();

    final request = fixture.backend.claimJobRequests.single;
    expect(request.queueName, 'emails');
    expect(request.workerId, fixture.workerId);
    expect(request.leaseDuration, leaseDuration);

    await worker.close();
  });

  // --- 续租与到期 -------------------------------------------------------

  test('会按 renewInterval 为每个 distributed execution 续约', () async {
    // 验证 distributed Worker 按 renewInterval 对每个 active execution 定期调用 renewLease
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    final signal = distributedSignal();
    final lease = DistributedLease(
      fixture,
      fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
    );
    installDistributedDefaults(fixture, lease);
    final renewals = <RenewLeaseRequest>[];
    fixture.backend.onRenewLease = (RenewLeaseRequest request) async {
      renewals.add(request);
      return changedWrite(lease.replace());
    };

    final entered = Completer<void>();
    final release = Completer<void>();
    final worker = distributedWorker(
      fixture,
      signal: signal,
      processor: (ActiveJob job) async {
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;
    expect(renewals, isEmpty);
    expect(pendingDelayCount(fixture, renewInterval), 1);

    fixture.runtime.completeNext(renewInterval);
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(renewals, hasLength(1));
    expect(renewals.single.renewedAt, fixture.runtime.now());
    expect(renewals.single.leaseDuration, leaseDuration);
    expect(
      renewals.single.credential.executionId,
      lease.claimed.credential.executionId,
    );
    // 续租后重新登记下一个周期，而不是热循环。
    expect(pendingDelayCount(fixture, renewInterval), 1);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test(
    '下一次续约和变更会使用最新 credential',
    () async {
      // 验证 renewLease 返回新 credential 后，后续续租和 mutation 都使用最新 credential
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = distributedSignal();
      final lease = DistributedLease(
        fixture,
        fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
      );
      installDistributedDefaults(fixture, lease);
      final renewals = <RenewLeaseRequest>[];
      var renewCount = 0;
      fixture.backend.onRenewLease = (RenewLeaseRequest request) async {
        renewals.add(request);
        renewCount++;
        return changedWrite(lease.replace(fencingToken: renewCount));
      };

      final mutating = Completer<void>();
      final release = Completer<void>();
      final worker = distributedWorker(
        fixture,
        signal: signal,
        processor: (ActiveJob job) async {
          await mutating.future;
          await job.updateProgress(0.5);
          await release.future;
          return null;
        },
      );

      await worker.start();

      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(renewals, hasLength(1));
      expect(lease.credential.fencingToken, 1);

      // 第二次续租必须携带第一次续租返回的新 credential。
      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(renewals, hasLength(2));
      expect(renewals[1].credential.fencingToken, 1);
      expect(lease.credential.fencingToken, 2);

      // 业务写入同样使用最新 credential。
      fixture.backend.onUpdateJobProgress =
          (UpdateJobProgressRequest request) async => changedWrite(
            lease.activeRecord(
              revision: request.expectedRevision + 1,
              progress: request.progress,
            ),
          );
      mutating.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.backend.updateJobProgressRequests, hasLength(1));
      expect(
        fixture
            .backend
            .updateJobProgressRequests
            .single
            .credential
            .fencingToken,
        2,
      );

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
    },
  );

  test('续约不会改变 ActiveJob revision', () async {
    // 验证 renewLease 成功后 ActiveJob 的 revision 不变，只有 credential 和 leaseExpiresAt 更新
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    final signal = distributedSignal();
    final lease = DistributedLease(
      fixture,
      fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
    );
    installDistributedDefaults(fixture, lease);

    final entered = Completer<void>();
    final release = Completer<void>();
    late ActiveJob observed;
    final worker = distributedWorker(
      fixture,
      signal: signal,
      processor: (ActiveJob job) async {
        observed = job;
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;
    final revisionBefore = observed.rawRecord.revision;

    fixture.runtime.advance(renewInterval);
    fixture.runtime.completeNext(renewInterval);
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(fixture.backend.renewLeaseRequests, hasLength(1));
    // 续租只替换 credential：snapshot revision 与控制状态都不变。
    expect(observed.rawRecord.revision, revisionBefore);
    expect(observed.controlState, ActiveJobControlState.running);
    expect(fixture.backend.countOf('getJob'), 0);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test(
    '确认的过期时间之前遇到 BackendUnavailable 时仍会继续执行',
    () async {
      // 验证 renewLease 临时失败（BackendUnavailable）但租约未真正到期时，Worker 继续执行 Processor
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = distributedSignal();
      final lease = DistributedLease(
        fixture,
        fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
      );
      installDistributedDefaults(fixture, lease);
      var renewCalls = 0;
      fixture.backend.onRenewLease = (RenewLeaseRequest request) {
        renewCalls++;
        if (renewCalls == 1) {
          return Future<BackendWriteResult<ExecutionCredential>>.error(
            BackendUnavailable(null, 'lease service is down'),
          );
        }
        return Future<BackendWriteResult<ExecutionCredential>>.value(
          changedWrite(lease.replace()),
        );
      };

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final worker = distributedWorker(
        fixture,
        signal: signal,
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;

      // T+3：第一次续租失败，但没有到达已确认的 leaseExpiresAt。
      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(renewCalls, 1);
      expect(fixture.logSink.forOperation('renewLease'), isNotEmpty);
      expect(observed.controlState, ActiveJobControlState.running);
      expect(worker.activeCount, 1);

      // T+6：第二次续租成功，Processor 可以继续完成。
      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(renewCalls, 2);
      expect(
        lease.credential.leaseExpiresAt,
        fixture.runtime.now().add(leaseDuration),
      );
      expect(observed.controlState, ActiveJobControlState.running);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.transitionJobRequests, hasLength(1));
      expect(
        fixture.backend.transitionJobRequests.single.targetStatus,
        JobStatus.completed,
      );

      await worker.close();
    },
  );

  test('会在下一个手动间隔重试续约', () async {
    // 验证 renewLease 连续失败只在下一个 renewInterval 重试，不会热循环
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    final signal = distributedSignal();
    final lease = DistributedLease(
      fixture,
      fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
    );
    installDistributedDefaults(fixture, lease);
    var renewCalls = 0;
    fixture.backend.onRenewLease = (RenewLeaseRequest request) {
      renewCalls++;
      if (renewCalls == 1) {
        return Future<BackendWriteResult<ExecutionCredential>>.error(
          BackendUnavailable(null, 'lease service is down'),
        );
      }
      return Future<BackendWriteResult<ExecutionCredential>>.value(
        changedWrite(lease.replace()),
      );
    };

    final entered = Completer<void>();
    final release = Completer<void>();
    final worker = distributedWorker(
      fixture,
      signal: signal,
      processor: (ActiveJob job) async {
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;
    expect(pendingDelayCount(fixture, renewInterval), 1);

    fixture.runtime.completeNext(renewInterval);
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(renewCalls, 1);
    // 失败后重新登记下一周期：由人工完成下一个 interval 才再次尝试。
    expect(pendingDelayCount(fixture, renewInterval), 1);

    fixture.runtime.advance(renewInterval);
    fixture.runtime.completeNext(renewInterval);
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(renewCalls, 2);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test(
    '到 leaseExpiresAt 仍无法确认续约时会发布 LeaseLost',
    () async {
      // 验证 renewLease 在 leaseExpiresAt 后仍无法成功时，判定 LeaseLost 并清理槽位
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = distributedSignal();
      final lease = DistributedLease(
        fixture,
        fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
      );
      installDistributedDefaults(fixture, lease);
      // 续租永远无法确认。
      fixture.backend.onRenewLease = (RenewLeaseRequest request) =>
          Future<BackendWriteResult<ExecutionCredential>>.error(
            BackendUnavailable(null, 'lease service is down'),
          );

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      final worker = distributedWorker(
        fixture,
        signal: signal,
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          return null;
        },
      );

      await worker.start();
      await entered.future;

      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);
      // 到期前保留执行。
      expect(observed.controlState, ActiveJobControlState.running);

      // 推进到已确认的 leaseExpiresAt。
      fixture.runtime.advance(leaseDuration - renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.leaseLost);
      // 到期判断发生在调用 Backend 之前。
      expect(fixture.backend.renewLeaseRequests, hasLength(1));

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('transitionJob'), 0);
      expect(worker.activeCount, 0);

      await worker.close();
    },
  );

  test('本地 lease 过期后会拒绝 Processor 完成操作', () async {
    // 验证 local Worker 的 claim credential 到期后，Processor 完成提交会被判定为 LeaseLost
    final fixture = WorkerFixture();
    // local Profile 不配置续租，但 claim 的 credential 仍可能带到期时间。
    final claimed = fixture.claim(
      jobId: 'job-1',
      leaseExpiresAt: fixture.runtime.now().add(leaseDuration),
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
    fixture.installSuccessfulTransitions();

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
    expect(pendingDelayCount(fixture, renewInterval), 0);

    fixture.runtime.advance(leaseDuration);
    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);

    // 到期后不得再提交业务结果。
    expect(observed.controlState, ActiveJobControlState.leaseLost);
    expect(fixture.backend.countOf('transitionJob'), 0);
    expect(worker.activeCount, 0);

    await worker.close();
  });

  test(
    'renewLease 返回 LeaseLost 后会停止变更并丢弃 Processor 结果',
    () async {
      // 验证 renewLease 返回 LeaseLost 后，所有 mutation 被阻止，Processor 的迟到返回被丢弃
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = distributedSignal();
      final lease = DistributedLease(
        fixture,
        fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
      );
      installDistributedDefaults(fixture, lease);
      fixture.backend.onRenewLease = (RenewLeaseRequest request) =>
          Future<BackendWriteResult<ExecutionCredential>>.error(
            LeaseLost(
              request.key,
              request.credential.executionId,
              'the lease was taken over',
            ),
          );

      final entered = Completer<void>();
      final release = Completer<void>();
      late ActiveJob observed;
      Object? mutationError;
      final worker = distributedWorker(
        fixture,
        signal: signal,
        processor: (ActiveJob job) async {
          observed = job;
          entered.complete();
          await release.future;
          try {
            await job.updateProgress(0.5);
          } catch (error) {
            mutationError = error;
          }
          return null;
        },
      );

      await worker.start();
      await entered.future;

      fixture.runtime.advance(renewInterval);
      fixture.runtime.completeNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.leaseLost);
      expect(fixture.logSink.forOperation('renewLease'), isNotEmpty);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);

      // 失去租约后业务写入以基础设施失败终止，且绝不触达 Backend。
      expect(mutationError, isA<ActiveJobInfrastructureFailure>());
      expect(
        (mutationError! as ActiveJobInfrastructureFailure).original,
        isA<LeaseLost>(),
      );
      expect(fixture.backend.countOf('updateJobProgress'), 0);
      expect(fixture.backend.countOf('transitionJob'), 0);
      expect(worker.activeCount, 0);

      await worker.close();
    },
  );

  test('续约通知只会使用 queuesToNotify', () async {
    // 验证 renewLease 成功后通知 Backend 声明的 queuesToNotify，不额外通知当前队列
    final fixture = WorkerFixture(capabilities: distributedCapabilities());
    final signal = distributedSignal();
    final lease = DistributedLease(
      fixture,
      fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
    );
    installDistributedDefaults(
      fixture,
      lease,
      renewQueuesToNotify: const <String>{'emails', 'reports'},
    );

    final entered = Completer<void>();
    final release = Completer<void>();
    final worker = distributedWorker(
      fixture,
      signal: signal,
      processor: (ActiveJob job) async {
        entered.complete();
        await release.future;
        return null;
      },
    );

    await worker.start();
    await entered.future;
    expect(signal.notifyCalls, isEmpty);

    fixture.runtime.advance(renewInterval);
    fixture.runtime.completeNext(renewInterval);
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(fixture.backend.renewLeaseRequests, hasLength(1));
    expect(signal.notifyCalls, <String>['emails', 'reports']);

    release.complete();
    await fixture.runtime.flushMicrotasks(turns: 24);
    await worker.close();
  });

  test('续约期间的 credential 替换会与 refresh、变更和终态提交串行处理', () async {
    // 验证 renewLease 返回 credential 替换后，与 ActiveJob 内部的 refresh、mutation、terminal commit 全部串行化
    final fixture = WorkerFixture();
    final claimed = fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration);
    final controller = fixture.controllerFor(claimed);
    final renewBarrier = Completer<void>();
    var latest = claimed.credential;
    fixture.backend.onRenewLease = (RenewLeaseRequest request) async {
      await renewBarrier.future;
      latest = ExecutionCredential(
        executionId: request.credential.executionId,
        fencingToken: 1,
        leaseExpiresAt: fixture.runtime.now().add(leaseDuration),
      );
      return changedWrite(latest);
    };
    fixture.backend.onGetJob = (JobKey key) async => fixture.record(
      queueName: claimed.record.key.queueName,
      jobId: claimed.record.key.jobId,
      name: claimed.record.name,
      status: JobStatus.active,
      revision: claimed.record.revision,
      attemptsMade: claimed.record.attemptsMade,
      execution: ExecutionOwner(
        workerId: fixture.workerId,
        credential: latest,
        startedAt: fixture.runtime.now(),
      ),
    );
    fixture.backend.onUpdateJobProgress =
        (UpdateJobProgressRequest request) async => changedWrite(
          fixture.record(
            queueName: claimed.record.key.queueName,
            jobId: claimed.record.key.jobId,
            name: claimed.record.name,
            status: JobStatus.active,
            revision: request.expectedRevision + 1,
            attemptsMade: claimed.record.attemptsMade,
            progress: request.progress,
            execution: ExecutionOwner(
              workerId: fixture.workerId,
              credential: latest,
              startedAt: fixture.runtime.now(),
            ),
          ),
        );

    // 同一个 controller 上并发排入：续租 → 控制 refresh → 业务 mutation。
    final renewal = controller.renewOrExpire(
      fixture.runtime.now(),
      leaseDuration,
    );
    final refresh = controller.refreshControl();
    final mutation = controller.updateProgress(0.5);
    await fixture.runtime.flushMicrotasks();

    // 续租仍在 Backend 中时，后两项不得插队触碰 Backend。
    expect(fixture.backend.countOf('renewLease'), 1);
    expect(fixture.backend.countOf('getJob'), 0);
    expect(fixture.backend.countOf('updateJobProgress'), 0);

    renewBarrier.complete();
    expect(await renewal, isTrue);
    await refresh;
    await mutation;

    expect(fixture.backend.calls, <String>[
      'renewLease',
      'getJob',
      'updateJobProgress',
    ]);
  });

  test(
    'renew Delay 失败时只发布一次 LeaseLost，且不会重新启动忙循环',
    () async {
      // 验证 renewInterval delay 失败只记一次日志并升级为 LeaseLost，不会热循环重试
      final fixture = WorkerFixture(capabilities: distributedCapabilities());
      final signal = distributedSignal();
      final lease = DistributedLease(
        fixture,
        fixture.claim(jobId: 'job-1', leaseDuration: leaseDuration),
      );
      installDistributedDefaults(fixture, lease);

      final entered = Completer<void>();
      final release = Completer<void>();
      final events = <ActiveJobControlState>[];
      late ActiveJob observed;
      final worker = distributedWorker(
        fixture,
        signal: signal,
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
      expect(pendingDelayCount(fixture, renewInterval), 1);

      fixture.runtime.failNext(renewInterval);
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(observed.controlState, ActiveJobControlState.leaseLost);
      expect(fixture.logSink.forOperation('renewLease'), hasLength(1));
      expect(events, <ActiveJobControlState>[ActiveJobControlState.leaseLost]);
      // 不调用 Backend，也不重新登记续租延迟。
      expect(fixture.backend.countOf('renewLease'), 0);
      expect(pendingDelayCount(fixture, renewInterval), 0);

      release.complete();
      await fixture.runtime.flushMicrotasks(turns: 24);
      expect(fixture.backend.countOf('transitionJob'), 0);
      expect(worker.activeCount, 0);

      await worker.close();
    },
  );
}
