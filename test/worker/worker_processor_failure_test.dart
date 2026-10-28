import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/observability/index.dart';
import 'package:tf_bull_mq/src/worker/index.dart';

import 'support/recording_backend.dart';
import 'support/worker_fixture.dart';

/// 可精确控制的 `StackTrace`，用于固定 stackTraceLimit 裁剪结果。
final class FakeStackTrace implements StackTrace {
  FakeStackTrace(this.frames);

  final List<String> frames;

  @override
  String toString() => frames.join('\n');
}

const Duration poll = Duration(seconds: 1);

JobRecord completedRecord(
  WorkerFixture fixture,
  TransitionJobRequest request,
) => fixture.record(
  queueName: request.key.queueName,
  jobId: request.key.jobId,
  status: request.targetStatus,
  revision: request.expectedRevision + 1,
);

void main() {
  // --- 成功与重试目标：成功路径终态提交、重试退避与重试上限 ---

  test('会将同步 Processor 结果提交为 completed', () async {
    // 验证同步返回可 JSON 序列化结果时，Worker 提交 JobStatus.completed 并携带 result
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(
      processor: (ActiveJob job) => <String, Object?>{'ok': true},
    );

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitions, hasLength(1));
    expect(transitions.single.targetStatus, JobStatus.completed);
    expect(transitions.single.result, <String, Object?>{'ok': true});
    expect(transitions.single.credential, pool.jobs.single.credential);
    expect(transitions.single.failure, isNull);
    expect(transitions.single.automaticErrorLog, isNull);
    expect(transitions.single.availableAt, isNull);
    expect(worker.activeCount, 0);

    await worker.close();
  });

  test('会将异步 Processor 结果提交为 completed', () async {
    // 验证异步返回可 JSON 序列化结果时，Worker 提交 completed
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(processor: (ActiveJob job) async => 'done');

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitions.single.targetStatus, JobStatus.completed);
    expect(transitions.single.result, 'done');
    expect(worker.activeCount, 0);

    await worker.close();
  });

  test('接受为 null 的 Processor 结果', () async {
    // 验证 Processor 返回 null 时 Worker 正常提交 completed（无 result）
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(processor: (ActiveJob job) => null);

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitions.single.targetStatus, JobStatus.completed);
    expect(transitions.single.result, isNull);
    expect(transitions.single.failure, isNull);
    expect(worker.activeCount, 0);

    await worker.close();
  });

  test(
    '会将非 JSON-like 的 Processor 结果视为 Processor 失败',
    () async {
      // 验证 Processor 返回不可 JSON 序列化对象（Stream、Sink 等）时，Worker 判定为 Processor failure 并重试
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      final transitions = <TransitionJobRequest>[];
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        transitions.add(request);
        return changedWrite(completedRecord(fixture, request));
      };
      final worker = fixture.worker(processor: (ActiveJob job) => Object());

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(transitions.single.targetStatus, JobStatus.failed);
      expect(transitions.single.failure, isNotNull);
      expect(transitions.single.failure!.type, 'ArgumentError');
      expect(transitions.single.automaticErrorLog, isNotNull);
      expect(transitions.single.automaticErrorLog!.level, JobLogLevel.error);
      expect(
        transitions.single.automaticErrorLog!.channel,
        JobLogChannel.system,
      );

      await worker.close();
    },
  );

  test('达到 maxAttempts 前会立即重试并转为 waiting', () async {
    // 验证 Processor 抛错且 attemptsMade < maxAttempts 时，Worker transition 为 waiting 并立即重试
    final fixture = WorkerFixture();
    fixture.installClaimPool(
      count: 1,
      attemptsMade: 1,
      options: JobOptions(retry: RetryPolicy(maxAttempts: 3)),
    );
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(
      processor: (ActiveJob job) => throw StateError('boom'),
    );

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitions.single.targetStatus, JobStatus.waiting);
    expect(transitions.single.availableAt, isNull);
    expect(transitions.single.failure, isNotNull);
    expect(transitions.single.result, isNull);

    await worker.close();
  });

  test('backoff 为正数时会设置 availableAt 并转为 delayed 后重试', () async {
    // 验证 Processor 抛错且 backoff>0 时，Worker transition 为 waiting 并携带 availableAt
    final fixture = WorkerFixture();
    fixture.installClaimPool(
      count: 1,
      attemptsMade: 1,
      options: JobOptions(
        retry: RetryPolicy(maxAttempts: 3, delay: const Duration(seconds: 2)),
      ),
    );
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(
      processor: (ActiveJob job) => throw StateError('boom'),
    );

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    final request = transitions.single;
    expect(request.targetStatus, JobStatus.delayed);
    expect(
      request.availableAt,
      fixture.runtime.now().add(const Duration(seconds: 2)),
    );
    // failure 的 failedAt 与 transition 的 occurredAt 来自同一次 now()。
    expect(request.failure!.failedAt, request.occurredAt);

    await worker.close();
  });

  test(
    '会为当前失败的尝试使用带上限的指数延迟',
    () async {
      // 验证 backoff 使用指数退避公式（min(baseDelay * 2^attempt, maxDelay)）计算等待时间
      final fixture = WorkerFixture();
      fixture.installClaimPool(
        count: 1,
        attemptsMade: 2,
        options: JobOptions(
          retry: RetryPolicy(
            maxAttempts: 5,
            backoff: BackoffPolicy.exponential,
            delay: const Duration(seconds: 3),
            maxDelay: const Duration(seconds: 5),
          ),
        ),
      );
      final transitions = <TransitionJobRequest>[];
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        transitions.add(request);
        return changedWrite(completedRecord(fixture, request));
      };
      final worker = fixture.worker(
        processor: (ActiveJob job) => throw StateError('boom'),
      );

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      // attemptsMade == 2：3 秒 × 2 = 6 秒，被 maxDelay 截断为 5 秒。
      expect(transitions.single.targetStatus, JobStatus.delayed);
      expect(
        transitions.single.availableAt,
        fixture.runtime.now().add(const Duration(seconds: 5)),
      );

      await worker.close();
    },
  );

  test('attemptsMade 达到 maxAttempts 时会提交为 failed', () async {
    // 验证 Processor 抛错且 attemptsMade >= maxAttempts 时，Worker transition 为 failed 并携带 failure 信息
    final fixture = WorkerFixture();
    fixture.installClaimPool(
      count: 1,
      attemptsMade: 1,
      options: JobOptions(retry: RetryPolicy(maxAttempts: 1)),
    );
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(
      processor: (ActiveJob job) => throw StateError('boom'),
    );

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitions.single.targetStatus, JobStatus.failed);
    expect(transitions.single.availableAt, isNull);
    expect(transitions.single.result, isNull);

    await worker.close();
  });

  // --- failure、基础设施与日志失败 -------------------------------------------

  test(
    '会限制持久化的 stack trace 行数，并保留 sink 中的完整堆栈',
    () async {
      // 验证 Processor failure 持久化的 stackTrace 受 stackTraceLimit 截断，同时完整 stackTrace 传给 WorkerLogSink
      final fixture = WorkerFixture();
      fixture.installClaimPool(
        count: 1,
        options: JobOptions(stackTraceLimit: 2),
      );
      final transitions = <TransitionJobRequest>[];
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        transitions.add(request);
        return changedWrite(completedRecord(fixture, request));
      };
      final stackTrace = FakeStackTrace(<String>[
        'frame-1',
        'frame-2',
        'frame-3',
        'frame-4',
      ]);
      final error = StateError('boom');
      Object? failingProcessor(ActiveJob job) =>
          Error.throwWithStackTrace(error, stackTrace);
      final worker = fixture.worker(processor: failingProcessor);

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(transitions.single.failure!.stackTrace, 'frame-1\nframe-2');
      final entry = fixture.logSink.forOperation('processor').single;
      expect(entry.error, same(error));
      expect(entry.stackTrace, same(stackTrace));

      await worker.close();
    },
  );

  test('stackTraceLimit 为 0 时会持久化 null stackTrace', () async {
    // 验证 stackTraceLimit=0 时，failure 中持久化的 stackTrace 为 null
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1, options: JobOptions(stackTraceLimit: 0));
    final transitions = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitions.add(request);
      return changedWrite(completedRecord(fixture, request));
    };
    final stackTrace = FakeStackTrace(<String>['frame-1', 'frame-2']);
    Object? failingProcessor(ActiveJob job) =>
        Error.throwWithStackTrace(StateError('boom'), stackTrace);
    final worker = fixture.worker(processor: failingProcessor);

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitions.single.failure!.stackTrace, isNull);
    // 完整堆栈仍然进入 Worker 运行日志。
    expect(
      fixture.logSink.forOperation('processor').single.stackTrace,
      same(stackTrace),
    );

    await worker.close();
  });

  test('每次 Processor 失败状态转移都会附加一个 error system JobLogDraft', () async {
    // 验证每次 Processor failure transition 后，Backend 追加一条 error 级别的系统日志草稿
    Future<TransitionJobRequest> runFailingJob(JobOptions options) async {
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1, attemptsMade: 1, options: options);
      final transitions = <TransitionJobRequest>[];
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        transitions.add(request);
        return changedWrite(completedRecord(fixture, request));
      };
      final worker = fixture.worker(
        processor: (ActiveJob job) => throw StateError('boom'),
      );
      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);
      await worker.close();
      return transitions.single;
    }

    final targets = <JobStatus>[];
    for (final options in <JobOptions>[
      JobOptions(retry: RetryPolicy(maxAttempts: 3)),
      JobOptions(
        retry: RetryPolicy(maxAttempts: 3, delay: const Duration(seconds: 2)),
      ),
      JobOptions(retry: RetryPolicy(maxAttempts: 1)),
    ]) {
      final request = await runFailingJob(options);
      targets.add(request.targetStatus);
      expect(request.failure, isNotNull);
      expect(request.failure!.type, 'StateError');
      expect(request.automaticErrorLog, isNotNull);
      expect(request.automaticErrorLog!.level, JobLogLevel.error);
      expect(request.automaticErrorLog!.channel, JobLogChannel.system);
    }
    expect(targets, <JobStatus>[
      JobStatus.waiting,
      JobStatus.delayed,
      JobStatus.failed,
    ]);
  });

  test('会记录带有 execution 上下文的原始 Processor 错误', () async {
    // 验证 WorkerLogSink 收到 Processor 原始错误时携带 execution context（workerId、executionId、attempt）
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    fixture.installSuccessfulTransitions();
    final error = StateError('boom');
    final worker = fixture.worker(processor: (ActiveJob job) => throw error);

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    final entry = fixture.logSink.forOperation('processor').single;
    expect(entry.level, WorkerLogLevel.error);
    expect(entry.error, same(error));
    expect(entry.context.workerId, worker.workerId);
    expect(entry.context.queueName, 'emails');
    expect(entry.context.jobKey, pool.jobs.single.record.key);
    expect(entry.context.attempt, 1);
    expect(entry.context.executionId, pool.jobs.single.credential.executionId);
    // JobLog 只保留 error/system 诊断，完整异常不进入持久化 JobLog data。
    expect(
      fixture.backend.transitionJobRequests.single.automaticErrorLog!.data,
      isNull,
    );

    await worker.close();
  });

  test('不会将 completed 状态转移中的 BackendUnavailable 归类为 Processor 失败', () async {
    // 验证 transition completed 时 Backend 临时失败（BackendUnavailable）不被当作 Processor failure 处理
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    final attempts = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      attempts.add(request);
      throw BackendUnavailable(null, 'backend is down');
    };
    final worker = fixture.worker(processor: (ActiveJob job) => 'ok');

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(attempts, hasLength(1));
    expect(attempts.single.targetStatus, JobStatus.completed);
    expect(attempts.single.failure, isNull);
    expect(attempts.single.automaticErrorLog, isNull);
    expect(fixture.backend.countOf('appendJobLog'), 0);
    // 基础设施错误只写 Worker 运行日志。
    expect(fixture.logSink.forOperation('transitionJob'), isNotEmpty);
    // holder、槽位与终态意图都被保留，等待下一次 poll 重试。
    expect(worker.activeCount, 1);
    expect(pool.served, 1);

    await worker.forceClose();
  });

  test(
    '不会将 RevisionConflict 或 LeaseLost 归类为 Processor 失败',
    () async {
      // 验证 RevisionConflict 和 LeaseLost 都不被当作 Processor failure 处理，Worker 丢弃结果后由 Backend 决定重试策略
      // (a) RevisionConflict 刷新后发现 execution 已经变化。
      final conflictFixture = WorkerFixture();
      conflictFixture.installClaimPool(count: 1);
      final conflictAttempts = <TransitionJobRequest>[];
      conflictFixture.backend.onTransitionJob =
          (TransitionJobRequest request) async {
            conflictAttempts.add(request);
            throw RevisionConflict(
              request.key,
              request.expectedRevision,
              request.expectedRevision + 1,
              'stale revision',
            );
          };
      conflictFixture.backend.onGetJob = (JobKey key) async =>
          conflictFixture.record(
            queueName: key.queueName,
            jobId: key.jobId,
            revision: 9,
            status: JobStatus.waiting,
            execution: ExecutionOwner(
              workerId: 'other-worker',
              credential: const ExecutionCredential(executionId: 'other-exec'),
              startedAt: conflictFixture.runtime.now(),
            ),
          );
      final conflictWorker = conflictFixture.worker(
        processor: (ActiveJob job) => 'ok',
      );
      await conflictWorker.start();
      await conflictFixture.runtime.flushMicrotasks(turns: 24);

      expect(
        conflictAttempts.every(
          (TransitionJobRequest request) =>
              request.targetStatus == JobStatus.completed,
        ),
        isTrue,
      );
      expect(
        conflictAttempts.every(
          (TransitionJobRequest request) =>
              request.failure == null && request.automaticErrorLog == null,
        ),
        isTrue,
      );
      expect(conflictFixture.logSink.forOperation('transitionJob'), isNotEmpty);
      expect(conflictWorker.activeCount, 0);
      await conflictWorker.close();

      // (b) Backend 直接以 LeaseLost 拒绝终态提交。
      final lostFixture = WorkerFixture();
      lostFixture.installClaimPool(count: 1);
      final lostAttempts = <TransitionJobRequest>[];
      lostFixture.backend.onTransitionJob =
          (TransitionJobRequest request) async {
            lostAttempts.add(request);
            throw LeaseLost(
              request.key,
              request.credential!.executionId,
              'lease gone',
            );
          };
      final lostWorker = lostFixture.worker(processor: (ActiveJob job) => 'ok');
      await lostWorker.start();
      await lostFixture.runtime.flushMicrotasks(turns: 24);

      expect(lostAttempts.single.targetStatus, JobStatus.completed);
      expect(lostAttempts.single.failure, isNull);
      expect(lostAttempts.single.automaticErrorLog, isNull);
      expect(lostFixture.logSink.forOperation('transitionJob'), isNotEmpty);
      await lostWorker.close();
    },
  );

  test(
    '不会将未知的 ActiveJob backend 错误归类为 Processor 失败',
    () async {
      // 验证 ActiveJob 内部未知 Backend 错误（如 appendJobLog 失败）不被当作 Processor failure
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 1);
      final attempts = <TransitionJobRequest>[];
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        attempts.add(request);
        return changedWrite(completedRecord(fixture, request));
      };
      final backendError = StateError('unexpected backend state');
      fixture.backend.onUpdateJobProgress = (
        UpdateJobProgressRequest request,
      ) async => throw backendError;
      final worker = fixture.worker(
        processor: (ActiveJob job) async {
          await job.updateProgress(0.5);
          return 'never reached';
        },
      );

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      // 基础设施错误绝不算作业务失败：没有 state transition，也没有 JobFailure。
      expect(attempts, isEmpty);
      expect(pool.served, 1);
      final entry = fixture.logSink.forOperation('processor').single;
      expect(entry.error, same(backendError));

      await worker.close();
    },
  );

  test(
    '遇到 BackendUnavailable 后会保留 holder 槽位和终态意图',
    () async {
      // 验证 transition 失败（BackendUnavailable）时 Worker 保留 holder 槽位和待提交的终态意图
      final fixture = WorkerFixture();
      final pool = fixture.installClaimPool(count: 2);
      var attempts = 0;
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
        attempts++;
        throw BackendUnavailable(null, 'backend is down');
      };
      final worker = fixture.worker(
        options: const WorkerOptions(concurrency: 1),
        processor: (ActiveJob job) => 'ok',
      );

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(attempts, 1);
      // 槽位保持占用，不领取新的 Job。
      expect(worker.activeCount, 1);
      expect(pool.served, 1);

      await worker.forceClose();
    },
  );

  test('下一轮 poll 会重试已保存的终态意图，而不会重新运行 Processor', () async {
    // 验证 transition 失败后 Worker 在下一次 poll 时重试同一终态意图，不重新运行 Processor
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    var processorRuns = 0;
    var transitionAttempts = 0;
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      transitionAttempts++;
      if (transitionAttempts == 1) {
        throw BackendUnavailable(null, 'backend is down');
      }
      return changedWrite(completedRecord(fixture, request));
    };
    final worker = fixture.worker(
      processor: (ActiveJob job) {
        processorRuns++;
        return 'ok';
      },
    );

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);
    expect(processorRuns, 1);
    expect(transitionAttempts, 1);
    expect(worker.activeCount, 1);
    expect(pool.served, 1);

    // 推进一个 poll：重试已冻结的终态意图。
    fixture.runtime.completeNext(poll);
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(transitionAttempts, 2);
    expect(processorRuns, 1);
    expect(worker.activeCount, 0);
    expect(pool.served, 1);

    await worker.close();
  });

  test('RevisionConflict 刷新后会识别已提交的终态意图', () async {
    // 说明 Worker 重试终态意图时遇到 RevisionConflict，refresh 发现 execution 已被他人替换则放弃
    final fixture = WorkerFixture();
    fixture.installClaimPool(count: 1);
    var attempts = 0;
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      attempts++;
      throw RevisionConflict(
        request.key,
        request.expectedRevision,
        request.expectedRevision + 1,
        'stale revision',
      );
    };
    fixture.backend.onGetJob = (JobKey key) async => fixture.record(
      queueName: key.queueName,
      jobId: key.jobId,
      status: JobStatus.completed,
      revision: 9,
      result: 'ok',
      finishedAt: fixture.runtime.now(),
    );
    final worker = fixture.worker(processor: (ActiveJob job) => 'ok');

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    // 事实与本执行的 intent 一致：视为已经提交，不重复 transition。
    expect(attempts, 1);
    expect(worker.activeCount, 0);
    expect(fixture.logSink.forOperation('transitionJob'), isEmpty);

    await worker.close();
  });

  test('RevisionConflict 后同一 execution 仍为 active 时，会刷新并重试终态意图', () async {
    // 说明 Worker 重试终态意图遇到 RevisionConflict，refresh 后同一 execution 仍 active 时，用新 revision 重试一次
    final fixture = WorkerFixture();
    final pool = fixture.installClaimPool(count: 1);
    final claimed = pool.jobs.single;
    final requests = <TransitionJobRequest>[];
    fixture.backend.onTransitionJob = (TransitionJobRequest request) async {
      requests.add(request);
      if (requests.length == 1) {
        throw RevisionConflict(
          request.key,
          request.expectedRevision,
          7,
          'stale revision',
        );
      }
      return changedWrite(completedRecord(fixture, request));
    };
    fixture.backend.onGetJob = (JobKey key) async => fixture.record(
      queueName: key.queueName,
      jobId: key.jobId,
      revision: 7,
      execution: claimed.record.execution,
    );
    final worker = fixture.worker(processor: (ActiveJob job) => 'ok');

    await worker.start();
    await fixture.runtime.flushMicrotasks(turns: 24);

    expect(requests, hasLength(2));
    expect(requests[0].expectedRevision, 1);
    expect(requests[1].expectedRevision, 7);
    expect(worker.activeCount, 0);

    await worker.close();
  });

  test(
    'WorkerLogSink 同步抛错时仍会保留 Job 状态转移结果',
    () async {
      // 说明 WorkerLogSink 同步抛错不影响已提交的 transition 结果
      final fixture = WorkerFixture();
      fixture.logSink.throwSynchronously = true;
      fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final worker = fixture.worker(processor: (ActiveJob job) => 'ok');

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.backend.transitionJobRequests, hasLength(1));
      expect(
        fixture.backend.transitionJobRequests.single.targetStatus,
        JobStatus.completed,
      );
      expect(worker.activeCount, 0);
      // sink 故障不改变 Worker 生命周期。
      expect(worker.state, WorkerState.running);

      await worker.close();
    },
  );

  test(
    'WorkerLogSink 异步失败时仍会保留 Job 状态转移结果',
    () async {
      // 说明 WorkerLogSink 异步失败不影响已提交的 transition 结果
      final fixture = WorkerFixture();
      fixture.logSink.failAsynchronously = true;
      fixture.installClaimPool(count: 1);
      fixture.installSuccessfulTransitions();
      final worker = fixture.worker(
        processor: (ActiveJob job) => throw StateError('boom'),
      );

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.backend.transitionJobRequests, hasLength(1));
      expect(
        fixture.backend.transitionJobRequests.single.targetStatus,
        JobStatus.failed,
      );
      expect(worker.activeCount, 0);
      expect(worker.state, WorkerState.running);

      await worker.close();
    },
  );

  test(
    '只会通知状态转移的 queuesToNotify，并忽略通知失败',
    () async {
      // 说明 transition 成功后只通知 Backend 声明的 queuesToNotify，Signal 通知失败不影响已提交的 transition
      // (a) 只通知 transition 结果声明的队列。
      final fixture = WorkerFixture();
      fixture.installClaimPool(count: 1);
      fixture.backend.onTransitionJob = (TransitionJobRequest request) async =>
          changedWrite(
            completedRecord(fixture, request),
            queuesToNotify: const <String>{'reports'},
          );
      final worker = fixture.worker(processor: (ActiveJob job) => 'ok');

      await worker.start();
      await fixture.runtime.flushMicrotasks(turns: 24);

      expect(fixture.signal.notifyCalls, <String>['reports']);
      expect(
        fixture.backend.transitionJobRequests.single.targetStatus,
        JobStatus.completed,
      );
      await worker.close();

      // (b) notify 失败不改变 Job 结果。
      final failing = WorkerFixture();
      failing.signal.notifyFails = true;
      failing.installClaimPool(count: 1);
      failing.backend.onTransitionJob = (TransitionJobRequest request) async =>
          changedWrite(
            completedRecord(failing, request),
            queuesToNotify: const <String>{'reports'},
          );
      final failingWorker = failing.worker(processor: (ActiveJob job) => 'ok');

      await failingWorker.start();
      await failing.runtime.flushMicrotasks(turns: 24);

      expect(failing.signal.notifyCalls, <String>['reports']);
      expect(
        failing.backend.transitionJobRequests.single.targetStatus,
        JobStatus.completed,
      );
      expect(failingWorker.activeCount, 0);
      await failingWorker.close();
    },
  );
}
