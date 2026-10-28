import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

MemoryQueueBackend _backend() {
  var counter = 0;
  final b = MemoryQueueBackend(
    now: () => DateTime.utc(2026, 1, 1),
    idGenerator: () => 'id-${++counter}',
  );
  b.open();
  return b;
}

final _t = DateTime.utc(2026, 1, 1);

Future<(JobRecord, ExecutionCredential)> _claim({
  required MemoryQueueBackend backend,
  required JobKey key,
  required String name,
  required Object? data,
  required String workerId,
  required DateTime t,
}) async {
  await backend.addJob(
    AddJobRequest(
      job: JobSpec(key: key, name: name, data: data),
      createdAt: t,
      availableAt: t,
    ),
  );
  final claimed = await backend.claimJob(
    ClaimJobRequest(queueName: key.queueName, workerId: workerId, claimedAt: t),
  );
  return (claimed.value!.record, claimed.value!.credential);
}

void main() {
  test(
    '更新 progress 会保留 execution，并可追加日志后完成',
    () async {
      final backend = _backend();
      const key = JobKey(queueName: 'q', jobId: 'j1');
      final (claimed, credential) = await _claim(
        backend: backend,
        key: key,
        name: 'do-work',
        data: const <String, Object?>{'a': 1},
        workerId: 'w1',
        t: _t,
      );
      expect(claimed.execution, isNotNull);

      final p1 = await backend.updateJobProgress(
        UpdateJobProgressRequest(
          key: key,
          expectedRevision: claimed.revision,
          credential: credential,
          occurredAt: _t,
          progress: 0.5,
        ),
      );
      expect(p1.changed, isTrue);
      expect(p1.value.progress, 0.5);
      expect(p1.value.execution, isNotNull, reason: 'progress 后 execution 仍存在');
      expect(
        p1.value.execution!.credential.executionId,
        credential.executionId,
      );

      final done = await backend.transitionJob(
        TransitionJobRequest(
          key: key,
          expectedRevision: p1.value.revision,
          credential: credential,
          targetStatus: JobStatus.completed,
          result: const <String, Object?>{'ok': true},
          occurredAt: _t,
        ),
      );
      expect(done.value.status, JobStatus.completed);
      expect(done.value.execution, isNull);
    },
  );

  test('updateData 接受 null，且不会丢失 execution', () async {
    final backend = _backend();
    const key = JobKey(queueName: 'q', jobId: 'j2');
    final (claimed, credential) = await _claim(
      backend: backend,
      key: key,
      name: 'clear-data',
      data: const <String, Object?>{'keep': 'me'},
      workerId: 'w1',
      t: _t,
    );
    final upd = await backend.updateJobData(
      UpdateJobDataRequest(
        key: key,
        expectedRevision: claimed.revision,
        credential: credential,
        occurredAt: _t,
        data: null,
      ),
    );
    expect(upd.value.data, isNull);
    expect(upd.value.execution, isNotNull);
  });

  test('重复发出相同的 active control 请求时状态不会改变', () async {
    final backend = _backend();
    const key = JobKey(queueName: 'q', jobId: 'j3');
    final (claimed, _) = await _claim(
      backend: backend,
      key: key,
      name: 'repeat-control',
      data: null,
      workerId: 'w1',
      t: _t,
    );
    final first = await backend.requestJobControl(
      RequestJobControlRequest(
        key: key,
        expectedRevision: claimed.revision,
        action: JobControlAction.pause,
        requestedAt: _t,
      ),
    );
    expect(first.changed, isTrue);

    final second = await backend.requestJobControl(
      RequestJobControlRequest(
        key: key,
        expectedRevision: first.value.revision,
        action: JobControlAction.pause,
        requestedAt: _t,
      ),
    );
    expect(second.changed, isFalse);
    expect(second.value.revision, first.value.revision);
  });

  test(
    'cancel 会升级 pause 请求，但 pause 不能降级 cancel 请求',
    () async {
      final backend = _backend();
      const key = JobKey(queueName: 'q', jobId: 'j4');
      final (claimed, _) = await _claim(
        backend: backend,
        key: key,
        name: 'upgrade-control',
        data: null,
        workerId: 'w1',
        t: _t,
      );
      final first = await backend.requestJobControl(
        RequestJobControlRequest(
          key: key,
          expectedRevision: claimed.revision,
          action: JobControlAction.pause,
          requestedAt: _t,
        ),
      );
      final second = await backend.requestJobControl(
        RequestJobControlRequest(
          key: key,
          expectedRevision: first.value.revision,
          action: JobControlAction.cancel,
          requestedAt: _t,
        ),
      );
      expect(second.changed, isTrue);

      final third = await backend.requestJobControl(
        RequestJobControlRequest(
          key: key,
          expectedRevision: second.value.revision,
          action: JobControlAction.pause,
          requestedAt: _t,
        ),
      );
      expect(third.changed, isFalse);
      expect(third.value.revision, second.value.revision);
      expect(third.value.controlRequest?.action, JobControlAction.cancel);
    },
  );

  test('恢复后，过期 credential 无法再写入', () async {
    final backend = _backend();
    const key = JobKey(queueName: 'q', jobId: 'j5');
    final (claimed, oldCredential) = await _claim(
      backend: backend,
      key: key,
      name: 'stale-cred',
      data: null,
      workerId: 'w1',
      t: _t,
    );
    await backend.recoverStalledJobs(
      RecoverStalledJobsRequest(queueName: 'q', now: _t),
    );
    final recovered = await backend.getJob(key);
    expect(recovered!.execution, isNull);
    expect(recovered.status, JobStatus.waiting);
    expect(
      () => backend.updateJobProgress(
        UpdateJobProgressRequest(
          key: key,
          expectedRevision: recovered.revision,
          credential: oldCredential,
          occurredAt: _t,
          progress: 0.5,
        ),
      ),
      throwsA(isA<LeaseLost>()),
    );
  });
}
