import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/queue/index.dart';

import 'support/queue_fixtures.dart';

/// 可识别堆栈：用于证明 in-flight 通知失败时保留的是原始堆栈。
final class _MarkerStackTrace implements StackTrace {
  const _MarkerStackTrace();

  @override
  String toString() => 'marker-stack-trace';
}

void main() {
  group('close 资源所有权', () {
    /// 验证默认（不 owned）模式下 Queue.close() 不触碰 Backend 和 Signal，
    /// 只把自身标记为 closed——资源所有权在外部。
    test('默认情况下，close 不会关闭共享的 backend 和 signal', () async {
      final QueueFixture fixture = QueueFixture();

      await fixture.queue.close();

      expect(fixture.queue.isClosed, isTrue);
      expect(fixture.backend.closeCalls, 0);
      expect(fixture.signal.closeCalls, 0);
    });

    /// 验证 ownsBackend=true 时 close 只关闭 Backend，Signal 保持原状。
    test('close 只会关闭自有 backend', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(ownsBackend: true),
      );

      await fixture.queue.close();

      expect(fixture.backend.closeCalls, 1);
      expect(fixture.signal.closeCalls, 0);
    });

    /// 验证 ownsSignal=true 时 close 只关闭 Signal，Backend 保持原状。
    test('close 只会关闭自有 signal', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(ownsSignal: true),
      );

      await fixture.queue.close();

      expect(fixture.signal.closeCalls, 1);
      expect(fixture.backend.closeCalls, 0);
    });

    /// 验证两个 owned 资源都需要关闭时，close 依次尝试两者，
    /// 并把第一个错误向上抛出（但第二个资源仍会被尝试关闭以避免泄露）。
    test(
      'close 会尝试关闭两个自有资源，并报告最先出现的关闭错误',
      () async {
        final StateError signalError = StateError('signal close failed');
        final QueueFixture signalFirst = QueueFixture(
          options: QueueOptions(ownsBackend: true, ownsSignal: true),
        );
        signalFirst.signal.closeError = signalError;

        await expectLater(
          signalFirst.queue.close(),
          throwsA(same(signalError)),
        );
        expect(signalFirst.signal.closeCalls, 1);
        expect(signalFirst.backend.closeCalls, 1);

        final StateError backendError = StateError('backend close failed');
        final QueueFixture backendFirst = QueueFixture(
          options: QueueOptions(ownsBackend: true, ownsSignal: true),
        );
        backendFirst.backend.closeError = backendError;

        await expectLater(
          backendFirst.queue.close(),
          throwsA(same(backendError)),
        );
        expect(backendFirst.signal.closeCalls, 1);
        expect(backendFirst.backend.closeCalls, 1);
      },
    );

    /// 验证 close 是幂等的：多次调用只会实际关闭一次 owned 资源，
    /// 第二次及后续调用返回同一个 close future。
    test('close 可重复调用', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(ownsBackend: true, ownsSignal: true),
      );

      await fixture.queue.close();
      await fixture.queue.close();

      expect(fixture.backend.closeCalls, 1);
      expect(fixture.signal.closeCalls, 1);
    });

    /// 验证并发 close 调用得到同一个 pending future——Queue 内部通过
    /// 单个 closeFuture 字段合并多次并发 close。
    test(
      '并发调用 close 时会返回同一个未完成的 close Future',
      () async {
        final QueueFixture fixture = QueueFixture();

        final Future<void> first = fixture.queue.close();
        final Future<void> second = fixture.queue.close();

        expect(identical(first, second), isTrue);
        await first;
      },
    );

    /// 验证多个并发 close 同时等待时，每个 owned 资源仍只关闭一次。
    test(
      '并发调用 close 时，每个自有资源均只会关闭一次',
      () async {
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(ownsBackend: true, ownsSignal: true),
        );

        await Future.wait<void>(<Future<void>>[
          fixture.queue.close(),
          fixture.queue.close(),
          fixture.queue.close(),
        ]);

        expect(fixture.backend.closeCalls, 1);
        expect(fixture.signal.closeCalls, 1);
      },
    );

    /// 验证 close 不触碰任何 Job 级 Backend 操作：不 remove、不 transition、
    /// 不 requestControl、不 setQueuePaused——只做资源关闭和状态标记。
    test('close 不会移除、取消、暂停或转移任何 Job', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(ownsBackend: true, ownsSignal: true),
      );
      final Job job = await fixture.attachJob(status: JobStatus.active);
      final JobSnapshot prior = job.snapshot;

      await fixture.queue.close();

      expect(fixture.backend.called('removeJob'), isFalse);
      expect(fixture.backend.called('transitionJob'), isFalse);
      expect(fixture.backend.called('requestJobControl'), isFalse);
      expect(fixture.backend.called('setQueuePaused'), isFalse);
      expect(
        fixture.backend.calls.where((String call) => call != 'getJob'),
        isEmpty,
      );
      expect(job.snapshot, same(prior));
      expect(job.snapshot.status, JobStatus.active);
    });
  });

  group('close 后的操作', () {
    /// 验证 Queue 关闭后，add/addBulk/getJob/query/getState/pause/resume 等所有公开操作
    /// 在触碰 Backend 之前就抛 LifecycleError，组件名标记为 'Queue'。
    test(
      'close 后对 Queue 执行操作会在访问 backend 前抛出 LifecycleError',
      () async {
        final QueueFixture fixture = QueueFixture();
        await fixture.queue.close();
        final List<String> callsAfterClose = List<String>.of(
          fixture.backend.calls,
        );

        Matcher lifecycle(String operation) => isA<LifecycleError>()
            .having(
              (LifecycleError error) => error.component,
              'component',
              'Queue',
            )
            .having(
              (LifecycleError error) => error.operation,
              'operation',
              operation,
            );

        expect(
          () => fixture.queue.add('task', data: null),
          throwsA(lifecycle('add')),
        );
        expect(
          () => fixture.queue.addBulk(<QueueJobDraft>[
            QueueJobDraft(name: 'task', data: null),
          ]),
          throwsA(lifecycle('addBulk')),
        );
        expect(
          () => fixture.queue.getJob('job-1'),
          throwsA(lifecycle('getJob')),
        );
        expect(() => fixture.queue.query(), throwsA(lifecycle('query')));
        expect(() => fixture.queue.getState(), throwsA(lifecycle('getState')));
        expect(() => fixture.queue.pause(), throwsA(lifecycle('pause')));
        expect(() => fixture.queue.resume(), throwsA(lifecycle('resume')));

        expect(fixture.backend.calls, callsAfterClose);
        expect(fixture.signal.notifiedQueues, isEmpty);
      },
    );

    /// 验证 Queue 关闭后，Job 句柄的 refresh/retry/cancel/resume/remove/queryLogs/clearLogs
    /// 同样在触碰 Backend 之前就抛 LifecycleError，operation 字段以 'job.' 前缀标识。
    test('Job 所属的 Queue 关闭后，对 Job 执行操作会在访问 backend 前抛出 LifecycleError', () async {
      final QueueFixture fixture = QueueFixture();
      final Job job = await fixture.attachJob();
      await fixture.queue.close();
      final List<String> callsAfterClose = List<String>.of(
        fixture.backend.calls,
      );

      Matcher lifecycle(String operation) => isA<LifecycleError>().having(
        (LifecycleError error) => error.operation,
        'operation',
        operation,
      );

      expect(() => job.refresh(), throwsA(lifecycle('job.refresh')));
      expect(() => job.retry(), throwsA(lifecycle('job.retry')));
      expect(() => job.cancel(), throwsA(lifecycle('job.cancel')));
      expect(() => job.resume(), throwsA(lifecycle('job.resume')));
      expect(() => job.remove(), throwsA(lifecycle('job.remove')));
      expect(() => job.queryLogs(), throwsA(lifecycle('job.queryLogs')));
      expect(() => job.clearLogs(), throwsA(lifecycle('job.clearLogs')));

      expect(fixture.backend.calls, callsAfterClose);
      expect(fixture.signal.notifiedQueues, isEmpty);
    });
  });

  group('close 与正在进行的操作', () {
    /// 验证 close 会先把 Queue 标记为 closed 以拒绝新请求，
    /// 但会等所有 in-flight mutation（包括 Backend 写入 + Signal 通知）完成后
    /// 才真正关闭 owned 资源。
    test(
      'close 正在等待进行中的变更时会拒绝新操作',
      () async {
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(ownsBackend: true, ownsSignal: true),
        );
        fixture.queueState = fixture.state(revision: 1);
        final Completer<BackendWriteResult<JobRecord>> addGate =
            Completer<BackendWriteResult<JobRecord>>();
        fixture.backend.responder = (String method, Object? argument) {
          if (method == 'addJob') {
            return addGate.future;
          }
          throw StateError('unexpected $method');
        };

        final Future<Job> pending = fixture.queue.add('task', data: null);
        await Future<void>.delayed(Duration.zero);
        expect(fixture.backend.called('addJob'), isTrue);

        final Future<void> closeFuture = fixture.queue.close();
        var closeCompleted = false;
        closeFuture.then<void>((_) => closeCompleted = true);
        await Future<void>.delayed(Duration.zero);

        expect(fixture.queue.isClosed, isTrue);
        expect(closeCompleted, isFalse);
        expect(fixture.backend.closeCalls, 0);
        expect(fixture.signal.closeCalls, 0);
        expect(() => fixture.queue.query(), throwsA(isA<LifecycleError>()));

        addGate.complete(
          fixture.writeResult(
            fixture.record(jobId: 'generated-1'),
            queuesToNotify: <String>{'work'},
          ),
        );
        await pending;
        await closeFuture;

        expect(fixture.backend.closeCalls, 1);
        expect(fixture.signal.closeCalls, 1);
      },
    );

    /// 验证 close 在 Signal 通知仍进行中时，不会提前关闭 owned 资源——
    /// 必须等通知完成后才会真正调用 close。
    test(
      'close 会等待通知完成后再关闭自有资源',
      () async {
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(ownsBackend: true, ownsSignal: true),
        );
        fixture.queueState = fixture.state(revision: 1);
        fixture.backend.respond(
          'addJob',
          fixture.writeResult(
            fixture.record(jobId: 'generated-1'),
            queuesToNotify: <String>{'work'},
          ),
        );
        final Completer<void> notifyGate = Completer<void>();
        fixture.signal.notifyResponder = (String queueName) =>
            notifyGate.future;

        final Future<Job> pending = fixture.queue.add('task', data: null);
        await Future<void>.delayed(Duration.zero);
        expect(fixture.signal.notifiedQueues, <String>['work']);

        final Future<void> closeFuture = fixture.queue.close();
        await Future<void>.delayed(Duration.zero);
        expect(fixture.signal.closeCalls, 0);
        expect(fixture.backend.closeCalls, 0);

        notifyGate.complete();
        await pending;
        await closeFuture;

        expect(fixture.signal.closeCalls, 1);
        expect(fixture.backend.closeCalls, 1);
      },
    );

    /// 验证即使某个通知已经快速失败，close 仍会等其他慢通知完成后才
    /// 关闭 owned 资源——确保不遗留任何正在进行中的异步工作。
    test(
      '某个通知快速失败后，close 仍会等待所有通知完成',
      () async {
        final QueueFixture fixture = QueueFixture(
          options: QueueOptions(ownsBackend: true, ownsSignal: true),
        );
        fixture.queueState = fixture.state(revision: 1);
        fixture.backend.respond(
          'addJob',
          fixture.writeResult(
            fixture.record(jobId: 'generated-1'),
            queuesToNotify: <String>{'alpha', 'beta'},
          ),
        );
        const StackTrace marker = _MarkerStackTrace();
        final StateError original = StateError('alpha failed');
        final Completer<void> betaGate = Completer<void>();
        fixture.signal.notifyResponder = (String queueName) {
          if (queueName == 'alpha') {
            Error.throwWithStackTrace(original, marker);
          }
          return betaGate.future;
        };

        final Future<Job> pending = fixture.queue.add('task', data: null);
        await Future<void>.delayed(Duration.zero);

        final Future<void> closeFuture = fixture.queue.close();
        var mutationSettled = false;
        pending.then<void>(
          (_) => mutationSettled = true,
          onError: (Object _) => mutationSettled = true,
        );
        await Future<void>.delayed(Duration.zero);

        expect(fixture.signal.notifiedQueues, <String>['alpha', 'beta']);
        expect(mutationSettled, isFalse);
        expect(fixture.signal.closeCalls, 0);

        betaGate.complete();
        await expectLater(
          pending,
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
        await closeFuture;

        expect(fixture.signal.closeCalls, 1);
        expect(fixture.backend.closeCalls, 1);
      },
    );

    /// 验证 in-flight mutation 的 Backend 错误由调用方捕获，
    /// close 不会因为 mutation 失败而失败——各自的错误路径独立。
    test('正在进行的变更错误仍会返回给调用方，且不会导致 close 失败', () async {
      final QueueFixture fixture = QueueFixture(
        options: QueueOptions(ownsBackend: true, ownsSignal: true),
      );
      fixture.queueState = fixture.state(revision: 1);
      final StateError backendError = StateError('backend exploded');
      final Completer<BackendWriteResult<JobRecord>> addGate =
          Completer<BackendWriteResult<JobRecord>>();
      fixture.backend.responder = (String method, Object? argument) {
        if (method == 'addJob') {
          return addGate.future;
        }
        throw StateError('unexpected $method');
      };

      final Future<Job> pending = fixture.queue.add('task', data: null);
      await Future<void>.delayed(Duration.zero);
      final Future<void> closeFuture = fixture.queue.close();

      addGate.completeError(backendError, StackTrace.current);

      await expectLater(pending, throwsA(same(backendError)));
      await closeFuture;

      expect(fixture.backend.closeCalls, 1);
      expect(fixture.signal.closeCalls, 1);
      expect(fixture.signal.notifiedQueues, isEmpty);
    });
  });
}
