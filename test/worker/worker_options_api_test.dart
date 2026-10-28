import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';
import 'package:tf_bull_mq/src/observability/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';
import 'package:tf_bull_mq/src/worker/index.dart';
import 'package:tf_bull_mq/src/worker/worker_options.dart'
    show validateWorkerOptions;

import 'support/manual_runtime.dart';

/// 同步 Processor：用于固定 `FutureOr<Object?>` 的同步返回值签名。
Object? synchronousProcessor(ActiveJob job) => <String, Object?>{'ok': true};

/// 异步 Processor：用于固定 `FutureOr<Object?>` 的异步分支。
Future<Object?> asynchronousProcessor(ActiveJob job) async => null;

/// 只做编译期签名固定的辅助函数：每个 tear-off 都必须在类型上匹配冻结签名。
void verifyWorkerApi(Worker worker, Processor processor) {
  final FutureOr<Object?> Function(ActiveJob) processorSignature = processor;
  final Future<void> Function() start = worker.start;
  final Future<void> Function() pause = worker.pause;
  final Future<void> Function() resume = worker.resume;
  final Future<void> Function() close = worker.close;
  final Future<void> Function() forceClose = worker.forceClose;
  final WorkerState state = worker.state;
  final int activeCount = worker.activeCount;
  final String queueName = worker.queueName;
  final String workerId = worker.workerId;
  final WorkerOptions options = worker.options;
  expect(<Object>[
    processorSignature,
    start,
    pause,
    resume,
    close,
    forceClose,
    state,
    activeCount,
    queueName,
    workerId,
    options,
  ], hasLength(11));
}

/// 固定 ActiveJob 的公开成员：类型不匹配即编译失败。
void verifyActiveJobApi(ActiveJob job) {
  final JobRecord record = job.rawRecord;
  final JobKey key = job.key;
  final String name = job.name;
  final Object? data = job.data;
  final Object? progress = job.progress;
  final int attempt = job.attempt;
  final ActiveJobControlState controlState = job.controlState;
  final Stream<ActiveJobControlState> controlStream = job.controlStream;
  final Future<JobRecord> Function(Object?) updateProgress = job.updateProgress;
  final Future<JobRecord> Function(Object?) updateData = job.updateData;
  final Future<JobLog> Function(JobLogDraft) log = job.log;
  final Future<JobRecord> Function() confirmPaused = job.confirmPaused;
  final Future<JobRecord> Function() confirmCanceled = job.confirmCanceled;
  final Future<Map<JobKey, Object?>> Function() dependencies =
      job.getDependenciesResult;
  expect(<Object?>[
    record,
    key,
    name,
    data,
    progress,
    attempt,
    controlState,
    controlStream,
    updateProgress,
    updateData,
    log,
    confirmPaused,
    confirmCanceled,
    dependencies,
  ], hasLength(14));
}

Worker buildWorker({
  ManualRuntime? runtime,
  WorkerOptions options = const WorkerOptions(),
  String queueName = 'emails',
  IdGenerator? idGenerator,
  Processor? processor,
  QueueBackend? backend,
  QueueSignal? signal,
  WorkerLogSink? logSink,
  bool ownsBackend = false,
  bool ownsSignal = false,
}) {
  final clock = runtime ?? ManualRuntime();
  return Worker(
    queueName: queueName,
    backend:
        backend ??
        MemoryQueueBackend(now: clock.now, idGenerator: clock.nextId),
    signal: signal ?? InProcessQueueSignal(),
    processor: processor ?? (ActiveJob job) => null,
    options: options,
    logSink: logSink ?? SilentWorkerLogSink(),
    now: clock.now,
    idGenerator: idGenerator ?? clock.nextId,
    delay: clock.delay,
    ownsBackend: ownsBackend,
    ownsSignal: ownsSignal,
  );
}

/// 不收集任何内容的 Worker 运行日志 sink，用于只关心 Worker 状态的测试。
final class SilentWorkerLogSink implements WorkerLogSink {
  @override
  FutureOr<void> write(WorkerLogEntry entry) {}
}

void main() {
  // --- API 签名冻结：保证 Processor 和 Worker 暴露的公开类型签名稳定 ---

  test('Processor 使用固定的 FutureOr 签名', () {
    // 验证 Processor 在编译期被固定为 `FutureOr<Object?> Function(ActiveJob)`，同步和异步分支都能赋值给同一类型
    final Processor synchronous = synchronousProcessor;
    final Processor asynchronous = asynchronousProcessor;
    final FutureOr<Object?> Function(ActiveJob) signature = synchronous;
    expect(signature, isA<FutureOr<Object?> Function(ActiveJob)>());
    expect(<Processor>[synchronous, asynchronous], hasLength(2));
  });

  // --- WorkerOptions 默认值与校验：配置对象的稳定默认值和前置条件检查 ---

  test('WorkerOptions 暴露稳定的默认值', () {
    // 验证 WorkerOptions 的默认值稳定：workerId=null, concurrency=1, pollInterval=1s, gracefulTimeout=30s, 无租约配置
    const options = WorkerOptions();
    expect(options.workerId, isNull);
    expect(options.concurrency, 1);
    expect(options.pollInterval, const Duration(seconds: 1));
    expect(options.gracefulTimeout, const Duration(seconds: 30));
    expect(options.leaseDuration, isNull);
    expect(options.renewInterval, isNull);
  });

  test('WorkerOptions 会拒绝空 workerId 和小于 1 的 concurrency', () {
    // 验证空 workerId 和 concurrency<1 在构造期被 AssertionError 拒绝
    expect(() => WorkerOptions(workerId: ''), throwsA(isA<AssertionError>()));
    expect(() => WorkerOptions(concurrency: 0), throwsA(isA<AssertionError>()));
    expect(
      () => WorkerOptions(workerId: 'w', concurrency: -1),
      throwsA(isA<AssertionError>()),
    );
  });

  test('WorkerOptions 要求 poll 和 graceful 时长均为正数', () {
    // 验证 pollInterval 和 gracefulTimeout 必须为正 Duration，否则 validate 和 Worker 构造都以 ArgumentError 拒绝
    expect(
      () => validateWorkerOptions(WorkerOptions(pollInterval: Duration.zero)),
      throwsArgumentError,
    );
    expect(
      () => validateWorkerOptions(
        WorkerOptions(pollInterval: const Duration(microseconds: -1)),
      ),
      throwsArgumentError,
    );
    expect(
      () =>
          validateWorkerOptions(WorkerOptions(gracefulTimeout: Duration.zero)),
      throwsArgumentError,
    );
    expect(
      () => validateWorkerOptions(
        WorkerOptions(gracefulTimeout: const Duration(microseconds: -1)),
      ),
      throwsArgumentError,
    );
    // Worker 构造必须在建立实例前以 ArgumentError 拒绝同一批无效配置。
    expect(
      () => buildWorker(options: WorkerOptions(pollInterval: Duration.zero)),
      throwsArgumentError,
    );
    expect(
      () => buildWorker(options: WorkerOptions(gracefulTimeout: Duration.zero)),
      throwsArgumentError,
    );
  });

  test('WorkerOptions 要求 lease 设置成对有效', () {
    // 验证租约配置必须成对出现，且 renewInterval < leaseDuration；单独一个或 renewInterval >= leaseDuration 都被拒绝
    expect(
      () => WorkerOptions(leaseDuration: const Duration(seconds: 10)),
      throwsA(isA<AssertionError>()),
    );
    expect(
      () => WorkerOptions(renewInterval: const Duration(seconds: 5)),
      throwsA(isA<AssertionError>()),
    );
    expect(
      () => validateWorkerOptions(
        WorkerOptions(
          leaseDuration: const Duration(seconds: 10),
          renewInterval: const Duration(seconds: 10),
        ),
      ),
      throwsArgumentError,
    );
    expect(
      () => validateWorkerOptions(
        WorkerOptions(
          leaseDuration: const Duration(seconds: 10),
          renewInterval: const Duration(seconds: 11),
        ),
      ),
      throwsArgumentError,
    );
    expect(
      () => validateWorkerOptions(
        WorkerOptions(
          leaseDuration: const Duration(seconds: 10),
          renewInterval: Duration.zero,
        ),
      ),
      throwsArgumentError,
    );
    expect(
      validateWorkerOptions(
        WorkerOptions(
          leaseDuration: const Duration(seconds: 10),
          renewInterval: const Duration(seconds: 3),
        ),
      ).renewInterval,
      const Duration(seconds: 3),
    );
  });

  // --- workerId 生成与注入：自动生成与显式指定的行为区分 ---

  test('Worker 会且仅会生成一次非空 workerId', () {
    // 验证 Worker 首次访问 workerId 时调用 IdGenerator 生成非空 ID，后续访问不重复生成
    final runtime = ManualRuntime();
    var generated = 0;
    final worker = buildWorker(
      runtime: runtime,
      idGenerator: () {
        generated++;
        return runtime.nextId();
      },
    );
    expect(worker.workerId, 'worker-1');
    expect(worker.workerId, isNotEmpty);
    expect(worker.workerId, 'worker-1');
    expect(generated, 1);
    expect(buildWorker(runtime: runtime).workerId, 'worker-2');
  });

  test('Worker 会保留显式 workerId，且不会调用 IdGenerator', () {
    // 验证显式传入 workerId 时 Worker 不调用 IdGenerator，直接使用用户提供的值
    var generated = 0;
    final worker = buildWorker(
      options: const WorkerOptions(workerId: 'explicit-worker'),
      idGenerator: () {
        generated++;
        return 'generated-worker';
      },
    );
    expect(worker.workerId, 'explicit-worker');
    expect(generated, 0);
  });

  // --- Worker 构造与 index 导出：构造参数校验和模块导出面 ---

  test('Worker 构造函数会验证 queue name 和公共 API', () {
    // 验证空 queueName 被 ArgumentError 拒绝，构造后的 Worker 公开成员签名完整且初始状态为 created
    expect(() => buildWorker(queueName: ''), throwsArgumentError);
    final worker = buildWorker();
    verifyWorkerApi(worker, (ActiveJob job) => null);
    expect(worker.state, WorkerState.created);
    expect(worker.activeCount, 0);
  });

  test('worker index 仅导出公开的 Worker 接口', () {
    // 验证 worker/index.dart 只导出公开类型（ActiveJob、Processor、Worker、WorkerOptions 等），内部实现保持私有
    final source = File('lib/src/worker/index.dart').readAsStringSync();
    final exports = source
        .split('\n')
        .map((String line) => line.trim())
        .where((String line) => line.startsWith('export '))
        .toList();
    expect(exports, <String>[
      "export 'active_job.dart' show ActiveJob, ActiveJobControlState;",
      "export 'processor.dart' show Processor;",
      "export 'worker.dart' show Worker, WorkerState;",
      "export 'worker_options.dart' show WorkerOptions;",
    ]);
    for (final internal in <String>[
      'ActiveJobController',
      'ActiveJobInfrastructureFailure',
      'ActiveJobTerminalIntent',
      'createActiveJobController',
      'validateWorkerOptions',
    ]) {
      expect(
        source,
        isNot(contains(internal)),
        reason: '$internal must stay internal',
      );
    }
  });
}
