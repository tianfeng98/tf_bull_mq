// ignore_for_file: prefer_initializing_formals

import '../backend/index.dart';
import '../core/index.dart';
import '../signal/index.dart';
import 'flow_job.dart';
import 'flow_notification_failure.dart';

/// 原子创建 Flow 的高层入口。
///
/// [FlowProducer] 接收一个已经成功 `open()` 的 [QueueBackend] 和
/// [QueueSignal]，以及一个 [Now] 时间源。它不调用 Backend/Signal
/// 的 `open()`，Backend 未打开时由阶段 01 契约抛 [LifecycleError]。
///
/// Producer 在 [add] 时依次完成 capability 预检、嵌套树校验，然后
/// 只调用一次 `backend.addFlow`，成功后按 Backend 返回的
/// `queuesToNotify` 通知对应队列；Signal 失败不回滚 Backend 写入，
/// 而是抛 [FlowNotificationFailure] 携带已提交的快照。
///
/// [codecs] 用于把业务 TData 编码为 Backend 原始 JSON-like；
/// 省略时使用默认 JSON-like Codec（透传）。
final class FlowProducer<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  FlowProducer({
    required QueueBackend backend,
    required QueueSignal signal,
    Now now = defaultNow,
    bool ownsBackend = false,
    bool ownsSignal = false,
    JobPayloadCodecs<TData, TProgress, TResult>? codecs,
  }) : _backend = backend,
       _signal = signal,
       _now = now,
       _ownsBackend = ownsBackend,
       _ownsSignal = ownsSignal,
       _codecs =
           codecs ?? JobPayloadCodecs<TData, TProgress, TResult>.jsonLike() {
    // distributed Backend 与 process Signal 不兼容
    if (backend.capabilities.profile == BackendProfile.distributed &&
        signal.scope == SignalScope.process) {
      throw UnsupportedCapability(
        'distributedSignal',
        'A distributed Backend requires a distributed Signal.',
      );
    }
  }

  final QueueBackend _backend;
  final QueueSignal _signal;
  final Now _now;
  final bool _ownsBackend;
  final bool _ownsSignal;
  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;
  var _closed = false;

  /// 预检 capability 和嵌套树，然后原子创建 Flow。
  ///
  /// 任何预检失败（atomicFlows、delayedJobs、priorities、嵌套树非法）
  /// 都不读取 [Now]、不调用 Backend、不调用 Signal。Backend 写入成功
  /// 后仅按 `queuesToNotify` 升序通知，Signal 失败不回滚，最终抛
  /// [FlowNotificationFailure] 或正常返回 [FlowSnapshot]。
  ///
  /// 业务 TData 通过 codecs.data 编码为 JSON-like。
  Future<FlowSnapshot> add(FlowJob<TData> definition) async {
    _ensureOpen('add');
    final capabilities = _backend.capabilities;
    if (!capabilities.atomicFlows) {
      throw UnsupportedCapability(
        'atomicFlows',
        'FlowProducer requires atomic Flow creation.',
      );
    }
    // FlowJob.toFlowJobSpec 内部会用 codecs.data.encodeNullable 编码
    final flow = definition.toFlowJobSpec(encode: _codecs.data.encodeNullable);
    for (final job in flow.jobs) {
      if (job.options.delay > Duration.zero && !capabilities.delayedJobs) {
        throw UnsupportedCapability(
          'delayedJobs',
          'A Flow Job requires delayed Job support.',
        );
      }
      if (job.options.priority > 0 && !capabilities.priorities) {
        throw UnsupportedCapability(
          'priorities',
          'A Flow Job requires priority support.',
        );
      }
    }
    final result = await _backend.addFlow(
      AddFlowRequest(flow: flow, createdAt: _now()),
    );
    return await _notifyAndReturn(result);
  }

  Future<FlowSnapshot> _notifyAndReturn(
    BackendWriteResult<FlowSnapshot> result,
  ) async {
    final queues = result.queuesToNotify.toList()..sort();
    final failures = <FlowNotificationError>[];
    for (final queueName in queues) {
      try {
        await _signal.notify(queueName);
      } catch (error, stackTrace) {
        failures.add(
          FlowNotificationError(
            queueName: queueName,
            error: error,
            stackTrace: stackTrace,
          ),
        );
      }
    }
    if (failures.isNotEmpty) {
      throw FlowNotificationFailure(
        committedFlow: result.value,
        failures: failures,
      );
    }
    return result.value;
  }

  /// 幂等关闭 Producer。
  ///
  /// 仅在构造时显式声明 [ownsBackend] / [ownsSignal] 时才会释放对应
  /// 资源；共享资源保持打开。首次关闭尝试两个 owned 资源，第一个
  /// 失败时仍继续关闭第二个，最终抛出第一个 error 并保留 stack trace。
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    Object? firstError;
    StackTrace? firstStack;

    if (_ownsSignal) {
      try {
        await _signal.close();
      } catch (error, stackTrace) {
        firstError ??= error;
        firstStack ??= stackTrace;
      }
    }
    if (_ownsBackend) {
      try {
        await _backend.close();
      } catch (error, stackTrace) {
        firstError ??= error;
        firstStack ??= stackTrace;
      }
    }
    if (firstError != null) {
      Error.throwWithStackTrace(firstError, firstStack!);
    }
  }

  void _ensureOpen(String operation) {
    if (_closed) {
      throw LifecycleError(
        'FlowProducer',
        operation,
        'Cannot $operation a Flow after FlowProducer.close().',
      );
    }
  }
}
