import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart';

void main() {
  // 验证根库（package root）导出的所有公开类型和工厂函数均可正常访问，
  // 相当于对外 API 的烟雾测试。
  test('根库导出所有受支持的集成类型和工厂函数', () {
    final BackendCapabilities capabilities = BackendCapabilities(
      profile: BackendProfile.local,
      durability: Durability.volatile,
      delayedJobs: true,
      priorities: true,
      atomicFlows: true,
      eventDelivery: EventDelivery.liveOnly,
    );
    final MemoryQueueBackend Function({
      required Now now,
      required IdGenerator idGenerator,
    })
    backendConstructor = MemoryQueueBackend.new;
    final InProcessQueueSignal Function() signalConstructor =
        InProcessQueueSignal.new;
    FutureOr<Object?> processor(ActiveJob job) async => null;
    final FutureOr<Object?> Function(ActiveJob) processorSignature = processor;
    Future<void> delay() async {}
    final JobStatus status = JobStatus.waiting;
    final QueueSignal signal = signalConstructor();

    // 校验 BackendCapabilities 字段可读。
    expect(capabilities.atomicFlows, isTrue);
    expect(backendConstructor, isNotNull);
    expect(processorSignature, isNotNull);
    expect(delay, isNotNull);
    expect(status, JobStatus.waiting);
    expect(signal.scope, SignalScope.process);
    // 校验 Queue / Worker / QueueEvents / FlowProducer / FlowQuery 构造器可见。
    expect(Queue.new, isNotNull);
    expect(Worker.new, isNotNull);
    expect(QueueEvents.new, isNotNull);
    expect(FlowProducer.new, isNotNull);
    expect(FlowQuery.new, isNotNull);
  });
}
