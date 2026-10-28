import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

void main() {
  // 验证 MemoryQueueBackend 的 BackendCapabilities 声明：
  // local + volatile 形态 + 完整的阶段 02 特性（delayedJobs / priorities / atomicFlows）。
  test('MemoryQueueBackend 声明 local volatile 能力及阶段 02 的所有特性', () {
    final backend = MemoryQueueBackend(
      now: () => DateTime.utc(2026, 1, 1),
      idGenerator: () => 'id-1',
    );

    expect(backend.capabilities.profile, BackendProfile.local);
    expect(backend.capabilities.durability, Durability.volatile);
    expect(backend.capabilities.delayedJobs, isTrue);
    expect(backend.capabilities.priorities, isTrue);
    expect(backend.capabilities.atomicFlows, isTrue);
    expect(backend.capabilities.eventDelivery, EventDelivery.liveOnly);
  });

  // 验证 MemoryQueueBackend 的生命周期门禁：open() 前和 close() 后
  // 读写方法都应抛异常，防止使用方在错误生命周期状态下调用。
  test('open() 前和 close() 后调用读写方法均抛异常', () async {
    final backend = MemoryQueueBackend(
      now: () => DateTime.utc(2026, 1, 1),
      idGenerator: () => 'id-1',
    );
    const key = JobKey(queueName: 'mail', jobId: 'job-1');

    // open 之前调用 getJob 抛异常。
    await expectLater(backend.getJob(key), throwsA(isA<Object>()));
    await backend.open();
    await backend.close();
    // close 之后再次调用 getJob 抛异常。
    await expectLater(backend.getJob(key), throwsA(isA<Object>()));
  });
}
