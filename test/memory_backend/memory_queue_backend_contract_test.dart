import '../support/contracts/index.dart';
import '../support/memory_backend_harness.dart';

void main() {
  // 通过基类 contract 为 MemoryQueueBackend 批量执行契约测试。
  // 契约中定义了 QueueBackend 必须满足的通用行为约束。
  defineBaseBackendContract(
    'MemoryQueueBackend',
    () async => MemoryQueueBackendHarness(),
  );
}
