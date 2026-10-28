import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

import 'contracts/backend_test_harness.dart';

/// 基于 [MemoryQueueBackend] 的契约测试夹具实现。
///
/// 利用 Memory Backend 纯进程内的特性，提供可推进的时钟和可预测 ID，
/// 使契约测试无需外部依赖即可运行。由于 Memory Backend 的状态不跨进程共享，
/// [openPeer] 直接抛 [UnsupportedError]。
final class MemoryQueueBackendHarness implements QueueBackendTestHarness {
  /// 构造时以固定时间点为起始时钟，ID 序列从 0 开始递增。
  MemoryQueueBackendHarness() : _clock = DateTime.utc(2026, 1, 1), _serial = 0 {
    backend = MemoryQueueBackend(now: now, idGenerator: idGenerator);
  }

  /// 当前模拟时钟，测试通过 [advance] 推进。
  DateTime _clock;

  /// 用于生成唯一 ID 的自增计数器。
  int _serial;

  /// 被测试的 [MemoryQueueBackend] 实例，构造时即创建但未 open。
  late final MemoryQueueBackend backend;

  @override
  Now get now =>
      () => _clock;

  @override
  IdGenerator get idGenerator => _nextId;

  /// 生成格式为 `memory-<自增序号>` 的唯一 ID。
  String _nextId() => 'memory-${++_serial}';

  @override
  void advance(Duration duration) {
    _clock = _clock.add(duration);
  }

  @override
  Future<QueueBackend> openBackend() async {
    await backend.open();
    return backend;
  }

  /// Memory Backend 不支持分布式 peer——进程内无共享状态。
  @override
  Future<QueueBackend> openPeer() => Future<QueueBackend>.error(
    UnsupportedError('Memory backend has process-local facts.'),
  );

  @override
  Future<QueueBackend> reopenBackend() async {
    await backend.close();
    await backend.open();
    return backend;
  }

  @override
  Future<void> dispose() => backend.close();
}
