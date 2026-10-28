import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

/// 构造 [QueueBackendTestHarness] 实例的工厂函数类型。
typedef QueueBackendHarnessFactory = Future<QueueBackendTestHarness> Function();

/// Backend 契约测试的测试夹具接口。
///
/// 为每个 Backend 实现提供统一的测试入口：可推进的时钟、可预测的 ID 生成器、
/// 打开/重开 Backend 的能力，以及分布式场景下打开 peer Backend 的能力。
/// 所有 Backend 契约测试都通过此接口获取被测对象，避免测试直接依赖具体实现。
abstract interface class QueueBackendTestHarness {
  /// 提供当前时间的函数引用，测试可通过 [advance] 推进时钟。
  Now get now;

  /// 提供单调递增唯一 ID 的函数引用。
  IdGenerator get idGenerator;

  /// 按给定时长推进内部时钟，所有 Backend 观察到的时间随之变化。
  void advance(Duration duration);

  /// 打开并返回主 Backend 实例。
  Future<QueueBackend> openBackend();

  /// 打开一个与主 Backend 共享持久事实的 peer 实例（仅分布式 Backend 需要支持）。
  Future<QueueBackend> openPeer();

  /// 关闭并重新打开主 Backend，用于验证持久化恢复行为。
  Future<QueueBackend> reopenBackend();

  /// 释放测试夹具持有的资源。
  Future<void> dispose();
}
