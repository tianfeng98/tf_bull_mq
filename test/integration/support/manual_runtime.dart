import 'dart:async';

/// 手动驱动的运行时依赖实现，用于集成测试中控制时间和延迟。
///
/// 统一提供 `Now` / `IdGenerator` / `Delay` 三个函数签名的可 mock 版本：
/// - [now] 返回可手动推进的当前时间；
/// - [nextId] 返回自增前缀 id；
/// - [delay] 不真正等待，而是挂起为 [pendingDelays]，测试代码通过 [completeNext]
///   或 [completeAll] 主动释放，或通过 [flushMicrotasks] 让微任务队列出队。
///
/// 这套手动时钟机制确保集成测试在纯同步可控的时间线上运行，不依赖 wall clock。
final class ManualRuntime {
  ManualRuntime() : _now = DateTime.utc(2026, 9, 22);

  DateTime _now;
  int _nextId = 0;
  final List<({Duration duration, Completer<void> completer})> _pending = [];

  /// 返回当前手动时钟的时间戳。
  DateTime now() => _now;

  /// 返回带前缀的自增 id，每次调用加一。
  String nextId() => 'integration-${++_nextId}';

  /// 将手动时钟向前推进指定时长；不支持负值。
  void advance(Duration duration) {
    if (duration.isNegative) {
      throw ArgumentError.value(duration, 'duration');
    }
    _now = _now.add(duration);
  }

  /// 注册一个挂起的 delay，等待测试代码手动释放。
  Future<void> delay(Duration duration) {
    final completer = Completer<void>();
    _pending.add((duration: duration, completer: completer));
    return completer.future;
  }

  /// 当前所有挂起 delay 的时长快照（只读）。
  List<Duration> get pendingDelays =>
      List<Duration>.unmodifiable(_pending.map((entry) => entry.duration));

  /// 释放下一个匹配指定时长的挂起 delay；不存在则抛 StateError。
  void completeNext(Duration duration) {
    final index = _pending.indexWhere((entry) => entry.duration == duration);
    if (index < 0) {
      throw StateError('No pending delay of $duration: $pendingDelays');
    }
    _pending.removeAt(index).completer.complete();
  }

  /// 释放所有挂起 delay，常用于 fixture close 时清理残留。
  void completeAll() {
    while (_pending.isNotEmpty) {
      _pending.removeAt(0).completer.complete();
    }
  }

  /// 空转若干次微任务，让 pending 的 Future 有机会出队。
  Future<void> flushMicrotasks([int turns = 20]) async {
    for (var index = 0; index < turns; index++) {
      await Future<void>.value();
    }
  }
}

/// 为异步等待添加看门狗超时保护。
///
/// 当 [future] 超过 [timeout] 仍未完成时，抛 [TimeoutException] 并附带 [description]、
/// 最后一次的 [phase] 和 [snapshot] 上下文，便于诊断。
Future<T> withWatchdog<T>(
  String description,
  Future<T> future, {
  required String Function() phase,
  required Object? Function() snapshot,
  Duration timeout = const Duration(seconds: 5),
}) => future.timeout(
  timeout,
  onTimeout: () => throw TimeoutException(
    'Timed out waiting for $description; phase=${phase()}; '
    'snapshot=${snapshot()}',
    timeout,
  ),
);
