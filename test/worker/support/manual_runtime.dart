import 'dart:async';

/// 「待完成」的手工延迟条目。
///
/// 只保存请求的 [duration] 与一个尚未完成的 [completer]，不做任何时间推算。
final class ManualDelayEntry {
  ManualDelayEntry(this.duration);

  /// Worker 请求的等待时长，用于断言调用方使用了预期的间隔。
  final Duration duration;

  /// 由测试显式完成的等待。Worker 永远看不到任何真实计时器。
  final Completer<void> completer = Completer<void>();
}

/// 注入 Worker 的确定性测试运行时。
///
/// 同时提供三类运行时依赖：[now] 为可推进的假时钟，[nextId] 为递增 ID 生成器，
/// [delay] 为只能由测试推进的手工延迟。三者都不依赖 wall clock、`Timer` 或
/// `Future.delayed`，因此 Worker 的并发、续租和关闭测试完全可复现。
///
/// 使用约定：测试先用 [completeNext] / [completeAll] / [failNext] 推进延迟，
/// 再 `await runtime.flushMicrotasks()` 让被唤醒的异步流程推进到下一个稳定点。
final class ManualRuntime {
  ManualRuntime() : _now = DateTime.utc(2026, 1, 1);

  DateTime _now;
  int _generatedIdCount = 0;

  final List<ManualDelayEntry> _pending = <ManualDelayEntry>[];
  final List<Duration> _completed = <Duration>[];

  /// 注入 Worker 的时钟。起点固定为 2026-01-01T00:00:00Z，只能通过
  /// [advance] 前进，保证续租与到期判断完全确定。
  DateTime now() => _now;

  /// 推进假时钟。不会完成任何已登记的延迟。
  void advance(Duration duration) {
    if (duration.isNegative) {
      throw ArgumentError.value(duration, 'duration', 'must not be negative');
    }
    _now = _now.add(duration);
  }

  /// 注入 Worker 的 ID 生成器，依次返回 `worker-1`、`worker-2`……
  String nextId() => 'worker-${++_generatedIdCount}';

  /// 注入 Worker 的延迟实现。只登记条目并返回未完成的 Future。
  Future<void> delay(Duration duration) {
    final entry = ManualDelayEntry(duration);
    _pending.add(entry);
    return entry.completer.future;
  }

  /// 当前尚未完成、也尚未失败的延迟条目（只读快照）。
  List<ManualDelayEntry> get pendingDelays =>
      List<ManualDelayEntry>.unmodifiable(_pending);

  /// 已完成指定 [duration] 的延迟次数，用于断言循环没有重新登记或热循环。
  int completedCount(Duration duration) =>
      _completed.where((Duration value) => value == duration).length;

  /// 完成最早登记的、时长为 [duration] 的延迟。
  ///
  /// 若不存在匹配条目，立即抛出 [StateError] 让测试失败，避免测试因为等待
  /// 错误时长或根本不存在的延迟而静默挂起。
  void completeNext(Duration duration) {
    final index = _pending.indexWhere(
      (ManualDelayEntry entry) => entry.duration == duration,
    );
    if (index < 0) {
      throw StateError(
        'No pending delay of $duration; pending: $_pendingDurations',
      );
    }
    final entry = _pending.removeAt(index);
    _completed.add(entry.duration);
    entry.completer.complete();
  }

  /// 完成当前所有时长为 [duration] 的延迟（快照语义，不跟进此后新登记的延迟）。
  void completeAll(Duration duration) {
    final matches = _pending
        .where((ManualDelayEntry entry) => entry.duration == duration)
        .toList();
    if (matches.isEmpty) {
      throw StateError(
        'No pending delay of $duration; pending: $_pendingDurations',
      );
    }
    for (final entry in matches) {
      _pending.remove(entry);
      _completed.add(entry.duration);
      entry.completer.complete();
    }
  }

  /// 让最早登记的、时长为 [duration] 的延迟以 [error] 失败。
  void failNext(Duration duration, [Object? error]) {
    final index = _pending.indexWhere(
      (ManualDelayEntry entry) => entry.duration == duration,
    );
    if (index < 0) {
      throw StateError(
        'No pending delay of $duration; pending: $_pendingDurations',
      );
    }
    final entry = _pending.removeAt(index);
    entry.completer.completeError(
      error ?? StateError('manual delay of $duration failed'),
      StackTrace.current,
    );
  }

  /// 让所有时长为 [duration] 的延迟失败。
  void failAll(Duration duration, [Object? error]) {
    final matches = _pending
        .where((ManualDelayEntry entry) => entry.duration == duration)
        .toList();
    if (matches.isEmpty) {
      throw StateError(
        'No pending delay of $duration; pending: $_pendingDurations',
      );
    }
    for (final entry in matches) {
      _pending.remove(entry);
      entry.completer.completeError(
        error ?? StateError('manual delay of $duration failed'),
        StackTrace.current,
      );
    }
  }

  /// 只连续 await 固定次数的已解决 Future，不等待真实时间，也不依赖
  /// 无边界的 `pumpEventQueue`。
  Future<void> flushMicrotasks({int turns = 12}) async {
    for (var index = 0; index < turns; index++) {
      await Future<void>.value();
    }
  }

  String get _pendingDurations => _pending
      .map((ManualDelayEntry entry) => entry.duration.toString())
      .join(', ');
}
