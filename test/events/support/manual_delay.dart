import 'dart:async';

import 'package:tf_bull_mq/src/core/runtime.dart';

/// 手动控制的 [Delay] 实现，不读真实时钟、不创建真实 Timer。
///
/// 测试向组件传入 [call] 作为 Delay 依赖后，可通过 [completeNext] / [completeAll] /
/// [cancelAll] 精确控制轮询循环的推进时机，同时通过 [requestedDurations] 断言
/// 组件传入的间隔值是否符合预期。所有 pending delay 在调用 cancel 或 close 时被丢弃。
final class ManualDelay {
  ManualDelay();

  /// 所有传入 [call] 的 Duration，按到达顺序记录。
  final List<Duration> requestedDurations = <Duration>[];

  final List<_ManualDelayTask> _tasks = <_ManualDelayTask>[];

  /// 当前尚未完成的 pending delay 数量。
  int get pendingCount => _tasks.length;

  /// 记录 [duration] 并返回一个 Future，该 Future 会在 [completeNext] 被调用时 resolve。
  Future<void> call(Duration duration) {
    requestedDurations.add(duration);
    final task = _ManualDelayTask();
    _tasks.add(task);
    return task.future;
  }

  /// 完成最早挂起的 delay。
  void completeNext() {
    if (_tasks.isEmpty) {
      throw StateError(
        'No pending Delay to complete — the poll loop has not been started or has exited.',
      );
    }
    final task = _tasks.removeAt(0);
    task.complete();
  }

  /// 完成所有挂起的 delay，清空队列。
  void completeAll() {
    for (final task in _tasks) {
      task.complete();
    }
    _tasks.clear();
  }

  /// 丢弃所有挂起的 delay，不触发 Future 完成。
  ///
  /// 模拟「stop before next delay completes」的场景，例如 close 时跳过剩余轮询。
  void cancelAll() {
    for (final task in _tasks) {
      task.cancel();
    }
    _tasks.clear();
  }
}

/// ManualDelay 内部的单条 pending delay 表示。
class _ManualDelayTask {
  _ManualDelayTask();

  final Completer<void> _completer = Completer<void>();
  bool _cancelled = false;

  Future<void> get future => _completer.future;

  void complete() {
    if (!_completer.isCompleted && !_cancelled) {
      _completer.complete();
    }
  }

  void cancel() {
    _cancelled = true;
  }
}
