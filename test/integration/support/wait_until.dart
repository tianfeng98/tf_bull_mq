import 'dart:async';

import 'package:test/test.dart';

/// 轮询等待直到 [condition] 成立或超过 [deadline]。
///
/// 每 2ms 检查一次条件；超时后调用 `fail` 并附带 [snapshot] 提供最后一次状态，
/// 用于在没有 Signal 通知的情况下等待 Worker 或 backend 状态变化。
Future<void> waitUntil(
  String description,
  bool Function() condition, {
  required Object? Function() snapshot,
  Duration deadline = const Duration(milliseconds: 500),
}) async {
  final stopwatch = Stopwatch()..start();
  var lastState = snapshot();
  while (!condition()) {
    if (stopwatch.elapsed >= deadline) {
      fail('Timed out waiting for $description. Last state: $lastState');
    }
    await Future<void>.delayed(const Duration(milliseconds: 2));
    lastState = snapshot();
  }
}
