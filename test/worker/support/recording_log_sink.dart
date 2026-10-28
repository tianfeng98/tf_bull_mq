import 'dart:async';

import 'package:tf_bull_mq/src/observability/index.dart';

/// 收集 entry，或按需同步抛错 / 返回失败 Future 的 `WorkerLogSink` 测试替身。
///
/// 无论是否注入故障，entry 都会被先记录，便于断言「日志内容正确」与「sink 故障
/// 不影响业务结果」两件事互不干扰。
final class RecordingLogSink implements WorkerLogSink {
  RecordingLogSink({
    this.throwSynchronously = false,
    this.failAsynchronously = false,
  });

  /// `true` 时 `write` 同步抛错。
  bool throwSynchronously;

  /// `true` 时 `write` 返回失败的 Future。
  bool failAsynchronously;

  final List<WorkerLogEntry> entries = <WorkerLogEntry>[];

  /// 按 operation 过滤 entry。
  List<WorkerLogEntry> forOperation(String operation) => entries
      .where((WorkerLogEntry entry) => entry.context.operation == operation)
      .toList();

  /// 按 level 过滤 entry。
  List<WorkerLogEntry> forLevel(WorkerLogLevel level) =>
      entries.where((WorkerLogEntry entry) => entry.level == level).toList();

  @override
  FutureOr<void> write(WorkerLogEntry entry) {
    entries.add(entry);
    if (throwSynchronously) {
      throw StateError('log sink failed synchronously');
    }
    if (failAsynchronously) {
      return Future<void>.error(StateError('log sink failed asynchronously'));
    }
  }
}
