import 'package:tf_bull_mq/src/signal/index.dart';

/// 记录 per-queue notify/close 并支持失败注入的 QueueSignal fake。
final class FakeQueueSignal implements QueueSignal {
  FakeQueueSignal({this.scope = SignalScope.process});

  @override
  final SignalScope scope;

  final List<String> notifiedQueues = <String>[];
  final Map<String, (Object, StackTrace)> signalFailures =
      <String, (Object, StackTrace)>{};
  var closeCalls = 0;
  Object? closeError;

  @override
  Future<void> notify(String queueName) async {
    notifiedQueues.add(queueName);
    final failure = signalFailures[queueName];
    if (failure != null) {
      final (error, stackTrace) = failure;
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  @override
  Stream<void> subscribe(String queueName) {
    throw StateError('Unexpected QueueSignal.subscribe call');
  }

  @override
  Future<void> close() async {
    closeCalls++;
    if (closeError != null) throw closeError!;
  }
}
