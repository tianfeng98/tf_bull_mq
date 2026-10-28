import 'dart:async';

import 'package:tf_bull_mq/src/signal/index.dart';

/// 可记录、可按 queue 失败、可吞掉通知进度的 [QueueSignal] fake。
///
/// 只实现冻结接口：不重试、不去重、不投递任何真实事件，
/// 仅用于精确观测“通知了哪些 queue”“通知是否已经启动/结束”。
final class FakeQueueSignal implements QueueSignal {
  /// 构造 fake；[scope] 用于构造期兼容性断言（process/distributed）。
  FakeQueueSignal({this.scope = SignalScope.process});

  /// 对外声明的信号作用域；测试可直接替换以验证 Queue 的构造校验。
  @override
  SignalScope scope;

  /// 已进入 notify 的 queue 名称，按进入顺序记录（用于断言“全部通知都已启动”）。
  final List<String> notifiedQueues = <String>[];

  /// 需要失败的 queue 集合；命中的 notify 在记录之后抛 [StateError]。
  final Set<String> failingQueues = <String>{};

  /// 可选通知过程控制：测试可用 Completer 精确控制延迟完成或快速失败。
  FutureOr<void> Function(String queueName)? notifyResponder;

  /// close 被调用的次数；用于断言 owned 资源只关闭一次。
  int closeCalls = 0;

  /// close 需要抛出的错误；为 null 表示正常关闭。
  Object? closeError;

  /// [closeError] 对应的原始堆栈。
  StackTrace? closeErrorStack;

  @override
  Future<void> notify(String queueName) async {
    notifiedQueues.add(queueName);
    final FutureOr<void> Function(String queueName)? responder =
        notifyResponder;
    if (responder != null) {
      await responder(queueName);
    }
    if (failingQueues.contains(queueName)) {
      throw StateError('signal notification failed for $queueName');
    }
  }

  @override
  Stream<void> subscribe(String queueName) =>
      const Stream<void>.empty().asBroadcastStream();

  @override
  Future<void> close() async {
    closeCalls += 1;
    final Object? error = closeError;
    if (error != null) {
      Error.throwWithStackTrace(error, closeErrorStack ?? StackTrace.current);
    }
  }
}
