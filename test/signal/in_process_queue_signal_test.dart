import 'package:test/test.dart';
import 'package:tf_bull_mq/src/signal/index.dart';

void main() {
  // 验证 InProcessQueueSignal 的 scope 属性报告为 process。
  test('InProcessQueueSignal 报告 scope 为 process', () {
    expect(InProcessQueueSignal().scope.name, 'process');
  });

  // 验证同一队列的所有订阅者都会收到同一条 notify 的广播提示。
  test('一次 notify 会广播给同一队列的所有订阅者', () async {
    final signal = InProcessQueueSignal();
    var first = 0;
    var second = 0;
    final firstSubscription = signal.subscribe('work').listen((_) => first++);
    final secondSubscription = signal.subscribe('work').listen((_) => second++);

    await signal.notify('work');

    // 两个订阅者都各收到一次提示。
    expect(first, 1);
    expect(second, 1);
    await firstSubscription.cancel();
    await secondSubscription.cancel();
    await signal.close();
  });

  // 验证 Signal 不回放历史通知：订阅前发送的 notify 不会被补发。
  test('订阅前发出的 notify 不会被回放', () async {
    final signal = InProcessQueueSignal();
    await signal.notify('work');
    var count = 0;
    final subscription = signal.subscribe('work').listen((_) => count++);
    // 订阅后再 notify 一次，只应收到这一次。
    await signal.notify('work');
    expect(count, 1);
    await subscription.cancel();
    await signal.close();
  });

  // 验证生命周期门禁：close() 后调用 notify 应抛异常。
  test('close() 后调用 notify 抛异常', () async {
    final signal = InProcessQueueSignal();
    await signal.close();
    expect(signal.notify('work'), throwsA(isA<Object>()));
  });
}
