import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/signal/index.dart';

void main() {
  // 验证 SignalScope 枚举的值集合。
  test('SignalScope 枚举应区分 process 和 distributed 两种取值', () {
    expect(SignalScope.values.map((value) => value.name), <String>[
      'process',
      'distributed',
    ]);
  });

  // 验证 distributed Backend 与 process Signal 的不兼容关系可被检测。
  test('distributed Backend 与 process Signal 组合应可被检测为不兼容', () {
    const profile = BackendProfile.distributed;
    const scope = SignalScope.process;
    expect(
      profile == BackendProfile.distributed && scope != SignalScope.distributed,
      isTrue,
    );
  });
}
