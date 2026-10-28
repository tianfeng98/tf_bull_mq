// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart' deferred as core;

void main() {
  /// 烟雾测试：通过 deferred import 验证 `src/core/index.dart` 能正常编译加载，
  /// 若模块导出有误或存在语法错误，这里会直接失败。
  test('core 模块入口可正常编译', () async {
    await core.loadLibrary();
  });
}
