// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/flow/index.dart' deferred as flow;

void main() {
  test('flow 模块入口可正常编译', () async {
    await flow.loadLibrary();
  });
}
