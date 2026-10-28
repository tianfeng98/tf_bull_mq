// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/signal/index.dart' deferred as signal;

void main() {
  test('signal 模块入口可正常编译', () async {
    await signal.loadLibrary();
  });
}
