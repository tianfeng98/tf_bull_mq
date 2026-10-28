// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/worker/index.dart' deferred as worker;

void main() {
  test('worker 模块入口可正常编译', () async {
    await worker.loadLibrary();
  });
}
