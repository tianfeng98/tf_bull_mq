// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/queue/index.dart' deferred as queue;

void main() {
  test('queue 模块入口可正常编译', () async {
    await queue.loadLibrary();
  });
}
