// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/events/index.dart' deferred as events;

void main() {
  test('events 模块入口可正常编译', () async {
    await events.loadLibrary();
  });
}
