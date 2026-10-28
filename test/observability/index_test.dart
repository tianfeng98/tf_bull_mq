// ignore_for_file: unused_import

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/observability/index.dart'
    deferred as observability;

void main() {
  test('observability 模块入口可正常编译', () async {
    await observability.loadLibrary();
  });
}
