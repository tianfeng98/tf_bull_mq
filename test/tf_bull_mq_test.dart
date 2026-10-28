// ignore_for_file: unused_import

import 'dart:io';

import 'package:test/test.dart';
import 'package:tf_bull_mq/tf_bull_mq.dart' deferred as tf_bull_mq;

void main() {
  // 延迟加载根库，验证 package root 能正常编译并被解析。
  test('根库可正常编译并加载', () async {
    await tf_bull_mq.loadLibrary();
  });

  // 验证根库只导出各模块的 index.dart，不直接暴露内部实现文件，
  // 强制外部调用方通过模块边界引用，保持架构依赖方向。
  test('根库只导出各模块的 index.dart', () {
    final source = File('lib/tf_bull_mq.dart').readAsStringSync();

    expect(source, '''library;

export 'src/backend/index.dart';
export 'src/core/index.dart';
export 'src/events/index.dart';
export 'src/flow/index.dart';
export 'src/memory_backend/index.dart';
export 'src/observability/index.dart';
export 'src/queue/index.dart';
export 'src/signal/index.dart';
export 'src/utils/index.dart';
export 'src/worker/index.dart';
''');
  });
}
