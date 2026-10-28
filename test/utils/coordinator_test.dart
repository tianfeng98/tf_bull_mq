import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/utils/index.dart';

void main() {
  group('QueuedOperationCoordinator', () {
    test('按 FIFO 顺序串行执行，不重叠', () async {
      final coordinator = QueuedOperationCoordinator();
      final order = <String>[];
      final executing = <String>{};

      Future<void> make(String name) async {
        order.add('start:$name');
        expect(executing, isEmpty, reason: '上一个操作未完成时下一个不应启动');
        executing.add(name);
        await Future<void>.delayed(const Duration(milliseconds: 10));
        executing.remove(name);
        order.add('end:$name');
      }

      final a = coordinator.run(() => make('A'));
      final b = coordinator.run(() => make('B'));
      final c = coordinator.run(() => make('C'));

      await Future.wait([a, b, c]);
      expect(order, [
        'start:A',
        'end:A',
        'start:B',
        'end:B',
        'start:C',
        'end:C',
      ]);
    });

    test('单个操作失败后，后续排队操作仍能继续执行', () async {
      final coordinator = QueuedOperationCoordinator();
      final calls = <String>[];
      var failA = true;

      Future<void> opA() async {
        calls.add('A');
        if (failA) throw StateError('A failed');
      }

      Future<void> opB() async {
        calls.add('B');
      }

      final futureA = coordinator.run(opA);
      final futureB = coordinator.run(opB);

      await expectLater(futureA, throwsStateError);
      await futureB;
      expect(calls, ['A', 'B']);
    });

    test('失败操作的错误独立传播，不覆盖后续操作结果', () async {
      final coordinator = QueuedOperationCoordinator();
      var failFirst = true;
      var secondExecuted = false;

      final firstFuture = coordinator.run(() async {
        if (failFirst) throw StateError('first');
      });

      final secondFuture = coordinator.run(() async {
        secondExecuted = true;
      });

      await expectLater(firstFuture, throwsStateError);
      await secondFuture; // 不应抛错
      expect(secondExecuted, isTrue);
    });

    test('并发提交的操作按提交顺序排队', () async {
      final coordinator = QueuedOperationCoordinator();
      final gate = Completer<void>();
      final order = <int>[];

      // 第一个操作挂起
      coordinator.run(() async {
        order.add(1);
        await gate.future;
      });

      final results = <Future<void>>[];
      // 紧接着提交三个操作
      for (var i = 2; i <= 4; i++) {
        final captured = i;
        results.add(
          coordinator.run(() async {
            order.add(captured);
          }),
        );
      }

      // 释放第一个
      gate.complete();
      await Future.wait(results);

      expect(order, [1, 2, 3, 4]);
    });

    test('操作在返回 Future 前同步抛错也能被队列捕获，不阻塞后续', () async {
      final coordinator = QueuedOperationCoordinator();
      var secondExecuted = false;

      // 同步抛错的操作（不是在 Future 内部抛错）
      await expectLater(
        coordinator.run(() {
          throw StateError('sync throw');
        }),
        throwsStateError,
      );

      // 错误传播为失败的 Future 后，队列仍能继续工作
      await coordinator.run(() async {
        secondExecuted = true;
      });
      expect(secondExecuted, isTrue);
    });
  });

  group('SingleFlightOperationCoordinator', () {
    test('重叠调用共享同一个 Future', () async {
      final coordinator = SingleFlightOperationCoordinator();
      final gate = Completer<void>();
      var executionCount = 0;

      Future<void> op() async {
        executionCount++;
        await gate.future;
      }

      final a = coordinator.run(op);
      final b = coordinator.run(op);
      final c = coordinator.run(op);

      expect(a, same(b));
      expect(b, same(c));

      gate.complete();
      await Future.wait([a, b, c]);
      expect(executionCount, 1);
    });

    test('操作完成后，新调用启动新一轮执行', () async {
      final coordinator = SingleFlightOperationCoordinator();
      var executionCount = 0;

      Future<void> op() async {
        executionCount++;
      }

      await coordinator.run(op);
      expect(executionCount, 1);

      await coordinator.run(op);
      expect(executionCount, 2);
    });

    test('操作失败后，新调用仍可启动新一轮执行', () async {
      final coordinator = SingleFlightOperationCoordinator();
      var executionCount = 0;
      var fail = true;

      final first = coordinator.run(() async {
        executionCount++;
        if (fail) throw StateError('failed');
      });

      await expectLater(first, throwsStateError);
      expect(executionCount, 1);

      fail = false;
      await coordinator.run(() async {
        executionCount++;
      });
      expect(executionCount, 2);
    });

    test('并发调用都收到同一个错误', () async {
      final coordinator = SingleFlightOperationCoordinator();
      final gate = Completer<void>();

      Future<void> op() async {
        await gate.future;
        throw StateError('shared failure');
      }

      final a = coordinator.run(op);
      final b = coordinator.run(op);
      final c = coordinator.run(op);

      gate.complete();

      await Future.wait([
        expectLater(a, throwsStateError),
        expectLater(b, throwsStateError),
        expectLater(c, throwsStateError),
      ]);
    });

    test('操作在返回 Future 前同步抛错，run 也返回失败的 Future 而不是同步抛错', () async {
      final coordinator = SingleFlightOperationCoordinator();
      var nextExecuted = false;

      // 同步抛错：不应该同步抛到调用栈上，而应该被包装成失败的 Future
      final future = coordinator.run(() {
        throw StateError('sync throw in single-flight');
      });

      // 同步抛错已被 Future.sync 捕获，future 是一个 Future——await 能拿到错误
      await expectLater(future, throwsStateError);

      // 失败后仍可启动新一轮执行
      await coordinator.run(() async {
        nextExecuted = true;
      });
      expect(nextExecuted, isTrue);
    });

    test('同步抛错的操作不会产生未处理异步错误', () async {
      final errors = <Object>[];
      final coordinator = SingleFlightOperationCoordinator();

      // 在一个专门收集未处理异步错误的 Zone 里运行——
      // 如果有 unhandled async error 泄漏出来，会被收集到 errors 里。
      await runZonedGuarded<Future<void>>(
        () async {
          final future = coordinator.run(() {
            throw StateError('sync throw');
          });
          // 正常 await 拿到错误（调用方正确处理的情况）
          await expectLater(future, throwsStateError);
        },
        (error, stackTrace) {
          errors.add(error);
        },
      );

      // 没有任何 unhandled async error 泄漏
      expect(
        errors,
        isEmpty,
        reason:
            '同步抛错应该被 Future.sync + try/catch 完全捕获，'
            '不应产生 unhandled async error',
      );
    });

    test('操作内同步重入 run 不会重复启动，而是共享同一个 Future', () async {
      final coordinator = SingleFlightOperationCoordinator();
      var outerCount = 0;
      var innerCount = 0;
      Future<void>? sharedFuture;

      // outerOp 是 async 函数但体里没有任何 await——
      // Dart 的 async 函数在被调用后会立即同步执行到函数体结束（因为没有 await 点）。
      // 所以 coordinator.run(outerOp) 的同步调用链就是：
      //   run → 登记 _running → 立即调用 outerOp → 同步执行完 outerOp 全部代码。
      // 在这个同步执行过程中，outerOp 内部会重入调用 run——
      // 此时 _running 已被登记好，重入调用应该直接返回同一个 Future。
      Future<void> outerOp() async {
        outerCount++;
        // 这行在同步执行阶段重入 run——_running 应该已被登记
        sharedFuture = coordinator.run(() async {
          innerCount++; // inner 操作不应该被执行
        });
        // 没有 await，函数体到此同步结束，返回 completed Future
      }

      // 同步调用 run
      final returned = coordinator.run(outerOp);

      // 此时 outerOp 的同步体已经全部执行完：
      // - sharedFuture 已被赋值（因为 outerOp 体已同步跑完）
      // - 它应该就是 run 返回的同一个 Future
      expect(sharedFuture, isNotNull);
      expect(identical(sharedFuture, returned), isTrue);

      await returned;

      // outerOp 只执行了一次（没有重复启动）
      // innerOp 从未被启动（重入没有触发新操作）
      expect(outerCount, 1);
      expect(innerCount, 0);
    });
  });
}
