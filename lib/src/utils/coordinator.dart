/// 通用异步操作协调器库。
///
/// 本库提供两种最常见的异步操作协调策略的基础实现：
///
/// - [QueuedOperationCoordinator]：FIFO 串行执行，保证所有操作按提交顺序依次执行；
///   单个操作失败不会阻塞后续排队操作，每个调用方独立收到自己的结果或错误。
/// - [SingleFlightOperationCoordinator]：合并重叠并发，同一时刻只允许一个操作执行；
///   多个并发调用会共享同一个执行中的 Future，避免重复劳动。
///
/// 这些协调器不依赖任何业务模块，可以跨项目复用。业务协调器（如上下文切换、资源关闭）
/// 通过**组合**使用它们，把自己的领域逻辑交给策略协调器调度，同时保留泛型结果、进度记录
/// 或失败重试等领域特有的语义。各策略实现声明为 `final class`，因为策略语义是固定的
/// （FIFO 就是 FIFO，single-flight 就是 single-flight），不应该被继承重写。
library;

import 'dart:async';

/// 异步操作协调器的基础接口。
///
/// 协调器是对异步操作执行策略的抽象——它关心「操作以什么方式被执行」，
/// 而不关心操作本身的业务含义。常见的策略包括 FIFO 排队、并发合并等。
///
/// 实现者通过 [run] 接收一个无参数的异步操作，并返回一个与该操作生命周期绑定的 Future。
/// 调用方通过 await 这个 Future 来感知操作的完成或失败。
abstract interface class AsyncOperationCoordinator {
  /// 提交一个异步操作，返回一个与该操作绑定的 Future。
  ///
  /// 返回的 Future 在操作执行完毕（成功或失败）后完成。具体语义取决于实现：
  ///
  /// - FIFO 排队实现中，每个调用方会收到独立的 Future，
  ///   按提交顺序被依次等待执行。
  /// - single-flight 实现中，重叠调用可能返回同一个 Future，
  ///   共享同一个执行中的操作。
  Future<void> run(Future<void> Function() operation);
}

/// FIFO 串行执行操作的协调器。
///
/// 保证所有操作按提交顺序依次执行，同一时刻只执行一个操作。
/// 操作失败不会阻塞后续排队的操作——失败后队列会释放，
/// 下一个排队的操作立即开始执行，错误传播给各自的调用方。
///
/// ## 使用场景
///
/// 适用于需要严格顺序且不能跳过任何一个操作的场景，
/// 例如 UI 状态变更、上下文切换、快照发布等。
///
/// ## 错误语义
///
/// 每个 [run] 的调用方独立收到操作的错误（通过 Future 拒绝）。
/// 一个操作失败不会影响其他操作的执行，也不会吞掉任何错误。
final class QueuedOperationCoordinator implements AsyncOperationCoordinator {
  QueuedOperationCoordinator();

  /// 队列中尚未完成的操作的「结算」Future。
  /// 每个操作提交时会创建自己的结算 Completer，并把其 Future 存到此字段。
  /// 前一个操作的结算 Future 完成后，后一个操作才会开始执行。
  Future<void>? _pending;

  @override
  Future<void> run(Future<void> Function() operation) async {
    final previous = _pending;
    final settled = Completer<void>();
    _pending = settled.future;
    try {
      // 等前置操作结算（无论成功或失败，settled 都会 complete），
      // 这样即使前一个操作抛错，队列也不会被阻塞。
      if (previous != null) await previous;
      // 用 Future.sync 包裹：把 operation 在返回 Future 之前同步抛出的异常
      // 也放进队列里——它会被 try/catch 捕获，然后 finally 释放队列，
      // 错误再通过上面的 await 传播给调用方，不会变成同步抛错绕过整个队列机制。
      await Future.sync(operation);
    } finally {
      // 只有自己仍是队尾时才清空 _pending，
      // 避免覆盖掉已排在后面的操作。
      if (identical(_pending, settled.future)) {
        _pending = null;
      }
      // 结算自己：释放下一个排队操作，同时保持自己的结果（值或错误）
      // 已被上面的 await 传播给调用方。
      settled.complete();
    }
  }
}

/// single-flight 操作协调器。
///
/// 当有操作正在执行时，新的 [run] 调用会直接返回同一个执行中的 Future，
/// 避免重复执行。当且仅当当前操作完成后，新的调用才会启动新一轮执行。
///
/// ## 使用场景
///
/// 适用于「同一时刻最多执行一轮，重叠调用共享结果」的场景，
/// 例如资源关闭操作、配置刷新、远程同步等。
///
/// ## 错误语义
///
/// 如果执行中的操作失败，所有共享该 Future 的调用方都会收到同一个错误。
/// 这是 single-flight 的核心语义——大家本来就在共享同一次执行。
///
/// ## 同步重入安全
///
/// 本实现先把一个共享 Future 登记到 [_running]，再通过立即调用的 async 函数体
/// 执行操作。Dart 的 async 函数在被调用后会立即执行到第一个 await 才暂停，
/// 因此即使回调在同步阶段重入 [run]，重入调用也能看到已登记的 Future，
/// 不会重复启动，也不会无限递归。操作的时序与直接调用保持一致，没有事件循环延迟。
///
/// ## 资源清理
///
/// [_running] 清除动作放在 async 函数体的 `try/finally` 中，
/// 不依赖 `whenComplete` 之类的链式 API——这样避免了失败路径上
/// 未监听的派生 Future 产生 uncaught async error。
final class SingleFlightOperationCoordinator
    implements AsyncOperationCoordinator {
  SingleFlightOperationCoordinator();

  /// 当前正在执行的操作的 Future。为 null 表示没有执行中的操作。
  Future<void>? _running;

  @override
  Future<void> run(Future<void> Function() operation) {
    // 如果已有操作在执行，直接返回它——这就是 single-flight 的核心：
    // 重叠调用合并为同一次执行。
    final running = _running;
    if (running != null) return running;

    // 先创建一个 Completer，并把它的 Future 登记到 [_running]。
    // 这一步必须在执行任何业务代码之前完成——如果 operation 在同步阶段
    // 重入 run，重入调用需要能看到这个 Future，否则会重复启动。
    final completer = Completer<void>();
    final future = completer.future;
    _running = future;

    // 立即启动 async 执行体（同步时序，没有 microtask 延迟）：
    //
    // Dart 的 async 函数在被调用后立即执行到第一个 await 才暂停。
    // 因此 operation 在当前事件循环中就会被调用，保持与直接调用一致的时序。
    //
    // 用 Future.sync(operation) 包裹是防御性的：
    // 如果 operation 在返回 Future 之前同步抛错，Future.sync 会立即捕获
    // 并把它包装成失败的 Future，确保 await 能正常进入 try/catch 路径。
    // 这覆盖了回调不是 async 函数、也不返回 Future（同步抛错）的边界。
    //
    // 不关心此 async 函数返回的 Future——我们的 completer 才是外部看到的 future。
    // 这样就不会产生任何未监听的派生 Future。
    () async {
      try {
        await Future.sync(operation);
        completer.complete();
      } on Object catch (error, stackTrace) {
        completer.completeError(error, stackTrace);
      } finally {
        // 只有自己仍是登记中的 Future 才清除——
        // 如果清除逻辑被下一次 run 覆盖，不要误删它的登记。
        if (identical(_running, future)) _running = null;
      }
    }();

    return future;
  }
}
