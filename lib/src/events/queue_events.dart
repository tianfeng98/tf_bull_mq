// ignore_for_file: prefer_initializing_formals

import 'dart:async';

import '../backend/index.dart';
import '../core/index.dart';
import '../signal/index.dart';
import 'queue_events_options.dart';

/// 从 [QueueBackend] 读取带位置信息的 Job 事件，并以单次订阅 [Stream] 形式推送。
/// 事件推送可由可选的 [QueueSignal] 提示唤醒，或通过周期性 [Delay] 轮询获取。
///
/// 本组件保持 Backend 原始 JSON-like 接口：事件 data 是已冻结的 `Object?`，
/// 不伪装成任务 payload 泛型。调用方若需要业务类型化，应自行用 PayloadCodec 解码。
///
/// ### 两条唤醒路径与 [Delay] 的定位
/// 拉取事件（drain）由两条彼此独立的路径触发：
///
/// 1. [QueueSignal] 提示——主路径，新事件落库后近乎实时唤醒；
/// 2. 周期性轮询——由 [Delay]（构造时注入；未提供时回退到 `Future<void>.delayed`）
///    按 [QueueEventsOptions.pollInterval] 计时，每到期一次触发一次 drain。
///
/// [Delay] 只服务于第 2 条路径，是 Signal 之外的可靠性兜底：提示丢失、
/// Signal 订阅断裂（组件退化为纯轮询）、或 Backend 侧没有通知能力时，
/// 组件仍能持续推进游标。它不参与读取失败的重试退避，也不参与 [start()]
/// 的首次 drain——首次 drain 不会先等待一个轮询周期。
///
/// ### At-least-once 投递
/// 组件保证每个成功推入内部 [StreamController] 的 [PositionedJobEvent] 都会
/// *至少一次*送达当前监听器。Backend 读取可能失败并重试，因此调用方需要通过
/// [PositionedJobEvent.event.eventId] 或自己的游标管理来容忍重复。内部游标只表示
/// "QueueEvents 已成功调用 StreamController.add"——它*不*表示监听器的业务逻辑
/// 已经确认了该事件。
///
/// ### Stream 语义
/// 每个 [QueueEvents] 实例独占一个单次订阅 Stream。第二次 [Stream.listen]
/// 调用抛 [StateError]。调用方可以在 [start()] 之前获取并监听 stream；
/// 底层 controller 会缓冲启动后产生的事件，等监听器挂载后再推送。
/// 没有按消费者单独维护的偏移量管理。
final class QueueEvents {
  QueueEvents({
    required this.queueName,
    required QueueBackend backend,
    required QueueSignal signal,

    /// 异步等待原语，可选。只在轮询循环中按 [QueueEventsOptions.pollInterval] 计时。
    ///
    /// 为空时回退到组件内部的默认实现 `_defaultDelay`（即 `Future<void>.delayed`），
    /// 生产环境通常无需注入。
    ///
    /// 需要注入的场景有两类：测试用假时钟推进时间，避免真实等待一个轮询周期；
    /// 以及调用方想把轮询计时接入自己的调度器或可观测埋点。无论是否注入，
    /// [close()] 都能通过停止 token 让循环立刻退出——未完成的手工 Delay
    /// 不会被取消，只是不再被等待。
    Delay? delay,
    QueueEventsOptions? options,
  }) : _backend = backend,
       _signal = signal,
       _delay = delay ?? _defaultDelay,
       options = options ?? QueueEventsOptions() {
    if (queueName.isEmpty) {
      throw ArgumentError.value(queueName, 'queueName', '必须非空');
    }
    // 校验 initialCursor 所属队列与本组件一致，防止跨队列消费。
    final initialCursor = this.options.initialCursor;
    if (initialCursor != null && initialCursor.queueName != queueName) {
      throw ArgumentError.value(
        initialCursor,
        'options.initialCursor',
        '游标所属队列 ${initialCursor.queueName} 与目标队列 $queueName 不匹配',
      );
    }
    _controller = StreamController<PositionedJobEvent>(sync: true);
  }

  /// 本组件固定的队列名。
  final String queueName;

  /// 运行配置。
  final QueueEventsOptions options;

  final QueueBackend _backend;
  final QueueSignal _signal;

  /// 轮询循环使用的异步等待原语：调用方注入的实现，或未提供时回退的
  /// `_defaultDelay`。按 [QueueEventsOptions.pollInterval] 计时，到期后请求
  /// 一次 drain。不在组件内被取消，也不参与读取失败后的退避重试。
  final Delay _delay;
  late final StreamController<PositionedJobEvent> _controller;

  /// 生命周期状态。
  _QueueEventsState _state = _QueueEventsState.created;

  /// 内部游标，只在每次成功 `add` 之后推进。
  EventCursor? _cursor;

  /// drain 已请求但尚未执行时为 `true`。
  bool _drainRequested = false;

  /// 当前 in-flight drain 的 Future（如有）。
  Future<void>? _drainFuture;

  /// [start()] 启动的轮询循环 Future。
  Future<void>? _pollFuture;

  StreamSubscription<void>? _signalSubscription;
  Future<void>? _closeFuture;

  /// `true` 表示 Signal 流订阅不再有效（`subscribe` 同步抛错或收到流错误后
  /// `cancelOnError` 生效）。提示不再触发 drain，只能依赖轮询。
  bool _signalBroken = false;

  final Completer<void> _stopPoll = Completer<void>();

  /// 事件流。仅在组件处于 running 状态时可访问；closing/closed 时抛 [LifecycleError]。
  Stream<PositionedJobEvent> get events {
    if (_state == _QueueEventsState.closing ||
        _state == _QueueEventsState.closed) {
      throw LifecycleError('QueueEvents', 'events', '关闭后不能再获取 Stream。');
    }
    return _controller.stream;
  }

  /// 启动事件消费循环。
  ///
  /// 从 `created` 进入 `running`；订阅 Signal、启动轮询循环并执行首次 drain。
  /// 能力兼容性校验在任何副作用之前完成。
  Future<void> start() async {
    if (_state == _QueueEventsState.running) {
      throw LifecycleError('QueueEvents', 'start', 'QueueEvents 已经启动过了。');
    }
    if (_state != _QueueEventsState.created) {
      throw LifecycleError(
        'QueueEvents',
        'start',
        'QueueEvents 不能从状态 $_state 启动。',
      );
    }

    // 在任何副作用之前做 Profile/Scope 兼容性校验。
    if (_backend.capabilities.profile == BackendProfile.distributed &&
        _signal.scope != SignalScope.distributed) {
      throw UnsupportedCapability(
        'distributedSignal',
        'distributed Backend 要求 distributed Signal。',
      );
    }

    _state = _QueueEventsState.running;
    _cursor = options.initialCursor;

    Stream<void> signalStream;
    try {
      signalStream = _signal.subscribe(queueName);
    } catch (_) {
      // 订阅同步失败时降级为只用轮询。
      _signalBroken = true;
      signalStream = const Stream<void>.empty();
    }

    if (!_signalBroken) {
      try {
        _signalSubscription = signalStream.listen(
          (_) => _requestDrain(),
          onError: (Object error, StackTrace? stackTrace) {
            // 收到流错误后标记 Signal 断裂并取消订阅。
            _signalBroken = true;
            _signalSubscription?.cancel();
            _signalSubscription = null;
          },
          cancelOnError: true,
        );
      } catch (_) {
        _signalBroken = true;
        _signalSubscription = null;
      }
    }

    // 启动轮询循环，保存 Future 以便 close() 等待。
    // 循环内部按 options.pollInterval 调用 Delay 计时（注入实现或默认的
    // Future<void>.delayed）；首次 drain 不经过它，因此不等待一个轮询周期。
    _pollFuture = _pollLoop();

    // 执行首次 drain——start() 会等待直到达到稳定点。
    await _drainOnce();
  }

  /// 关闭事件消费组件。幂等，返回同一个 Future。
  Future<void> close() {
    if (_closeFuture != null) return _closeFuture!;
    _state = _QueueEventsState.closing;
    _closeFuture = _closeInternal();
    return _closeFuture!;
  }

  // -------- 内部方法 --------

  /// 请求一次 drain。通过检查 [_drainFuture] 保持 single-flight 语义；
  /// drain 执行期间的多次调用通过标志位合并。
  void _requestDrain() {
    if (_state != _QueueEventsState.running) return;
    _drainRequested = true;
    if (_drainFuture != null) return;
    _drainFuture = _drainBackend();
    _drainFuture!.whenComplete(() {
      // 清理时重新检查边界 handoff：drain 刚结束的瞬间可能有新提示
      // 到达把 _drainRequested 置为 true，但没有 drain 在运行了。
      _drainFuture = null;
      if (_drainRequested && _state == _QueueEventsState.running) {
        _drainFuture = _drainBackend();
        _drainFuture!.whenComplete(() {
          _drainFuture = null;
        });
      }
    });
  }

  /// 请求一次 drain 并等待其完成。如果 drain 已在执行则直接等待现有 Future。
  /// *不会*循环——跟进来的提示由 [_drainBackend] 外层循环处理合并。
  Future<void> _drainOnce() async {
    _drainRequested = true;
    Future<void>? drain = _drainFuture;
    if (drain == null) {
      _drainFuture = _drainBackend();
      drain = _drainFuture;
    }
    await drain;
    _drainFuture = null;
  }

  /// 默认的等待实现：直接委托给 `Future<void>.delayed`。
  ///
  /// 仅在构造时未提供 `delay` 时使用。它依赖真实计时器，因此测试必须注入
  /// 假时钟版本，否则每个轮询周期都会真实等待
  /// [QueueEventsOptions.pollInterval]（默认 5 秒）。
  static Future<void> _defaultDelay(Duration duration) =>
      Future<void>.delayed(duration);

  /// 周期性轮询循环：等待 pollInterval 或关闭信号，到期时触发 drain。
  ///
  /// 每轮用 [Delay]（注入的实现或 `_defaultDelay`）计一个
  /// [QueueEventsOptions.pollInterval]，并与停止 token 用 `Future.any` 竞争：
  ///
  /// - token 先完成（[close()] 或游标缺失进入终态）→ 立即退出循环，尚未完成的
  ///   手工 Delay 被丢弃（不取消、不等它），因此关闭不会被挂起的计时器阻塞；
  /// - 计时先完成 → 请求一次 drain，然后进入下一轮。
  ///
  /// 注入的 Delay 抛出异常会终止整个轮询循环，此后只有 Signal 路径还能触发
  /// drain；该错误在 [close()] 等待轮询 Future 时被吞掉，不会传播给调用方。
  Future<void> _pollLoop() async {
    while (_state == _QueueEventsState.running) {
      final duration = options.pollInterval;
      // 唯一使用 Delay 的地方：为这一轮轮询计时。返回的 Future 只在这里被
      // 等待一次，不会被保存或取消——关闭时它若仍未完成就自然悬挂到结束。
      final tickFuture = _delay(duration);
      final stopFuture = _stopPoll.future;
      await Future.any<void>([tickFuture, stopFuture]);
      if (_stopPoll.isCompleted) return;
      if (_state == _QueueEventsState.running) {
        _requestDrain();
      }
    }
  }

  /// 从 Backend 拉取事件的核心循环。
  ///
  /// 外层循环处理 drain 执行期间到达的新提示（通过 [_drainRequested] 合并）；
  /// 内层循环处理分页读取，直到 Backend 返回空页或未满页。
  /// 在 pagination 过程中会逐条校验事件位置的单调性和一致性，防止 Backend
  /// 违反契约时产生静默错误。
  Future<void> _drainBackend() async {
    outer:
    while (_state == _QueueEventsState.running) {
      // 开始内层分页循环前清除残留标志。
      _drainRequested = false;

      inner:
      while (true) {
        if (_state != _QueueEventsState.running) return;

        final requestCursor = _cursor;
        final request = ReadJobEventsRequest(
          queueName: queueName,
          cursor: requestCursor,
          limit: options.pageSize,
        );
        final JobEventPage page;
        try {
          page = await _backend.readJobEvents(request);
        } on EventCursorGap catch (gap, stackTrace) {
          // 游标缺失是终态错误：事件历史被裁剪导致无法继续消费。
          _handleGap(gap, stackTrace);
          return;
        } catch (error, stackTrace) {
          // 普通读取错误推送到 stream，但组件继续运行（下次轮询会重试）。
          if (_state == _QueueEventsState.running) {
            _controller.addError(error, stackTrace);
          }
          return;
        }

        if (_state != _QueueEventsState.running) return;

        final items = page.items;
        if (items.isEmpty) break inner;

        // 逐条校验并推送事件。
        int? previousPosition;
        for (var i = 0; i < items.length; i++) {
          final item = items[i];
          // 校验事件所属队列与本组件一致。
          if (item.cursor.queueName != queueName) {
            _emitStateErrorAndStop(
              '事件所属队列 ${item.cursor.queueName} 与目标队列 $queueName 不匹配',
            );
            return;
          }
          if (i == 0) {
            // 首条事件必须严格大于请求游标位置。
            final req = requestCursor;
            if (req != null && item.cursor.position <= req.position) {
              _emitStateErrorAndStop(
                '首条事件位置 ${item.cursor.position} 必须严格大于请求游标位置 ${req.position}',
              );
              return;
            }
          } else {
            // 后续事件位置必须严格递增。
            if (item.cursor.position <= previousPosition!) {
              _emitStateErrorAndStop(
                '事件位置 ${item.cursor.position} 必须严格大于前一条 $previousPosition!',
              );
              return;
            }
          }
          previousPosition = item.cursor.position;
          if (_state != _QueueEventsState.running) return;
          // 推送原始事件（已冻结 JSON-like，不做 cast）。
          _controller.add(item);
          _cursor = item.cursor;
        }

        if (_state != _QueueEventsState.running) return;
        // 未满页表示没有更多事件，结束内层循环。
        if (items.length < options.pageSize) break inner;
        if (page.nextCursor == null) break inner;

        // 校验 nextCursor 与最后一条事件一致。
        final last = items.last;
        final nc = page.nextCursor!;
        if (nc.queueName != last.cursor.queueName ||
            nc.position != last.cursor.position) {
          _emitStateErrorAndStop(
            'nextCursor (${nc.queueName}, ${nc.position}) 与最后一条事件游标 (${last.cursor.queueName}, ${last.cursor.position}) 不匹配',
          );
          return;
        }

        // 校验 drain 后的游标确实推进了（有请求游标时）。
        if (requestCursor != null &&
            (_cursor == null || _cursor!.position <= requestCursor.position)) {
          _emitStateErrorAndStop(
            '最终游标 ${_cursor?.position} 没有推进到请求游标 ${requestCursor.position} 之后',
          );
          return;
        }
        // 满页且 nextCursor 有效——继续读下一页。
      }

      // 内层分页结束。如果处理期间有新提示到达，执行下一轮外层循环；否则结束。
      if (_drainRequested) continue outer;
      return;
    }
  }

  /// 向 stream 推送 StateError（如果仍在运行）。
  void _emitStateErrorAndStop(String message) {
    if (_state != _QueueEventsState.running) return;
    _controller.addError(StateError(message));
  }

  /// 处理游标缺失终态：推进到 terminal 状态，停止轮询和信号订阅，
  /// 推送 gap 错误并关闭 stream。
  void _handleGap(EventCursorGap gap, StackTrace stackTrace) {
    if (_state != _QueueEventsState.running) return;
    _state = _QueueEventsState.terminal;
    _stopPoll.complete();
    _signalSubscription?.cancel();
    _signalSubscription = null;
    _controller.addError(gap, stackTrace);
    _controller.close();
  }

  /// 内部关闭实现：停止轮询、取消 Signal 订阅、等待 in-flight drain 和 poll、
  /// 关闭 stream controller，最后按所有权关闭 Backend 和 Signal。
  ///
  /// Signal 和 Backend 的关闭各自 try/catch，始终尝试二者；最后抛出
  /// 第一个错误（保留原 stack trace）。
  Future<void> _closeInternal() async {
    try {
      if (!_stopPoll.isCompleted) {
        _stopPoll.complete();
      }
      await _signalSubscription?.cancel();
      _signalSubscription = null;
      final pollFuture = _pollFuture;
      final drainFuture = _drainFuture;
      if (pollFuture != null) {
        try {
          await pollFuture;
        } catch (_) {}
      }
      if (drainFuture != null) {
        try {
          await drainFuture;
        } catch (_) {}
      }
      _controller.close();

      // 分别尝试关闭 Signal 和 Backend，收集错误
      Object? firstError;
      StackTrace? firstStack;
      if (options.ownsSignal) {
        try {
          await _signal.close();
        } catch (error, stackTrace) {
          firstError ??= error;
          firstStack ??= stackTrace;
        }
      }
      if (options.ownsBackend) {
        try {
          await _backend.close();
        } catch (error, stackTrace) {
          firstError ??= error;
          firstStack ??= stackTrace;
        }
      }
      if (firstError != null) {
        Error.throwWithStackTrace(firstError, firstStack!);
      }
    } finally {
      if (_state == _QueueEventsState.closing) {
        _state = _QueueEventsState.closed;
      }
    }
  }
}

/// [QueueEvents] 的生命周期状态（私有于本文件）。
enum _QueueEventsState { created, running, terminal, closing, closed }
