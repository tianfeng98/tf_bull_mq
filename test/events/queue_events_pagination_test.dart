import 'dart:async';

import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/events/index.dart';

import 'support/event_fixtures.dart';
import 'support/fake_queue_backend.dart';
import 'support/fake_queue_signal.dart';
import 'support/manual_delay.dart';

void main() {
  late FakeQueueBackend backend;
  late FakeQueueSignal signal;
  late ManualDelay delay;
  late QueueEvents queueEvents;
  late List<PositionedJobEvent> seen;
  late List<Object> errors;
  late StreamSubscription<PositionedJobEvent> subs;

  /// 统一的启动辅助：构造 QueueEvents、挂起 listener 并执行 start()。
  Future<void> startWith({
    QueueEventsOptions? options,
    required Future<JobEventPage> Function(ReadJobEventsRequest) handler,
  }) async {
    backend = FakeQueueBackend(
      capabilities: eventCapabilities(),
      onReadEvents: handler,
    );
    signal = FakeQueueSignal();
    delay = ManualDelay();
    queueEvents = QueueEvents(
      queueName: 'orders',
      backend: backend,
      signal: signal,
      delay: delay.call,
      options: options,
    );
    seen = <PositionedJobEvent>[];
    errors = <Object>[];
    subs = queueEvents.events.listen(
      (e) => seen.add(e),
      onError: (e) => errors.add(e),
    );
    await queueEvents.start();
  }

  tearDown(() async {
    try {
      await queueEvents.close();
    } catch (_) {}
    await subs.cancel();
  });

  // liveOnly 模式且无 initialCursor 时，首次读取应从 null cursor 开始拉取最旧事件。
  test(
    'cursor 为 null 时，liveOnly 会从 backend 最早的事件开始读取',
    () async {
      await startWith(
        handler: (_) async => JobEventPage(
          items: [positionedEvent('orders', 1)],
          nextCursor: null,
        ),
      );
      expect(backend.readRequests, hasLength(1));
      expect(backend.readRequests.first.cursor, isNull);
      expect(seen, hasLength(1));
      expect(seen.first.cursor.position, 1);
    },
  );

  // replayable 模式下传入 initialCursor 后，Backend 必须收到严格大于 initialCursor 的请求。
  test('replayable 会从调用方提供的 cursor 之后开始读取', () async {
    await startWith(
      options: QueueEventsOptions(
        initialCursor: EventCursor(queueName: 'orders', position: 42),
      ),
      handler: (req) async {
        expect(req.cursor?.position, 42);
        return JobEventPage(
          items: [positionedEvent('orders', 43)],
          nextCursor: null,
        );
      },
    );
    expect(seen.first.cursor.position, 43);
  });

  // 分页 drain：满页且有 nextCursor 时必须继续读下一页，直到遇到未满页才停止。
  test('会连续读取完整分页，直到遇到短页', () async {
    int pageIndex = 0;
    await startWith(
      handler: (req) async {
        pageIndex++;
        final pageSize = req.limit;
        final reqPos = req.cursor?.position ?? 0;
        if (pageIndex == 2) {
          // 第二页返回半页，drain 在此停止。
          final startPos = reqPos + 1;
          final items = List.generate(
            pageSize ~/ 2,
            (i) => positionedEvent('orders', startPos + i),
          );
          return JobEventPage(items: items, nextCursor: null);
        }
        // 第一页返回满页，nextCursor 推进到最后一条事件位置。
        final startPos = reqPos + 1;
        final items = List.generate(
          pageSize,
          (i) => positionedEvent('orders', startPos + i),
        );
        final last = items.last;
        return JobEventPage(
          items: items,
          nextCursor: EventCursor(
            queueName: 'orders',
            position: last.cursor.position,
          ),
        );
      },
    );
    expect(pageIndex, 2);
    expect(seen.length, greaterThan(2));
  });

  // Backend 返回空页时应立即停止 drain，不再继续发 read 请求。
  test('遇到空页时立即停止', () async {
    await startWith(
      handler: (req) async => JobEventPage(items: [], nextCursor: null),
    );
    expect(backend.readRequests, hasLength(1));
    expect(seen, isEmpty);
  });

  // 未满页即使 nextCursor 非 null 也必须停止——nextCursor 只在满页时才有意义。
  test('即使 nextCursor 非 null，遇到短页时也会停止', () async {
    await startWith(
      handler: (req) async {
        return JobEventPage(
          items: [positionedEvent('orders', 1)],
          nextCursor: EventCursor(queueName: 'orders', position: 99),
        );
      },
    );
    expect(backend.readRequests, hasLength(1));
  });

  // 满页但 nextCursor 为 null 也应停止——表示 Backend 没有更多数据。
  test('完整页的 nextCursor 为 null 时会停止', () async {
    int pageSize = 5;
    await startWith(
      options: QueueEventsOptions(pageSize: 5),
      handler: (req) async {
        pageSize = req.limit;
        final startPos = req.cursor?.position ?? 0;
        final items = List.generate(
          pageSize,
          (i) => positionedEvent('orders', startPos + i + 1),
        );
        return JobEventPage(items: items, nextCursor: null);
      },
    );
    expect(backend.readRequests, hasLength(1));
    expect(seen, hasLength(pageSize));
  });

  // 下一页的 cursor 推进必须发生在前一页所有事件都 add 到 stream 之后，
  // 保证消费者看到的顺序与 Backend 返回顺序一致。
  test(
    '只有在每个事件添加后才会推进下一次请求的 cursor',
    () async {
      bool firstPageObserved = false;
      await startWith(
        options: QueueEventsOptions(pageSize: 2),
        handler: (req) async {
          if (req.cursor == null) {
            final items = [
              positionedEvent('orders', 1),
              positionedEvent('orders', 2),
            ];
            return JobEventPage(
              items: items,
              nextCursor: EventCursor(queueName: 'orders', position: 2),
            );
          }
          // 第二页请求只有在第一页事件已全部入队后才会发生。
          expect(seen.length, 2);
          firstPageObserved = true;
          return JobEventPage(
            items: [positionedEvent('orders', 3)],
            nextCursor: null,
          );
        },
      );
      expect(firstPageObserved, isTrue);
      expect(seen.length, 3);
    },
  );

  // 某条事件的 queueName 与目标队列不一致时，必须拒绝该条之后的事件（已入队的保留）。
  test(
    '添加事件前会拒绝属于其他 queue 的 item cursor',
    () async {
      await startWith(
        handler: (req) async {
          return JobEventPage(
            items: [positionedEvent('orders', 1), positionedEvent('other', 2)],
            nextCursor: null,
          );
        },
      );
      expect(errors, hasLength(1));
      expect(errors.first, isA<StateError>());
      expect(seen.length, 1);
    },
  );

  // 事件位置重复或递减（违反单调性）时应在该条位置停止并拒绝。
  test(
    '添加事件前会拒绝重复或递减的 item position',
    () async {
      await startWith(
        handler: (req) async {
          return JobEventPage(
            items: [
              positionedEvent('orders', 1),
              positionedEvent('orders', 1), // 重复位置
            ],
            nextCursor: null,
          );
        },
      );
      expect(errors, hasLength(1));
      expect(errors.first, isA<StateError>());
      expect(seen.length, 1);
    },
  );

  // 满页的 nextCursor 应按 queueName + position 比较，而不是按对象 identity 比较——
  // 这与 Backend 可能重建 EventCursor 的实现兼容。
  test('比较完整分页的 cursor 时依据 queue 和 position，而非对象身份', () async {
    final List<ReadJobEventsRequest> allRequests = [];
    await startWith(
      options: QueueEventsOptions(pageSize: 2),
      handler: (req) async {
        allRequests.add(req);
        if (req.cursor == null) {
          final items = [
            positionedEvent('orders', 1),
            positionedEvent('orders', 2),
          ];
          // 新建一个 queueName/position 匹配的 EventCursor——应能通过相等性校验。
          return JobEventPage(
            items: items,
            nextCursor: EventCursor(queueName: 'orders', position: 2),
          );
        }
        return JobEventPage(
          items: [positionedEvent('orders', 3)],
          nextCursor: null,
        );
      },
    );
    // 两次 read：第一次 null cursor，第二次 position=2。
    expect(allRequests.length, greaterThanOrEqualTo(2));
    expect(allRequests[0].cursor, isNull);
    expect(allRequests[1].cursor?.position, 2);
  });

  // 满页但位置未推进（最后一条事件位置 ≤ 请求 cursor）时必须拒绝，防止 Backend 返回不合法游标。
  test(
    '完整分页未推进时会报告该情况，并等待下一次触发',
    () async {
      // pageSize=3，首条 pos=1、次条 pos=2、第三条 pos=2（未推进）。
      await startWith(
        options: QueueEventsOptions(pageSize: 3),
        handler: (req) async {
          return JobEventPage(
            items: [
              positionedEvent('orders', 1),
              positionedEvent('orders', 2),
              positionedEvent('orders', 2), // 位置未推进
            ],
            nextCursor: EventCursor(queueName: 'orders', position: 2),
          );
        },
      );
      expect(errors, hasLength(1));
      expect(errors.first, isA<StateError>());
      expect(seen.length, 2);
    },
  );

  // 跨页事件顺序必须严格单调递增，QueueEvents 不能破坏 Backend 给出的事件顺序。
  test('跨分页读取时会保留 backend 事件顺序', () async {
    int pageIndex = 0;
    await startWith(
      options: QueueEventsOptions(pageSize: 3),
      handler: (req) async {
        pageIndex++;
        final size = req.limit;
        final base = req.cursor?.position ?? 0;
        final startPos = base + 1;
        if (pageIndex >= 3) {
          // 第三页返回半页，触发 drain 终止。
          return JobEventPage(
            items: [positionedEvent('orders', startPos)],
            nextCursor: null,
          );
        }
        final items = List.generate(
          size,
          (i) => positionedEvent('orders', startPos + i),
        );
        return JobEventPage(
          items: items,
          nextCursor: EventCursor(
            queueName: 'orders',
            position: startPos + size - 1,
          ),
        );
      },
    );
    // 验证所有事件位置严格递增。
    for (var i = 1; i < seen.length; i++) {
      expect(seen[i].cursor.position, greaterThan(seen[i - 1].cursor.position));
    }
  });
}
