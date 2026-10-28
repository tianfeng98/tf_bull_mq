import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';

void main() {
  /// 验证 [JobEventType] 枚举覆盖架构层定义的全部事件类型，
  /// 不允许遗漏或引入非规划的事件类型。
  test('JobEventType 仅包含架构定义的事件类型', () {
    expect(JobEventType.values.map((value) => value.name), <String>[
      'added',
      'waiting',
      'active',
      'progress',
      'completed',
      'failed',
      'delayed',
      'paused',
      'resumed',
      'canceled',
      'stalled',
      'removed',
    ]);
  });

  /// 验证 [EventCursor.position] 必须 >= 1，0 是非法下界，拒绝构造。
  test('event cursor 会拒绝小于 1 的 position', () {
    expect(
      () => EventCursor(queueName: 'mail', position: 0),
      throwsArgumentError,
    );
  });

  /// 验证 [JobEvent.data] 必须是 JSON-like 值，DateTime 等非可序列化对象被拒绝。
  test('JobEvent 会验证 data 是否为 JSON-like', () {
    expect(
      () => JobEvent(
        eventId: 'e1',
        jobKey: const JobKey(queueName: 'mail', jobId: '1'),
        revision: 1,
        type: JobEventType.added,
        occurredAt: DateTime.utc(2026),
        data: DateTime.utc(2026),
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [JobLog.sequence] 和 [JobLog.attempt] 必须为正整数，0 被拒绝。
  test('JobLog 的 sequence 和 attempt 均为正数', () {
    expect(
      () => JobLog(
        logId: 'l1',
        jobKey: const JobKey(queueName: 'mail', jobId: '1'),
        sequence: 0,
        attempt: 0,
        executionId: 'x',
        level: JobLogLevel.info,
        channel: JobLogChannel.application,
        message: 'hello',
        data: null,
        occurredAt: DateTime.utc(2026),
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [FlowJobSpec] 中不允许出现重复的 [JobKey]，
  /// 同一份 jobs 列表中 key 必须唯一。
  test('FlowJobSpec 会拒绝重复的 job key', () {
    final key = const JobKey(queueName: 'mail', jobId: '1');
    expect(
      () => FlowJobSpec(
        root: key,
        jobs: <JobSpec>[
          JobSpec(key: key, name: 'a', data: null),
          JobSpec(key: key, name: 'b', data: null),
        ],
        edges: const <FlowEdge>[],
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [FlowJobSpec] 拒绝三类非法拓扑：root 不在 jobs 列表中、
  /// edges 形成有向环、存在从 root 无法到达的孤立节点。
  test('FlowJobSpec 会拒绝缺少 root、存在 cycle 或不可达的 Job', () {
    final root = const JobKey(queueName: 'mail', jobId: 'root');
    final child = const JobKey(queueName: 'mail', jobId: 'child');
    expect(
      () => FlowJobSpec(
        root: root,
        jobs: <JobSpec>[JobSpec(key: child, name: 'child', data: null)],
        edges: const <FlowEdge>[],
      ),
      throwsArgumentError,
    );
    expect(
      () => FlowJobSpec(
        root: root,
        jobs: <JobSpec>[
          JobSpec(key: root, name: 'root', data: null),
          JobSpec(key: child, name: 'child', data: null),
        ],
        edges: <FlowEdge>[
          FlowEdge(parent: root, child: child),
          FlowEdge(parent: child, child: root),
        ],
      ),
      throwsArgumentError,
    );
  });

  /// 验证 [FlowEdge.weight] 接受 0 权重，但拒绝负数或非有限数值（NaN/Infinity）。
  test(
    'FlowEdge 接受权重为 0，并拒绝负数或非有限权重',
    () {
      expect(FlowEdge(parent: _root, child: _child, weight: 0).weight, 0);
      expect(
        () => FlowEdge(parent: _root, child: _child, weight: -1),
        throwsArgumentError,
      );
      expect(
        () => FlowEdge(parent: _root, child: _child, weight: double.infinity),
        throwsArgumentError,
      );
    },
  );

  /// 用 10000 层的线性链路验证 [FlowJobSpec] 的拓扑校验是迭代实现（而非递归），
  /// 不会因深度过大触发栈溢出。
  test('FlowJobSpec 会通过迭代方式验证一万层链结构', () {
    const depth = 10000;
    final jobs = <JobSpec>[];
    final edges = <FlowEdge>[];
    for (var index = 0; index < depth; index++) {
      final key = JobKey(queueName: 'flow', jobId: '$index');
      jobs.add(JobSpec(key: key, name: 'node', data: null));
      if (index > 0) {
        edges.add(
          FlowEdge(
            parent: JobKey(queueName: 'flow', jobId: '${index - 1}'),
            child: key,
          ),
        );
      }
    }
    final flow = FlowJobSpec(root: jobs.first.key, jobs: jobs, edges: edges);
    expect(flow.jobs, hasLength(depth));
  });

  /// 同样用 10000 层链路验证 [FlowSnapshot] 的拓扑校验是迭代实现，
  /// 确保产品路径上也不会因深链路栈溢出。
  test('FlowSnapshot 会通过迭代方式验证一万层链结构', () {
    const depth = 10000;
    final records = <JobRecord>[];
    final edges = <FlowEdge>[];
    for (var index = 0; index < depth; index++) {
      final key = JobKey(queueName: 'flow', jobId: '$index');
      records.add(_record(key));
      if (index > 0) {
        edges.add(
          FlowEdge(
            parent: JobKey(queueName: 'flow', jobId: '${index - 1}'),
            child: key,
          ),
        );
      }
    }
    final snapshot = FlowSnapshot(
      root: records.first.key,
      jobs: records,
      edges: edges,
    );
    expect(snapshot.jobs, hasLength(depth));
  });

  /// 验证 [JobSpec] 在构造时递归冻结 [data]：
  /// 修改原始输入集合不影响已构造对象，直接写对象内部集合抛 UnsupportedError。
  test('JobSpec 会在构造时冻结嵌套 data', () {
    final original = <String, Object?>{
      'config': <Object?>[1, 2, 3],
    };
    final spec = JobSpec(
      key: const JobKey(queueName: 'flow', jobId: 'spec'),
      name: 'node',
      data: original,
    );

    // 修改原始输入不应影响 spec.data
    (original['config'] as List<Object?>).add(99);
    original['extra'] = 'hack';
    expect(spec.data, <String, Object?>{
      'config': <Object?>[1, 2, 3],
    });

    // 冻结集合自身不可修改
    final frozen = spec.data as Map<String, Object?>;
    expect(() => frozen['new'] = 'x', throwsUnsupportedError);
    final nestedList = frozen['config'] as List<Object?>;
    expect(() => nestedList.add(99), throwsUnsupportedError);
  });

  /// 验证 [JobEvent] 和 [JobLog] 在构造时递归冻结各自的 [data]：
  /// 修改原始输入集合不影响已构造对象，直接写对象内部集合抛 UnsupportedError。
  test('JobEvent 和 JobLog 会冻结嵌套 data', () {
    final eventData = <String, Object?>{
      'payload': <Object?>['a', 'b'],
    };
    final logData = <String, Object?>{
      'metric': <Object?>[42],
    };

    final event = JobEvent(
      eventId: 'e1',
      jobKey: const JobKey(queueName: 'mail', jobId: '1'),
      revision: 1,
      type: JobEventType.progress,
      occurredAt: DateTime.utc(2026),
      data: eventData,
    );
    final log = JobLog(
      logId: 'l1',
      jobKey: const JobKey(queueName: 'mail', jobId: '1'),
      sequence: 1,
      attempt: 1,
      executionId: 'exec-1',
      level: JobLogLevel.info,
      channel: JobLogChannel.application,
      message: 'hello',
      data: logData,
      occurredAt: DateTime.utc(2026),
    );

    // 修改原始输入不应影响 event.data 和 log.data
    (eventData['payload'] as List<Object?>).add('c');
    eventData['new'] = 'hack';
    expect(event.data, <String, Object?>{
      'payload': <Object?>['a', 'b'],
    });

    (logData['metric'] as List<Object?>).add(99);
    expect(log.data, <String, Object?>{
      'metric': <Object?>[42],
    });

    // 冻结集合自身不可修改
    final eventFrozen = event.data as Map<String, Object?>;
    expect(() => eventFrozen['x'] = 'boom', throwsUnsupportedError);
    final logFrozen = log.data as Map<String, Object?>;
    expect(() => logFrozen['y'] = 0, throwsUnsupportedError);
  });
}

const _root = JobKey(queueName: 'mail', jobId: 'root');
const _child = JobKey(queueName: 'mail', jobId: 'child');

JobRecord _record(JobKey key) => JobRecord(
  schemaVersion: 1,
  key: key,
  name: 'node',
  data: null,
  status: JobStatus.waiting,
  progress: 0,
  result: null,
  failure: null,
  revision: 1,
  attemptsMade: 0,
  stalledCount: 0,
  priority: 0,
  availableAt: DateTime.utc(2026),
  options: JobOptions(),
  parentKey: null,
  createdAt: DateTime.utc(2026),
  updatedAt: DateTime.utc(2026),
  startedAt: null,
  finishedAt: null,
  execution: null,
  controlRequest: null,
);
