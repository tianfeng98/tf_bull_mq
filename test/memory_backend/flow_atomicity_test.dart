import 'package:test/test.dart';
import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';
import 'package:tf_bull_mq/src/memory_backend/index.dart';

MemoryQueueBackend _backend() {
  var counter = 0;
  final b = MemoryQueueBackend(
    now: () => DateTime.utc(2026, 1, 1),
    idGenerator: () => 'id-${++counter}',
  );
  b.open();
  return b;
}

final _t = DateTime.utc(2026, 1, 1);

void main() {
  test('会原样存储并返回非默认边权重', () async {
    final backend = _backend();
    final root = const JobKey(queueName: 'w', jobId: 'root');
    final a = const JobKey(queueName: 'w', jobId: 'a');
    final b = const JobKey(queueName: 'w', jobId: 'b');

    await backend.addFlow(
      AddFlowRequest(
        flow: FlowJobSpec(
          root: root,
          jobs: <JobSpec>[
            JobSpec(key: root, name: 'root', data: null),
            JobSpec(key: a, name: 'a', data: null),
            JobSpec(key: b, name: 'b', data: null),
          ],
          edges: <FlowEdge>[
            FlowEdge(parent: root, child: a, weight: 3.0),
            FlowEdge(parent: root, child: b, weight: 1.0),
          ],
        ),
        createdAt: _t,
      ),
    );

    final snapshot = await backend.getFlow(root);
    expect(snapshot, isNotNull);
    expect(snapshot!.edges.length, 2);
    final byChild = <JobKey, double>{
      for (final edge in snapshot.edges) edge.child: edge.weight,
    };
    expect(byChild[a], 3.0);
    expect(byChild[b], 1.0);
  });

  test(
    '查询两个 flow 之一时，会排除另一个 flow 的所有边',
    () async {
      final backend = _backend();
      final f1Root = const JobKey(queueName: 'f', jobId: 'f1-root');
      final f1Child = const JobKey(queueName: 'f', jobId: 'f1-child');
      final f2Root = const JobKey(queueName: 'f', jobId: 'f2-root');
      final f2Child = const JobKey(queueName: 'f', jobId: 'f2-child');

      await backend.addFlow(
        AddFlowRequest(
          flow: FlowJobSpec(
            root: f1Root,
            jobs: <JobSpec>[
              JobSpec(key: f1Root, name: 'f1-root', data: null),
              JobSpec(key: f1Child, name: 'f1-child', data: null),
            ],
            edges: <FlowEdge>[FlowEdge(parent: f1Root, child: f1Child)],
          ),
          createdAt: _t,
        ),
      );
      await backend.addFlow(
        AddFlowRequest(
          flow: FlowJobSpec(
            root: f2Root,
            jobs: <JobSpec>[
              JobSpec(key: f2Root, name: 'f2-root', data: null),
              JobSpec(key: f2Child, name: 'f2-child', data: null),
            ],
            edges: <FlowEdge>[FlowEdge(parent: f2Root, child: f2Child)],
          ),
          createdAt: _t,
        ),
      );

      final f1Snap = await backend.getFlow(f1Root);
      expect(f1Snap, isNotNull);
      expect(
        f1Snap!.jobs.map((j) => j.key).toSet(),
        equals(<JobKey>{f1Root, f1Child}),
      );
      expect(f1Snap.edges.length, 1);

      final f2Snap = await backend.getFlow(f2Root);
      expect(f2Snap, isNotNull);
      expect(
        f2Snap!.jobs.map((j) => j.key).toSet(),
        equals(<JobKey>{f2Root, f2Child}),
      );
      expect(f2Snap.edges.length, 1);
    },
  );
}
