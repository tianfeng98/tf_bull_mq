import 'package:tf_bull_mq/src/backend/index.dart';
import 'package:tf_bull_mq/src/core/index.dart';

/// 只实现 Flow 相关方法的 QueueBackend fake。
///
/// 仅提供 [addFlow]、[getFlow] 和 [close] 有状态实现；其他方法均抛
/// StateError，保证测试不会意外触达未预期的 Backend 能力。
final class FakeFlowBackend implements QueueBackend {
  FakeFlowBackend({required this.capabilities});

  @override
  final BackendCapabilities capabilities;

  final List<AddFlowRequest> addFlowRequests = <AddFlowRequest>[];
  final List<JobKey> getFlowRequests = <JobKey>[];
  late BackendWriteResult<FlowSnapshot> addFlowResult;
  FlowSnapshot? flowResult;
  Object? addFlowError;
  var closeCalls = 0;
  Object? closeError;

  @override
  Future<BackendWriteResult<FlowSnapshot>> addFlow(
    AddFlowRequest request,
  ) async {
    addFlowRequests.add(request);
    if (addFlowError != null) {
      // ignore: null_check_on_nullable_type_parameter
      throw addFlowError!;
    }
    return addFlowResult;
  }

  @override
  Future<FlowSnapshot?> getFlow(JobKey root) async {
    getFlowRequests.add(root);
    return flowResult;
  }

  @override
  Future<void> close() async {
    closeCalls++;
    if (closeError != null) throw closeError!;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => throw StateError(
    'Unexpected QueueBackend call: ${invocation.memberName}',
  );
}
