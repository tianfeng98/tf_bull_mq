import 'job_key.dart';
import 'job_options.dart';
import 'job_record.dart';
import 'job_status.dart';
import 'payload_codec.dart';
import 'queue_state.dart';

/// 类型化的 Job 快照：Backend 原始事实 + 业务类型解码视图。
///
/// [JobSnapshot] 内部持有非泛型的原始 [JobRecord]（所有载荷字段都是已冻结的
/// JSON-like Object?），通过注入的 [JobPayloadCodecs] 把 data/progress/result
/// 解码为业务类型供调用方使用。它不写 Backend、不修改原始记录，是纯投影。
///
/// 替换 snapshot 必须先完整解码成功再原子替换；解码失败时保留旧 snapshot。
/// Job 句柄会同时缓存原始记录和已解码快照。
final class JobSnapshot<TData extends Object?, TProgress extends Object?, TResult extends Object?> {
  JobSnapshot({required this.rawRecord, required JobPayloadCodecs<TData, TProgress, TResult> codecs})
    : _codecs = codecs,
      data = codecs.data.decodeNullable(rawRecord.data),
      progress = codecs.progress.decodeNullable(rawRecord.progress),
      result = codecs.result.decodeNullable(rawRecord.result);

  /// Backend 返回的原始 JSON-like 冻结记录，无业务泛型。
  final JobRecord rawRecord;

  final JobPayloadCodecs<TData, TProgress, TResult> _codecs;

  /// 业务 data，已通过 codecs.data 从原始值解码。
  final TData? data;

  /// 业务 progress，已通过 codecs.progress 从原始值解码。
  final TProgress? progress;

  /// 业务 result，已通过 codecs.result 从原始值解码。
  final TResult? result;

  // --- 只读 getter 委托给 rawRecord，避免维护重复状态 -----------------------

  /// 复合标识（queueName + jobId）。
  JobKey get key => rawRecord.key;

  /// Job 名称。
  String get name => rawRecord.name;

  /// 当前生命周期状态。
  JobStatus get status => rawRecord.status;

  /// 失败摘要，可空。仅 Job 处于 failed 状态时有值。
  JobFailure? get failure => rawRecord.failure;

  /// Backend 乐观并发版本号。每次写入递增。
  int get revision => rawRecord.revision;

  /// 已完成的尝试次数（从 0 开始；首次 claim 后为 1）。
  int get attemptsMade => rawRecord.attemptsMade;

  /// Stall 恢复计数。
  int get stalledCount => rawRecord.stalledCount;

  /// 优先级，数值越大越优先领取。
  int get priority => rawRecord.priority;

  /// 下次可领取的时间点。
  DateTime get availableAt => rawRecord.availableAt;

  /// Job 级运行选项。
  JobOptions get options => rawRecord.options;

  /// Flow 父 Job 的复合标识；无 Flow 时为 null。
  JobKey? get parentKey => rawRecord.parentKey;

  /// Job 创建时间。
  DateTime get createdAt => rawRecord.createdAt;

  /// 最后一次 Backend 写入该 Job 的时间。
  DateTime get updatedAt => rawRecord.updatedAt;

  /// 首次 claim 的时间；Job 从未 claim 时为 null。
  DateTime? get startedAt => rawRecord.startedAt;

  /// 到达终态的时间；非终态时为 null。
  DateTime? get finishedAt => rawRecord.finishedAt;

  /// 当前执行所有者信息；Job 未 claim 时为 null。
  ExecutionOwner? get execution => rawRecord.execution;

  /// 未处理的暂停/取消控制请求；无请求时为 null。
  JobControlRequest? get controlRequest => rawRecord.controlRequest;

  /// 载荷字段再次解码；当 Codec 失败且 backendCommitted=true 时调用。
  ///
  /// 用于 Backend 已提交事实但返回值解码失败后的错误报告，
  /// 提供 rawCommittedValue 供上层聚合异常。
  PayloadCodecException? tryRedecode() {
    PayloadCodecException? first;
    try {
      _codecs.data.decodeNullable(rawRecord.data);
    } on PayloadCodecException catch (e) {
      first ??= e;
    }
    try {
      _codecs.progress.decodeNullable(rawRecord.progress);
    } on PayloadCodecException catch (e) {
      first ??= e;
    }
    try {
      _codecs.result.decodeNullable(rawRecord.result);
    } on PayloadCodecException catch (e) {
      first ??= e;
    }
    return first;
  }
}
