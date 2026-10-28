import 'json_like.dart';

/// Codec 阶段标识，用于异常报告。
enum PayloadCodecStage { encode, decode }

/// 载荷 Codec 异常。
///
/// 当 Codec 编码/解码失败时抛出，携带字段位置、阶段、原始 cause 和堆栈，
/// 帮助调用方快速定位出错的 Codec 和数据。
final class PayloadCodecException implements Exception {
  PayloadCodecException({
    required this.field,
    required this.stage,
    required this.cause,
    required this.stackTrace,
    this.backendCommitted = false,
    this.rawCommittedValue,
  });

  /// 出错字段：'data' / 'progress' / 'result'。
  final String field;

  /// Codec 阶段：encode 或 decode。
  final PayloadCodecStage stage;

  /// 原始错误对象。
  final Object cause;

  /// 原始堆栈。
  final StackTrace stackTrace;

  /// Backend 是否已提交。编码失败时一定为 false；解码失败时可能为 true。
  final bool backendCommitted;

  /// Backend 已提交的原始值（仅解码失败且 backendCommitted=true 时有意义）。
  final Object? rawCommittedValue;

  @override
  String toString() => 'PayloadCodecException($field, ${stage.name}): $cause';
}

/// 业务载荷与 JSON-like 持久化值之间的双向编解码器。
///
/// [PayloadCodec] 是同步、纯函数对象，不持有状态、不访问 Backend。
/// 构造时传入 [encode]（业务值 → JSON-like Object?）和 [decode]（JSON-like Object → 业务值）
/// 两个回调，Codec 外壳统一处理 null 透传、编码后的 freezeJsonLike 校验和解码异常包装。
///
/// 约束：
/// - null 由外壳统一透传，不调用用户回调；
/// - encodeNullable 对回调结果执行 freezeJsonLike，非法输出在任何 Backend 写入前抛错；
/// - decodeNullable 只接收 Backend 已冻结事实；
/// - 默认构造函数 jsonLike() 保留标量及 `Map<String, Object?>` / `List<Object?>` 的兼容用法。
final class PayloadCodec<T extends Object?> {
  /// 构造自定义 Codec。
  ///
  /// [encode]：把业务值转换为 JSON-like Object?；返回 null 表示业务值本身就是 null
  /// （此时外壳直接透传 null，不会再调用 encode）。返回值必须满足 freezeJsonLike 校验。
  /// [decode]：把 Backend 冻结的 JSON-like Object 转回业务值。输入不会为 null。
  const PayloadCodec({
    required Object? Function(T value) encode,
    required T Function(Object value) decode,
  }) : _encode = encode,
       _decode = decode;

  /// 构造默认的 JSON-like Codec：直接透传，仅执行 freezeJsonLike 校验。
  ///
  /// 适用于标量（null、bool、String、num）以及 `Map<String, Object?>` / `List<Object?>`
  /// 等已经是 JSON-like 的值。自定义业务模型或窄泛型集合（`Map<String, int>` 等）
  /// 必须显式提供 encode/decode 回调。
  factory PayloadCodec.jsonLike() => PayloadCodec<T>(
    encode: (T value) => value,
    decode: (Object value) => value as T,
  );

  final Object? Function(T value) _encode;
  final T Function(Object value) _decode;

  /// 将业务值编码为 JSON-like。
  ///
  /// 对 null 直接返回 null，不调用用户回调；非 null 先经用户 [_encode] 转成 JSON-like，
  /// 再经 freezeJsonLike 深度冻结并校验结果合法性。任何异常包装为 PayloadCodecException。
  Object? encodeNullable(T? value) {
    if (value == null) return null;
    try {
      final raw = _encode(value);
      return freezeJsonLike(raw, argumentName: 'encoded value');
    } catch (error, stackTrace) {
      throw PayloadCodecException(
        field: '<unknown>',
        stage: PayloadCodecStage.encode,
        cause: error,
        stackTrace: stackTrace,
      );
    }
  }

  /// 将 Backend 冻结值解码为业务值。
  ///
  /// 对 null 直接返回 null，不调用用户回调；非 null 调用用户 [_decode] 转换。
  /// 任何异常包装为 PayloadCodecException。
  T? decodeNullable(Object? value) {
    if (value == null) return null;
    try {
      return _decode(value);
    } catch (error, stackTrace) {
      throw PayloadCodecException(
        field: '<unknown>',
        stage: PayloadCodecStage.decode,
        cause: error,
        stackTrace: stackTrace,
      );
    }
  }
}

/// 三个 Codec 组成的 bundle：data / progress / result 各一个 PayloadCodec。
///
/// Queue、Worker、Flow 共用同一个 Codec bundle，保证端到端类型一致。
final class JobPayloadCodecs<
  TData extends Object?,
  TProgress extends Object?,
  TResult extends Object?
> {
  /// 构造显式 Codec bundle。
  const JobPayloadCodecs({
    required this.data,
    required this.progress,
    required this.result,
  });

  /// 构造默认的 JSON-like bundle。
  factory JobPayloadCodecs.jsonLike() =>
      JobPayloadCodecs<TData, TProgress, TResult>(
        data: PayloadCodec<TData>.jsonLike(),
        progress: PayloadCodec<TProgress>.jsonLike(),
        result: PayloadCodec<TResult>.jsonLike(),
      );

  final PayloadCodec<TData> data;
  final PayloadCodec<TProgress> progress;
  final PayloadCodec<TResult> result;
}
