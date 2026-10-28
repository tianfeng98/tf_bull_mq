/// 判断 [value] 是否为 JSON-like 值。
///
/// JSON-like 值包含：`null`、`bool`、`String`、有限 `num`、
/// 元素为 JSON-like 的 `List<Object?>`、以及 key 为 `String` 且 value 为
/// JSON-like 的 `Map<Object?, Object?>`。
///
/// 注意：`double.nan`、`double.infinity` 和 `double.negativeInfinity` 不是有限数，
/// 因此也不是 JSON-like。
bool isJsonLike(Object? value) {
  if (value == null || value is bool || value is String) {
    return true;
  }
  if (value is num) {
    return value.isFinite;
  }
  if (value is List<Object?>) {
    return value.every(isJsonLike);
  }
  if (value is Map<Object?, Object?>) {
    return value.entries.every(
      (entry) => entry.key is String && isJsonLike(entry.value),
    );
  }
  return false;
}

/// 断言 [value] 为 JSON-like，否则抛 [ArgumentError]。
///
/// [argumentName] 用于构造错误消息中的参数名，默认为 `'value'`。
void validateJsonLike(Object? value, {String argumentName = 'value'}) {
  if (!isJsonLike(value)) {
    throw ArgumentError.value(value, argumentName, '必须为 JSON-like');
  }
}

/// 递归冻结 JSON-like 数据，返回不可变的深拷贝。
///
/// 标量（`null`、`bool`、`String`、有限 `num`）直接返回原值；
/// `List` 递归转换为 `List<Object?>.unmodifiable`；
/// `Map` 递归转换为 `Map<String, Object?>.unmodifiable`。
/// 调用方后续修改原始集合不会影响冻结结果；对冻结结果自身的
/// 写操作会抛 `UnsupportedError`。
///
/// 输入若不是 JSON-like（如 `DateTime`、`double.nan` 等），
/// 按 [validateJsonLike] 同样的语义抛 `ArgumentError`，
/// 参数名默认为 `'value'`，可通过 [argumentName] 指定以定位出错字段。
Object? freezeJsonLike(Object? value, {String argumentName = 'value'}) {
  // 非法输入复用现有校验，保持错误格式一致
  if (!isJsonLike(value)) {
    throw ArgumentError.value(value, argumentName, '必须为 JSON-like');
  }
  return _freeze(value);
}

/// 递归冻结实现：假设输入已校验为 JSON-like。
Object? _freeze(Object? value) {
  if (value == null || value is bool || value is String || value is num) {
    return value;
  }
  if (value is List<Object?>) {
    // 递归冻结每个元素，再包装为 unmodifiable List
    final frozen = value.map(_freeze).toList(growable: false);
    return List<Object?>.unmodifiable(frozen);
  }
  if (value is Map<Object?, Object?>) {
    // key 必定是 String（isJsonLike 已保障），递归冻结 value 后包装
    final frozen = <String, Object?>{};
    for (final entry in value.entries) {
      frozen[entry.key as String] = _freeze(entry.value);
    }
    return Map<String, Object?>.unmodifiable(frozen);
  }
  // 理论上不会到达此处，isJsonLike 已排除其他类型
  throw ArgumentError.value(value, 'value', '必须为 JSON-like');
}
