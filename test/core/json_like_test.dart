import 'package:test/test.dart';
import 'package:tf_bull_mq/src/core/index.dart';

void main() {
  /// 验证 [isJsonLike] 接受合法的嵌套 JSON-like 结构，
  /// 包括嵌套 Map、List 以及 null 值。
  test('接受包括 null 在内的嵌套 JSON-like 值', () {
    expect(
      isJsonLike(<String, Object?>{
        'ok': true,
        'items': <Object?>[1, 'two', null],
      }),
      isTrue,
    );
  });

  /// 验证 [isJsonLike] 在任意深度都拒绝 NaN 或 Infinity 这类非有限数值，
  /// 保证序列化后的 JSON 不携带非法数字。
  test('拒绝任意嵌套层级中的非有限数字', () {
    expect(isJsonLike(<Object?>[double.nan]), isFalse);
    expect(isJsonLike(<String, Object?>{'x': double.infinity}), isFalse);
  });

  /// 验证 [isJsonLike] 拒绝非 String key 的 Map，
  /// 以及任何无法映射到 JSON 原始类型的 Dart 对象。
  test('拒绝键非字符串的 Map 和任意对象', () {
    expect(isJsonLike(<Object?, Object?>{1: 'x'}), isFalse);
    expect(isJsonLike(DateTime.utc(2026)), isFalse);
  });

  /// 验证 [validateJsonLike] 抛出的 [ArgumentError.name] 正确反映
  /// 传入的参数名，便于调用方在断言失败时定位出错字段。
  test('validateJsonLike 会指出无效参数的名称', () {
    expect(
      () => validateJsonLike(double.nan, argumentName: 'data'),
      throwsA(isA<ArgumentError>().having((e) => e.name, 'name', 'data')),
    );
  });

  // ==================== freezeJsonLike 测试 ====================

  /// 验证 [freezeJsonLike] 对 JSON-like 标量（null、bool、String、num）
  /// 直接返回原值，不做额外处理。
  test('freezeJsonLike 会原样返回标量值', () {
    expect(freezeJsonLike(null), isNull);
    expect(freezeJsonLike(true), isTrue);
    expect(freezeJsonLike('hello'), 'hello');
    expect(freezeJsonLike(42), 42);
    expect(freezeJsonLike(3.14), 3.14);
  });

  /// 验证 [freezeJsonLike] 将 List 递归转换为 unmodifiable List，
  /// 嵌套层级中的 Map/List 同样被冻结。
  test('freezeJsonLike 会将 List 深度设为不可修改', () {
    final original = <Object?>[
      1,
      <String, Object?>{'a': 2},
      <Object?>[3],
    ];
    final frozen = freezeJsonLike(original) as List<Object?>;

    expect(frozen, isA<List<Object?>>());
    expect(() => frozen.add(4), throwsUnsupportedError);
    // 嵌套 Map 也必须不可修改
    final nestedMap = frozen[1] as Map<String, Object?>;
    expect(() => nestedMap['b'] = 3, throwsUnsupportedError);
    // 嵌套 List 也必须不可修改
    final nestedList = frozen[2] as List<Object?>;
    expect(() => nestedList.add(4), throwsUnsupportedError);
  });

  /// 验证 [freezeJsonLike] 将 Map 递归转换为 unmodifiable Map，
  /// 且与原始输入解耦——修改原始 Map 不影响冻结结果。
  test('freezeJsonLike 会将 Map 深度设为不可修改并与原对象脱离', () {
    final original = <String, Object?>{
      'a': 1,
      'nested': <String, Object?>{'b': 2},
      'list': <Object?>[1, 2],
    };
    final frozen = freezeJsonLike(original) as Map<String, Object?>;

    expect(frozen, isA<Map<String, Object?>>());
    expect(() => frozen['c'] = 3, throwsUnsupportedError);
    // 修改原始 Map 不影响冻结结果
    original['a'] = 999;
    expect(frozen['a'], 1);

    // 嵌套 Map 冻结且解耦
    (original['nested'] as Map<String, Object?>)['b'] = 999;
    final nestedMap = frozen['nested'] as Map<String, Object?>;
    expect(nestedMap['b'], 2);

    // 嵌套 List 冻结且解耦
    (original['list'] as List<Object?>).add(999);
    final nestedList = frozen['list'] as List<Object?>;
    expect(nestedList, hasLength(2));
  });

  /// 验证 [freezeJsonLike] 对非法输入（如 DateTime、NaN）抛出
  /// [ArgumentError]，与 [validateJsonLike] 保持一致。
  test('freezeJsonLike 会拒绝非 JSON-like 值', () {
    expect(() => freezeJsonLike(DateTime.utc(2026)), throwsArgumentError);
    expect(() => freezeJsonLike(double.nan), throwsArgumentError);
  });
}
