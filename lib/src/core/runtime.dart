import 'dart:math';

/// Backend 运行时注入的类型别名。
///
/// 这些类型是 Backend 实现与高层组件之间的运行时依赖抽象：
/// - [Now]：获取当前时间
/// - [IdGenerator]：生成唯一字符串标识
/// - [Delay]：等待指定时长后返回
typedef Now = DateTime Function();
typedef IdGenerator = String Function();
typedef Delay = Future<void> Function(Duration duration);

/// 默认时间源：直接返回当前时间，供所有构造器在调用方省略 [Now] 时使用。
///
/// 使用常量函数引用而非字面闭包，使各构造器可共享同一默认引用，避免散落
/// 多处的 `() => DateTime.now()` 重复定义，同时满足可选参数默认值必须常量的约束。
const Now defaultNow = _defaultNowValue;

/// 默认 ID 生成器：进程内唯一，无需引入额外依赖。
///
/// 实现要点：时间戳（微秒）+ 单调递增计数器 + 随机后缀三段组合。
/// 计数器保证同一进程内严格不重复，随机后缀将跨进程时钟对齐时的碰撞概率
/// 降到可忽略；用于 executionId / eventId / logId / workerId / jobId。
const IdGenerator defaultIdGenerator = _defaultIdValue;

/// [defaultNow] 的底层实现。
DateTime _defaultNowValue() => DateTime.now();

/// [defaultIdGenerator] 的底层实现。
///
/// 序列号单调递增避免 `int` 溢出影响唯一性（实际远不会达到），
/// 三段均以 36 进制编码以缩短字符串长度。
int _idSequence = 0;
final Random _idRandom = Random();

String _defaultIdValue() {
  final int sequence = _idSequence++;
  final int timestamp = DateTime.now().microsecondsSinceEpoch;
  final int random = _idRandom.nextInt(1 << 24);
  return '${timestamp.toRadixString(36)}'
      '-${sequence.toRadixString(36)}'
      '-${random.toRadixString(36)}';
}
