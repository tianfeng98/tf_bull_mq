import 'memory_store.dart';

/// 内存 Backed 的原子写入函数类型。
///
/// mutation 接收 draft store，在其上执行任意修改并返回结果值。
typedef MemoryMutation<T> = T Function(MemoryStore draft);

/// 内存 Backed 的简单事务包装。
///
/// 实现 copy-on-write 语义：写入时先对 committed store 做深拷贝得到 draft，
/// 在 draft 上执行 mutation，mutation 返回后把 draft 作为新的 committed store。
/// 这保证了单次写入的原子性——要么 mutation 成功并提交完整新状态，
/// 要么 mutation 抛错且 committed store 保持不变。
final class MemoryTransaction {
  const MemoryTransaction._();

  /// 执行一次原子写入。
  ///
  /// - [committed]：当前 committed store（读取）
  /// - [mutation]：在 draft 上执行的修改逻辑
  /// - [commit]：mutation 成功后被调用，接收 draft 作为新的 committed store
  ///
  /// mutation 抛错时 commit 不会被调用，committed store 不变。
  static T write<T>(
    MemoryStore committed,
    MemoryMutation<T> mutation,
    void Function(MemoryStore draft) commit,
  ) {
    // 先深拷贝得到 draft，保证 mutation 不影响 committed store
    final draft = committed.copy();
    // 在 draft 上执行 mutation
    final result = mutation(draft);
    // mutation 成功后提交 draft
    commit(draft);
    return result;
  }
}
