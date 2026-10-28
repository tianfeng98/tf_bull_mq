/// 可持久化的 Job 复合标识（queueName + jobId）。
///
/// 构造时要求 [queueName] 和 [jobId] 都非空。
/// 实现了 [Comparable]，按 queueName 再按 jobId 的字典序比较，
/// 可作为有序集合和 Map 的 key 使用。
final class JobKey implements Comparable<JobKey> {
  const JobKey({required this.queueName, required this.jobId})
    : assert(queueName != ''),
      assert(jobId != '');

  /// 队列名称。非空字符串。
  final String queueName;

  /// Job 标识。非空字符串，在同一队列内唯一。
  final String jobId;

  @override
  int compareTo(JobKey other) {
    final queueComparison = queueName.compareTo(other.queueName);
    return queueComparison == 0
        ? jobId.compareTo(other.jobId)
        : queueComparison;
  }

  @override
  bool operator ==(Object other) =>
      other is JobKey && other.queueName == queueName && other.jobId == jobId;

  @override
  int get hashCode => Object.hash(queueName, jobId);

  @override
  String toString() => '$queueName/$jobId';
}
