# 版本修订记录

## 1.0.0

首个正式发布版本。基于 Backend 抽象契约提供 Queue / Job / Worker / QueueEvents / Flow 全链路作业队列，内置 local + volatile 的 MemoryBackend 与进程内 Signal 原语。

### 新增

- `QueueBackend` 抽象契约，冻结 Queue/Worker/Flow 与底层存储的交互边界。所有写入返回 `BackendWriteResult` 携带 Backend 通知集合；revision 单调递增、原子提交、状态机校验和执行凭证统一由契约保证。
- `Queue<TData, TProgress, TResult>`：生产端和管理端 API，支持 add / addBulk 添加任务、getJob / query 分页查询、pause / resume / close 队列控制、setQueuePolicy 设置默认策略。所有 mutation 携带 revision 做乐观并发控制。
- `Job<TData, TProgress, TResult>`：轻量持久化句柄，支持 refresh / retry / cancel / resume / remove / pause / queryLogs / clearLogs。JobKey 由 `queueName + jobId` 唯一定位。
- `Worker<TData, TProgress, TResult>`：claim-based 消费模型，支持并发控制、Processor 执行、结果 commit、按 maxAttempts + backoff 自动重试、协作式控制（cancel / pause）、local Profile 下的 lease 续期和优雅关闭。
- `Processor` 可调用契约：接收 `ActiveJob` 执行上下文，异步返回结果或抛出业务异常；框架不解释 Processor 内部实现。
- `ActiveJob<TData, TProgress, TResult>`：本次 claim 期间有效的执行上下文，支持 updateProgress / updateData / appendLog / 读取 canceled 信号 / getDependenciesResult<T>()。执行凭证内部携带，业务代码不直接操作。
- `QueueEvents<TEventData>`：基于 caller cursor 的事件订阅，支持 start / close / drain / wakeup，游标落后于已裁剪范围时抛 `EventCursorGap`。
- `FlowProducer`：原子创建任意深度父子依赖树，父子 Job 可跨 Queue。整棵 Flow 同时可见或全部不可见。
- `FlowQuery`：只读依赖投影，支持 direct（展开一层，`child.children` 为空）和 recursive（完整后代树）两种深度，提供加权进度、子结果、依赖状态统计。
- `FlowJob<TData>` / `JobSpec<TData>` / `QueueJobDraft<TData>`：Flow 创建时的类型安全构建器，只需指定载荷类型。
- `MemoryQueueBackend`：完整 Backend 实现，声明 `local` + `volatile` Profile，支持 delayed jobs / priorities / atomicFlows / liveOnly events。使用 `MemoryTransaction` copy-on-write 原子性保证。
- `QueueSignal` 抽象契约 + `InProcessQueueSignal` 进程内实现：低延迟唤醒提示，多订阅者独立感知，Backend 与 Signal 解耦。Signal 可重复、可延迟、可丢失，Worker 和 QueueEvents 都会独立 poll Backend 补偿。
- `WorkerLogSink`：Worker 启动、连接、领取循环、续租、关闭等基础设施诊断日志 sink，与 JobLog 分离。
- `JobLog<TLogData>` / `JobLogDraft<TLogData>`：按 Job 持久化的业务执行日志，支持 attempt / executionId / level / channel 等维度；追加不改变 Job revision，不产生 JobEvent。
- Backend 契约测试套件（base suite）：覆盖 add / claim / progress / retry / failure / Flow / Event cursor 等核心语义，同时预留 distributed / durable 专属测试注册口。
- 全量集成测试与夹具：覆盖 active execution、Flow 原子性、事件边界、stalled 恢复、QueueEvents drain handoff、Worker 控制流程、Recovery 失败可见等场景。
- example/tf_bull_mq_example.dart：完整流水线示例，覆盖 progress → data → JobLog → result、QueueEvents 等待 completed、Flow 加权 3:2、direct vs recursive 投影、所有资源 try/finally 反向关闭。
- 全量简体中文注释与 AGENTS.md 开发规范。

### 变更

- Flow 原子性：MemoryStore 以完整 FlowEdge（含 weight）持久化边，getFlow 只返回 root 可达子树，多 Flow 互不污染。direct projection 只展开一层（child.children 为空），weightedProgress 数值与 recursive 保持一致。
- Flow 父 Job retry 三重判定顺序：存在 failed / canceled 子依赖时 Backend 抛 `FlowDependencyFailure` 拒绝；存在运行中子时覆盖为 `waitingChildren`；无子依赖或子全部完成时使用请求的 waiting / delayed。
- 活跃执行保留：首次 progress / data 更新后 execution 不再丢失，后续写入不会误判 LeaseLost。
- Flow 依赖恢复传播：子 Job retry 回 waiting 后，父 Job 正确恢复为 `waitingChildren` 而非 `waiting`。
- 事件游标 Gap：裁剪后即使事件列表为空，旧 cursor 仍正确抛出 `EventCursorGap`。
- QueueEvents close 聚合错误：Signal 与 Backend 独立 try/catch，始终尝试关闭二者，不遗漏任何待释放资源。
- JSON-like 事实边界：所有 Backend 返回的 DTO（JobRecord / JobEvent / JobLog / JobSpec / WorkerLogEntry）在构造时递归冻结，调用方无法修改 Backend 事实。
- JobControl 幂等语义：cancel 可升级 pause，但 pause 不能降级 cancel；相同 action 返回 `changed=false`。
- 资源所有权：Queue / Worker / QueueEvents / Flow 共享 Backend 和 Signal；默认 ownership 为 false，关闭操作不擅自关闭外部注入的共享资源。

### 修复

- MemoryStore.trimmedEventPositionByQueue 正确记录裁剪位置，EventCursorGap 检测不再误报。
- QueueEvents drain handoff 边界竞态修复，completed 事件即时可见无需额外 poll。
- recovery failure 可见：forceClose 时 recoverStalledJobs 失败向上传播，Worker 保持 closing 状态以允许后续重试。
- 根库导出顺序与 lint directive 位置修复，dart format / analyze 全绿。
- docs → doc 目录重命名，内部引用全部同步。
