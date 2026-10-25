# v0.1 实现审查记录

2026-10-24。对比基线 `b38a280`（空仓库）与初版实现 `13b63bc`，按 code-review 技能分别执行 Standards 和 Spec 审查。规格来源为用户提供的 v0.1 文本和本地文档。发现项均已修复；修复后的独立复核没有发现遗留问题。

## Standards

初版发现 3 项硬性违规，未追加启发式 smell 建议。

1. **P1：父资源可早于仍被依赖的子树释放。** 仅为本轮零引用子树保留父实例。P 包含 C，独立 R 通过 Construction 端口依赖 C；同时删除 R 和 P 子树、阻塞 R.Stop 时，P.Stop 仍执行。违反归属停止顺序。现改为实例退出活动拓扑时固定一次最后 owner，覆盖仍被生命周期依赖持有的子树。回归：`TestLifecycleConsumerRetainsChildAndOwnerUntilStop`。
2. **P1：准备 Scope 的并发回调无法阻止回滚释放资源。** A.Factory 启动已经进入的 Construction Ref.Use(C) 并阻塞回调后返回；A.Start 失败时曾提前 Stop C。现关闭准备 Scope 后先取消 Context、等待已进入回调结束，再继续下一阶段或回滚。回归：`TestPreparationJoinsEnteredCallbacksBeforeRollback`。
3. **P2：Stop 失败缺少结构化上下文。** 普通 Stop 错误和 panic 转换结果原样进入 Wait，无法提取树及失败阶段。现统一包装 stop/tree 上下文并保留原始 cause，超时 cause 也可通过 errors.Is 检查。回归：`TestStopFailureRetainsStructuredContext`。

Standards：3 项发现，最高 P1；全部修复，复核通过。

## Spec

初版发现 3 项可复现的规格偏差。

1. **P1：父资源可能先于子实例释放。** 对应“优先停止使用者，再释放其依赖资源”。与 Standards 第一项相同，已修复。
2. **P1：回滚释放仍被候选 Scope 使用的资源。** 对应“不允许释放仍被活动执行作用域引用的实例”。与 Standards 第二项相同，已修复。
3. **P2：合法传递依赖无法构造。** A ← B ← C 全部声明 Construction、无环且排序正确，C.Factory 调 B 能力后，B 的 Ref 因固定最外层消费者限制无法访问 A。违反依赖按已声明拓扑组合的设计。现按阶段和真实实例身份检查，允许合法传递调用。回归：`TestConstructionCanCallThroughTransitiveDeclaredDependencies`。

修复中额外发现的历史归属反转引用环也已解决：g1 中 A 包含 B，g2 安全反转为 B 包含 A，各有未退出 Scope；g3 删除后按多个历史图追加 owner 曾产生互相持有。现在 owner 只在离开活动拓扑时记录一次。回归：`TestReversedOwnershipAcrossPinnedVersionsDoesNotLeak`。

Spec：3 项初版发现，最高 P1；另有 1 项修复回归，全部修复，复核通过。复核对相关测试连续运行 10 次通过；完整测试及 race 验证见 README 的可复现命令。

## 对 2740874 的后续审查：复用 Link ID

用户指出一项 P2：删除旧消费者 A 并将其 Link ID 用于保留消费者 B 的可选构造端口时，`validateRetained` 仅检查该 ID 是否存在于旧图，遗漏旧绑定实际所属的消费者和端口，因此 B 没有重建也可获得新的生命周期依赖。

已通过公开 Apply API 复现。Construction/Startup 下跨消费者、跨端口和 Nest 装配共六个子测试，在修复前均返回成功、发布版本 2。现改为比较完整 Binding，保留提供实例身份检查；拒绝时保留原拓扑和实例，同一计划显式替换消费者后允许提交。另验证多值生命周期依赖不能通过删后重建相同 ID 改变提供者顺序。测试名和最新执行结果见 [validation.md](validation.md)。

本轮以 `2740874` 为基线进行两轴独立复核：Standards 0 项行动项，Spec 0 项行动项。复核确认 Runtime 重绑及保持原绑定的 Rebind 不受影响，新增回归和相关现有测试通过。
