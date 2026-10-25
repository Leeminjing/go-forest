# 多框架森林验收

2026-10-25。实现基线 12960b0，审查修复 d2b641f。本地已批准行为规格和设计不提交；下列记录描述实际执行证据。

## 实现边界

独立 adapters/gorm、adapters/nats、extensions/telemetry 与 examples/ecosystem；Observatory 增加可选运行数据读取与展示。Core、根 go.mod、Python Focus、Focus-Go 和 go-forest-agent 源码均未修改。复用发布的 Agent v0.2.0；未增加框架专属 Core 分支。

## 逐项验收

| 项目 | 自动化证据与结果 |
| --- | --- |
| A1 数据库 | GORM `TestNativeTransactionRollbackAndBorrowedOwnership`：真实 PostgreSQL，失败事务零记录、成功事务两记录、pg_sleep 期间取消无第三条记录；借用池在 Forest 退出后仍可查询。未知 Commit 单测 `TestUnknownCommitIsTaggedWithoutRetry` 通过；该分类单测使用故障 driver，不冒充真实网络事务验收。 |
| A2 消息 | NATS `TestExplicitAdmissionHotReplacementAndQuiesce`、`TestCandidateRollbackNeverConsumes`：未 Enable 不投递，候选失败不消费，最多配置并行数，停用等待旧工作。`TestRedeliveryIsBoundedAndFailuresObservable` 验证三次失败后终止诊断。 |
| A3 版本 | NATS 旧消息 v1/gen1，新消息 v2/gen2；完整示例 `TestOldHTTPAndRPCPinBusinessVersion` 验证旧 HTTP/RPC 与新 HTTP 版本交接和 Draining；遥测记录固定版本而非当前快照。原有 gRPC 流测试继续保留。 |
| A4 崩溃/重复 | `TestRealProcessCrashRecoveryAndInboxDedup` 用真实 Go 子进程在发布确认后/outbox 记账前、SQL 提交后/Ack 前、Agent 创建后结算前被 Kill；恢复后完成原任务。发布场景越过一秒去重窗口，再发新传输 ID；真实 DB 的 inbox 仍一份。 |
| A5 拓扑 | 必需 Handler 单独断开拒绝、消费资源不安全替换拒绝、候选启动失败版本不增加。既有 Core 套件继续验证同计划重绑、原子多操作、歧义和并发版本冲突；本轮未另造事务实现。 |
| A6 资源 | GORM `TestOwnedPoolWaitsForPinnedWorkAndRollbackCleans`：活动调用结束前池可用，结束后 owned 池真实关闭，准备失败的 owned 池清理。NATS 停用超时仍报告在途，旧工作释放后可 Remove；borrowed 连接仍可发布。 |
| A7 组合 | `TestSixFrameworksCompleteAndIdempotent`：同 Forest 18 树、16 Nest、15 Link、1 BindNest；HTTP/RPC、GORM/PostgreSQL、JetStream、Agent、Observatory 完成结果 `model v1: 5`；相同请求返回同一完成结果。W3C trace 从入口经持久 outbox 到消费保持关联。 |
| A7 恢复规则 | `TestApprovalAndExplicitUnknownResolutionResumeSettlement`：暂停→原生授权→完成；公开 Checkpoint 故障 fixture 模拟未保存结果的 ToolRunning，Recover 后保持 OutcomeUnknown，多次 Tick 不重放，显式 Resolve 后完成。该未知工具 fixture 不宣称又做了一次真实工具中途进程 Kill。 |
| A8 观测 | OTel `TestCountersSurviveTraceSamplingAndBoundedDetails`：NeverSample 下计数仍准确，详情丢弃可见、在途归零、过期可见。Observatory 测试鉴权、无数据源、非法/超限 JSON；Chromium 验证真实运行数据、幂等任务、断开后过期以及桌面/手机布局。 |
| A9 工程 | 分模块 vet/test；Linux Core 和五个相关模块分别三轮 race；GORM/PostgreSQL 与 NATS 服务真实重启后保持 Forest 身份。Spec/Design/.scratch 通过 ignore 检查，未纳入提交。 |

发布收尾另增加 `TestTaskIdentityIsUnchangedAcrossURLAndMessageHeaders`：路径分隔、空白和 CRLF 等不稳定 ID 在写任务/outbox 前被拒绝，合法 ID 保持幂等；最终示例再次通过三轮 Linux race。

## 实际命令与环境

- Windows Go 1.26.5、Docker 29.0.1：`python scripts/test-ecosystem.py` PASS，启动独立 PostgreSQL 18 和 NATS 2.12.4 JetStream 容器，包含两类服务重启及三处应用崩溃恢复。
- Linux Docker `golang:1.26`、CGO/race：Core 执行 vet 与 `go test -race ./... -count=3` PASS；五个相关模块分别执行 vet 与同样三轮 race PASS。后者挂载 Docker socket，使服务重启用例实际执行，未只跳过后宣称通过。
- `python examples/ecosystem/tests/browser_test.py` PASS；原 Observatory 完整浏览器回归 PASS（断连/缺口、键盘、恶意标识、移动布局、保留 SSE 时 Close）。新页面截图仅本地 `.scratch/ecosystem/`。
- 本地 Docker DNS 无法访问默认 Go proxy，Linux 使用已有模块缓存及各模块 go.sum；正式发布后的仓库外下载另行验收，不以缓存编译替代发布验证。

CI 已按嵌套模块分别执行测试，并增加真实服务 job、Linux race 三轮和 Chromium。远程 CI/模块标签结果单独记录在 release.md，不能把以上本地结果冒充远程结果。

## 明确限制

池、监听器和消费者入口不承诺无缝热替换；首轮替换业务树。消息入口须提交后 Enable、Quiesce 排空后 Remove。数据库迁移/broker 配置是独立管理步骤。单 driver/namespace，无分布式调度。outbox/inbox 只治理本例数据库效果，不提供任意工具 exactly-once。运行统计覆盖已采集边界，采样 trace 和拓扑连线不能表示完整 Link 流量。
