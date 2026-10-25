# gRPC 开发与 v0.4 发布收尾审查

2026-10-25。固定基线为用户审查的 71536e0，审查 HEAD 为 bf564fd。按 implement / code-review 技能，两名独立审查者分别检查 Standards 与 Spec；规格包括本地 v0.4/v0.5 文档、公开 README 和原始用户目标。

## Standards

硬性违规 0 项，启发式 smell 0 项。

检查了注册失败/panic 后已分配原生 Server 的回滚、重复服务及类型校验、Scope 单一拦截器槽、Unary/整流 Context、忽略取消后的资源保留、Stop 超时与真实 handler 结束、已发布模块无 replace、仓库外消费者及 CI。未发现可行动问题。

审查者定向运行 gRPC 模块测试通过，没有重复完整 race。

## Spec

G01–G08 未发现缺失、错误实现或范围扩张，0 项行动项。

检查了固定 Generation、原生拦截器/状态码、注册与监听失败回滚、超时后继续等待真实 handler，以及同森林 HTTP/RPC/Observatory 的组合。审查者定向重跑 Unary、双向流、注册失败、回滚超时和完整组合测试，均通过。

v0.4 R12 已有真实发布与外部消费者证据；新 gRPC 模块保持未发布，符合本轮开发交付范围。

## 主流程验证

七模块 Linux 三轮 race、两个新模块最终定向三轮 race、Windows vet/test、真实服务端及 RPC 客户端进程加 Chromium 回归均通过。主流程未修改 Core、bridge 或已发布扩展实现。具体测试映射见 [validation.md](validation.md)，正式 v0.4 发布见 [release.md](../v0.4/release.md)。

## v0.5 P0 发布收尾复核

随后用户授权正式发布 gRPC，并明确 Focus-Go P1 沿用原聊天。固定基线 220fadb，复核 HEAD bf157ff；只改发布说明、组合依赖、外部消费者与验证记录，没有改 gRPC 实现或 Core。

Standards：硬性违规 0 项、启发式 smell 0 项。审查者重跑仓库外消费者脚本，确认 GOWORK=off、四模块 v0.1.0、无 Replace、模块完整性校验及真实网络验收通过。

Spec：P0 四项均满足，0 项行动项。独立确认 tags/adapters/grpc/v0.1.0 指向 04bc232，Release 非草稿、非预发布；无 replace 消费者与正式版本组合测试通过，gRPC 从下载缓存读取且校验和一致。未审查或修改 Focus-Go 的并行工作。

主流程的消费者/frameworks Linux 三轮 race、Windows 外部测试与组合进程/Chromium验收均通过，详见 [发布验收](release.md)。
