# v0.1 验证记录

2026-10-24，Windows/amd64 Go 1.26.5 主机；race detector 在 Docker 的 Linux/amd64 `golang:1.26` 环境运行。

| 验证 | 结果 |
| --- | --- |
| `go vet ./...` | 通过 |
| `go test ./... -timeout 60s` | 28 个顶层验收/回归测试通过，另含子测试 |
| `go test -race ./... -count=3 -timeout 90s` | Linux Docker，完整测试连续三轮通过，无 data race 报告 |
| 核心语句覆盖率 | 87.6%，使用 `-coverpkg=go-forest/forest` |
| Hello Forest | `forest: 2026-10-24T08:00:00Z`，版本 1、三树、两条 Link |
| Nesting | clock → formatter → printer 两级父子关系及实际调用通过 |
| Hot Swap | 新调用返回 2，替换前进入的旧调用返回 1 |
| Web/Agent | 本机 HTTP 请求返回 `agent: model: hello`，自动停止 |
| 两轴独立审查 | 初版问题修复后复核通过，记录见 [review.md](review.md) |

普通测试在主机运行；Windows 原生 race 未执行，因为主机没有已配置的 C 工具链。Linux Docker race 命令已经实际执行，完整命令见 [README](../README.md#验证)。CI 配置覆盖 Windows/Linux 普通测试与 Linux race；此记录不表示远程 CI 已经运行。

测试从公开 Go API 验证，十项用户验收场景的逐项映射见 [Spec](spec.md#验收映射)。热替换、删除占用、并发冲突、同一调用跨 Link 版本一致性、构造资源持有、准备回调回滚保护和历史归属反转都有自动化用例。

本版安全边界：只承诺单进程内存拓扑；树必须遵守 Context、作用域与资源准备协议。Stop 超时不强制释放资源；忽略取消的准备回调会继续被等待。HotSwappable、Reparentable、PreserveChildren 等声明是树实现者需自行验证的行为承诺。
