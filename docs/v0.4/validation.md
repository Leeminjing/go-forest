# v0.4 验证记录

日期：2026-10-25。固定实现基线：`fbb1334`。规格及设计按仓库约定只在本地保存。

## 实现范围

新增 extensions/observe、adapters/gin、extensions/observatory 三个独立模块，分别负责诊断转换、原生 Gin 适配和只读观察产品。Core、Scope、生命周期、compose、bridge 与根 go.mod 未修改。Gin v1.12.0 仅进入 Go 1.25 的扩展，Observe 维持 Go 1.23。

## 按规格核对

| 验收 | 证据 |
| --- | --- |
| R01 树与关系 | Observe 的 `TestDefinitionTopologyCursorAndDetachedData`、`TestCrossBranchManyBindingsPreserveOrderAndContracts` 经真实 Forest 验证 Nest、BindNest、跨分支 Link、契约、阶段、多值绑定顺序与副本隔离；浏览器验证图和详情。 |
| R02 原生 Gin | Gin Adapter 的 `TestNativeGinAndPinnedHotReplacement` 使用真实 TCP，覆盖中间件、参数、JSON 绑定和状态码；业务依赖图检查只含标准库与 forest。 |
| R03 版本固定 | 同一 Gin 测试阻塞 v1，替换 v2 后新请求两次调用均为 v2、旧请求两次均为 v1，检查请求取消；Observatory 集成和浏览器重复验证完整 HTTP 链路。 |
| R04 排空采样 | `TestHTTPHotReplacementAndLifecycleSamplingWithoutPinnedStream` 及浏览器脚本在旧请求阻塞期间看到 Draining，释放后无额外 Apply 仍看到 Stopped。 |
| R05 时间线 | Observe 的 `TestGapRestartFailuresAndConcurrentRead`、`TestDefinitionTopologyCursorAndDetachedData` 检查失败、冲突、空计划及独立序列，失败不发布拓扑。 |
| R06 恢复 | `TestAuthenticationCursorRecoveryAndReadOnlyRoutes` 检查缺口、正常恢复和 boot 变化；`TestRestartOnSameAddressesResetsOldCursor` 在原监听地址重建真实运行时。浏览器通过 TCP 转发真正断开连接，离线期间生成超出窗口的变更，恢复后显示缺口且事件不重复。 |
| R07 一致性窗口 | `TestPublishedTopologyDoesNotAdvanceDeliveredCursor` 用可控内部读源构造合法错位窗口，保证游标不取 Snapshot.EventSequence；真实 Forest 并发读取另有回归。未为测试修改 Core。 |
| R08 长流 | 集成测试保持 SSE 时替换、排空和 Close；`TestTwoPlacementsAndRemovalTerminatesStream` 验证移除与多个组合实例；`TestSlowSocketWriteTimeoutAndStopCancellation` 用真实小接收缓冲 TCP 套接字及大诊断数据阻塞写入，分别验证写超时和 Stop 主动取消。 |
| R09 资源 | Gin 的 `TestPreparationRollbackReleasesListeningServer` 占用端口制造失败，确认原 Generation 不变且先启动的 Server 端口回收；集成及浏览器保留 SSE 时 Close/quit 正常。 |
| R10 只读 | 认证、非法/超前/溢出游标、跨 Origin、POST 拒绝及匿名静态资源均有 HTTP 验证；Observe 检查 raw error 不出网和 uint64 JSON 精度；浏览器恶意树名仅显示为文本。 |
| R11 浏览器 | `tests/browser_test.py` 使用真实 Chromium，检查拓扑、详情、关系筛选与直接依赖高亮、关系阶段/契约、键盘选择、热替换、排空、恢复提示、390px 窄屏无页面横向溢出、空数据和无事件状态。审查后增加历史折叠区在普通采样与数据变更后保持展开/焦点、独立查看已移除和未发布树的实例诊断。 |
| R12 模块 | 根与各扩展的本地 vet/test 通过；多模块 CI 已配置。Linux 四模块三轮 race 已通过。独立扩展尚未发布，无 replace 的已发布版本消费者验收尚未执行。 |

空森林及恶意标识为明确的浏览器 UI 输入；HTTP 热替换、SSE 生命周期和恢复使用真实 Forest、进程和 TCP，不以模拟传输替代。

## 执行状态

- Windows Go 1.26.5：根模块及三个扩展的 `go vet ./...`、`go test ./...` 已通过。
- Python 3.12 / Playwright 1.58.0 / Chromium：浏览器验收通过，截图位于本地 `.scratch/v0.4/overview.png`、`draining.png`、`mobile.png`。
- Linux Docker Go 1.26，CGO=1：四个模块 `go vet ./...` 和 `go test -race ./... -count=3 -timeout 120s` 通过，无 race 报告；Core/bridge、compose、Observe、Gin Adapter 和 Observatory 均在验证范围内。
- Go 1.23 Linux 容器单独运行 Observe 测试通过，验证其最低版本。
- Core、bridge、根 go.mod 差异检查为空。独立代码审查与修复见 [review.md](review.md)。

## 发布边界

本轮实现没有推送、打标签或发布。各扩展 go.mod 使用本地 replace 和待发布 v0.1.0 版本。源码实现与本地验收不能证明这些版本可被公网 Go Proxy 下载；发布后必须另行在无 replace 的独立消费者中验证。因此 R12 的发布消费者部分尚未完成，不能宣称 v0.4 已正式发布。
