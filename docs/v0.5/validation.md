# v0.5 gRPC 开发验收

2026-10-25。开发基线为 v0.4 发布闭环提交 ddbe2b9；Spec/Design 按仓库约定仅在本地保存。本期 gRPC 模块尚未发布版本标签。

## 范围

新增 adapters/grpc（Go 1.25、grpc-go v1.84.0、Core v0.3.0，无 replace）和 examples/frameworks 组合示例。Core、bridge、根 go.mod、已发布 Gin/Observe/Observatory 实现没有改动。示例仅通过显式 replace 使用尚未发布的 gRPC Adapter，其他依赖使用正式 v0.4 版本。

## 验收证据

| 验收 | 测试及结果 |
| --- | --- |
| G01 原生服务 | grpc-go 自带生成 TestService 覆盖 Unary/服务端流/客户端流/双向流；`TestNativeInterceptorsMetadataStatusAndLimits` 验证原生链式拦截器前后均有 Scope、metadata、应用状态码和消息限制；`TestNativeTransportCredentials` 用实际 TLS 连接验证原生 Credentials。 |
| G02 Unary | `TestUnaryHotReplacementPinsWholeCall` 阻塞 v1、替换 v2；旧 RPC 两次读取均 v1/generation=1，新 RPC 两次均 v2/generation=2。 |
| G03 流 | `TestBidirectionalStreamPinsAcrossMessages` 在已有流的两条消息之间替换；新 Unary/新流用 v2，旧流第二条消息仍 v1。`TestServerAndClientStreamingRemainNative` 覆盖另外两种流，旧 Provider 至流结束才 Stop。 |
| G04 取消 | `TestCancellationDeadlineAndIgnoredCancellation` 验证客户端取消、截止、CancelGeneration；忽略取消的业务不提前 Stop。`TestStreamCancellationReleasesScopedProvider` 对双向流分别验证客户端、截止与协作版本取消。 |
| G05 资源 | 注册错误/panic、重复服务/类型不匹配、Scope 槽冲突导致候选失败；`TestNativeServerIsStoppedAfterRegistrationPanic` 确认真正停止原生 Server；端口占用验证已启动监听器回滚；`TestRollbackStopDeadlineDoesNotReleaseRunningHandler` 观察超时 Draining，直到真实 handler 完成才 Stopped。 |
| G06 Close | 活动且忽略取消的 RPC 导致 Close 截止返回 ErrDrainTimeout，后续新调用为脱敏 Unavailable，资源保留至实际完成；监听树不声明 HotSwappable。 |
| G07 完整组合 | `TestGinGRPCAndObservatoryShareOneBusinessTree` 以真实 TCP 检查五树、两 Link、一 BindNest；HTTP/RPC 共用 Provider，旧 HTTP 请求和双向流都固定 v1，新请求 v2，旧流结束后才 Stopped，保留 SSE 时 Close 完成。 |
| G08 交付 | Windows 新模块 vet/test 通过；Linux 七模块 vet 和三轮 race 通过；真实服务端/RPC 客户端进程及 Chromium 演示验收通过。CI 已增加新模块与组合浏览器验收。独立两轴审查进行中。 |

Python `examples/frameworks/tests/demo_test.py` 构建并启动两个真实 Go 进程，与 HTTP 请求和 Chromium 同步操作；stdin 只用于显式演示命令，不替代 RPC 业务通信。截图保存在本地 `.scratch/v0.5/frameworks.png`。业务包依赖图只包含标准库和 forest。

## 执行

Windows Go 1.26.5：新模块各自 `go vet ./...`、`go test ./... -timeout 60s` PASS。组合 Python/Playwright 1.58.0/Chromium PASS。

Linux Docker Go 1.26，CGO=1、GOWORK=off：根、Observe、Gin、Observatory、已发布版本消费者、gRPC、frameworks 七模块分别执行 vet 和 `go test -race ./... -count=3 -timeout 120s` PASS。后续新增定向测试在最终审查后再做相关模块回归。

## 明确语义

流的 Scope 覆盖整条 RPC，不在每条消息间自动迁移版本。客户端取消不证明忽略 Context 的业务已经完成；CancelGeneration 也不能保证打断原生 RecvMsg 或第三方阻塞。新增原生服务类型仍须在 Serve 前注册，运行中仅替换业务实现。注册描述预校验只服务于候选回滚，没有业务接口反射代理。

v0.4 的三个 v0.1.0 模块与 R12 已正式发布并验收，记录见 [v0.4 发布验收](../v0.4/release.md)。gRPC 本轮为开发交付，不复用或修改已经发布的标签。
