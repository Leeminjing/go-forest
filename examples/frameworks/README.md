# 同一业务树上的 Gin、gRPC 与 Observatory

五棵普通树：HTTP、gRPC、Business、Observatory 根及其 Inspector 子树。HTTP 与 gRPC 都通过自己的 Runtime Ref 使用同一业务树，观察组合通过 BindNest 读取诊断。Core 和业务包都没有传输框架分支。

本示例最低 Go 1.25。依赖正式发布的 Core v0.3.0、Gin/Observatory v0.1.0；仅尚未发布的 gRPC Adapter 使用 `../../adapters/grpc` 本地 replace。这不影响 v0.4 已完成的无 replace 外部发布验收。

## 运行

PowerShell 在本目录：

```powershell
$env:FOREST_OBSERVATORY_TOKEN = 'replace-with-your-random-token'
go run ./cmd/demo
```

默认 HTTP 为 127.0.0.1:8080，gRPC 为 127.0.0.1:8090，观察页面为 127.0.0.1:8089。ready JSON 给出实际地址；三个地址标志都支持 `127.0.0.1:0`。在观察页面中填入相同 token。

另开终端请求 HTTP：

```powershell
curl.exe 'http://127.0.0.1:8080/work'
go run ./cmd/client -target 127.0.0.1:8090
```

两种调用都返回 value/again=v1、generation=1。再开一个流客户端：

```powershell
go run ./cmd/client -target 127.0.0.1:8090 -stream
```

第一次输出 v1 后等待 Enter。在服务端终端输入 `replace`，新 HTTP 与 Unary RPC 都返回 v2；旧流客户端按 Enter 后仍返回 v1/generation=1，然后正常结束。观察页面在流结束前显示旧业务实例 Draining，结束后采样到 Stopped。

也可用 HTTP `/work?hold=1` 或 Unary 客户端 `-hold` 保持旧调用。服务端的 `wait` 等业务进入，`release` 释放被保持的业务调用。所有管理操作仅通过 stdin，观察 HTTP 接口保持只读。

先结束长流，再用服务端 `quit` 或 Ctrl+C 退出。无限流会固定旧版本；Close 的截止只限制等待，不会释放仍在运行的业务。

示例使用 grpc-go 自带的生成互操作协议，不需要安装 protoc。实际应用在 Register 回调里注册自己的生成服务；这不是通过反射代理业务接口。

## 验证

```sh
go vet ./...
go test ./... -timeout 60s
python tests/demo_test.py
```

Python 验收复用 Observatory 测试所需 Playwright 1.58.0/Chromium。它启动真实 Go 服务端和 RPC 客户端进程，配合 HTTP 与浏览器完成新旧版本、流保持、排空与关闭验收。截图位于仓库 `.scratch/v0.5/frameworks.png`。
