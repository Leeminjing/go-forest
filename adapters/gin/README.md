# Gin Tree Adapter

以普通 Definition 管理 Gin Engine 和 http.Server。最低 Go 1.25，当前依赖 Gin v1.12.0。Core 的 Go 1.23 和标准库依赖范围保持不变。

`ginforest.Definition(f, Options)` 要求 TypeID、Version 和 BuildRouter；Address 默认 `127.0.0.1:0`。Ports 是应用自己声明的消费方接口。每次 BuildRouter 都应创建新 Engine、通过 BuildEnv 创建业务 Ref，并在返回前配置全部路由与中间件。不可保留准备 Context 或 BuildEnv，也不可运行中修改 Engine。

```go
router := gin.New()
business := router.Group("/", ginforest.Scope(f))
business.GET("/work", func(c *gin.Context) {
    err := ref.Use(c.Request.Context(), func(service Worker) error {
        value, err := service.Work(c.Request.Context())
        if err != nil { return err }
        c.String(http.StatusOK, value)
        return nil
    })
    if err != nil && !c.Writer.Written() { c.Status(http.StatusServiceUnavailable) }
})
```

Scope 在整个后续路由链中固定一个 Generation。把需要业务能力的中间件放在它之后，业务继续传递 `c.Request.Context()`。Scope 不自动安装到所有路由：SSE 等无限观察连接须每次短读取单独 Invoke，不能持续占用完整森林的版本。

Definition 固定提供 Public `http` 能力：

```go
type Endpoint interface {
    URL() string
    ServeError() error
}
```

应用在普通能力回调中读取 URL 或 ServeError，不保留原始 Endpoint。Start 同步绑定端口，失败可被 Apply 回滚；Stop 先取消 Server 自有请求 Context，再 Shutdown。运行中 Serve 异常不会自动改变 Core 生命周期状态，可通过 Endpoint 本地诊断记录。

监听树不声明 HotSwappable。首期只热换业务 Provider；不热换路由表、共享同一 Engine、或支持 Hijack/WebSocket 生命周期。Adapter 本身不添加认证、TLS 或业务错误模型；Observatory 的回环限制与凭据策略由该扩展实现。

此扩展尚未发布独立版本，本地 go.mod 使用 replace。完整用法见同仓库 `extensions/observatory/internal/demo`。验证：

```sh
go vet ./...
go test ./... -timeout 60s
```
