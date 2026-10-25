# gRPC Tree Adapter

普通 Definition 管理原生 grpc-go Server，生成的服务与消息保持原有用法。独立模块最低 Go 1.25，当前锁定 grpc-go v1.84.0，仅依赖已发布 Core v0.3.0 与 gRPC，不引入 Gin 或观察产品。

```go
definition, err := grpcforest.Definition(f, grpcforest.Options{
    TypeID: "application.grpc", Version: "1", Address: "127.0.0.1:8090",
    Ports: []forest.PortDescriptor{forest.Port[Worker]("worker", forest.Required())},
    ServerOptions: []grpc.ServerOption{
        grpc.ChainUnaryInterceptor(authenticate),
        grpc.ChainStreamInterceptor(authenticateStream),
        grpc.MaxRecvMsgSize(1 << 20),
    },
    Register: func(ctx context.Context, env *forest.BuildEnv, registrar grpc.ServiceRegistrar) error {
        ref, err := forest.Reference[Worker](env, "worker")
        if err != nil { return err }
        api.RegisterWorkerServer(registrar, &adapter{worker: ref})
        return nil
    },
})
```

应用按普通流程 Register/Apply，并 Connect 到业务树。业务包自己提供接口实现，不导入 gRPC。示例中的 api/adapter/Worker 是应用自己的生成服务、协议适配和消费方接口，完整可运行用法见同仓库 examples/frameworks。

## 调用约定

- Unary 每个 RPC 一个 Invoke；应用链式拦截器的前后处理和业务方法均在其中。
- 服务端流、客户端流、双向流都以整条 RPC 为一个 Invoke。包装后的 stream.Context() 携带 Scope，处理器继续把这个 Context 传给 Ref.Use。
- 业务热替换后新 RPC/新流使用新实现，旧流的后续消息仍使用旧实现。长流长期保留旧实例是明确语义。
- Adapter 保留原生 UnaryInterceptor/StreamInterceptor 的单一槽作为 Scope 外层。用户使用原生 ChainUnaryInterceptor/ChainStreamInterceptor；重复占用单一槽会导致准备失败并回滚，不会静默绕过 Scope。
- 认证、TLS、状态码、限流、消息上限、业务错误与 panic 恢复策略由应用通过原生 gRPC 配置拥有。入口 Invoke 拒绝映射为 Unavailable，业务 handler 的原生错误保持原样。

## 生命周期与关闭

Register 只在准备阶段执行，可以创建 Ref 并注册生成服务。不可保留 BuildEnv/准备 Context，不可在 Serve 后增加服务或把 registrar 当成运行时管理入口。注册错误或 panic 会把已分配 Server 交回 Forest 回滚。重复服务名与实现类型不匹配会在准备阶段被拦截并返回失败，避免原生注册 API 的进程退出行为；回调结束后 registrar 封闭。它也提供原生 GetServiceInfo，可供反射服务等注册扩展读取信息。

Start 同步 Listen，再后台 Serve。Definition 提供 Public `rpc` 能力：

```go
type Endpoint interface {
    Target() string
    ServeError() error
}
```

在能力回调内读取 Target 或本地诊断，不保留裸 Endpoint。监听树不声明 HotSwappable；本阶段只热换已注册服务使用的业务 Provider。

Stop 先 GracefulStop，截止时请求原生 Stop 取消传输，但仍等待实际 handler 结束再确认成功。Forest 会将超时过程显示为 Draining；业务最后完成后可进入 Stopped。断开连接不等于业务已经释放资源。

Forest.Close 先拒绝新 Scope，再等已有 Scope。请先结束/取消客户端长流，再 Close。CancelGeneration 可请求业务 Context 协作取消，但不能保证打断只阻塞在原生 RecvMsg 或第三方 I/O、且不查看 Context 的代码。客户端取消会取消相应传输；忽略取消的业务仍保留引用，Close 截止返回不会提前 Stop 它。

## 版本与验证

独立模块版本为 v0.1.0，go.mod 使用已发布 Core v0.3.0，无本地 replace。最低 Go 1.25：

```sh
go get github.com/Leeminjing/go-forest/adapters/grpc@v0.1.0
```

项目 v0.5 是交付里程碑，不重发 Core。v0.4 的三个已发布 v0.1.0 标签保持不变。

```sh
go vet ./...
go test ./... -timeout 60s
go test -race ./... -count=3 -timeout 120s
```

真实 TCP 验证包括生成服务四种 RPC、版本固定、metadata/拦截器、业务状态码、消息限制、TLS、取消、截止、忽略取消、注册失败/panic、监听回滚和 Stop 超时后的最终资源释放。
