# go-forest

一个只依赖 Go 标准库的动态组合运行时。Tree 自治，Nest 管归属，Link 管协作，Forest 管版本与生命周期。

核心不包含 Web、Agent、RPC 或数据库模型。普通 Go 包通过自己的接口声明依赖、公开能力，在运行中嵌套、连接、重绑和替换。

本仓库实现单进程内存森林，Go 1.23+。v0.1 提供动态组合运行时；v0.2 在独立 `forest/compose` 包中增加可复用、可递归组合的模板。v0.3 在可选扩展 `bridge/httpjson` 中增加跨进程能力调用，各个 Forest 仍独立管理自己的拓扑与版本。正式模块路径为 `github.com/Leeminjing/go-forest`。

```sh
go get github.com/Leeminjing/go-forest@v0.3.0
```

应用导入 `github.com/Leeminjing/go-forest/forest`。发布版本遵循 Go 模块的语义版本标签约定。

v0.3.0 包含核心、组合层与可选桥接扩展。桥接应用导入 `github.com/Leeminjing/go-forest/bridge/httpjson`；单进程应用可以只导入 forest 或 forest/compose。

## 多框架生态适配

独立 [GORM](adapters/gorm/README.md)、[NATS JetStream](adapters/nats/README.md) 与可选 [运行观测](extensions/telemetry/README.md) 组合现有框架。[多框架森林示例](examples/ecosystem/README.md) 使用真实数据库、消息、Agent、HTTP/RPC 和 Observatory 验证事务、重复消息、热替换与恢复。接入约定见 [框架适配指南](docs/adapter-guide.md)；Core 依赖保持不变。

GORM、NATS、Telemetry 各自发布 v0.1.0，Observatory 运行观察为 v0.2.0；示例固定依赖这些版本。运行示例需配置独立测试数据库/消息服务，详见示例 README。

## 先运行

```sh
go run ./examples/hello-forest
go run ./examples/nesting
go run ./examples/hot-swap
go run ./examples/web-agent
go run ./examples/composite
```

第一个例子动态种下 Clock、Formatter、Printer 三棵独立树并连接：

```text
forest: 2026-10-24T08:00:00Z
generation=1 trees=3 links=2
```

热替换例子保持一个旧调用未结束，然后替换 Clock：

```text
new invocation: 2
old invocation: 1
```

三棵树各自位于 `examples/trees/` 的独立包。Formatter 自己定义 Clock 接口；Clock 提供者不导入 Formatter。Printer 同样只依赖自己声明的 Formatter 接口。

`web-agent` 在同一个 Forest 内运行 HTTP、Agent 和确定性 Model 三种普通扩展树，通过本机临时端口完成一次请求后自动退出，无需 API Key，输出 `agent: model: hello`。它们全部在 examples 中，核心没有领域分支。

## v0.2：可复用组合树

模板使用局部名称，展开成普通 ChangePlan，沿用已有原子提交、Scope 和资源回收机制。

```go
blueprint, err := compose.New(compose.Spec{
    Root: rootDefinition.Ref(),
    Trees: []compose.Tree{
        {ID: "provider", Definition: providerDefinition.Ref()},
    },
    Bindings: []forest.Link{
        {ID: "source", Port: "source", Provider: "provider", Capability: "read"},
    },
})
if err != nil {
    return err
}
left, err := blueprint.Instantiate("left", "")
if err != nil {
    return err
}
right, err := blueprint.Instantiate("right", "")
if err != nil {
    return err
}
plan := left.Plan(f.Snapshot().Generation)
plan.Operations = append(plan.Operations, right.Plan(plan.BaseGeneration).Operations...)
_, err = f.Apply(ctx, plan)
```

Definition 通过原 Registry 在每个 Forest 注册一次，再重复实例化。例中得到 `left/provider`、`right/provider` 两个独立实例；内部绑定身份分别为 `left/@source`、`right/@source`，根到后代的关系自动展开为 BindNest。

`compose.Tree{ID: "engine", Composite: innerBlueprint}` 可以嵌入已有模板。外层模板只接线到 `engine` 根，不穿透 `engine/internal`。封闭根保持 `ExportChildren: false`，通过自己的 Ref 和公开接口使用内部树；没有反射代理或核心专用分支。

内部管理可用 `instance.Tree("engine/provider")` 查询真实 TreeID，再用原有 Replace；整体删除用 `RemoveSubtree(instance.Root())`。这些路径按模板命名空间组织，普通节点的 Parent 决定拓扑归属，不改变局部名称。

`examples/composite` 使用独立 Clockwork 组合库，复用已有 Clock/Formatter 和外部 Printer；一次提交两套组合和外部连接，输出七棵树、版本 1。逐项验收见 [v0.2 验收记录](docs/v0.2/validation.md)。

## v0.3：跨进程能力桥接

桥接作为普通树组合，未改动 Core 或 compose：

```text
Forest A: Formatter --Link--> Clock Proxy
                                  |
                              HTTP/JSON
                                  |
Forest B: Clock     <--Link-- Clock Host
```

两个终端使用同一份随机凭据。在仓库根目录分别运行以下 PowerShell 命令（将示例值换成自己的凭据）：

```powershell
# 终端 B：启动提供者及 Host，Ctrl+C 退出。
$env:FOREST_BRIDGE_TOKEN = 'replace-with-your-random-token'
go run ./examples/remote-clock/host -address 127.0.0.1:8088
```

```powershell
# 终端 A：启动 Proxy 及 Formatter，完成一次真实远程调用后退出。
$env:FOREST_BRIDGE_TOKEN = 'replace-with-your-random-token'
go run ./examples/remote-clock/client -url http://127.0.0.1:8088
```

Formatter 定义自己的 Clock 接口；Proxy 显式实现它。Client 命令不导入具体 `localtime` 提供者包。Host 的类型化 Route 通过自己的 Ref 调用 Clock，远端请求不能指定任意 TreeID 或方法名。新领域通过显式适配器使用 `httpjson.Handle[Input, Output]` 和 `Client.Call`，无需生成接口代理。

Proxy 启动时检查协议及每条路由的精确契约，每次调用也检查契约；Host 使用凭据对应的路由白名单授权。默认请求及响应上限为 1 MiB，Client 超时 5 秒，Host 最大执行时间 30 秒。取消和截止传播到远端 Context，业务需要协作处理。错误支持 `errors.Is`，例如 `ErrUnauthorized`、`ErrContractMismatch`、`ErrTimeout` 和 `ErrUnavailable`；内部业务错误及 panic 内容不会发送给客户端。

默认明文只用于回环地址；跨机器需配置 TLS 证书及信任根。`AllowInsecure` 是应用的显式不安全选择。桥接不自动重试或跟随重定向；通信失败不能证明业务没有执行。凭据通过请求头传递，示例不将其打印或放入 URL。

B 的每个请求单独进入本地 Scope，远端替换仍保护旧调用及其资源；后续请求使用新实例。B 在同一地址重启后，A 的原代理可再次调用。A/B 的 Generation 相互独立，同一个 A Scope 的两次远程调用可以看到不同 B 版本；本版没有全局快照或跨进程事务。

真实双进程测试覆盖启动失败回滚、热替换、取消、活动调用期间杀死对端及恢复，并验证 A 的其他本地树继续工作。逐项结果见 [v0.3 验收记录](docs/v0.3/validation.md)。

## v0.4：只读观察网关与 Gin 扩展

本地实现新增三个独立 Go Module，Core、compose 与 bridge 的实现不变：

| 模块 | 最低 Go 版本 | 职责 |
| --- | --- | --- |
| [extensions/observe](extensions/observe/README.md) | 1.23 | Inspector Tree、脱敏 DTO、事件游标与状态采样 |
| [adapters/gin](adapters/gin/README.md) | 1.25 | 原生 Gin 路由与中间件、HTTP 生命周期、有限请求 Scope |
| [extensions/observatory](extensions/observatory/README.md) | 1.25 | 自观察的组合树、JSON/SSE、拓扑和实例诊断 UI、热替换演示 |

```powershell
$env:FOREST_OBSERVATORY_TOKEN = 'replace-with-your-random-token'
cd extensions/observatory
go run ./cmd/demo
```

打开 `http://127.0.0.1:8089` 并输入相同凭据。应用请求使用 `http://127.0.0.1:8080/work`；另开终端请求 `/work?hold=1`，在演示终端输入 `wait`、`replace`，即可查看旧实例 Draining 与新请求返回 v2。输入 `release` 后旧请求返回 v1，页面继续采样到 Stopped；`quit` 退出。

Observatory 只读且仅监听回环地址，分别显示 Nest、Link、BindNest。SSE 采用短读取，等待和网络写入不固定旧 Generation。状态来自采样，Active 不代表业务健康；近期事件不支持历史图回放。gRPC 和性能指标留到后续版本。

根模块仍只依赖标准库。三个扩展均已发布独立 v0.1.0，使用 Core v0.3.0 和正式版本依赖，不含本地 replace；根目录 `go test ./...` 不涵盖嵌套模块，需逐模块验证。仓库外消费者已验证真实 HTTP/SSE 与热替换，发布版本及复现方式见 [v0.4 发布验收](docs/v0.4/release.md)。

## v0.5：gRPC Tree Adapter

[adapters/grpc](adapters/grpc/README.md) 用普通 Definition 承载原生生成服务、Unary 和三种 Streaming RPC。应用链式拦截器与业务处理位于同一 Forest Scope，已注册服务的业务实现可以热替换。流内各条消息固定在同一 Generation，流结束后才释放旧实例。

[examples/frameworks](examples/frameworks/README.md) 在同一 Forest 中种下 Gin、gRPC、Business 与 Observatory，支持真实 HTTP/RPC 调用、长流和旧请求保持、替换及排空观察：

```powershell
$env:FOREST_OBSERVATORY_TOKEN = 'replace-with-your-random-token'
cd examples/frameworks
go run ./cmd/demo
```

gRPC Adapter 已正式发布独立 v0.1.0，最低 Go 1.25，锁定 grpc-go v1.84.0；Core、Gin 和已发布 v0.4 模块没有改动。组合示例使用正式版本，无本地 replace。RPC 状态码、TLS、认证及恢复策略继续使用原生 gRPC 配置，不加入 Core。发布与仓库外消费者结果见 [v0.5 发布验收](docs/v0.5/release.md)。

## 声明与组合

消费方定义接口和端口，在 Factory 中创建动态引用：

```go
type Clock interface {
    Now(context.Context) time.Time
}

// Definition.Ports:
forest.Port[Clock]("clock", forest.Required())

// Factory:
clock, err := forest.Reference[Clock](env, "clock")
```

提供者声明自己的公开承诺，并登记实现：

```go
// Definition.Capabilities:
forest.Capability[TimeSource]("time", forest.Public)

// Factory:
err := forest.Provide(env, "time", myClock)
```

TimeSource 可以是提供方自己定义的接口。Forest 检查 myClock 的实际 Go 类型是否满足消费方的 Clock 接口。

注册 Definition 后，以一个计划发布实例与关系：

```go
_, err := f.Apply(ctx, forest.ChangePlan{
    BaseGeneration: f.Snapshot().Generation,
    Operations: []forest.Operation{
        forest.AddRoot("clock", clockDefinition.Ref()),
        forest.AddRoot("formatter", formatterDefinition.Ref()),
        forest.Connect(forest.Link{
            ID: "time",
            Consumer: "formatter", Port: "clock",
            Provider: "clock", Capability: "time",
        }),
    },
})
```

必需依赖必须在最终候选图中满足。没有隐式按类型选提供者。多值端口用 `Many()` 声明，通过 `UseAll` 按绑定插入顺序访问；重绑保持原顺序。

## 调用必须进入 Scope

应用用 Invoke 建立版本边界，树方法继续传递得到的 callCtx：

```go
err := f.Invoke(ctx, func(callCtx context.Context) error {
    return forest.Use[printer.Output](callCtx, f, "printer", "print",
        func(p printer.Output) error {
            line, err := p.Print(callCtx)
            if err != nil {
                return err
            }
            fmt.Println(line)
            return nil
        })
})
```

树内部使用已声明的 Ref：

```go
err := clock.Use(ctx, func(c Clock) error {
    now := c.Now(ctx)
    // 使用 now。
    return nil
})
```

Scope 固定完整拓扑及依赖版本。跨多条 Link 始终使用同一版本；新独立调用进入新版本。没有开放 Scope 时，Use/Ref.Use 返回 ErrScopeClosed。不要把能力对象带出回调，不要用 Background 替换调用 Context。

## 动态变更

操作包括 AddRoot、Nest、Move、Unnest、Connect、BindNest、Unlink、Rebind、Replace、Remove、RemoveSubtree，均是传给 Apply 的计划值。

- Nest 确定归属；祖先关系中的协作通过 BindNest 显式装配，使用 Descendants/Ancestors 可见性。
- Link 连接没有祖先关系的两树，包括兄弟和不同分支。子树向外提供 Public 能力需各层所有者声明 ExportChildren。
- 必需依赖可在同一计划重绑并移除旧提供者；单独断开会被拒绝。
- 热替换需新旧定义都声明 HotSwappable。该声明意味着资源允许新旧实例同时预热。
- 保留子树需显式 `Replace(id, next, true)` 和新定义 PreserveChildren 承诺。
- 保留实例移动需被移动子树声明 Reparentable，并重新验证所有绑定。
- 默认端口为 Runtime；构造和就绪依赖分别用 During(Construction)/During(Startup) 声明。改变这类资源依赖时需一起替换消费者。

版本冲突可用 `errors.Is(err, forest.ErrGenerationConflict)` 检查，`errors.As` 提取 GenerationConflict 的 Expected/Actual。失败不改活动拓扑；失败资源异步清理，可用 `Wait(ctx)` 等待。

## 观察与退出

```go
snapshot := f.Snapshot()
watcher := f.Watch(snapshot.EventSequence)
event, err := watcher.Next(ctx)
```

Snapshot 返回独立的结构副本和实例诊断。事件涵盖成功、失败、冲突，Sequence 独立于 Generation；事件历史有容量限制。ErrEventGap 时重新读取 Snapshot 并从其 EventSequence 继续。

移除和替换不提前关闭旧调用使用的实例。旧实例显示 Draining，Scope 及资源依赖释放后才 Stop。Stop 超时或失败保持可观察，不强制释放依赖资源。`CancelGeneration` 请求协作取消旧调用。

`Close(ctx)` 停止新调用并等待退出。Context 截止只限制等待，不会释放仍在使用的资源；之后可重试。生命周期回调不得调用会等待自己的 Apply/Wait/Close。树需要为自己的资源协议和行为契约编写测试。

## 验证

```sh
go vet ./...
go test ./... -timeout 60s
go test -race ./... -count=3 -timeout 90s
go test -coverpkg=github.com/Leeminjing/go-forest/forest -coverprofile=coverage.out ./tests
```

Windows 的 Go race detector 需要适当的 C 工具链。没有时可在 PowerShell 中通过 Docker 执行 Linux 并发验收：

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26 go test -race ./... -count=3 -timeout 90s
go test '-coverpkg=github.com/Leeminjing/go-forest/forest' '-coverprofile=coverage.out' ./tests
```

测试覆盖十项验收及资源顺序、事件缺口、取消、类型/语义契约、独占资源拒绝、子树安全保留等约束。CI 在 Windows/Linux 运行测试，在 Linux 运行 race detector。

本次执行结果见 [验证记录](docs/validation.md)，独立审查及修复见 [审查记录](docs/review.md)。

本版不持久化拓扑，不加载进程启动时不存在的机器代码。桥接不提供服务发现、分布式调度或远程拓扑管理。
