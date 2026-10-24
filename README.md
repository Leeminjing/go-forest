# go-forest

一个只依赖 Go 标准库的动态组合运行时。Tree 自治，Nest 管归属，Link 管协作，Forest 管版本与生命周期。

核心不包含 Web、Agent、RPC 或数据库模型。普通 Go 包通过自己的接口声明依赖、公开能力，在运行中嵌套、连接、重绑和替换。

本仓库实现单进程内存版 v0.1，Go 1.23+。行为基线见 [Spec](docs/spec.md)，实现协议见 [Design](docs/design.md)。模块路径暂为 `go-forest`；发布到代码托管平台时可改为实际仓库路径。

## 先运行

```sh
go run ./examples/hello-forest
go run ./examples/nesting
go run ./examples/hot-swap
go run ./examples/web-agent
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
go test -coverpkg=go-forest/forest -coverprofile=coverage.out ./tests
```

Windows 的 Go race detector 需要适当的 C 工具链。没有时可在 PowerShell 中通过 Docker 执行 Linux 并发验收：

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26 go test -race ./... -count=3 -timeout 90s
go test '-coverpkg=go-forest/forest' '-coverprofile=coverage.out' ./tests
```

测试覆盖十项验收及资源顺序、事件缺口、取消、类型/语义契约、独占资源拒绝、子树安全保留等约束。CI 在 Windows/Linux 运行测试，在 Linux 运行 race detector。

本次执行结果见 [验证记录](docs/validation.md)，独立审查及修复见 [审查记录](docs/review.md)。

本版不持久化拓扑，不加载进程启动时不存在的机器代码。跨进程和协议转换可以在后续通过适配树扩展。
