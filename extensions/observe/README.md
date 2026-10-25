# Observe

将 Forest 公开诊断转换成独立、可序列化的数据。此模块只依赖 Core 和 Go 标准库，最低 Go 1.23。

`Definition(f)` 返回普通 Inspector Definition，提供 `read` 能力，默认可见性为 Ancestors。所有者用必需 Runtime 端口及 BindNest 装配，在短 `Invoke` 内通过自己的 `Ref` 调用：

```go
type Reader interface {
    Read(context.Context, *observe.Cursor) (observe.Frame, error)
}
```

nil cursor 读取初始化快照及保留的近期事件；后续使用 `ParseCursor(frame.Cursor)`。Read 不阻塞、不订阅、不调用业务能力。能力对象不能离开 Ref 回调；返回的 Frame 是独立数据，可以在回调外编码和传输。

每个 Inspector 实例有独立 boot_id。过期游标产生 `history_gap`，其他观察会话的游标产生 `runtime_changed`，二者均返回 reset 原因、当前快照与保留窗口。同一会话内超前或格式错误的游标返回 `ErrInvalidCursor`。

事件先取副本，再读取 Snapshot；Frame.Cursor 只推进到已交付事件批次的位置。Snapshot.ObservedEventSequence 不是交付游标。拓扑、事件与实例诊断不是同一个原子快照。定时再次 Read 才能看见没有新 Apply 的生命周期变化；短暂状态可能被采样跳过。

JSON 中 uint64 使用十进制字符串，类型使用文本描述，error 仅转换成分类码及结构化上下文。所有已停止实例仍来自 Core 保留的诊断，因此读取成本随实例历史增加。

此扩展尚未发布独立版本，本地 go.mod 使用 replace 指向仓库根。验证：

```sh
go vet ./...
go test ./... -timeout 60s
```
