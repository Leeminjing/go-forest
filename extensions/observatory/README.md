# Forest Observatory

同一 Forest 中的只读观察网关。Gin 组合根包含 Inspector 子树，通过 BindNest 使用 Reader；应用 Gin 树与它共用同一个 Adapter。最低 Go 1.25。

## 运行演示

从仓库根目录，在 PowerShell 中配置自己的观察凭据（16–256 字节，不含空白），然后启动：

```powershell
$env:FOREST_OBSERVATORY_TOKEN = 'replace-with-your-random-token'
cd extensions/observatory
go run ./cmd/demo
```

默认应用地址为 `http://127.0.0.1:8080`，观察页面为 `http://127.0.0.1:8089`。在页面中输入同一 token；启动输出不含 token。也可传 `-address 127.0.0.1:0 -app-address 127.0.0.1:0` 使用临时端口，实际地址在 ready JSON 中。

另一个终端保持旧请求：

```powershell
curl.exe -N 'http://127.0.0.1:8080/work?hold=1'
```

在演示进程终端输入 `wait`，收到 blocked 后输入 `replace`。新请求 `GET /work` 返回 v2，页面显示新 Generation 和旧业务实例 Draining。输入 `release` 后旧请求仍返回 v1，页面在没有额外 Apply 的情况下采样到 Stopped。输入 `quit` 或 Ctrl+C 退出；SSE 页面保持打开也能完成退出。

管理命令只经本地 stdin 进入演示进程，不属于观察 HTTP 接口。业务代码位于 `internal/demo/business`，其依赖图不含 Gin 或 Observe。

## 接入已有 Forest

```go
bundle, err := observatory.New(f, observatory.Options{Token: token})
if err != nil { return err }
for _, definition := range bundle.Definitions {
    if err := f.Register(definition); err != nil { return err }
}
placement, err := bundle.Blueprint.Instantiate("observatory", "")
if err != nil { return err }
_, err = f.Apply(ctx, placement.Plan(f.Snapshot().Generation))
```

通过组合根的 `http` Endpoint 能力读取地址。Definitions 注册一次，同一 Blueprint 可在不同根重复实例化，默认各自持有临时端口与独立观察会话。如果需要不同 token 或地址，用不同 Options.TypeID 创建其他 Bundle。

## 读取与恢复

- `GET /api/v1/state?cursor=<boot_id>:<sequence>` 返回观察 Frame。
- `GET /api/v1/events` 使用 SSE，Last-Event-ID 为同样的游标格式。
- 数据接口要求 `Authorization: Bearer <token>`；缺失/错误为 401，无效/超前游标为 400，依赖不可用为 503。
- 初始 sync 帧、缺口/重启 reset 帧包含 Frame；change 帧包含单个事件；snapshot 帧刷新状态但不推进事件 id。
- 默认每秒采样。事件可能晚于对应拓扑出现；实例状态是诊断采样。线条没有流量含义，Active 不是业务健康检查。

观察只监听数值回环地址，页面凭据只在内存中。接口不暴露拓扑写入或任意能力调用，不提供跨机器部署。断线保留最后快照并标为过期；历史缺口明确提示，不伪造完整时间线。

SSE 在等待和网络写入期间不持有 Scope。写入默认 5 秒截止，服务停止时立即取消阻塞写。已停止实例由 Core 保留，UI 默认折叠历史；观察读取成本随历史增加。独立实例表可查看已经离开当前拓扑的实例，也可展开已停止的候选/历史记录。

## 验证与发布

```sh
go vet ./...
go test ./... -timeout 60s
pip install -r tests/requirements.txt
python -m playwright install chromium
python tests/browser_test.py
```

浏览器验收启动真实 demo 进程，通过 stdin 触发变更，经真实 HTTP/SSE 和可断开的 TCP 转发验证恢复。空数据、恶意标识使用明确的 UI 测试输入。截图保存在仓库 `.scratch/v0.4`。

三个扩展都是嵌套 Go Module，根目录 `go test ./...` 不会测试它们。CI 逐模块测试，Linux 增加 race 和独立浏览器任务。

模块固定依赖已发布的 Core v0.3.0、Observe v0.1.0 与 Gin Adapter v0.1.0，不需要本地 replace。

```sh
go get github.com/Leeminjing/go-forest/extensions/observatory@v0.1.0
go install github.com/Leeminjing/go-forest/extensions/observatory/cmd/demo@v0.1.0
```

发布顺序为 Observe、Gin Adapter、Observatory，标签分别使用 `extensions/observe/v0.1.0`、`adapters/gin/v0.1.0`、`extensions/observatory/v0.1.0`。仓库的 `scripts/verify-release.py` 将固定版本消费者复制到仓库外，禁用 workspace 并确认不存在 replace，再运行真实 HTTP/SSE 与热替换验证；本地源码测试不替代这项发布验收。
