# v0.4 发布验收

2026-10-25。用户明确授权完成独立扩展发布，然后进入 gRPC 开发。Core 未修改或重发。

| 模块 | 版本 / 标签 | 发布提交 | GitHub Release |
| --- | --- | --- | --- |
| extensions/observe | extensions/observe/v0.1.0 | e150526 | [Observe v0.1.0](https://github.com/Leeminjing/go-forest/releases/tag/extensions/observe/v0.1.0) |
| adapters/gin | adapters/gin/v0.1.0 | e150526 | [Gin Adapter v0.1.0](https://github.com/Leeminjing/go-forest/releases/tag/adapters/gin/v0.1.0) |
| extensions/observatory | extensions/observatory/v0.1.0 | 2f5073b | [Observatory v0.1.0](https://github.com/Leeminjing/go-forest/releases/tag/extensions/observatory/v0.1.0) |

三个发布模块均不含 replace，依赖已发布 Core v0.3.0；Observatory 使用 Observe/Gin v0.1.0。子目录标签遵循 [Go 模块发布规则](https://go.dev/doc/modules/publishing)，已发布标签不再改动。

## 外部消费者

v0.4 验收时，`python scripts/verify-release.py` 把 tests/release-consumer 复制到系统临时目录（仓库之外），强制 GOWORK=off，然后执行三个固定版本的 go get、go mod tidy、go list -m -json all、vet/test。共享脚本后来随 v0.5 扩展为四模块，并增加真实 RPC 与模块完整性校验，见 [v0.5 发布验收](../v0.5/release.md)；下文保留 v0.4 当时的验收事实。

检查不仅是可编译：

- 三个模块版本都为 v0.1.0，Core 为 v0.3.0；依赖列表没有 Replace，模块路径来自下载缓存而非本地 checkout。
- 实际构建一个 Forest、业务树、Gin 入口和 Observatory 组合，观察四棵树、一条 Link、一条 BindNest。
- 通过真实 TCP 获取 JSON 与 SSE，解析 Observe DTO 和恢复游标。
- 阻塞 v1 HTTP 请求、替换 v2：新请求两次业务读取均返回 v2，旧请求两次仍为 v1；观察到 Draining，释放后无新 Apply 看到 Stopped。
- SSE 保持打开时 Forest.Close 完成。

Windows Go 1.26.5 外部测试 PASS。发布模块已由 Go 工具解析到对应远程标签，记录下载 Sum/GoModSum。另以临时 GOBIN 执行 `go install github.com/Leeminjing/go-forest/extensions/observatory/cmd/demo@v0.1.0` 成功，生成独立 demo.exe，未使用本地 replace。

## CI

发布前移除 replace 后的两个 main 提交均已通过所有模块及浏览器 CI：

- [e150526](https://github.com/Leeminjing/go-forest/actions/runs/40167726735)
- [2f5073b](https://github.com/Leeminjing/go-forest/actions/runs/40167891645)

CI 新增固定版本消费者模块和仓库外复制验证任务，防止今后只靠 checkout 中的包通过验收。标签推送不重复运行整套分支工作流，发布代码仍经 main 分支 CI 验证。

结论：v0.4 源码行为与 R12 发布消费者验收均已完成。gRPC 为下一阶段，不影响本次发布。
