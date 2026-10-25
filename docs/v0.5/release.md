# v0.5 gRPC 发布验收

日期：2026-10-25。本轮按用户指示完成 P0；Focus-Go 的 P1 沿用原聊天，未修改该仓库正在实施的文件。

## 发布版本

- 模块：github.com/Leeminjing/go-forest/adapters/grpc，v0.1.0。
- 标签：adapters/grpc/v0.1.0，发布提交 04bc232dea4484a7055ab2709f5739aac68c777f。
- [GitHub Release](https://github.com/Leeminjing/go-forest/releases/tag/adapters/grpc/v0.1.0)。
- 已审查的实现基线为 220fadb；发布准备只更新模块 README，没有修改 gRPC 实现或 Core。
- Go 工具成功从远程标签下载，Sum 为 `h1:NAcB+NpeKhie95PWVpFI0fY22e5J0S3xOQF61jjwfPs=`，GoModSum 为 `h1:DmQfEYSMfZgrLWBhaKSAQI6X8nOw87QXmkOYsifiFCk=`。

安装要求 Go 1.25+：

```sh
go get github.com/Leeminjing/go-forest/adapters/grpc@v0.1.0
```

Core 继续使用已发布 v0.3.0，grpc-go 固定 v1.84.0；已有 Gin、Observe、Observatory v0.1.0 标签不变。examples/frameworks 已删除本地 replace，使用正式 gRPC 版本。

## 仓库外消费者

`python scripts/verify-release.py` 将固定版本消费者复制到系统临时目录（仓库之外），强制 GOWORK=off，对 Observe/Gin/gRPC/Observatory 四个 v0.1.0 执行 go get、tidy、模块校验及 vet/test。go list -m -json all 断言全部依赖没有 Replace，四个模块从下载缓存读取，Core 精确为 v0.3.0。

真实消费者行为已通过 Windows Go 1.26.5 验证：

1. 注册 Gin 与 gRPC 两个独立框架树，共用同一业务 Provider，另有 Observatory 根和 Inspector，总计五树、两 Link、一 BindNest。
2. 通过实际 HTTP、Unary RPC、双向流和 SSE 通信。观察 JSON 使用正式 Observe DTO，游标可解析。
3. 保持 v1 HTTP 请求和 v1 RPC 流，替换业务为 v2；新 HTTP 和 Unary 两次读取均为 v2，旧流第二条消息两次读取仍为 v1/generation=1。
4. 释放 HTTP 请求后旧实例仍 Draining；RPC 流结束后才 Stopped，保持 SSE 时 Forest.Close 完成。

消费者只依赖发布模块，没有使用 checkout 的内部包、本地 replace 或 workspace。新 RPC 通信不是模拟 Transport。

## 组合与回归

删除 examples/frameworks 的 replace 后，其 vet/test 和真实服务端/RPC 客户端进程加 Chromium 验收通过。浏览器看到五树、两 Link、Generation 切换、Draining 与最终 Stopped，说明正式 Gin/gRPC/Observatory 可共同运行。

Linux race 和独立审查的最终结果随本轮收尾记录。CI 的 published-consumer 任务已使用扩展后的四模块仓库外验证；现有多模块及组合浏览器验收继续执行。

v0.5 发布验收完成不意味着新增 Core 能力；本轮没有重构 Forest Core，也没有修改 go-forest-agent 或 Focus-Go。
