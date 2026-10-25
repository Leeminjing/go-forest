# 生态适配发布验收

2026-10-25。独立扩展发布；Core 保持 v0.3.0，Agent 使用其已发布 v0.2.0。

| 模块 | 最终版本 |
| --- | --- |
| adapters/gorm | v0.1.0 |
| adapters/nats | v0.1.0 |
| extensions/telemetry | v0.1.0 |
| extensions/observatory | v0.2.1 |
| examples/ecosystem | v0.1.1 |

前三个标签指向 8dfdbb7；Observatory v0.2.1 指向 d3d4534；示例 v0.1.1 指向 f9a4193。初始 Observatory v0.2.0 和示例 v0.1.0 标签保留，未修改既有发布标签。v0.2.1 修复运行表格样式；示例 v0.1.1 固定新版本并限制任务 ID，保证 URL、消息头和业务存储使用相同身份。

## 外部依赖验证

`python scripts/test-ecosystem.py --published` PASS：新建隔离真实 PostgreSQL 18 / NATS 2.12.4 服务，先对五个模块执行 vet/test，再运行 `verify-ecosystem-release.py`。

验证器在仓库外创建临时 Go 项目，GOWORK=off，下载上述固定版本，`go mod verify` PASS；遍历 `go list -m -json all` 确认无 Replace，模块来源目录均不在本 checkout。随后 vet/build、安装发布的 `examples/ecosystem/cmd/demo@v0.1.1`，用该二进制执行独立初始化、启动、HTTP 请求、JetStream、数据库、Agent 和实际 Chromium 页面检查。

发布 CLI 完成任务，结果为 `model v1: 5`；相同请求返回已完成结果，运行表格显示已采集调用，桌面/手机无页面横向溢出，断开后标为过期。二进制与截图只留本地忽略区；Windows 避免在退出瞬间删除可能仍被 AV 持有的验证程序。临时模块源仍在仓库外。

主机验证命令（已配置测试服务时也可直接运行）：

```sh
python scripts/verify-ecosystem-release.py
```

完整行为和故障注入映射见 [validation.md](validation.md)。最终任务身份修改后，Linux 的示例模块再执行 vet 与三轮 race PASS；Windows 完整隔离测试及发布二进制浏览器验收 PASS。

## 持续集成与边界

- [固定发布依赖的源码 CI（6061272）](https://github.com/Leeminjing/go-forest/actions/runs/40185515424)：成功。
- [观察表格修正 CI（d3d4534）](https://github.com/Leeminjing/go-forest/actions/runs/40185741962)：成功。
- [最终发布代码 CI（f9a4193）](https://github.com/Leeminjing/go-forest/actions/runs/40186152664)：状态以 GitHub 的该次运行记录为准。

CI 包含 Windows/Linux 逐模块检查、Linux 三轮 race、独立真实服务重启及应用进程 Kill/恢复、Chromium，以及既有发布消费者回归。普通离线测试的跳过不算真实服务验收。

最终依赖图没有开发 replace。Core、根 go.mod、Focus-Go、Python Focus 和 Agent 源码未变。Spec/Design/.scratch 未纳入提交；可达 Git 历史文件名审计未发现 Spec/Design。所有临时验证容器与网络已清理。
