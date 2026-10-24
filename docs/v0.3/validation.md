# v0.3 验收记录

日期：2026-10-24。实施前规格提交为 `d8bd6f1`，对应 [Spec](spec.md) 和 [Design](design.md)。

## 稳定基线

已将 `v0.2.0` 标签发布到 `1161b62`。独立 Go 模块实际执行 `go get github.com/Leeminjing/go-forest@v0.2.0`，导入 forest 和 compose，注册普通 Definition、实例化 Blueprint 并 Apply，得到 `generation=1 trees=1`。该验证使用独立 go.mod，未使用本地 replace。

## 按 Spec 验收

| 验收 | 对应测试及结果 |
| --- | --- |
| R01，两个进程通过普通树调用 | `TestTwoProcessesHotReplacementCancellationCrashAndRecovery` 启动两个独立 OS 进程，检查 PID 各异；Formatter → 本地 Proxy → HTTP/TCP → Host → 本地 Clock 完成调用。stdio 只传管理命令。Client 示例的依赖不包含具体 localtime 提供者包。 |
| R02，协议、契约与准备失败 | `TestExplicitContractsAuthorizationAndConfigurationSnapshots`、`TestContractIsRecheckedAfterRemoteRestart`、`TestNoAutomaticRetryOrRedirect` 检查协议、契约 ID/版本及每次调用的校验；`TestRemoteStartupRejectionDoesNotPublishProxyForest` 用真实进程验证错误凭据及不兼容契约使 Apply 失败，A 仍为版本 0、树集合为空。 |
| R03，超时与协作取消 | `TestDeadlinesAndCancellationReachRemoteContext` 覆盖调用者取消、Client 截止、Host 上限、Client Stop；同时检查本地可识别错误及远端 Context 信号。双进程测试再次验证超时和主动取消，调用次数只增加一次。 |
| R04，断连与不重试 | 双进程测试在远端活动调用阻塞时真正 Kill/Wait B；A 明确返回 unavailable，其他本地能力仍返回 local-alive。`TestNoAutomaticRetryOrRedirect` 用真实套接字在收到业务请求后断开连接或返回重定向，确认业务请求只发送一次。 |
| R05，远端热替换 | 双进程测试阻塞 v1 调用、替换 v2，确认新调用返回 v2、v1 尚未 Stop；释放后旧调用仍返回 v1，Wait 后旧实例才停止。 |
| R06，同地址重启恢复 | 双进程测试在原监听地址启动全新 B 进程，原 A 的后续调用恢复成功，A Generation 不变，未重新创建 A 森林。 |
| R07，访问控制与消息限制 | 授权测试覆盖错误 token、被拒绝路由、未开放路由及配置副本；`TestMalformedAndOversizedMessagesDoNotInvokeProvider` 覆盖无效 JSON、未知字段、多余值、无效预算及请求上限；`TestBoundedResponsesAndSanitizedBusinessFailures` 覆盖响应上限、内部错误和 panic 脱敏。 |
| R07，TLS 安全边界 | `TestTLSVerificationAndInsecureConfiguration` 使用真实 TLS 连接：不受信证书失败，显式信任 CA 后成功；默认拒绝非回环明文、URL 凭据/查询和未经显式许可的跳过证书验证。TLS 配置回调无法降低 TLS 1.2 或改为 HTTP/2；修改构造时传入的证书链和信任池不改变已构造实例。 |
| R08，进程间版本边界 | 双进程测试保持同一个 A Scope，期间替换 B Provider，两次请求返回不同的 B Generation 和实现结果；A 版本保持不变。 |
| R09，生命周期与隔离 | Host/Client 实现原 Lifecycle；准备失败由 Forest 回收，Client Stop 取消请求并拒绝新调用。双进程替换测试证明活动 Scope 内的 Provider 未被提前释放。原有 Core、compose 测试继续通过。 |

所有网络验收使用真实套接字。关键集成验收运行两个带各自 Forest 的真实 Go 进程，未使用模拟 Transport 代替。子进程复用测试二进制，因此 race 验证也覆盖其执行。

## 执行

Windows 本地：

```text
go vet ./...                                      PASS
go test ./... -timeout 90s                        PASS
go test ./bridge/httpjson -cover                  PASS，86.3%
```

Linux Docker（Go 1.26，race）：

```sh
docker run --rm --mount type=bind,source=D:\go-forest,target=/src -w /src golang:1.26 go test -race ./... -count=3 -timeout 120s
```

修复审查问题后的最终结果 PASS：桥接包三轮合计 14.591 秒，compose 1.150 秒，原核心测试 1.266 秒；没有 race 报告。故意终止远端子进程时也检查其 stderr，避免忽略终止前已有的 race 报告。

另外分别编译并运行 examples/remote-clock/host 和 client 两个独立命令，使用临时回环端口完成实际时间格式化调用，输出 `2026-10-24T10:15:05Z`，PASS。

CI 使用 Go 1.23，Windows/Linux 都运行 vet 和完整测试，Linux 另运行三轮 race。独立审查结果见 [review.md](review.md)。

## 范围检查

`git diff v0.2.0 -- forest` 为空：未修改 Core、transaction、Scope、生命周期或 compose。go.mod 仍无第三方依赖。新增实现位于 bridge/httpjson，业务示例位于 examples。未加入服务发现、分布式调度、全局事务、机器代码加载或 Agent Runtime。

通信失败仍可能对应已经执行的副作用；取消仍要求业务协作。Host/Proxy 的初次检查不保证远端永远在线，B 每个请求独立固定自己的版本。本次验收没有将测试覆盖率解释为对所有网络及业务行为的形式化证明。
