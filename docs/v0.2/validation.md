# go-forest v0.2 验收记录

2026-10-24。按实施前建立的 [Spec](spec.md) 和 [Design](design.md) 逐项验收。

## 发布与模块边界

- 正式路径：`github.com/Leeminjing/go-forest`，Go 1.23+，仅标准库依赖。
- `v0.1.0` 标签指向 `f47d5ed`，包含正式模块路径和 v0.1 核心，不包含 v0.2 组合层。
- 独立外部模块 `example.com/go-forest-consumer` 执行 `go get github.com/Leeminjing/go-forest@v0.1.0` 并运行，输出 `external module: generation=0`。未使用 replace 或工作区源码。
- v0.2 逻辑仅在 `forest/compose`，相对 v0.1.0 未修改 Core 的 Apply、Scope、Lifecycle 或其他运行时代码。

## Spec 逐项结果

| 验收项 | 自动化证据 | 结果 |
| --- | --- | --- |
| 同模板同 Forest 两次、不同 Forest 再次种植；物理实例和状态独立 | `TestRepeatedCompositeInstantiationIsRecursiveAndIndependent` | 通过 |
| 三层递归、局部命名和完整调用链 | 同上；`TestInstantiateRecursiveBlueprintNamesAndPlans` | 通过 |
| 根能力外部消费、内部隐藏、禁止模板穿透子组合 | `TestExternalConsumerUsesOnlyCompositeRootBoundary`；`TestBlueprintRejectsInvalidStructureAndBoundaryCrossing` | 通过 |
| 根 Port 接受外层注入，仍由消费者定义接口 | `TestNestedCompositeConsumesAnExternalRootPort` | 通过 |
| 内部热替换，旧调用保持版本，其他组合独立 | `TestCompositeInternalReplacementPinsOldCalls` | 通过 |
| 整体删除等待旧调用、必需依赖保护和同计划重绑 | `TestCompositeRemovalProtectsExternalDependenciesAndActiveCalls` | 通过 |
| 无效结构、缺失依赖、错误端口/类型、启动失败回滚 | `TestBlueprintRejectsInvalidStructureAndBoundaryCrossing`；`TestCompositeRollbackOnInvalidContractAndStartupFailure` | 通过 |
| 准备期间原森林可服务，提交没有部分结构 | `TestCompositePreparationPublishesAllNodesAtOnce` | 通过 |
| 一个计划种植多组合与外部关系，只增加一版本 | 重复种植测试；`examples/composite` 七树、两组合、一个外部消费者 | 通过 |
| 并发模板展开、业务调用与内部替换 | `TestConcurrentBlueprintInstantiationAndCalls` | 通过 |
| 并发提交竞争同一基础版本，无部分种植和丢失更新 | `TestConcurrentCompositePlansConflictWithoutPartialPlanting` | 通过 |
| 模板和计划输入副本隔离，包含原子模板变量替换 | `TestInstantiateRecursiveBlueprintNamesAndPlans` | 通过 |
| v0.1 行为继续有效 | 原有 `tests/` 全部测试及四个原有示例 | 通过 |

## 执行方式

Windows/amd64 主机 Go 1.26.5：`go vet ./...`、`go test ./... -timeout 90s` 和五个示例均通过。共 41 个顶层测试（原有 30 个、组合层 11 个），另含子测试。组合包测试覆盖率 95.5%。

Linux/amd64 Docker：`golang:1.26` 中执行 `go test -race ./... -count=3 -timeout 120s`，完整测试连续三轮通过，无 data race 报告。CI 继续使用 Go 1.23，Windows/Linux 普通测试，Linux race，并增加 Composite 示例。

模板注册仍采用原有 Registry 的一次性应用安装协议；实例化是纯计划展开，没有注册副作用。封闭的能力边界由根 Definition 的 Visibility、ExportChildren 和普通接口实现负责，compose 另限制模板间只连接根。Forest 管理者仍可显式定位和修改内部树。

没有增加跨进程适配、持久化、机器代码加载、Agent Runtime，或其他 v0.3 功能。

两轴独立复核均通过：Standards 0 项行动项，Spec 0 项行动项，记录见 [review.md](review.md)。
