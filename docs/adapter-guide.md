# 接入现有框架

新增适配模块使用独立 go.mod；Forest Core 不导入第三方框架。原生资源管理与业务能力分开，由消费方声明 Port，通过结构兼容的接口连接。

接入说明与公开 API 测试需要覆盖：能力和可见性、owned/borrowed 归属、Build/Start/Ready 的工作和失败清理、调用 Scope 单元、后台接收门禁与排空、允许的替换/重绑、原生对象有效期及运行诊断。

Gin 保留路由/中间件；gRPC 保留原生注册、metadata、Unary/Streaming；GORM 保留会话和事务；NATS 保留连接配置及 JetStream 确认/重投。共同遵守生命周期约定，不使用抹平差异的万能 Wrapper。

先让池、入口和消费者稳定，替换使用 Ref 的业务树。Construction/Startup 资源与结构归属不能形成环；父节点需要的构造资源通常应是兄弟树，不是由它拥有的子树。

Apply 原子性覆盖本地拓扑与路由。数据库迁移、broker 配置、发布事件、消费启停各有外部效果与错误结果；准备阶段不执行不可回滚的业务副作用。

可选观测采集明确的调用边界。trace 关联不提供跨进程 Forest 版本一致性；Scope 引用计数不是真实 Tree/Link 流量。缺失、过期与采样必须区别于零调用。
