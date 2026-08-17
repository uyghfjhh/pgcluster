# pgcluster 开发进度

## 当前状态

项目维护一套不带版本字段的声明式 YAML 配置模型。默认配置优先级为
`pgcluster.local.yaml`，其次是 `pgcluster.yaml`；显式 `-f/--file` 优先级最高。

模型覆盖 PostgreSQL/FBase 安装、实例、物理流复制、逻辑复制、Citus 和 FBase MMR。

## 已完成

- 主机、安装、插件、许可证、实例、端口和数据目录校验
- 流复制、逻辑复制、Citus、MMR 拓扑引用校验
- 同步/异步复制、MMR streaming、故障槽和两阶段提交选项校验
- 本机直连、远程 SSH 和操作日志
- `validate`、`graph`、`list`、`help` 命令
- `tui` 实时监控界面：部署关系图、集群健康卡片、实例状态、连接/TPS/缓存命中率和复制指标
- `list` 的四个顶层集群树状展示、IP/端口/数据目录和部署标记状态
- `doctor`：实例目录、数据库工具、扩展控制文件和 FBase 许可证检查
- `install`：在使用安装的主机上构建和安装缺失插件，支持 `--force`
- `create`：流复制、逻辑复制、Citus、MMR 创建及依赖编排
- `status`、`health`、`verify`、`lag`、`monitor --once`
- `start`、`stop`、`restart`、`clean`、`delete`
- `failover`、`switchover`、`rejoin`
- 修改型命令的阶段进度输出，避免长时间操作期间无反馈
- 配置锁、PGDATA `.pgcluster-managed` 标记和危险操作确认
- PostgreSQL/Citus 远程部署验证
- FBase MMR 许可证、插件、双向复制和等保元数据兼容处理
- 配置、执行器和运行时单元测试

## 已知限制

- 没有自动故障检测和长期运行的监控守护进程
- Citus 没有配置 `source_dir`，缺少 Citus 时只能报告前置条件不足
- `list` 的部署状态依赖主机可达和 `.pgcluster-managed` 标记，不负责发现配置之外的数据库实例

未实现的操作应直接报错，不回退到旧配置格式。
