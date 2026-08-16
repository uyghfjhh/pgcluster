# pgcluster 开发进度

## 当前状态

项目只维护一套无版本字段的声明式配置模型。当前正式入口是 `pgcluster.yaml`，模型包含：

- PostgreSQL/FBase 安装、许可证和插件前置条件；
- 单实例和流复制集群；
- `pub`/`sub` 逻辑复制关系；
- Coordinator/Worker 组成的 Citus 集群；
- 每个成员引用流复制集群的 FBase MMR 集群。

## 已完成

- v2 YAML 结构校验；
- 主机、安装、实例、端口和数据目录引用校验；
- 流复制、逻辑复制、Citus、MMR 拓扑引用校验；
- MMR `streaming`、故障槽、两阶段提交选项校验；
- 主机连接方式推断：本机直连，其他地址默认 SSH；
- `validate` 命令和对应单元测试。
- `doctor`：检查实例目录、数据库工具、扩展控制文件和 FBase 许可证；缺项时返回非零。
- `install <installation>`：在使用该安装的每台主机上构建并安装缺失插件，`--force` 可重装。
- `create`：流复制、逻辑复制、Citus、MMR 实际创建；
- `status`、`health`、`verify`：实例和四类集群检查；
- `start`、`stop`、`restart`、`clean`：带 marker 校验和配置锁的生命周期操作；
- `failover`、`switchover`、`rejoin`：流复制主备切换、旧主库重建回归；
- `monitor --once` 和 `lag`：健康与复制进度检查；
- 远程 PG17.2 Citus 编译安装与真实分布式表验证；
- FBase MMR 许可证、插件、双向复制和等保元数据兼容处理。

## 未完成

- 自动故障检测和长期运行监控守护进程；
- Citus 的 `source_dir` 尚未配置；目标主机缺少 Citus 时会明确报错，不能自动构建。

未实现的操作会直接报错，不会回退到旧配置格式。
