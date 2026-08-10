# pgcluster 开发进度

更新日期：2026-08-10

## 当前目标

`pgcluster` 当前只负责创建 PostgreSQL 实例和集群拓扑，包括启动、停止、状态检查、
验证和安全清理。暂不负责自动故障检测、主备切换和高可用编排。

## 已完成

### 配置与拓扑

- 节点和集群分层配置。
- 一份 YAML 可配置多个独立拓扑。
- 逻辑复制的 `publisher` 和 `subscriber` 均可引用单节点或流复制集群。
- 根据拓扑依赖计算创建顺序和清理顺序，不依赖 YAML 中的书写顺序。
- `graph` 显示拓扑树、依赖关系、创建顺序和清理顺序。
- 环境变量必须占据完整字段，支持 `${NAME:-default}` 格式，不进行路径拼接。

### 集群类型

- 单节点 PostgreSQL 实例。
- 使用物理复制槽的一主一备或一主多备流复制。
- 两个单节点之间的逻辑复制。
- 发布端流复制集群 → 逻辑复制 → 订阅端流复制集群。
- PostgreSQL 17+ 原生 failover logical slot，包括发布端备库的逻辑槽同步。
- 流复制和逻辑复制均支持 `replication_mode: async|sync`。
- 同步复制支持 `remote_write`、`on` 和 `remote_apply`，默认 `remote_apply`。
- 同一发布端的同步物理复制和同步逻辑复制会合并为统一的同步等待目标。

### 命令和生命周期

- `validate`：校验配置和拓扑引用。
- `graph`：显示拓扑树和依赖关系。
- `doctor`：检查 SSH、PostgreSQL 二进制、端口、PGDATA 和目录权限。
- `create`：按依赖顺序幂等创建或继续未完成的部署。
- `status`：以树状形式显示 `OK/FAILED`、节点角色、端口和 PGDATA。
- `verify`：写入随机 token，验证逻辑复制以及订阅端物理复制。
- `start/stop/restart`：管理目标拓扑中的实例。
- `clean --yes`：按反向依赖顺序停止并删除由 pgcluster 管理的 PGDATA。
- 上层逻辑集群仍依赖底层集群时，禁止单独停止或清理底层集群。

### 运行、安全和日志

- 支持本机和 SSH 免密远程主机；远程命令、文件写入和 PGDATA 操作都在目标主机执行。
- `operation.log` 记录命令、SQL、配置文件内容、输出和退出码，每次操作覆盖。
- PostgreSQL 默认开启日志收集、连接/断开、SQL、耗时、checkpoint 和复制命令日志。
- PGDATA 使用 `.pgcluster-managed` marker 校验归属，避免误删其他实例。
- 修改类命令使用配置级文件锁，防止并发操作同一拓扑。
- `listen_addresses='*'`，测试环境中的 HBA 默认允许普通连接和复制连接。

## 已验证

- 21 个单元测试全部通过。
- Python 模块编译检查通过。
- 两节点普通逻辑复制完整流程通过：`doctor → create → verify → status → stop/start → clean`。
- `c1` 已验证发布端主备、订阅端主备、逻辑复制和 failover slot。
- `c2` 已验证带物理复制槽的流复制。
- 三节点同步复制实测通过：主库同时等待一个物理备库和一个逻辑订阅端，
  两个 sender 的 `sync_state` 均为 `sync`。
- SSH 执行和远程文件传输已有单元测试；尚未在两台真实主机上完成全流程验证。

## 暂未实现

- FBase/MAC 故障槽接口。
- FBase MMR 集群创建。
- 等保插件安装。
- Citus 集群（仅预留后续扩展方向）。
- 自动故障检测、主备切换、VIP/DNS 切换和长期运行监控。
- 跨主机 SSH 真实环境端到端验证。

## 建议下一步

1. 使用两台真实主机执行 SSH 端到端测试。
2. 增加一主多备下的同步策略，支持“等待全部备库”和“任意 N 个备库”。
3. 定义 FBase provider 的故障槽接口并增加相应测试。
4. 实现 MMR 拓扑 provider。
5. 实现等保插件安装与验证。
