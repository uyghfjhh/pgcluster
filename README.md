# pgcluster

`pgcluster` 用于快速创建 PostgreSQL 测试集群。目前支持：

- 单节点实例；
- 带物理复制槽的流复制集群；
- 两个节点之间的简单逻辑复制；
- 流复制和逻辑复制的同步/异步模式；
- 两套流复制集群之间的逻辑复制和 PostgreSQL 17+ 原生故障转移槽；
- 本机和 SSH 远程主机；
- 依赖检查、幂等创建、健康检查和安全清理。

FBase 故障槽、等保插件和 MMR 暂未实现，接口已经隔离，配置使用时会明确报错。

## 使用

仓库内的 [pgcluster.yaml](pgcluster.yaml) 包含三个拓扑：

- `c1`：发布端主备 → 逻辑复制 → 订阅端主备；
- `c2`：一主一备流复制；
- `c3`：两个单节点之间的简单逻辑复制。

```bash
./pgcluster validate c1
./pgcluster graph c1
./pgcluster doctor c1
./pgcluster create c1
./pgcluster status c1
./pgcluster verify c1
```

`create` 可以重复执行：已有且 marker 匹配的实例会被复用，缺少的资源会继续创建。配置冲突或
非 pgcluster 管理的 PGDATA 会直接报错。

```bash
./pgcluster stop c1
./pgcluster start c1
./pgcluster restart c1
./pgcluster clean c1 --yes
```

停止或清理底层集群前会检查反向依赖。例如 `publisher` 仍被 `c1` 使用时，不能单独
`stop` 或 `clean publisher`。

## 默认行为

- PostgreSQL：`${PGHOME:-/usr/local/pgsql18.3}`
- 操作日志：`${PGCLUSTER_LOG:-/home/postgres/operation.log}`
- PostgreSQL 日志：`<PGDATA>/log/`
- Unix Socket：使用 PostgreSQL 编译默认值，本环境为 `/tmp`
- `listen_addresses = '*'`
- 测试环境的 `pg_hba.conf` 默认允许所有普通连接和复制连接

环境变量必须占据整个字段，例如：

```yaml
data_dir: ${PGDATA1:-/data/c1/pub1}
```

不支持把环境变量与路径片段拼接。

远程主机使用当前操作系统用户进行 SSH 免密登录；PostgreSQL 二进制需要安装在各主机相同的
`postgres.home`。创建 `/data` 等目录时可能需要免交互 `sudo`。

## 状态与验证

`status` 不只检查端口，还验证主备角色、WAL receiver/sender、物理槽、publication、
subscription、逻辑槽和故障槽同步状态。集群为 `FAILED` 时命令退出码为 `1`。

`verify` 使用专用的 `pgcluster_verify.probe` 表写入随机 token，不占用业务表。

## 安全与日志

`clean` 只删除 marker 内容与节点配置完全一致的 PGDATA，并通过依赖图阻止错误的底层清理。
修改命令使用配置级文件锁，避免两个 pgcluster 进程同时操作同一套环境。

`operation.log` 每次覆盖，记录实际执行的本机/SSH命令、SQL、文件写入内容、输出和退出码。

## 同步与异步复制

流复制和逻辑复制都使用同一个字段；未配置时默认为 `async`：

```yaml
replication_mode: async
```

同步复制配置为：

```yaml
replication_mode: sync
synchronous_commit: remote_apply
```

`synchronous_commit` 可选 `remote_write`、`on` 或 `remote_apply`，默认
`remote_apply`。同步集群中的任一备库/订阅端停止都会阻塞发布端的事务提交。

## 测试

```bash
python3 -m unittest discover -v
```
