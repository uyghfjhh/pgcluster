# pgcluster

`pgcluster` 使用单一声明式 YAML 模型描述 PostgreSQL、FBase、流复制、逻辑复制、Citus 和 MMR 拓扑。

当前正式配置为 [pgcluster.yaml](pgcluster.yaml)，示例副本为
[pgcluster.example.yaml](pgcluster.example.yaml)。配置不包含版本号字段。

## 配置结构

- `hosts`：主机地址；省略 `transport`，本机地址直连，其他地址默认 SSH。
- `postgresql_installations`：PostgreSQL/FBase 安装目录、许可证和插件前置条件。
- `instances`：单个数据库实例，名称使用 `_node`。
- `streaming_clusters`：物理流复制集群，名称使用 `_cluster`。
- `logical_replications`：一组 `pub`/`sub` 逻辑复制关系。
- `citus_clusters`：Coordinator 和 Worker 流复制集群组成的 Citus 集群。
- `mmr_clusters`：FBase MMR 集群，每个成员引用一个流复制集群。

## 使用

当前命令支持校验、拓扑查看和集群创建：

```bash
./pgcluster validate streaming.basic_cluster
./pgcluster validate logical.pub_sub
./pgcluster validate citus.citus_cluster
./pgcluster validate mmr.mmr_cluster
./pgcluster graph mmr.mmr_cluster
./pgcluster doctor
./pgcluster install fbase15
./pgcluster install postgresql17
./pgcluster create streaming.basic_cluster
./pgcluster create logical.pub_sub
./pgcluster create citus.citus_cluster
./pgcluster create mmr.mmr_cluster
./pgcluster status streaming.basic_cluster
./pgcluster health mmr.mmr_cluster
./pgcluster lag streaming.basic_cluster
./pgcluster verify logical.pub_sub
./pgcluster stop streaming.basic_cluster
./pgcluster start streaming.basic_cluster
./pgcluster restart streaming.basic_cluster
./pgcluster clean streaming.basic_cluster --yes
./pgcluster delete streaming.basic_cluster --yes
./pgcluster failover streaming.basic_cluster --yes
./pgcluster rejoin streaming.basic_cluster --yes
./pgcluster monitor streaming.basic_cluster --once
```

`clean` 与 `delete` 都要求 `--yes`，并且只会操作含有
`.pgcluster-managed` 标记的数据目录。清理会先删除目标拓扑的复制元数据：
逻辑复制的 subscription/publication/slot、Citus 的分布式对象和节点元数据、
以及 MMR 的成员、订阅和扩展；随后才停止实例并删除 PGDATA。

Citus、FBase 插件和 MMR 创建均使用当前配置模型；未实现的操作不会回退到旧配置格式。

`doctor` 检查实例目录、数据库工具、配置扩展和 FBase 许可证；`install` 在每个
使用该安装的主机上补装缺失插件。插件已存在时不会重新构建，传入 `--force` 才会重装。
没有 `source_dir` 的插件（当前的 Citus）由操作系统或已有 PostgreSQL 安装提供；若缺失，
命令会明确报出该前置条件。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
