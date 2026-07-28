# pgcluster

`pgcluster` 是一个面向自用测试环境的 PostgreSQL 集群搭建工具。首版支持：

- 单主单备流复制；
- 发布端流复制集群 → 逻辑复制（故障转移槽）→ 订阅端流复制集群；
- PostgreSQL 17+ 原生逻辑故障转移槽；
- `validate`、`graph`、`doctor`、`create`、`status`、`verify`、`clean`；
- 每次操作覆盖一个可手工复现的 `operation.log`。

FBase 故障槽、等保插件和 MMR 已预留 provider 接口，首版会明确报“尚未实现”。

## 快速开始

本仓库的 `pgcluster.yaml` 是可直接运行的 `c1` 示例，默认使用本机 PostgreSQL 18.3：

```bash
./pgcluster validate c1
./pgcluster doctor c1
./pgcluster create c1
./pgcluster status c1
./pgcluster verify c1
```

`verify` 会在 `pub1`、`sub1` 创建：

```sql
CREATE TABLE public.test_tbl (
  id integer NOT NULL,
  name text
);
```

然后写入 `1, 'c1-probe'`，并确认该行从 `pub1` 逻辑复制到 `sub1`，再物理复制到 `sub2`。

默认端口和 PGDATA：

| 节点 | 端口 | 默认 PGDATA |
| --- | ---: | --- |
| pub1 | 15432 | `data/c1/pub1` |
| pub2 | 15433 | `data/c1/pub2` |
| sub1 | 25432 | `data/c1/sub1` |
| sub2 | 25433 | `data/c1/sub2` |

环境变量只能占据整个字段，例如 `PGDATA1=/data/pub1`。不支持路径拼接。

## 清理

```bash
./pgcluster clean c1 --yes
```

`clean` 只删除带有 `PGDATA/.pgcluster-managed` 且内容与节点配置一致的目录。它会先停止实例，再删除 PGDATA。

## 日志

- 工具操作日志：默认 `/home/postgres/operation.log`（可用 `PGCLUSTER_LOG` 或 YAML 的 `operation_log` 修改）。
- PostgreSQL 日志：`<PGDATA>/log/`。

实例默认启用 `logging_collector`、`log_statement = 'all'` 和 `log_error_verbosity = verbose`；普通 `.log` 中会包含 PostgreSQL 内部函数、C 源文件和行号。

## 测试

```bash
python3 -m unittest discover -v
```
