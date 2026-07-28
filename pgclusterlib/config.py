import os
import re
from pathlib import Path

import yaml

from .errors import ConfigError
from .topology import Topology


ENV = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}$")
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _expand(value):
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if not isinstance(value, str):
        return value
    match = ENV.match(value)
    if not match:
        return os.path.expanduser(value)
    name, default = match.groups()
    actual = os.environ.get(name)
    if actual:
        return os.path.expanduser(actual)
    if default is not None:
        return os.path.expanduser(default)
    raise ConfigError("环境变量未设置: %s" % name)


class Config:
    def __init__(self, path, raw):
        self.path = Path(path).resolve()
        self.raw = _expand(raw)
        self.postgres = self.raw.get("postgres") or {}
        self.hosts = self.raw.get("hosts") or {}
        self.nodes = self.raw.get("nodes") or {}
        self.clusters = self.raw.get("clusters") or {}
        self.extensions = self.raw.get("extensions") or {}
        operation_log = self.raw.get("operation_log", "~/operation.log")
        if not isinstance(operation_log, str) or not operation_log:
            raise ConfigError("operation_log 必须是非空路径")
        self.operation_log = Path(operation_log).expanduser()
        self._validate()
        self.topology = Topology(self)

    @property
    def home(self):
        return Path(self.postgres["home"])

    def binary(self, name):
        return str(self.home / "bin" / name)

    def node(self, name):
        try:
            return self.nodes[name]
        except KeyError:
            raise ConfigError("未知节点: %s" % name)

    def cluster(self, name):
        try:
            return self.clusters[name]
        except KeyError:
            raise ConfigError("未知集群: %s" % name)

    def logical_names(self, name):
        cluster = self.cluster(name)
        return {
            "publication": cluster.get("publication") or "%s_pub" % name,
            "subscription": cluster.get("subscription") or "%s_sub" % name,
            "slot": cluster.get("slot") or (
                "%s_failover_slot" % name if cluster.get("failover", False) else "%s_slot" % name
            ),
        }

    def physical_slot(self, streaming_name, standby):
        return standby.get("slot") or "%s_%s_physical_slot" % (streaming_name, standby["node"])

    def dependencies(self, name):
        return list(self.topology.dependencies(name))

    def logical_node(self, name, field):
        return self.topology.logical_group(name, field).primary

    def logical_streaming(self, name, field):
        target = self.cluster(name)[field]
        return target if target in self.clusters else None

    def closure(self, target):
        return self.topology.closure(target)

    def _validate_identifier(self, value, field):
        if not isinstance(value, str) or len(value.encode("utf-8")) > 63 or not IDENT.match(value):
            raise ConfigError("%s 不是合法 PostgreSQL 标识符: %r" % (field, value))

    def _validate(self):
        if self.raw.get("version") != 1:
            raise ConfigError("仅支持 version: 1")
        for field, value in (("postgres", self.postgres), ("hosts", self.hosts),
                             ("nodes", self.nodes), ("clusters", self.clusters),
                             ("extensions", self.extensions)):
            if not isinstance(value, dict):
                raise ConfigError("%s 必须是对象" % field)
        home = self.postgres.get("home")
        if not isinstance(home, str) or not home or not Path(home).is_absolute():
            raise ConfigError("postgres.home 必须是绝对路径")
        provider = self.postgres.get("provider", "postgres")
        if provider not in {"postgres", "fbase"}:
            raise ConfigError("postgres.provider 不支持: %s" % provider)
        if not self.hosts or not self.nodes or not self.clusters:
            raise ConfigError("hosts、nodes、clusters 不能为空")
        overlap = set(self.nodes) & set(self.clusters)
        if overlap:
            raise ConfigError("节点名和集群名不能重复: %s" % ", ".join(sorted(overlap)))
        for name, address in self.hosts.items():
            if not isinstance(address, str) or not address:
                raise ConfigError("hosts.%s 必须是非空地址" % name)
        endpoints, data_dirs = set(), set()
        for name, node in self.nodes.items():
            if not isinstance(node, dict):
                raise ConfigError("nodes.%s 必须是对象" % name)
            if node.get("host") not in self.hosts:
                raise ConfigError("nodes.%s 引用了未知 host" % name)
            try:
                port = int(node.get("port"))
            except (TypeError, ValueError):
                raise ConfigError("nodes.%s.port 必须是整数" % name)
            if not 1 <= port <= 65535:
                raise ConfigError("nodes.%s.port 超出范围" % name)
            data_dir = node.get("data_dir")
            if not isinstance(data_dir, str) or not Path(data_dir).is_absolute():
                raise ConfigError("nodes.%s.data_dir 必须是绝对路径" % name)
            if Path(data_dir) == Path("/"):
                raise ConfigError("nodes.%s.data_dir 不能是根目录" % name)
            endpoint = (self.hosts[node["host"]], port)
            if endpoint in endpoints:
                raise ConfigError("重复节点端口: %s" % (endpoint,))
            data_location = (self.hosts[node["host"]], data_dir)
            if data_location in data_dirs:
                raise ConfigError("同一主机存在重复 data_dir: %s" % data_dir)
            endpoints.add(endpoint)
            data_dirs.add(data_location)
        node_owners = {}
        logical_names = {"publication": set(), "subscription": set(), "slot": set()}
        for name, cluster in self.clusters.items():
            if not isinstance(cluster, dict):
                raise ConfigError("clusters.%s 必须是对象" % name)
            kind = cluster.get("type")
            if kind not in {"streaming", "logical", "mmr"}:
                raise ConfigError("clusters.%s.type 不支持: %r" % (name, kind))
            if kind == "streaming":
                primary = cluster.get("primary")
                if primary not in self.nodes:
                    raise ConfigError("clusters.%s.primary 引用未知节点" % name)
                standbys = cluster.get("standbys") or []
                if not isinstance(standbys, list):
                    raise ConfigError("clusters.%s.standbys 必须是列表" % name)
                members = [primary]
                slots = set()
                for standby in standbys:
                    if not isinstance(standby, dict):
                        raise ConfigError("clusters.%s.standbys 条目必须是对象" % name)
                    if standby.get("node") not in self.nodes or standby["node"] == primary:
                        raise ConfigError("clusters.%s.standbys 节点无效" % name)
                    if standby["node"] in members:
                        raise ConfigError("clusters.%s 存在重复节点: %s" % (name, standby["node"]))
                    members.append(standby["node"])
                    slot = self.physical_slot(name, standby)
                    self._validate_identifier(slot, "physical slot")
                    if slot in slots:
                        raise ConfigError("clusters.%s 存在重复物理槽: %s" % (name, slot))
                    slots.add(slot)
                for node in members:
                    owner = node_owners.setdefault(node, name)
                    if owner != name:
                        raise ConfigError("节点 %s 同时属于集群 %s 和 %s" % (node, owner, name))
            elif kind == "logical":
                for field in ("publisher", "subscriber"):
                    target = cluster.get(field)
                    if target in self.nodes:
                        continue
                    if target not in self.clusters or self.clusters[target].get("type") != "streaming":
                        raise ConfigError("clusters.%s.%s 必须引用节点或 streaming 集群" % (name, field))
                publisher = cluster["publisher"]
                subscriber = cluster["subscriber"]
                if publisher == subscriber:
                    raise ConfigError("clusters.%s 发布端和订阅端不能相同" % name)
                publisher_node = self.nodes.get(publisher) and publisher
                subscriber_node = self.nodes.get(subscriber) and subscriber
                if publisher_node and subscriber_node and publisher_node == subscriber_node:
                    raise ConfigError("clusters.%s 发布端和订阅端不能是同一节点" % name)
                for field in ("copy_data", "failover"):
                    if field in cluster and not isinstance(cluster[field], bool):
                        raise ConfigError("clusters.%s.%s 必须是布尔值" % (name, field))
                if cluster.get("failover", False):
                    target = cluster["publisher"]
                    if target not in self.clusters or not (self.clusters[target].get("standbys") or []):
                        raise ConfigError("clusters.%s.failover=true 要求发布端是带备库的 streaming 集群" % name)
                if cluster.get("tables", "all") != "all":
                    raise ConfigError("首版仅支持 tables: all")
                for field, value in self.logical_names(name).items():
                    self._validate_identifier(value, "logical %s" % field)
                    if value in logical_names[field]:
                        raise ConfigError("重复 logical %s: %s" % (field, value))
                    logical_names[field].add(value)
            else:
                mmr_nodes = cluster.get("nodes")
                if not isinstance(mmr_nodes, list) or not mmr_nodes:
                    raise ConfigError("clusters.%s.nodes 必须是非空节点列表（MMR provider 尚未实现）" % name)
                if any(node not in self.nodes for node in mmr_nodes):
                    raise ConfigError("clusters.%s.nodes 引用了未知节点" % name)

        for name, cluster in self.clusters.items():
            if cluster.get("type") != "logical":
                continue
            for field in ("publisher", "subscriber"):
                endpoint = cluster[field]
                if endpoint in node_owners:
                    raise ConfigError(
                        "clusters.%s.%s 直接引用了 streaming 集群 %s 的节点 %s；请引用集群名" %
                        (name, field, node_owners[endpoint], endpoint)
                    )

        # Force dependency resolution now so validate cannot defer graph errors.
        topology = Topology(self)
        for name in self.clusters:
            topology.closure(name)


def load(path):
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError as exc:
        raise ConfigError("无法读取配置 %s: %s" % (path, exc))
    except yaml.YAMLError as exc:
        raise ConfigError("YAML 解析失败: %s" % exc)
    return Config(path, raw)
