import os
import re
from pathlib import Path

import yaml

from .errors import ConfigError


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
        self.operation_log = Path(self.raw.get("operation_log", "~/operation.log")).expanduser()
        self._validate()

    @property
    def home(self):
        return Path(self.postgres["home"])

    def binary(self, name):
        return str(self.home / "bin" / name)

    def node(self, name):
        return self.nodes[name]

    def cluster(self, name):
        return self.clusters[name]

    def logical_names(self, name):
        cluster = self.cluster(name)
        return {
            "publication": cluster.get("publication") or "%s_pub" % name,
            "subscription": cluster.get("subscription") or "%s_sub" % name,
            "slot": cluster.get("slot") or "%s_failover_slot" % name,
        }

    def physical_slot(self, streaming_name, standby):
        return standby.get("slot") or "%s_%s_physical_slot" % (streaming_name, standby["node"])

    def dependencies(self, name):
        cluster = self.cluster(name)
        return [cluster["publisher"], cluster["subscriber"]] if cluster["type"] == "logical" else []

    def closure(self, target):
        result, seen = [], set()

        def visit(name):
            if name in seen:
                return
            seen.add(name)
            for dependency in self.dependencies(name):
                visit(dependency)
            result.append(name)

        visit(target)
        return result

    def _validate_identifier(self, value, field):
        if not isinstance(value, str) or len(value.encode("utf-8")) > 63 or not IDENT.match(value):
            raise ConfigError("%s 不是合法 PostgreSQL 标识符: %r" % (field, value))

    def _validate(self):
        if self.raw.get("version") != 1:
            raise ConfigError("仅支持 version: 1")
        home = self.postgres.get("home")
        if not home or not Path(home).is_absolute():
            raise ConfigError("postgres.home 必须是绝对路径")
        if not (Path(home) / "bin" / "initdb").is_file():
            raise ConfigError("postgres.home 中没有 bin/initdb: %s" % home)
        if not self.hosts or not self.nodes or not self.clusters:
            raise ConfigError("hosts、nodes、clusters 不能为空")
        for name, address in self.hosts.items():
            if not isinstance(address, str) or not address:
                raise ConfigError("hosts.%s 必须是非空地址" % name)
        endpoints, data_dirs = set(), set()
        for name, node in self.nodes.items():
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
            endpoint = (node["host"], port)
            if endpoint in endpoints:
                raise ConfigError("重复节点端口: %s" % (endpoint,))
            if data_dir in data_dirs:
                raise ConfigError("重复 data_dir: %s" % data_dir)
            endpoints.add(endpoint)
            data_dirs.add(data_dir)
        for name, cluster in self.clusters.items():
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
                for standby in standbys:
                    if standby.get("node") not in self.nodes or standby["node"] == primary:
                        raise ConfigError("clusters.%s.standbys 节点无效" % name)
                    self._validate_identifier(self.physical_slot(name, standby), "physical slot")
            elif kind == "logical":
                for field in ("publisher", "subscriber"):
                    target = cluster.get(field)
                    if target not in self.clusters or self.clusters[target].get("type") != "streaming":
                        raise ConfigError("clusters.%s.%s 必须引用 streaming 集群" % (name, field))
                if cluster.get("tables", "all") != "all":
                    raise ConfigError("首版仅支持 tables: all")
                for field, value in self.logical_names(name).items():
                    self._validate_identifier(value, "logical %s" % field)


def load(path):
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError as exc:
        raise ConfigError("无法读取配置 %s: %s" % (path, exc))
    except yaml.YAMLError as exc:
        raise ConfigError("YAML 解析失败: %s" % exc)
    return Config(path, raw)
