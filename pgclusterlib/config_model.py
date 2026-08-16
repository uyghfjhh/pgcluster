from pathlib import Path

from .config import _expand
from .errors import ConfigError


class ConfigModel:
    """Validated desired-state model for the cluster configuration.

    This model owns the complete versionless configuration contract. Runtime
    operations consume this validated model directly.
    """

    def __init__(self, path, raw):
        self.path = Path(path).resolve()
        self.raw = _expand(raw)
        self.hosts = self.raw.get("hosts") or {}
        self.installations = self.raw.get("postgresql_installations") or {}
        self.instances = self.raw.get("instances") or {}
        self.streaming_clusters = self.raw.get("streaming_clusters") or {}
        self.logical_replications = self.raw.get("logical_replications") or {}
        self.citus_clusters = self.raw.get("citus_clusters") or {}
        self.mmr_clusters = self.raw.get("mmr_clusters") or {}
        self._validate()

    @staticmethod
    def _bool(value, field):
        if not isinstance(value, bool):
            raise ConfigError("%s 必须是布尔值" % field)

    @staticmethod
    def _identifier(value, field):
        if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 63:
            raise ConfigError("%s 必须是非空 PostgreSQL 标识符" % field)
        if not (value[0].isalpha() or value[0] == "_") or any(
            not (char.isalnum() or char in "_$") for char in value
        ):
            raise ConfigError("%s 不是合法 PostgreSQL 标识符: %r" % (field, value))

    def _validate_hosts(self):
        if not self.hosts:
            raise ConfigError("hosts 不能为空")
        for name, host in self.hosts.items():
            if not isinstance(host, dict) or not isinstance(host.get("address"), str):
                raise ConfigError("hosts.%s.address 必须是非空地址" % name)
            if not host["address"]:
                raise ConfigError("hosts.%s.address 必须是非空地址" % name)
            if "transport" in host:
                raise ConfigError("hosts.%s 不需要配置 transport，连接方式由地址推断" % name)

    def _validate_installations(self):
        if not self.installations:
            raise ConfigError("postgresql_installations 不能为空")
        for name, installation in self.installations.items():
            if not isinstance(installation, dict):
                raise ConfigError("postgresql_installations.%s 必须是对象" % name)
            if installation.get("provider") not in {"postgres", "fbase"}:
                raise ConfigError("postgresql_installations.%s.provider 不支持" % name)
            home = installation.get("home")
            if not isinstance(home, str) or not Path(home).is_absolute():
                raise ConfigError("postgresql_installations.%s.home 必须是绝对路径" % name)
            if installation["provider"] == "fbase":
                license_config = installation.get("license")
                if not isinstance(license_config, dict):
                    raise ConfigError("FBase 安装必须配置 license")
                source = license_config.get("source_file")
                if not isinstance(source, str) or not Path(source).is_absolute():
                    raise ConfigError("postgresql_installations.%s.license.source_file 必须是绝对路径" % name)
                data_file = license_config.get("data_file", "license.dat")
                if not isinstance(data_file, str) or not data_file or "/" in data_file:
                    raise ConfigError("postgresql_installations.%s.license.data_file 必须是 PGDATA 内的文件名" % name)
            for plugin_name, plugin in (installation.get("plugins") or {}).items():
                if not isinstance(plugin, dict):
                    raise ConfigError("安装插件 %s.%s 必须是对象" % (name, plugin_name))
                if "required" in plugin:
                    self._bool(plugin["required"], "安装插件 %s.%s.required" % (name, plugin_name))
                if "install" in plugin:
                    self._bool(plugin["install"], "安装插件 %s.%s.install" % (name, plugin_name))
                source = plugin.get("source_dir")
                if source is not None and (not isinstance(source, str) or not Path(source).is_absolute()):
                    raise ConfigError("安装插件 %s.%s.source_dir 必须是绝对路径" % (name, plugin_name))
                extension = plugin.get("extension")
                if extension is not None:
                    self._identifier(extension, "安装插件 %s.%s.extension" % (name, plugin_name))
                preload = plugin.get("preload_library")
                if preload is not None and (not isinstance(preload, str) or not preload):
                    raise ConfigError("安装插件 %s.%s.preload_library 必须是非空字符串" % (name, plugin_name))

    def _validate_instances(self):
        if not self.instances:
            raise ConfigError("instances 不能为空")
        endpoints = set()
        locations = set()
        for name, instance in self.instances.items():
            if not isinstance(instance, dict):
                raise ConfigError("instances.%s 必须是对象" % name)
            host_name = instance.get("host")
            if host_name not in self.hosts:
                raise ConfigError("instances.%s.host 引用了未知 host" % name)
            if instance.get("installation") not in self.installations:
                raise ConfigError("instances.%s.installation 引用了未知安装" % name)
            try:
                port = int(instance.get("port"))
            except (TypeError, ValueError):
                raise ConfigError("instances.%s.port 必须是整数" % name)
            if not 1 <= port <= 65535:
                raise ConfigError("instances.%s.port 超出范围" % name)
            data_dir = instance.get("data_dir")
            if not isinstance(data_dir, str) or not Path(data_dir).is_absolute() or data_dir == "/":
                raise ConfigError("instances.%s.data_dir 必须是非根绝对路径" % name)
            endpoint = (self.hosts[host_name]["address"], port)
            location = (self.hosts[host_name]["address"], data_dir)
            if endpoint in endpoints:
                raise ConfigError("重复实例端口: %s" % (endpoint,))
            if location in locations:
                raise ConfigError("同一主机存在重复 data_dir: %s" % data_dir)
            endpoints.add(endpoint)
            locations.add(location)

    def _validate_streaming(self):
        if not self.streaming_clusters:
            raise ConfigError("streaming_clusters 不能为空")
        owners = {}
        for name, cluster in self.streaming_clusters.items():
            if not isinstance(cluster, dict):
                raise ConfigError("streaming_clusters.%s 必须是对象" % name)
            primary = cluster.get("primary")
            if primary not in self.instances:
                raise ConfigError("streaming_clusters.%s.primary 引用了未知实例" % name)
            members = [primary]
            standbys = cluster.get("standbys") or []
            if not isinstance(standbys, list):
                raise ConfigError("streaming_clusters.%s.standbys 必须是列表" % name)
            for index, standby in enumerate(standbys):
                if not isinstance(standby, dict) or standby.get("instance") not in self.instances:
                    raise ConfigError("streaming_clusters.%s.standbys[%d] 无效" % (name, index))
                instance = standby["instance"]
                if instance in members:
                    raise ConfigError("streaming_clusters.%s 存在重复实例: %s" % (name, instance))
                members.append(instance)
                slot = standby.get("slot") or "%s_%s_slot" % (name, instance)
                self._identifier(slot, "streaming_clusters.%s standby slot" % name)
            mode = (cluster.get("replication") or {}).get("mode", "async")
            if mode not in {"async", "sync"}:
                raise ConfigError("streaming_clusters.%s.replication.mode 无效" % name)
            if mode == "sync" and not standbys:
                raise ConfigError("streaming_clusters.%s 同步复制至少需要一个备库" % name)
            for instance in members:
                previous = owners.setdefault(instance, name)
                if previous != name:
                    raise ConfigError("实例 %s 同时属于 %s 和 %s" % (instance, previous, name))

    def _streaming_ref(self, value, field):
        if value not in self.streaming_clusters:
            raise ConfigError("%s 必须引用 streaming_clusters 中的集群" % field)

    def _validate_logical(self):
        for name, link in self.logical_replications.items():
            if not isinstance(link, dict) or set(link) != {"pub", "sub"}:
                raise ConfigError("logical_replications.%s 必须包含 pub 和 sub" % name)
            self._streaming_ref(link["pub"].get("streaming_cluster"), "%s.pub.streaming_cluster" % name)
            self._streaming_ref(link["sub"].get("streaming_cluster"), "%s.sub.streaming_cluster" % name)
            sub = link["sub"]
            if "copy_data" in sub:
                self._bool(sub["copy_data"], "%s.sub.copy_data" % name)
            slot = sub.get("slot") or {}
            if not isinstance(slot, dict):
                raise ConfigError("%s.sub.slot 必须是对象" % name)
            if "name" in slot:
                self._identifier(slot["name"], "%s.sub.slot.name" % name)
            if "failover" in slot:
                self._bool(slot["failover"], "%s.sub.slot.failover" % name)

    def _validate_citus(self):
        for name, cluster in self.citus_clusters.items():
            if not isinstance(cluster, dict):
                raise ConfigError("citus_clusters.%s 必须是对象" % name)
            self._streaming_ref(cluster.get("coordinator", {}).get("streaming_cluster"),
                                "%s.coordinator.streaming_cluster" % name)
            workers = cluster.get("workers")
            if not isinstance(workers, dict) or not workers:
                raise ConfigError("citus_clusters.%s.workers 必须是非空对象" % name)
            for worker_name, worker in workers.items():
                self._streaming_ref(worker.get("streaming_cluster"),
                                    "%s.workers.%s.streaming_cluster" % (name, worker_name))
            extensions = cluster.get("extensions")
            if not isinstance(extensions, list) or "citus" not in extensions:
                raise ConfigError("citus_clusters.%s.extensions 必须包含 citus" % name)
            factor = (cluster.get("postgresql_config") or {}).get("parameters", {}).get(
                "citus.shard_replication_factor", 1)
            if not isinstance(factor, int) or factor < 1 or factor > len(workers):
                raise ConfigError("%s 的 citus.shard_replication_factor 必须在 1 到 Worker 数量之间" % name)

    def _validate_mmr(self):
        for name, cluster in self.mmr_clusters.items():
            if not isinstance(cluster, dict):
                raise ConfigError("mmr_clusters.%s 必须是对象" % name)
            extensions = cluster.get("extensions")
            if not isinstance(extensions, list) or not {"fbase_mac", "fdd_mmr", "fb_license"}.issubset(extensions):
                raise ConfigError("mmr_clusters.%s.extensions 必须包含 FBase 多活和许可证扩展" % name)
            members = cluster.get("members")
            if not isinstance(members, dict) or not members:
                raise ConfigError("mmr_clusters.%s.members 必须是非空对象" % name)
            for member_name, member in members.items():
                self._streaming_ref(member.get("streaming_cluster"),
                                    "%s.members.%s.streaming_cluster" % (name, member_name))
                options = member.get("mmr_node") or {}
                if options.get("streaming", "off") not in {"off", "on", "parallel"}:
                    raise ConfigError("%s.members.%s.mmr_node.streaming 无效" % (name, member_name))
                for field in ("failover_slot", "two_phase"):
                    if field in options:
                        self._bool(options[field], "%s.members.%s.mmr_node.%s" % (name, member_name, field))
                join = member.get("join")
                if join is not None:
                    if not isinstance(join, dict):
                        raise ConfigError("%s.members.%s.join 必须是对象" % (name, member_name))
                    if join.get("synchronize_structure", "all") not in {"none", "all", "schema-only", "data-only"}:
                        raise ConfigError("%s.members.%s.join.synchronize_structure 无效" % (name, member_name))
                    if join.get("precheck", "table_exist_error") not in {"ignore", "table_exist_error"}:
                        raise ConfigError("%s.members.%s.join.precheck 无效" % (name, member_name))

    def _validate_global_config(self):
        capacity = self.raw.get("postgresql_config", {}).get("replication_capacity", {})
        if not isinstance(capacity, dict):
            raise ConfigError("postgresql_config.replication_capacity 必须是对象")
        for field in ("wal_senders", "replication_slots"):
            value = capacity.get(field, "auto")
            if value != "auto" and (not isinstance(value, int) or value < 1):
                raise ConfigError("postgresql_config.replication_capacity.%s 必须是 auto 或正整数" % field)

    def _validate(self):
        self._validate_hosts()
        self._validate_installations()
        self._validate_global_config()
        self._validate_instances()
        self._validate_streaming()
        self._validate_logical()
        self._validate_citus()
        self._validate_mmr()

    def validate(self, target):
        collections = self._collections()
        self._target(collections, target)
        return "配置有效: %s" % target

    def _collections(self):
        return {
            "streaming": self.streaming_clusters,
            "logical": self.logical_replications,
            "citus": self.citus_clusters,
            "mmr": self.mmr_clusters,
        }

    @staticmethod
    def _target(collections, target):
        if "." not in target:
            raise ConfigError("目标必须使用 kind.name，例如 citus.citus_cluster")
        kind, name = target.split(".", 1)
        if kind not in collections or name not in collections[kind]:
            raise ConfigError("未知集群: %s" % target)

    def graph(self, target):
        collections = self._collections()
        self._target(collections, target)
        kind, name = target.split(".", 1)
        lines = ["CLUSTER %s [%s]" % (target, kind)]
        if kind == "streaming":
            cluster = self.streaming_clusters[name]
            lines.append("  primary: %s" % cluster["primary"])
            for standby in cluster.get("standbys") or []:
                lines.append("  standby: %s (slot: %s)" % (
                    standby["instance"], standby.get("slot") or "auto"
                ))
        elif kind == "logical":
            link = self.logical_replications[name]
            lines.append("  pub: %s" % link["pub"]["streaming_cluster"])
            lines.append("  sub: %s" % link["sub"]["streaming_cluster"])
        elif kind == "citus":
            cluster = self.citus_clusters[name]
            lines.append("  coordinator: %s" % cluster["coordinator"]["streaming_cluster"])
            for worker_name, worker in cluster["workers"].items():
                lines.append("  worker %s: %s" % (worker_name, worker["streaming_cluster"]))
        else:
            cluster = self.mmr_clusters[name]
            for member_name, member in cluster["members"].items():
                lines.append("  member %s: %s" % (member_name, member["streaming_cluster"]))
        return "\n".join(lines)

    def host(self, name):
        if name not in self.hosts:
            raise ConfigError("未知 host: %s" % name)
        return self.hosts[name]

    def installation(self, name):
        if name not in self.installations:
            raise ConfigError("未知安装: %s" % name)
        return self.installations[name]

    def instance(self, name):
        if name not in self.instances:
            raise ConfigError("未知实例: %s" % name)
        value = dict(self.instances[name])
        value["name"] = name
        value["host_config"] = self.host(value["host"])
        value["installation_config"] = self.installation(value["installation"])
        return value

    def cluster_instances(self, name):
        """Return instance names in a streaming cluster, in primary order."""
        cluster = self.streaming_clusters.get(name)
        if cluster is None:
            raise ConfigError("未知流复制集群: %s" % name)
        return [cluster["primary"]] + [item["instance"] for item in cluster.get("standbys") or []]
