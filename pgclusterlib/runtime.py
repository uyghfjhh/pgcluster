import json
import time
from pathlib import Path

from .errors import OperationError, SafetyError
from .executor import Executor
from .sql import quote_ident, quote_literal

MARKER = ".pgcluster-managed"


class Runtime:
    """Lifecycle, topology verification, and replication primitives."""

    def __init__(self, config, executor=None, progress=None):
        self.config = config
        self.executor = executor or Executor()
        self.progress = progress

    def _progress(self, message):
        if self.progress:
            self.progress(message)

    def _state_path(self):
        return self.config.path.with_name(".%s.state.json" % self.config.path.name)

    def _state(self):
        path = self._state_path()
        if not path.exists():
            return {"primaries": {}}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise OperationError("无法读取运行状态 %s: %s" % (path, exc))
        if not isinstance(value, dict) or not isinstance(value.get("primaries", {}), dict):
            raise OperationError("运行状态文件格式无效: %s" % path)
        return value

    def _save_state(self, state):
        path = self._state_path()
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(json.dumps(state, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)

    def _primary(self, cluster_name):
        cluster = self.config.streaming_clusters[cluster_name]
        return self._state().get("primaries", {}).get(cluster_name, cluster["primary"])

    def _streaming_standbys(self, cluster_name):
        cluster = self.config.streaming_clusters[cluster_name]
        primary = self._primary(cluster_name)
        configured = [item["instance"] for item in cluster.get("standbys") or []]
        if primary == cluster["primary"]:
            return configured
        return [cluster["primary"]] + [item for item in configured if item != primary]

    def _host(self, instance):
        address = instance["host_config"]["address"]
        return "local" if self.executor.is_local(address) else address

    @staticmethod
    def _extension_controls(home, extension):
        return [
            home + "/share/extension/" + extension + ".control",
            home + "/share/postgresql/extension/" + extension + ".control",
        ]

    def _plugin_installed(self, host, home, plugin):
        extension = plugin.get("extension")
        if not extension:
            return True
        return any(self.executor.exists(host, control) for control in self._extension_controls(home, extension))

    def _installation_hosts(self, installation_name):
        hosts = []
        for name, instance in self.config.instances.items():
            if instance["installation"] != installation_name:
                continue
            host = self._host(self.config.instance(name))
            if host not in hosts:
                hosts.append(host)
        if not hosts:
            raise OperationError("安装 %s 没有被任何实例使用" % installation_name)
        return hosts

    def doctor_instance(self, name):
        instance = self.config.instance(name)
        host = self._host(instance)
        install = instance["installation_config"]
        home = install["home"]
        checks = {
            "data_dir": self.executor.is_dir(host, instance["data_dir"]),
            "postgres": self.executor.exists(host, home + "/bin/postgres"),
            "pg_ctl": self.executor.exists(host, home + "/bin/pg_ctl"),
            "initdb": self.executor.exists(host, home + "/bin/initdb"),
            "pg_basebackup": self.executor.exists(host, home + "/bin/pg_basebackup"),
            "psql": self.executor.exists(host, home + "/bin/psql"),
        }
        for plugin_name, plugin in (install.get("plugins") or {}).items():
            if plugin.get("extension"):
                checks["extension.%s" % plugin_name] = self._plugin_installed(host, home, plugin)
        license_config = install.get("license")
        if license_config:
            checks["license.source"] = self.executor.exists(host, license_config["source_file"])
            if checks["data_dir"]:
                checks["license.data"] = self.executor.exists(
                    host, str(Path(instance["data_dir"]) / license_config.get("data_file", "license.dat"))
                )
        return checks

    def install_installation(self, installation_name, force=False):
        install = self.config.installation(installation_name)
        home = install["home"]
        hosts = self._installation_hosts(installation_name)
        self._progress("检查安装 %s 的插件前置条件" % installation_name)
        messages = []
        for plugin_name, plugin in (install.get("plugins") or {}).items():
            extension = plugin.get("extension")
            if not extension:
                messages.append("%s: 无扩展文件，跳过" % plugin_name)
                continue
            for host in hosts:
                if self._plugin_installed(host, home, plugin) and not force:
                    self._progress("插件 %s@%s 已安装，跳过" % (plugin_name, host))
                    messages.append("%s@%s: 已安装" % (plugin_name, host))
                    continue
                self._progress("编译并安装插件 %s@%s" % (plugin_name, host))
                source = plugin.get("source_dir")
                if not source:
                    raise OperationError(
                        "插件 %s@%s 未安装，且未配置 source_dir；请先安装到 %s" %
                        (plugin_name, host, home)
                    )
                if not self.executor.is_dir(host, source):
                    raise OperationError("插件源码目录不存在: %s (%s)" % (source, host))
                pg_config = home + "/bin/pg_config"
                if not self.executor.exists(host, pg_config):
                    raise OperationError("缺少 pg_config: %s" % pg_config)
                make_args = ["make", "-C", source, "PG_CONFIG=" + pg_config]
                self.executor.run(make_args, host=host)
                self.executor.run(["sudo", "-n"] + make_args + ["install"], host=host)
                if not self._plugin_installed(host, home, plugin):
                    raise OperationError("插件安装后仍未找到扩展控制文件: %s" % plugin_name)
                messages.append("%s@%s: 已安装" % (plugin_name, host))
        return "安装完成: %s\n%s" % (installation_name, "\n".join(messages) if messages else "没有配置插件")

    def status_instance(self, name):
        instance = self.config.instance(name)
        host = self._host(instance)
        install = instance["installation_config"]
        result = self.executor.run(
            [install["home"] + "/bin/pg_ctl", "status", "-D", instance["data_dir"]],
            host=host, check=False,
        )
        return {
            "running": result.returncode == 0,
            # pg_ctl uses exit code 3 for a reachable, stopped instance.
            "known": result.returncode in {0, 3},
            "message": (result.stdout or result.stderr).strip(),
        }

    def status_cluster(self, name):
        return {instance: self.status_instance(instance) for instance in self.config.cluster_instances(name)}

    def target_instances(self, target):
        """Resolve a CLI target to instance names from the current configuration model."""
        if target in self.config.instances:
            return [target]
        if "." not in target:
            raise OperationError("未知目标: %s" % target)
        kind, name = target.split(".", 1)
        streams = []
        if kind == "streaming":
            streams = [name]
        elif kind == "logical":
            link = self.config.logical_replications.get(name)
            if link:
                streams = [link["pub"]["streaming_cluster"], link["sub"]["streaming_cluster"]]
        elif kind == "citus":
            cluster = self.config.citus_clusters.get(name)
            if cluster:
                streams = [cluster["coordinator"]["streaming_cluster"]]
                streams.extend(item["streaming_cluster"] for item in cluster["workers"].values())
        elif kind == "mmr":
            cluster = self.config.mmr_clusters.get(name)
            if cluster:
                streams = [item["streaming_cluster"] for item in cluster["members"].values()]
        else:
            raise OperationError("未知目标类型: %s" % kind)
        if not streams or any(stream not in self.config.streaming_clusters for stream in streams):
            raise OperationError("未知目标: %s" % target)
        result = []
        for stream in streams:
            for instance in self.config.cluster_instances(stream):
                if instance not in result:
                    result.append(instance)
        return result

    def status_target(self, target):
        return {name: self.status_instance(name) for name in self.target_instances(target)}

    def status_display(self, target):
        """Collect per-instance status while preserving individual probe errors."""
        states = {}
        for name in self.target_instances(target):
            try:
                state = self.status_instance(name)
                if not state["known"]:
                    state["running"] = None
                states[name] = state
            except OperationError as exc:
                states[name] = {"running": None, "message": str(exc)}
        return states

    def deployment_states(self, target=None):
        """Classify declared topology by the managed-data marker on each host."""
        targets = []
        collections = self.config._collections()
        if target:
            self._target_for_deployment(collections, target)
            targets = [target]
        else:
            for kind, values in collections.items():
                targets.extend("%s.%s" % (kind, name) for name in values)
        names = []
        for item in targets:
            for instance in self.target_instances(item):
                if instance not in names:
                    names.append(instance)
        instance_states = {}
        for name in names:
            instance = self.config.instance(name)
            result = self.executor.run(
                ["sh", "-c", 'test -d "$1" && test -e "$1/.pgcluster-managed"',
                 "sh", instance["data_dir"]],
                host=self._host(instance), check=False,
            )
            if result.returncode == 0:
                instance_states[name] = "已部署"
            elif result.stderr.strip():
                instance_states[name] = "未知"
            else:
                instance_states[name] = "未部署"

        states = {}
        for kind, values in collections.items():
            for name in values:
                target = "%s.%s" % (kind, name)
                if target not in targets:
                    continue
                members = [instance_states[item] for item in self.target_instances(target)]
                if all(item == "已部署" for item in members):
                    states[target] = "已部署"
                elif any(item == "未知" for item in members):
                    states[target] = "未知"
                elif any(item == "已部署" for item in members):
                    states[target] = "部分部署"
                else:
                    states[target] = "未部署"
        return states

    @staticmethod
    def _target_for_deployment(collections, target):
        if "." not in target:
            raise OperationError("未知目标: %s" % target)
        kind, name = target.split(".", 1)
        if kind not in collections or name not in collections[kind]:
            raise OperationError("未知目标: %s" % target)

    def start_instance(self, name):
        instance = self.config.instance(name)
        host = self._host(instance)
        install = instance["installation_config"]
        return self.executor.run(
            [install["home"] + "/bin/pg_ctl", "start", "-D", instance["data_dir"],
             "-l", str(Path(instance["data_dir"]) / "pg_ctl.log"), "-w"], host=host
        )

    def stop_instance(self, name):
        instance = self.config.instance(name)
        host = self._host(instance)
        install = instance["installation_config"]
        return self.executor.run(
            [install["home"] + "/bin/pg_ctl", "stop", "-D", instance["data_dir"], "-m", "fast", "-w"], host=host
        )

    def start_target(self, target):
        names = self.target_instances(target)
        self._progress("启动目标 %s" % target)
        for name in names:
            if not self._managed(name):
                raise SafetyError("实例不是 pgcluster 创建的，拒绝启动: %s" % name)
            if not self.status_instance(name)["running"]:
                self._progress("启动实例 %s" % name)
                self.start_instance(name)
            else:
                self._progress("实例 %s 已运行，跳过" % name)
        return "已启动: %s" % target

    def stop_target(self, target):
        names = self.target_instances(target)
        self._progress("停止目标 %s" % target)
        for name in names:
            if not self._managed(name):
                raise SafetyError("实例不是 pgcluster 创建的，拒绝停止: %s" % name)
        for name in reversed(names):
            if self.status_instance(name)["running"]:
                self._progress("停止实例 %s" % name)
                self.stop_instance(name)
            else:
                self._progress("实例 %s 已停止，跳过" % name)
        return "已停止: %s" % target

    def restart_target(self, target):
        self._progress("重启目标 %s" % target)
        self.stop_target(target)
        self.start_target(target)
        return "已重启: %s" % target

    def clean_target(self, target, yes=False):
        if not yes:
            raise SafetyError("clean 会删除 PGDATA；请使用 --yes")
        names = self.target_instances(target)
        self._progress("校验目标 %s 的受管数据目录" % target)
        for name in names:
            if not self._managed(name):
                raise SafetyError("实例不是 pgcluster 创建的，拒绝清理: %s" % name)

        self._progress("删除目标 %s 的复制元数据" % target)
        self._teardown_metadata(target)
        for name in reversed(names):
            if self.status_instance(name)["running"]:
                self._progress("停止实例 %s" % name)
                self.stop_instance(name)
        for name in names:
            instance = self.config.instance(name)
            self._progress("删除实例 %s 的数据目录" % name)
            self.executor.remove_tree(self._host(instance), instance["data_dir"])
        self._progress("清理运行状态记录")
        self._clear_state(names)
        return "已清理: %s" % target

    def _clear_state(self, instances):
        state = self._state()
        changed = False
        for cluster_name, primary in list(state.get("primaries", {}).items()):
            if primary in instances or cluster_name not in self.config.streaming_clusters:
                state["primaries"].pop(cluster_name, None)
                state.get("failovers", {}).pop(cluster_name, None)
                changed = True
        if changed:
            self._save_state(state)

    def _drop_physical_slots(self, streaming_name):
        cluster = self.config.streaming_clusters[streaming_name]
        primary = self._primary(streaming_name)
        # A physical slot cannot be removed while its receiver is connected.
        for standby in self._streaming_standbys(streaming_name):
            if standby in self.config.instances and self.status_instance(standby)["running"]:
                self.stop_instance(standby)
        for item in cluster.get("standbys") or []:
            slot = item.get("slot") or "%s_%s_slot" % (streaming_name, item["instance"])
            self._psql(primary, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                       "WHERE slot_name=%s AND NOT active" % quote_literal(slot), tuples=True)

    def _drop_logical(self, name):
        link = self.config.logical_replications[name]
        pub_cluster = link["pub"]["streaming_cluster"]
        sub_cluster = link["sub"]["streaming_cluster"]
        publisher = self._primary(pub_cluster)
        subscriber = self._primary(sub_cluster)
        database = link["sub"].get("database", link["pub"].get("database", "postgres"))
        subscription = "%s_sub" % name
        publication = "%s_pub" % name
        if self._psql(subscriber, "SELECT count(*) FROM pg_subscription WHERE subname=%s" % quote_literal(subscription), database, True) == "1":
            self._psql(subscriber, "ALTER SUBSCRIPTION %s DISABLE" % quote_ident(subscription), database)
            self._psql(subscriber, "DROP SUBSCRIPTION %s" % quote_ident(subscription), database)
        if self._psql(publisher, "SELECT count(*) FROM pg_publication WHERE pubname=%s" % quote_literal(publication), link["pub"].get("database", "postgres"), True) == "1":
            self._psql(publisher, "DROP PUBLICATION %s" % quote_ident(publication), link["pub"].get("database", "postgres"))
        slot = (link["sub"].get("slot") or {}).get("name") or "%s_slot" % name
        self._psql(publisher, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                   "WHERE slot_name=%s AND NOT active" % quote_literal(slot), link["pub"].get("database", "postgres"), True)

    def _drop_citus(self, name):
        cluster = self.config.citus_clusters[name]
        coordinator = self._primary(cluster["coordinator"]["streaming_cluster"])
        database = cluster.get("database", "postgres")
        # A Worker with shard placements cannot be removed first.  Dropping
        # the Coordinator extension with CASCADE removes distributed tables
        # and pg_dist_node metadata as one consistent operation.
        self._psql(coordinator, "DROP EXTENSION IF EXISTS citus CASCADE", database)
        streams = [cluster["coordinator"]["streaming_cluster"]]
        streams.extend(item["streaming_cluster"] for item in cluster["workers"].values())
        for streaming in streams:
            primary = self._primary(streaming)
            if primary != coordinator:
                self._psql(primary, "DROP EXTENSION IF EXISTS citus CASCADE", database)

    def _drop_mmr(self, name):
        cluster = self.config.mmr_clusters[name]
        database = cluster.get("database", "postgres")
        members = list(cluster["members"].values())
        control = self._primary(members[0]["streaming_cluster"])
        # FBase requires PARTED before DROP.  Part and drop non-control nodes
        # from the still-ACTIVE control member, then part/drop the control
        # member itself as the final group operation.
        for member in reversed(members[1:]):
            exists = self._psql(control, "SELECT count(*) FROM fdd.mmr_node WHERE node_name=%s" %
                                quote_literal(member["node_name"]), database, True)
            if exists == "1":
                self._psql(control, "SELECT fdd.part_node(%s,true,true)" % quote_literal(member["node_name"]), database)
                self._psql(control, "SELECT fdd.drop_node(%s,true)" % quote_literal(member["node_name"]), database)
        first = members[0]
        exists = self._psql(control, "SELECT count(*) FROM fdd.mmr_node WHERE node_name=%s" %
                            quote_literal(first["node_name"]), database, True)
        if exists == "1":
            self._psql(control, "SELECT fdd.part_node(%s,true,true)" % quote_literal(first["node_name"]), database)
            self._psql(control, "SELECT fdd.drop_node(%s,true)" % quote_literal(first["node_name"]), database)
        for member in members:
            node = self._primary(member["streaming_cluster"])
            # MMR creates subscriptions owned by the extension workflow.
            subscriptions = self._psql(node, "SELECT subname FROM pg_subscription WHERE subname LIKE 'fmmr_%'", database, True)
            for subscription in filter(None, subscriptions.splitlines()):
                self._psql(node, "ALTER SUBSCRIPTION %s DISABLE" % quote_ident(subscription), database)
                self._psql(node, "DROP SUBSCRIPTION %s" % quote_ident(subscription), database)
            for extension in ("fbase_mac", "fdd_mmr", "fb_license"):
                self._psql(node, "DROP EXTENSION IF EXISTS %s CASCADE" % quote_ident(extension), database)

    def _teardown_metadata(self, target):
        if "." not in target:
            return
        kind, name = target.split(".", 1)
        streams = []
        if kind == "logical":
            self._drop_logical(name)
            link = self.config.logical_replications[name]
            streams = [link["pub"]["streaming_cluster"], link["sub"]["streaming_cluster"]]
        elif kind == "citus":
            self._drop_citus(name)
            cluster = self.config.citus_clusters[name]
            streams = [cluster["coordinator"]["streaming_cluster"]]
            streams.extend(item["streaming_cluster"] for item in cluster["workers"].values())
        elif kind == "mmr":
            self._drop_mmr(name)
            streams = [item["streaming_cluster"] for item in self.config.mmr_clusters[name]["members"].values()]
        elif kind == "streaming":
            streams = [name]
        for streaming in streams:
            self._drop_physical_slots(streaming)

    def verify_target(self, target):
        names = self.target_instances(target)
        states = self.status_target(target)
        if target.startswith("streaming.") and target.split(".", 1)[1] in self._state().get("primaries", {}):
            effective = self._primary(target.split(".", 1)[1])
            if not states.get(effective, {}).get("running"):
                raise OperationError("切换后的主库未运行: %s" % effective)
        elif not all(item["running"] for item in states.values()):
            raise OperationError("存在未运行实例: %s" % target)
        if "." not in target:
            return "验证通过: %s" % target
        kind, name = target.split(".", 1)
        if kind == "streaming":
            cluster = self.config.streaming_clusters[name]
            primary = self._primary(name)
            expected = len(self._streaming_standbys(name))
            failover_info = self._state().get("failovers", {}).get(name, {})
            if failover_info and not failover_info.get("rejoined", False) and expected:
                # After a promotion the former primary is intentionally
                # stopped until `rejoin`; verify the promoted role itself.
                expected = 0
            actual = int(self._psql(primary, "SELECT count(*) FROM pg_stat_replication WHERE state='streaming'", tuples=True))
            if actual < expected:
                raise OperationError("流复制备库未全部 streaming: %s/%s" % (actual, expected))
        elif kind == "logical":
            link = self.config.logical_replications[name]
            sub = self._primary(link["sub"]["streaming_cluster"])
            state = self._psql(sub, "SELECT count(*) FROM pg_stat_subscription WHERE latest_end_lsn IS NOT NULL", link["sub"].get("database", "postgres"), True)
            if state != "1":
                raise OperationError("逻辑订阅尚未收到 WAL: %s" % name)
        elif kind == "citus":
            cluster = self.config.citus_clusters[name]
            coordinator = self._primary(cluster["coordinator"]["streaming_cluster"])
            expected = len(cluster["workers"])
            actual = int(self._psql(coordinator, "SELECT count(*) FROM pg_dist_node WHERE groupid > 0 AND isactive", cluster.get("database", "postgres"), True))
            if actual != expected:
                raise OperationError("Citus Worker 不完整: %s/%s" % (actual, expected))
        elif kind == "mmr":
            cluster = self.config.mmr_clusters[name]
            for member in cluster["members"].values():
                node = self._primary(member["streaming_cluster"])
                state = self._psql(node, "SELECT count(*) FROM fdd.mmr_node WHERE node_state='ACTIVE'", cluster.get("database", "postgres"), True)
                if int(state) < len(cluster["members"]):
                    raise OperationError("MMR 节点尚未全部 ACTIVE: %s" % name)
        return "验证通过: %s" % target

    def failover(self, target, yes=False, force=False):
        if not yes:
            raise SafetyError("failover 会改变主备角色；请使用 --yes")
        if not target.startswith("streaming."):
            raise OperationError("failover 当前只支持 streaming.<cluster>")
        cluster_name = target.split(".", 1)[1]
        if cluster_name not in self.config.streaming_clusters:
            raise OperationError("未知流复制集群: %s" % target)
        cluster = self.config.streaming_clusters[cluster_name]
        old_primary = self._primary(cluster_name)
        members = [cluster["primary"]] + [item["instance"] for item in cluster.get("standbys") or []]
        candidates = [item for item in members if item != old_primary]
        if len(candidates) != 1:
            raise OperationError("failover 要求恰好一个可提升备库: %s" % target)
        new_primary = candidates[0]
        self._progress("校验切换目标 %s (%s -> %s)" % (target, old_primary, new_primary))
        if not self._managed(old_primary) or not self._managed(new_primary):
            raise SafetyError("主库或备库不是 pgcluster 管理的数据目录")
        if not self.status_instance(new_primary)["running"]:
            raise OperationError("备库未运行，不能提升: %s" % new_primary)
        if self._psql(new_primary, "SELECT pg_is_in_recovery()", tuples=True) != "t":
            raise OperationError("目标实例不是备库: %s" % new_primary)
        if self.status_instance(old_primary)["running"]:
            instance = self.config.instance(old_primary)
            mode = "immediate" if force else "fast"
            self._progress("停止旧主库 %s" % old_primary)
            self.executor.run([self._bin(old_primary, "pg_ctl"), "stop", "-D", instance["data_dir"], "-m", mode, "-w"], host=self._host(instance))
        new_instance = self.config.instance(new_primary)
        self._progress("提升备库 %s 为主库" % new_primary)
        self.executor.run([self._bin(new_primary, "pg_ctl"), "promote", "-D", new_instance["data_dir"], "-w"], host=self._host(new_instance))
        self._progress("等待新主库 %s 完成提升" % new_primary)
        until = time.monotonic() + 30
        while time.monotonic() < until:
            if self._psql(new_primary, "SELECT pg_is_in_recovery()", tuples=True) == "f":
                state = self._state()
                state.setdefault("primaries", {})[cluster_name] = new_primary
                state.setdefault("failovers", {})[cluster_name] = {
                    "old_primary": old_primary, "new_primary": new_primary, "rejoined": False,
                }
                self._save_state(state)
                return "已切换: %s (%s -> %s)" % (target, old_primary, new_primary)
            time.sleep(.5)
        raise OperationError("提升后的实例未退出 recovery: %s" % new_primary)

    def rejoin(self, target, yes=False):
        if not yes:
            raise SafetyError("rejoin 会重建旧主库数据目录；请使用 --yes")
        if not target.startswith("streaming."):
            raise OperationError("rejoin 当前只支持 streaming.<cluster>")
        cluster_name = target.split(".", 1)[1]
        cluster = self.config.streaming_clusters.get(cluster_name)
        if not cluster:
            raise OperationError("未知流复制集群: %s" % target)
        state = self._state()
        new_primary = self._primary(cluster_name)
        old_primary = state.get("failovers", {}).get(cluster_name, {}).get("old_primary", cluster["primary"])
        if old_primary == new_primary:
            raise OperationError("没有待重新加入的旧主库: %s" % target)
        standby_spec = next((item for item in cluster.get("standbys") or [] if item["instance"] == new_primary), None)
        slot = (standby_spec or {}).get("slot") or "%s_%s_slot" % (cluster_name, old_primary)
        source = self.config.instance(new_primary)
        destination = self.config.instance(old_primary)
        self._progress("将旧主库 %s 重新加入 %s" % (old_primary, target))
        if self.status_instance(old_primary)["running"]:
            self._progress("停止旧主库 %s" % old_primary)
            self.stop_instance(old_primary)
        if not self._managed(old_primary):
            raise SafetyError("旧主库数据目录不是 pgcluster 管理目录")
        self._progress("删除旧主库 %s 的数据目录" % old_primary)
        self.executor.remove_tree(self._host(destination), destination["data_dir"])
        if self._psql(new_primary, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=%s" % quote_literal(slot), tuples=True) == "0":
            self._psql(new_primary, "SELECT pg_create_physical_replication_slot(%s)" % quote_literal(slot))
        self._progress("从新主库 %s 重建旧主库 %s" % (new_primary, old_primary))
        self.executor.run(["mkdir", "-p", str(Path(destination["data_dir"]).parent)], host=self._host(destination))
        self.executor.run([self._bin(old_primary, "pg_basebackup"), "-h", source["host_config"]["address"], "-p", str(source["port"]), "-U", "postgres", "-D", destination["data_dir"], "-R", "-X", "stream", "-S", slot], host=self._host(destination))
        self.executor.write_text(self._host(destination), self._marker(old_primary), json.dumps({"node": old_primary}) + "\n")
        self._write_config(old_primary, new_primary, slot)
        self._progress("启动重新加入的备库 %s" % old_primary)
        self.start_instance(old_primary)
        self._progress("等待备库 %s 就绪" % old_primary)
        self._wait(old_primary)
        state.setdefault("failovers", {}).setdefault(cluster_name, {})["rejoined"] = True
        self._save_state(state)
        return "已重新加入: %s (%s -> standby)" % (target, old_primary)

    def health_target(self, target):
        """Return machine-readable health details for status/monitoring callers."""
        result = self.status_target(target)
        if "." in target:
            try:
                self.verify_target(target)
                result["health"] = "ok"
            except OperationError as exc:
                result["health"] = "failed"
                result["reason"] = str(exc)
        else:
            result["health"] = "ok" if all(item["running"] for item in result.values()) else "failed"
        return result

    def lag_target(self, target):
        """Return replication-specific details for an already-created target."""
        if "." not in target:
            raise OperationError("lag 需要集群目标，例如 streaming.basic_cluster")
        kind, name = target.split(".", 1)
        rows = []
        if kind == "streaming":
            cluster = self.config.streaming_clusters.get(name)
            if not cluster:
                raise OperationError("未知目标: %s" % target)
            primary = self._primary(name)
            rows.append({"cluster": target, "role": "primary", "instance": primary})
            sql = ("SELECT application_name||'|'||state||'|'||"
                   "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::text,'-1') "
                   "FROM pg_stat_replication ORDER BY application_name")
            for line in filter(None, self._psql(primary, sql, tuples=True).splitlines()):
                app, state, lag = line.split("|", 2)
                rows.append({"cluster": target, "role": "standby", "application": app, "state": state, "lag_bytes": int(float(lag)) if lag != "-1" else None})
        elif kind == "logical":
            link = self.config.logical_replications.get(name)
            if not link:
                raise OperationError("未知目标: %s" % target)
            sub = self._primary(link["sub"]["streaming_cluster"])
            sql = "SELECT subname||'|'||COALESCE(received_lsn::text,'')||'|'||COALESCE(latest_end_lsn::text,'') FROM pg_stat_subscription"
            for line in filter(None, self._psql(sub, sql, link["sub"].get("database", "postgres"), True).splitlines()):
                subscription, received, latest = line.split("|", 2)
                rows.append({"cluster": target, "subscription": subscription, "received_lsn": received, "latest_end_lsn": latest})
        elif kind == "citus":
            cluster = self.config.citus_clusters.get(name)
            if not cluster:
                raise OperationError("未知目标: %s" % target)
            coordinator = self._primary(cluster["coordinator"]["streaming_cluster"])
            sql = "SELECT nodename||'|'||nodeport||'|'||isactive FROM pg_dist_node WHERE groupid > 0 ORDER BY nodeport"
            for line in filter(None, self._psql(coordinator, sql, cluster.get("database", "postgres"), True).splitlines()):
                host, port, active = line.split("|")
                rows.append({"cluster": target, "worker": "%s:%s" % (host, port), "active": active.lower() in {"t", "true", "1"}})
        elif kind == "mmr":
            cluster = self.config.mmr_clusters.get(name)
            if not cluster:
                raise OperationError("未知目标: %s" % target)
            for member_name, member in cluster["members"].items():
                node = self._primary(member["streaming_cluster"])
                state = self._psql(node, "SELECT node_name||'|'||node_state FROM fdd.mmr_node ORDER BY node_id", cluster.get("database", "postgres"), True)
                rows.append({"cluster": target, "member": member_name, "nodes": state.splitlines()})
        else:
            raise OperationError("未知目标类型: %s" % kind)
        return rows

    def _bin(self, name, command):
        return self.config.instance(name)["installation_config"]["home"] + "/bin/" + command

    def _psql(self, name, sql, database="postgres", tuples=False):
        instance = self.config.instance(name)
        args = [self._bin(name, "psql"), "-X", "-v", "ON_ERROR_STOP=1", "-h",
                instance["host_config"]["address"], "-p", str(instance["port"]), "-U", "postgres", "-d", database]
        if tuples:
            args.append("-At")
        args += ["-c", sql]
        return self.executor.run(args, host=self._host(instance)).stdout.strip()

    def _marker(self, name):
        return str(Path(self.config.instance(name)["data_dir"]) / MARKER)

    def _managed(self, name):
        instance = self.config.instance(name)
        host = self._host(instance)
        if not self.executor.is_nonempty_dir(host, instance["data_dir"]):
            return False
        marker = self._marker(name)
        if not self.executor.exists(host, marker):
            raise SafetyError("拒绝操作非 pgcluster 创建的数据目录: %s" % instance["data_dir"])
        return True

    def _write_config(self, name, primary=None, slot=None, extra=None):
        instance = self.config.instance(name)
        host, data_dir = self._host(instance), instance["data_dir"]
        params = dict((self.config.raw.get("postgresql_config") or {}).get("parameters") or {})
        params.update(extra or {})
        params["port"] = instance["port"]
        params["listen_addresses"] = "*"
        if primary:
            parent = self.config.instance(primary)
            params["primary_conninfo"] = "host=%s port=%s user=postgres application_name=%s" % (parent["host_config"]["address"], parent["port"], name)
            params["primary_slot_name"] = slot
        lines = ["# Managed by pgcluster"]
        for key, value in sorted(params.items()):
            if isinstance(value, list):
                value = ",".join(value)
            lines.append("%s = %s" % (key, quote_literal(value) if isinstance(value, str) else value))
        self.executor.write_text(host, str(Path(data_dir) / "pgcluster.conf"), "\n".join(lines) + "\n")
        postgres_conf = str(Path(data_dir) / "postgresql.conf")
        existing_conf = self.executor.read_text(host, postgres_conf)
        if "include_if_exists = 'pgcluster.conf'" not in existing_conf:
            self.executor.append_text(host, postgres_conf, "\ninclude_if_exists = 'pgcluster.conf'\n")
        hba = "local all all trust\nhost all all 0.0.0.0/0 trust\nhost replication all 0.0.0.0/0 trust\n"
        self.executor.write_text(host, str(Path(data_dir) / "pg_hba.conf"), hba)

    def _init_primary(self, name, extra=None):
        instance = self.config.instance(name)
        host, data_dir = self._host(instance), instance["data_dir"]
        if self.executor.is_nonempty_dir(host, data_dir):
            self._managed(name)
            return
        self.executor.run(["mkdir", "-p", str(Path(data_dir).parent)], host=host)
        self.executor.run([self._bin(name, "initdb"), "-D", data_dir, "-U", "postgres", "--auth-local=trust", "--auth-host=trust"], host=host)
        license_config = instance["installation_config"].get("license")
        if license_config:
            source = license_config["source_file"]
            target = str(Path(data_dir) / license_config.get("data_file", "license.dat"))
            self.executor.run(["cp", source, target], host=host)
        self.executor.write_text(host, self._marker(name), json.dumps({"node": name}) + "\n")
        self._write_config(name, extra=extra)
        self.start_instance(name)

    def _wait(self, name, seconds=30):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            try:
                if self._psql(name, "SELECT 1", tuples=True) == "1":
                    return
            except OperationError:
                pass
            time.sleep(.5)
        raise OperationError("实例未在 %s 秒内就绪: %s" % (seconds, name))

    def create_streaming(self, cluster_name, extra=None):
        cluster = self.config.streaming_clusters[cluster_name]
        primary = self._primary(cluster_name)
        self._progress("创建流复制集群 streaming.%s" % cluster_name)
        self._progress("初始化主库 %s" % primary)
        self._init_primary(primary, extra=extra)
        self._progress("等待主库 %s 就绪" % primary)
        self._wait(primary)
        for standby in cluster.get("standbys") or []:
            name = standby["instance"]
            if name == primary:
                continue
            self._progress("配置备库 %s" % name)
            slot = standby.get("slot") or "%s_%s_slot" % (cluster_name, name)
            instance, host = self.config.instance(name), self._host(self.config.instance(name))
            exists = self._psql(primary, "SELECT slot_type FROM pg_replication_slots WHERE slot_name=%s" % quote_literal(slot), tuples=True)
            if not exists:
                self._psql(primary, "SELECT pg_create_physical_replication_slot(%s)" % quote_literal(slot))
            if not self._managed(name):
                self.executor.run(["mkdir", "-p", str(Path(instance["data_dir"]).parent)], host=host)
                p = self.config.instance(primary)
                self.executor.run([self._bin(name, "pg_basebackup"), "-h", p["host_config"]["address"], "-p", str(p["port"]), "-U", "postgres", "-D", instance["data_dir"], "-R", "-X", "stream", "-S", slot], host=host)
                self.executor.write_text(host, self._marker(name), json.dumps({"node": name}) + "\n")
            self._write_config(name, primary, slot, extra)
            if not self.status_instance(name)["running"]:
                self.start_instance(name)
            self._progress("等待备库 %s 就绪" % name)
            self._wait(name)
        self._progress("流复制集群 streaming.%s 已就绪" % cluster_name)
        return "创建完成: streaming.%s" % cluster_name

    def create_logical(self, name):
        link = self.config.logical_replications[name]
        pub_cluster = link["pub"]["streaming_cluster"]
        sub_cluster = link["sub"]["streaming_cluster"]
        self._progress("创建逻辑复制 logical.%s" % name)
        self.create_streaming(pub_cluster, {"wal_level": "logical", "max_replication_slots": 16,
                                            "max_wal_senders": 16, "max_logical_replication_workers": 8})
        self.create_streaming(sub_cluster, {"wal_level": "logical", "max_replication_slots": 16,
                                            "max_wal_senders": 16, "max_logical_replication_workers": 8})
        publisher = self._primary(pub_cluster)
        subscriber = self._primary(sub_cluster)
        database = link["pub"].get("database", "postgres")
        publication, subscription = "%s_pub" % name, "%s_sub" % name
        slot = (link["sub"].get("slot") or {}).get("name") or "%s_slot" % name
        self._progress("创建 publication %s" % publication)
        if self._psql(publisher, "SELECT count(*) FROM pg_publication WHERE pubname=%s" % quote_literal(publication), database, True) != "1":
            self._psql(publisher, "CREATE PUBLICATION %s FOR ALL TABLES" % quote_ident(publication), database)
        p = self.config.instance(publisher)
        conn = "host=%s port=%s user=postgres dbname=%s application_name=%s" % (p["host_config"]["address"], p["port"], database, subscription)
        exists = self._psql(subscriber, "SELECT count(*) FROM pg_subscription WHERE subname=%s" % quote_literal(subscription), database, True)
        self._progress("创建或启用 subscription %s" % subscription)
        if exists != "1":
            failover = "true" if (link["sub"].get("slot") or {}).get("failover", False) else "false"
            self._psql(subscriber, "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (slot_name=%s, failover=%s)" %
                       (quote_ident(subscription), quote_literal(conn), quote_ident(publication), quote_literal(slot), failover), database)
        else:
            self._psql(subscriber, "ALTER SUBSCRIPTION %s CONNECTION %s; ALTER SUBSCRIPTION %s ENABLE" %
                       (quote_ident(subscription), quote_literal(conn), quote_ident(subscription)), database)
        return "创建完成: logical.%s" % name

    def create_citus(self, name):
        cluster = self.config.citus_clusters[name]
        names = [cluster["coordinator"]["streaming_cluster"]] + [v["streaming_cluster"] for v in cluster["workers"].values()]
        self._progress("创建 Citus 集群 citus.%s" % name)
        self._progress("检查 Citus 扩展前置条件")
        for streaming in names:
            primary = self._primary(streaming)
            instance = self.config.instance(primary)
            home = instance["installation_config"]["home"]
            controls = [home + "/share/extension/citus.control",
                        home + "/share/postgresql/extension/citus.control"]
            if not any(self.executor.exists(self._host(instance), item) for item in controls):
                raise OperationError("Citus 环境缺失: %s" % " 或 ".join(controls))
        extra = cluster.get("postgresql_config", {}).get("parameters", {})
        for streaming in names:
            self.create_streaming(streaming, extra)
        database = cluster.get("database", "postgres")
        coordinator = self._primary(names[0])
        self._progress("启用 Citus 扩展")
        for streaming in names:
            primary = self._primary(streaming)
            self._psql(primary, "CREATE EXTENSION IF NOT EXISTS citus", database)
        for worker in cluster["workers"].values():
            node = self.config.instance(self._primary(worker["streaming_cluster"]))
            exists = self._psql(coordinator, "SELECT count(*) FROM pg_dist_node WHERE nodename=%s AND nodeport=%s" %
                                (quote_literal(node["host_config"]["address"]), node["port"]), database, True)
            if exists == "0":
                self._progress("向 Coordinator 注册 Worker %s:%s" %
                               (node["host_config"]["address"], node["port"]))
                self._psql(coordinator, "SELECT citus_add_node(%s,%s)" %
                           (quote_literal(node["host_config"]["address"]), node["port"]), database)
        return "创建完成: citus.%s" % name

    def create_mmr(self, name):
        cluster = self.config.mmr_clusters[name]
        extra = cluster.get("postgresql_config", {}).get("parameters", {})
        members = list(cluster["members"].items())
        self._progress("创建 MMR 集群 mmr.%s" % name)
        for _, member in members:
            self.create_streaming(member["streaming_cluster"], extra)
        database = cluster.get("database", "postgres")
        first_name, first = members[0]
        first_node = self._primary(first["streaming_cluster"])
        first_instance = self.config.instance(first_node)
        first_dsn = "host=%s port=%s dbname=%s user=postgres" % (first_instance["host_config"]["address"], first_instance["port"], database)
        # fbase_mac protects its own catalog tables from TRUNCATE.  Creating it
        # before join_group makes the initial table-copy worker fail while it
        # prepares those tables.  Build the MMR group first, then enable the
        # security extension on the converged members.
        bootstrap_extensions = [item for item in cluster["extensions"] if item != "fbase_mac"]
        deferred_extensions = [item for item in cluster["extensions"] if item == "fbase_mac"]
        for _, member in members:
            node = self._primary(member["streaming_cluster"])
            self._progress("配置 MMR 成员 %s" % member["node_name"])
            for extension in bootstrap_extensions:
                self._psql(node, "CREATE EXTENSION IF NOT EXISTS %s" % quote_ident(extension), database)
            options = member.get("mmr_node") or {}
            instance = self.config.instance(node)
            dsn = "host=%s port=%s dbname=%s user=postgres" % (instance["host_config"]["address"], instance["port"], database)
            exists = self._psql(node, "SELECT count(*) FROM fdd.mmr_node WHERE node_name=%s" % quote_literal(member["node_name"]), database, True)
            if exists == "0":
                self._psql(node, "SELECT fdd.create_node(%s,%s,%s,%s,%s)" % (quote_literal(member["node_name"]), quote_literal(dsn), "true" if options.get("failover_slot", True) else "false", quote_literal(options.get("streaming", "off")), "true" if options.get("two_phase", False) else "false"), database)
        if self._psql(first_node, "SELECT count(*) FROM fdd.mmr_group WHERE group_name=%s" % quote_literal(cluster["group_name"]), database, True) == "0":
            self._progress("创建 MMR 组 %s" % cluster["group_name"])
            self._psql(first_node, "SELECT fdd.create_group(%s)" % quote_literal(cluster["group_name"]), database)
        self._psql(first_node, "CREATE SCHEMA IF NOT EXISTS pgcluster; "
                   "CREATE TABLE IF NOT EXISTS pgcluster.mmr_probe "
                   "(node_name text PRIMARY KEY, token text NOT NULL)", database)
        for _, member in members[1:]:
            node = self._primary(member["streaming_cluster"])
            join = member.get("join") or {}
            if self._psql(node, "SELECT count(*) FROM fdd.mmr_group WHERE group_name=%s" % quote_literal(cluster["group_name"]), database, True) == "0":
                self._progress("将成员 %s 加入 MMR 组" % member["node_name"])
                self._psql(node, "SELECT fdd.join_group(%s,%s,%s,%s,%s)" % (quote_literal(cluster["group_name"]), quote_literal(first_dsn), "true" if join.get("wait_for_completion", True) else "false", quote_literal(join.get("synchronize_structure", "all")), quote_literal(join.get("precheck", "table_exist_error"))), database)
        for _, member in members:
            node = self._primary(member["streaming_cluster"])
            for extension in deferred_extensions:
                self._psql(node, "CREATE EXTENSION IF NOT EXISTS %s" % quote_ident(extension), database)
        return "创建完成: mmr.%s" % name
