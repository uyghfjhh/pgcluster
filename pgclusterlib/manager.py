import json
import re
import time
import uuid
from pathlib import Path

from .errors import OperationError, SafetyError
from .executor import Executor, OperationLog
from .health import HealthChecker
from .providers import provider_for
from .sql import quote_ident, quote_literal


MARKER = ".pgcluster-managed"


class Manager:
    def __init__(self, config):
        self.config = config
        self.log = None
        self.executor = Executor()
        self.provider = provider_for(config)
        self.last_status_ok = None

    def open_log(self):
        self.log = OperationLog(self.config.operation_log)
        self.executor.log = self.log

    def close_log(self):
        if self.log:
            self.log.close()
            self.log = None
            self.executor.log = None

    def _host_address(self, node_name):
        return self.config.hosts[self.config.node(node_name)["host"]]

    def _is_local(self, address):
        return self.executor.is_local(address)

    def run(self, args, host="local", cwd=None, check=True, stdin=None):
        return self.executor.run(args, host=host, cwd=cwd, check=check, stdin=stdin)

    def psql(self, node_name, sql, database="postgres", tuples=False):
        node = self.config.node(node_name)
        args = [self.config.binary("psql"), "-X", "-v", "ON_ERROR_STOP=1", "-h", self._host_address(node_name),
                "-p", str(node["port"]), "-U", "postgres", "-d", database]
        if tuples:
            args += ["-At"]
        args += ["-c", sql]
        return self.run(args, host=self._host_address(node_name)).stdout.strip()

    def validate(self, target):
        if target not in self.config.clusters:
            raise OperationError("未知集群: %s" % target)
        self.config.closure(target)
        return "配置有效: %s" % target

    def _require_supported_features(self, target):
        self.provider.require_available()
        if self.config.extensions:
            raise OperationError("FBase 等保插件 provider 尚未实现")
        if any(
            self.config.cluster(name)["type"] == "mmr"
            for name in self.config.closure(target)
        ):
            raise OperationError("MMR provider 尚未实现")

    def graph(self, target):
        order = self.config.topology.action_order(target)
        root = self.config.cluster(target)
        lines = ["CLUSTER %s [%s]" % (target, root["type"])]

        def streaming_lines(name, prefix="", last=True, include_name=True):
            cluster = self.config.cluster(name)
            if include_name:
                branch = "└──" if last else "├──"
                lines.append("%s%s %s [streaming]" % (prefix, branch, name))
                prefix += "    " if last else "│   "
            members = [("primary", cluster["primary"])]
            members.extend(("standby", standby["node"]) for standby in cluster.get("standbys") or [])
            for index, (role, node) in enumerate(members):
                node_last = index == len(members) - 1
                branch = "└──" if node_last else "├──"
                suffix = ""
                if role == "standby":
                    standby = (cluster.get("standbys") or [])[index - 1]
                    suffix = " (slot: %s)" % self.config.physical_slot(name, standby)
                lines.append("%s%s %s: %s%s" % (prefix, branch, role, node, suffix))

        if root["type"] == "mmr":
            lines.append("└── provider: not implemented")
        elif root["type"] == "streaming":
            streaming_lines(target, include_name=False)
        else:
            for index, field in enumerate(("publisher", "subscriber")):
                last = index == 1
                streaming = self.config.logical_streaming(target, field)
                if streaming:
                    streaming_lines(streaming, last=last)
                else:
                    node = self.config.logical_node(target, field)
                    lines.append("%s %s: %s" % ("└──" if last else "├──", field, node))
        lines.append("")
        lines.append("DEPENDENCIES")
        lines.extend("%s -> %s" % edge for edge in self.config.topology.edges(target))
        lines.append("")
        lines.append("CREATE ORDER")
        lines.extend(order)
        lines.append("CLEAN ORDER")
        lines.extend(reversed(order))
        return "\n".join(lines)

    def doctor(self, target):
        self.validate(target)
        self._require_supported_features(target)
        binaries = ("postgres", "initdb", "pg_ctl", "psql", "pg_basebackup", "pg_isready")
        needs_failover = any(
            self.config.cluster(name).get("type") == "logical" and
            self.config.cluster(name).get("failover", False)
            for name in self.config.closure(target)
        )
        checked_hosts = set()
        for node_name in self.config.topology.nodes(target):
            node = self.config.node(node_name)
            address = self._host_address(node_name)
            if address not in checked_hosts:
                if not self._is_local(address):
                    self.run(["true"], host=address)
                for binary in binaries:
                    path = self.config.binary(binary)
                    if self.run(["test", "-x", path], host=address, check=False).returncode:
                        raise OperationError("主机 %s 缺少可执行文件: %s" % (address, path))
                if needs_failover:
                    version = self.run([self.config.binary("postgres"), "--version"], host=address).stdout
                    match = re.search(r"(\d+)(?:\.\d+)?", version)
                    if not match or int(match.group(1)) < 17:
                        raise OperationError(
                            "主机 %s 的 PostgreSQL 不支持原生故障转移槽: %s" %
                            (address, version.strip())
                        )
                for command in ("sh", "install", "find", "readlink", "cat", "ss"):
                    if self.run(["sh", "-c", "command -v \"$1\" >/dev/null", "sh", command],
                                host=address, check=False).returncode:
                        raise OperationError("主机 %s 缺少命令: %s" % (address, command))
                checked_hosts.add(address)
            managed = False
            if self.executor.is_nonempty_dir(address, node["data_dir"]):
                try:
                    self._require_marker(node_name)
                    managed = True
                except SafetyError:
                    raise SafetyError("PGDATA 已被占用且不属于 pgcluster: %s" % node["data_dir"])
            probe = self.run(
                ["sh", "-c", '! ss -ltn "sport = :$1" | grep -q LISTEN', "sh", str(node["port"])],
                host=address,
                check=False,
            )
            if probe.returncode and not managed:
                raise OperationError("端口已被占用: %s:%s" % (address, node["port"]))
            if probe.returncode and managed and not self._is_running(node_name):
                raise OperationError(
                    "端口已被其他进程占用，和 marker 指向的实例不一致: %s:%s" %
                    (address, node["port"])
                )
            if not managed:
                parent = str(Path(node["data_dir"]).parent)
                writable = self.run(
                    ["sh", "-c",
                     'p=$1; while test ! -e "$p"; do p=${p%/*}; test -n "$p" || p=/; done; '
                     'test -d "$p" && test -w "$p"',
                     "sh", parent],
                    host=address,
                    check=False,
                )
                if writable.returncode and self.run(
                    ["sudo", "-n", "true"], host=address, check=False
                ).returncode:
                    raise OperationError(
                        "无法创建 PGDATA 父目录且 sudo 不可用: %s:%s" %
                        (address, parent)
                    )
        return "运行环境可用: %s" % target

    def _marker_path(self, node_name):
        return str(Path(self.config.node(node_name)["data_dir"]) / MARKER)

    def _write_marker(self, node_name):
        data_dir = self.config.node(node_name)["data_dir"]
        address = self._host_address(node_name)
        payload = {
            "version": 1,
            "node": node_name,
            "data_dir": self.executor.realpath(address, data_dir),
            "created_by": "pgcluster",
        }
        self.executor.write_text(
            address,
            self._marker_path(node_name),
            json.dumps(payload, sort_keys=True) + "\n",
        )

    def _require_marker(self, node_name):
        data_dir = self.config.node(node_name)["data_dir"]
        address = self._host_address(node_name)
        marker = self._marker_path(node_name)
        is_file = self.run(["test", "-f", marker], host=address, check=False).returncode == 0
        is_link = self.run(["test", "-L", marker], host=address, check=False).returncode == 0
        if is_link or not is_file:
            raise SafetyError("拒绝操作没有 pgcluster marker 的目录: %s" % data_dir)
        try:
            payload = json.loads(self.executor.read_text(address, marker))
        except (OSError, ValueError) as exc:
            raise SafetyError("marker 无效: %s" % exc)
        expected = {
            "version": 1,
            "node": node_name,
            "data_dir": self.executor.realpath(address, data_dir),
            "created_by": "pgcluster",
        }
        if payload != expected:
            raise SafetyError("marker 与节点配置不匹配: %s" % data_dir)

    def _nodes_for_streaming(self, cluster_name):
        cluster = self.config.cluster(cluster_name)
        return [cluster["primary"]] + [item["node"] for item in cluster.get("standbys") or []]

    def _nodes_for_target(self, target):
        return self.config.topology.nodes(target)

    def _write_files(
        self,
        node_name,
        publisher=False,
        standby_slot=None,
        primary_name=None,
        sync_slots=False,
    ):
        node = self.config.node(node_name)
        data_dir = node["data_dir"]
        address = self._host_address(node_name)
        settings = {
            "port": int(node["port"]), "listen_addresses": "'*'", "logging_collector": "on",
            "log_destination": "'stderr'", "log_directory": "'log'", "log_filename": "'postgresql-%Y-%m-%d_%H%M%S.log'",
            "log_file_mode": "0600", "log_rotation_age": "'1d'", "log_rotation_size": "'100MB'",
            "log_line_prefix": "'%m [%p] user=%u db=%d app=%a client=%r '", "log_error_verbosity": "verbose",
            "log_statement": "'all'", "log_duration": "on", "log_connections": "on", "log_disconnections": "on",
            "log_checkpoints": "on", "log_replication_commands": "on",
            "max_wal_senders": 10, "max_replication_slots": 10,
            "max_logical_replication_workers": 4,
        }
        if publisher:
            settings.update({"wal_level": "logical", "hot_standby_feedback": "on"})
        if sync_slots:
            settings.update({"sync_replication_slots": "on", "hot_standby_feedback": "on"})
        if primary_name:
            primary = self.config.node(primary_name)
            settings["primary_conninfo"] = quote_literal(
                "host=%s port=%s user=postgres dbname=postgres application_name=%s" %
                (self._host_address(primary_name), primary["port"], node_name))
            settings["primary_slot_name"] = quote_literal(standby_slot)
        lines = ["# Managed by pgcluster"] + ["%s = %s" % item for item in sorted(settings.items())]
        managed_conf = str(Path(data_dir) / "pgcluster.conf")
        postgres_conf = str(Path(data_dir) / "postgresql.conf")
        managed_content = "\n".join(lines) + "\n"
        old_managed = (
            self.executor.read_text(address, managed_conf)
            if self.executor.exists(address, managed_conf) else None
        )
        changed = old_managed != managed_content
        if changed:
            self.executor.write_text(address, managed_conf, managed_content)
        include = "include_if_exists = 'pgcluster.conf'"
        existing = self.executor.read_text(address, postgres_conf)
        if include not in existing:
            self.executor.append_text(address, postgres_conf, "\n%s\n" % include)
        hba_path = str(Path(data_dir) / "pg_hba.conf")
        hba_content = (
            "local all all trust\n"
            "host all all 0.0.0.0/0 trust\n"
            "host all all ::/0 trust\n"
            "host replication all 0.0.0.0/0 trust\n"
            "host replication all ::/0 trust\n"
        )
        old_hba = self.executor.read_text(address, hba_path) if self.executor.exists(address, hba_path) else None
        if old_hba != hba_content:
            self.executor.write_text(address, hba_path, hba_content)
        return changed

    def _init_primary(self, node_name, publisher):
        node = self.config.node(node_name)
        data_dir = node["data_dir"]
        address = self._host_address(node_name)
        if self.executor.is_nonempty_dir(address, data_dir):
            raise SafetyError("data_dir 已存在且非空，先 clean: %s" % data_dir)
        self._ensure_data_parent(node_name)
        self.run(
            [self.config.binary("initdb"), "-D", data_dir, "-U", "postgres", "--auth-local=trust", "--auth-host=trust"],
            host=address,
        )
        self._write_marker(node_name)
        self._write_files(node_name, publisher=publisher)
        self._start(node_name)

    def _is_running(self, node_name):
        node = self.config.node(node_name)
        return self.run(
            [self.config.binary("pg_ctl"), "-D", node["data_dir"], "status"],
            host=self._host_address(node_name),
            check=False,
        ).returncode == 0

    def _managed_data_exists(self, node_name):
        node = self.config.node(node_name)
        address = self._host_address(node_name)
        if not self.executor.exists(address, node["data_dir"]):
            return False
        if not self.executor.is_nonempty_dir(address, node["data_dir"]):
            return False
        self._require_marker(node_name)
        return True

    def _ensure_primary(self, node_name, publisher):
        if not self._managed_data_exists(node_name):
            self._init_primary(node_name, publisher)
            return
        running = self._is_running(node_name)
        changed = self._write_files(node_name, publisher=publisher)
        if not running:
            self._start(node_name)
        elif changed:
            self._restart(node_name)

    def _write_recovery_settings(self, standby_name, primary_name, slot):
        """Override pg_basebackup -R's conninfo with dbname required by slot sync."""
        data_dir = self.config.node(standby_name)["data_dir"]
        address = self._host_address(standby_name)
        primary = self.config.node(primary_name)
        conninfo = "host=%s port=%s user=postgres dbname=postgres application_name=%s" % (
            self._host_address(primary_name), primary["port"], standby_name)
        self.executor.append_text(
            address,
            str(Path(data_dir) / "postgresql.auto.conf"),
            "primary_conninfo = %s\nprimary_slot_name = %s\n" %
            (quote_literal(conninfo), quote_literal(slot)),
        )

    def _start(self, node_name):
        node = self.config.node(node_name)
        self.run([self.config.binary("pg_ctl"), "-D", node["data_dir"], "-l",
                  str(Path(node["data_dir"]) / "pg_ctl.log"), "-w", "start"],
                 host=self._host_address(node_name))
        self._wait(node_name)

    def _restart(self, node_name):
        node = self.config.node(node_name)
        self.run(
            [self.config.binary("pg_ctl"), "-D", node["data_dir"], "-l",
             str(Path(node["data_dir"]) / "pg_ctl.log"), "-m", "fast", "-w", "restart"],
            host=self._host_address(node_name),
        )
        self._wait(node_name)

    def _wait(self, node_name, seconds=30):
        until = time.monotonic() + seconds
        error = ""
        while time.monotonic() < until:
            result = self.run(
                [self.config.binary("psql"), "-X", "-h", self._host_address(node_name), "-p",
                 str(self.config.node(node_name)["port"]), "-U", "postgres", "-d", "postgres",
                 "-Atqc", "SELECT 1"],
                host=self._host_address(node_name),
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip() == "1":
                return
            error = result.stderr
            time.sleep(0.5)
        raise OperationError("PostgreSQL 未在 %s 秒内启动: %s" % (seconds, error.strip()))

    def _ensure_physical_slot(self, primary, slot):
        slot_type = self.psql(
            primary,
            "SELECT slot_type FROM pg_replication_slots WHERE slot_name = %s" %
            quote_literal(slot),
            tuples=True,
        )
        if slot_type and slot_type != "physical":
            raise OperationError("复制槽 %s 已存在但不是物理槽: %s" % (slot, slot_type))
        if not slot_type:
            self.psql(primary, "SELECT pg_create_physical_replication_slot(%s)" % quote_literal(slot))

    def _reset_inherited_primary_settings(self, standby):
        # pg_basebackup copies postgresql.auto.conf from the primary.  These
        # settings describe the old primary's downstream standbys and must not
        # survive on a promoted copy.
        self.psql(standby, "ALTER SYSTEM RESET synchronized_standby_slots")
        self.psql(standby, "SELECT pg_reload_conf()")

    def _create_streaming(self, cluster_name, publisher, sync_slots=False):
        cluster = self.config.cluster(cluster_name)
        primary = cluster["primary"]
        self._ensure_primary(primary, publisher)
        standby_slots = [
            self.config.physical_slot(cluster_name, standby)
            for standby in cluster.get("standbys") or []
        ]
        if sync_slots and standby_slots:
            self.psql(
                primary,
                "ALTER SYSTEM SET synchronized_standby_slots = %s" %
                quote_literal(",".join(standby_slots)),
            )
            self.psql(primary, "SELECT pg_reload_conf()")
        else:
            self.psql(primary, "ALTER SYSTEM RESET synchronized_standby_slots")
            self.psql(primary, "SELECT pg_reload_conf()")
        for standby in cluster.get("standbys") or []:
            standby_name = standby["node"]
            slot = self.config.physical_slot(cluster_name, standby)
            standby_address = self._host_address(standby_name)
            self._ensure_physical_slot(primary, slot)
            if self._managed_data_exists(standby_name):
                running = self._is_running(standby_name)
                changed = self._write_files(
                    standby_name,
                    publisher=publisher,
                    standby_slot=slot,
                    primary_name=primary,
                    sync_slots=sync_slots,
                )
                if not running:
                    self._start(standby_name)
                elif changed:
                    self._restart(standby_name)
                self._reset_inherited_primary_settings(standby_name)
                continue
            data_dir = self.config.node(standby_name)["data_dir"]
            if self.executor.is_nonempty_dir(standby_address, data_dir):
                raise SafetyError("data_dir 已存在且非空，先 clean: %s" % data_dir)
            self._ensure_data_parent(standby_name)
            self.run([self.config.binary("pg_basebackup"), "-h", self._host_address(primary), "-p",
                      str(self.config.node(primary)["port"]), "-U", "postgres", "-D", data_dir, "-R", "-X", "stream",
                      "-c", "fast", "-S", slot], host=standby_address)
            self._write_recovery_settings(standby_name, primary, slot)
            self._write_marker(standby_name)
            self._write_files(
                standby_name,
                publisher=publisher,
                standby_slot=slot,
                primary_name=primary,
                sync_slots=sync_slots,
            )
            self._start(standby_name)
            self._reset_inherited_primary_settings(standby_name)

    def _create_logical(self, cluster_name):
        cluster = self.config.cluster(cluster_name)
        names = self.config.logical_names(cluster_name)
        failover = "true" if cluster.get("failover", False) else "false"
        publisher = self.config.logical_node(cluster_name, "publisher")
        subscriber = self.config.logical_node(cluster_name, "subscriber")
        if self.config.logical_streaming(cluster_name, "publisher") is None:
            self._ensure_primary(publisher, publisher=True)
        if self.config.logical_streaming(cluster_name, "subscriber") is None:
            self._ensure_primary(subscriber, publisher=False)
        database = cluster.get("database", "postgres")
        publication_exists = self.psql(
            publisher,
            "SELECT count(*) FROM pg_publication WHERE pubname = %s" % quote_literal(names["publication"]),
            database,
            tuples=True,
        )
        if publication_exists != "1":
            self.psql(publisher, "CREATE PUBLICATION %s FOR ALL TABLES" % quote_ident(names["publication"]), database)
        self.provider.ensure_logical_slot(
            self.config, self._provider_psql, cluster_name, publisher, database
        )
        publisher_node = self.config.node(publisher)
        connection = "host=%s port=%s user=postgres dbname=%s" % (
            self._host_address(publisher), publisher_node["port"], database)
        sql = (
            "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "
            "WITH (connect = true, create_slot = false, enabled = true, "
            "slot_name = %s, copy_data = %s, failover = %s)"
        ) % (
            quote_ident(names["subscription"]),
            quote_literal(connection),
            quote_ident(names["publication"]),
            quote_literal(names["slot"]),
            "true" if cluster.get("copy_data", True) else "false",
            failover,
        )
        subscription_exists = self.psql(
            subscriber,
            "SELECT count(*) FROM pg_subscription WHERE subname = %s" % quote_literal(names["subscription"]),
            database,
            tuples=True,
        )
        if subscription_exists != "1":
            self.psql(subscriber, sql, database)
        else:
            self.psql(subscriber, "ALTER SUBSCRIPTION %s ENABLE" % quote_ident(names["subscription"]), database)
        self._wait_for_slot_sync(cluster_name)

    def _wait_for_slot_sync(self, logical_name, seconds=30):
        self.provider.wait_failover_slot(
            self.config, self._provider_psql, logical_name, seconds
        )

    def _provider_psql(self, node, sql, database="postgres", tuples=False):
        return self.psql(node, sql, database=database, tuples=tuples)

    def create(self, target):
        self._require_supported_features(target)
        self.doctor(target)
        order = self.config.closure(target)
        for name in order:
            cluster = self.config.cluster(name)
            if cluster["type"] == "streaming":
                publisher_links = [
                    logical for logical in self.config.clusters.values()
                    if logical.get("type") == "logical" and logical.get("publisher") == name
                ]
                publisher = bool(publisher_links)
                sync_slots = any(logical.get("failover", False) for logical in publisher_links)
                self._create_streaming(name, publisher, sync_slots=sync_slots)
            elif cluster["type"] == "logical":
                self._create_logical(name)
            else:
                raise OperationError("MMR provider 尚未实现")
        self._wait_topology_healthy(target)
        return "创建完成: %s" % target

    def _ensure_data_parent(self, node_name):
        """Create the PGDATA parent with explicit PostgreSQL ownership.

        initdb must receive an empty PGDATA, so this deliberately creates only
        its parent.  A local test host may need sudo to create /data; remote
        hosts must grant the SSH user permission beforehand.
        """
        data_dir = Path(self.config.node(node_name)["data_dir"])
        parent = data_dir.parent
        install = ["install", "-d", "-o", "postgres", "-g", "postgres", "-m", "0700", str(parent)]
        address = self._host_address(node_name)
        result = self.run(install, host=address, check=False)
        if result.returncode == 0:
            return
        elevated = self.run(["sudo", "-n"] + install, host=address, check=False)
        if elevated.returncode == 0:
            return
        raise OperationError(
            "无法创建 PGDATA 父目录 %s。请先执行: sudo install -d -o postgres -g postgres -m 0700 %s" %
            (parent, parent))

    def _health(self, target):
        self._require_supported_features(target)
        root = self.config.cluster(target)
        checker = HealthChecker(self.config, self.psql, self.provider)
        group_health = {}
        for group in self.config.topology.groups(target):
            group_health[group.key] = checker.group(group)
        root_ok = all(item.ok for item in group_health.values())
        if root["type"] == "logical":
            root_ok = root_ok and checker.logical(target, group_health)
        return root_ok, group_health

    def _wait_topology_healthy(self, target, seconds=30):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            root_ok, _ = self._health(target)
            if root_ok:
                return
            time.sleep(0.5)
        raise OperationError("集群未在 %s 秒内达到健康状态: %s" % (seconds, target))

    def status(self, target):
        root = self.config.cluster(target)
        root_ok, group_health = self._health(target)
        self.last_status_ok = root_ok

        def line(label, state):
            return "%-48s %s" % (label, state)

        root_kind = root["type"]
        lines = [line("CLUSTER %s [%s]" % (target, root_kind), "OK" if root_ok else "FAILED")]

        groups = self.config.topology.groups(target)
        for group_index, group in enumerate(groups):
            health = group_health[group.key]
            last_group = group_index == len(groups) - 1
            group_branch = "└──" if last_group else "├──"
            group_indent = "    " if last_group else "│   "
            show_group = root["type"] == "logical"
            if show_group:
                kind = "streaming" if group.explicit else "single"
                lines.append(line("%s %s [%s]" % (group_branch, group.name, kind),
                                  "OK" if health.ok else "FAILED"))
            else:
                group_indent = ""
            for node_index, node_health in enumerate(health.nodes):
                last_node = node_index == len(health.nodes) - 1
                node_branch = "└──" if last_node else "├──"
                node_indent = group_indent + ("    " if last_node else "│   ")
                node = self.config.node(node_health.node)
                lines.append(line(
                    "%s%s %s [%s]" % (group_indent, node_branch, node_health.node, node_health.role),
                    "OK" if node_health.ok else "FAILED",
                ))
                lines.append("%s├── listen: %s:%s" %
                             (node_indent, self._host_address(node_health.node), node["port"]))
                lines.append("%s└── data_dir: %s" % (node_indent, node["data_dir"]))
        return "\n".join(lines)

    def start(self, target):
        self._require_supported_features(target)
        for node_name in self._nodes_for_target(target):
            self._require_marker(node_name)
            ready = self.run([self.config.binary("pg_isready"), "-h", self._host_address(node_name), "-p",
                              str(self.config.node(node_name)["port"])], check=False)
            if ready.returncode:
                self._start(node_name)
        self._wait_topology_healthy(target)
        return "已启动: %s" % target

    def stop(self, target):
        self._require_supported_features(target)
        self.config.topology.require_removable(target, "停止")
        for node_name in reversed(self._nodes_for_target(target)):
            self._require_marker(node_name)
            node = self.config.node(node_name)
            self.run([self.config.binary("pg_ctl"), "-D", node["data_dir"], "-m", "fast", "-w", "stop"],
                     host=self._host_address(node_name), check=False)
        return "已停止: %s" % target

    def restart(self, target):
        self.stop(target)
        self.start(target)
        return "已重启: %s" % target

    def verify(self, target):
        self._require_supported_features(target)
        cluster = self.config.cluster(target)
        if cluster["type"] != "logical":
            raise OperationError("verify 当前要求 logical 集群")
        publisher = self.config.logical_node(target, "publisher")
        subscriber = self.config.logical_node(target, "subscriber")
        subscriber_name = self.config.logical_streaming(target, "subscriber")
        subscriber_stream = self.config.cluster(subscriber_name) if subscriber_name else {"standbys": []}
        subscriber_standby = (subscriber_stream.get("standbys") or [{}])[0].get("node")
        schema = (
            "CREATE SCHEMA IF NOT EXISTS pgcluster_verify; "
            "CREATE TABLE IF NOT EXISTS pgcluster_verify.probe "
            "(cluster_name text PRIMARY KEY, token text NOT NULL)"
        )
        self.psql(publisher, schema)
        self.psql(subscriber, schema)
        subscription = self.config.logical_names(target)["subscription"]
        self.psql(subscriber, "ALTER SUBSCRIPTION %s REFRESH PUBLICATION WITH (copy_data = false)" %
                  quote_ident(subscription))
        token = uuid.uuid4().hex
        self.psql(publisher,
                  "INSERT INTO pgcluster_verify.probe(cluster_name, token) VALUES (%s, %s) "
                  "ON CONFLICT (cluster_name) DO UPDATE SET token = EXCLUDED.token" %
                  (quote_literal(target), quote_literal(token)))
        until = time.monotonic() + 30
        nodes = [subscriber] + ([subscriber_standby] if subscriber_standby else [])
        while time.monotonic() < until:
            values = [
                self.psql(
                    node,
                    "SELECT token FROM pgcluster_verify.probe WHERE cluster_name = %s" %
                    quote_literal(target),
                    tuples=True,
                )
                for node in nodes
            ]
            if all(value == token for value in values):
                if subscriber_standby:
                    return "验证通过: %s 已逻辑复制到 %s，并物理复制到 %s" % (
                        publisher, subscriber, subscriber_standby)
                return "验证通过: %s 已逻辑复制到 %s" % (publisher, subscriber)
            time.sleep(1)
        raise OperationError("逻辑复制测试失败: pgcluster_verify.probe 未在订阅端收敛")

    def clean(self, target, yes=False):
        if not yes:
            raise SafetyError("clean 会删除 PGDATA；请使用 --yes")
        self._require_supported_features(target)
        self.config.topology.require_removable(target, "清理")
        nodes = self._nodes_for_target(target)
        existing_nodes = [
            node_name for node_name in nodes
            if self.executor.exists(self._host_address(node_name), self.config.node(node_name)["data_dir"])
        ]
        for node_name in existing_nodes:
            self._require_marker(node_name)
        for node_name in reversed(existing_nodes):
            node = self.config.node(node_name)
            self.run([self.config.binary("pg_ctl"), "-D", node["data_dir"], "-m", "fast", "-w", "stop"],
                     host=self._host_address(node_name), check=False)
        for node_name in existing_nodes:
            self.executor.remove_tree(
                self._host_address(node_name),
                self.config.node(node_name)["data_dir"],
            )
        return "已清理: %s" % target
