import json
import os
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path

from .errors import OperationError, SafetyError


MARKER = ".pgcluster-managed"
LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost", socket.gethostname(), socket.getfqdn()}


def quote_ident(value):
    return '"%s"' % value.replace('"', '""')


def quote_literal(value):
    return "'%s'" % str(value).replace("'", "''")


class OperationLog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("w", encoding="utf-8")
        self.step = 0

    def record(self, host, cwd, command, stdout, stderr, code):
        self.step += 1
        self.stream.write(
            "STEP %d\nHOST: %s\nCWD: %s\n\nCOMMAND:\n%s\n\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\nEXIT_CODE: %s\n\n" %
            (self.step, host, cwd, command, stdout, stderr, code))
        self.stream.flush()

    def close(self):
        self.stream.close()


class Manager:
    def __init__(self, config):
        self.config = config
        self.log = None

    def open_log(self):
        self.log = OperationLog(self.config.operation_log)

    def close_log(self):
        if self.log:
            self.log.close()
            self.log = None

    def _host_address(self, node_name):
        return self.config.hosts[self.config.node(node_name)["host"]]

    def _is_local(self, address):
        return address in LOCAL_HOSTS

    def run(self, args, host="local", cwd=None, check=True):
        args = [str(item) for item in args]
        display = shlex.join(args)
        if host != "local" and not self._is_local(host):
            display = "ssh %s -- %s" % (shlex.quote(host), display)
            actual = ["ssh", host, "--", shlex.join(args)]
        else:
            actual = args
        process = subprocess.run(actual, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if self.log:
            self.log.record(host, cwd or os.getcwd(), display, process.stdout, process.stderr, process.returncode)
        if check and process.returncode:
            raise OperationError("命令失败（退出码 %s）: %s\n%s" %
                                 (process.returncode, display, process.stderr.strip()))
        return process

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

    def graph(self, target):
        order = self.config.closure(target)
        lines = ["CLUSTER %s [%s]" % (target, self.config.cluster(target)["type"])]
        for name in order:
            cluster = self.config.cluster(name)
            if cluster["type"] == "streaming":
                lines.append("├── %s [streaming]" % name)
                lines.append("│   ├── primary: %s" % cluster["primary"])
                for standby in cluster.get("standbys") or []:
                    lines.append("│   └── standby: %s (slot: %s)" %
                                 (standby["node"], self.config.physical_slot(name, standby)))
            elif cluster["type"] == "logical":
                names = self.config.logical_names(name)
                lines.append("└── %s [logical]" % name)
                lines.append("    ├── publisher: %s" % cluster["publisher"])
                lines.append("    ├── subscriber: %s" % cluster["subscriber"])
                lines.append("    └── slot: %s" % names["slot"])
        lines.append("")
        lines.append("CREATE ORDER")
        lines.extend(order)
        lines.append("CLEAN ORDER")
        lines.extend(reversed(order))
        return "\n".join(lines)

    def doctor(self, target):
        self.validate(target)
        for binary in ("initdb", "pg_ctl", "psql", "pg_basebackup"):
            if not Path(self.config.binary(binary)).is_file():
                raise OperationError("缺少 PostgreSQL 二进制: %s" % self.config.binary(binary))
        for cluster_name in self.config.closure(target):
            cluster = self.config.cluster(cluster_name)
            if cluster["type"] != "streaming":
                continue
            for node_name in [cluster["primary"]] + [item["node"] for item in cluster.get("standbys") or []]:
                node = self.config.node(node_name)
                address = self._host_address(node_name)
                if not self._is_local(address):
                    self.run(["true"], host=address)
                probe = self.run(["sh", "-c", "! ss -ltn 'sport = :%s' | grep -q LISTEN" % node["port"]],
                                 host=address, check=False)
                if probe.returncode:
                    raise OperationError("端口已被占用: %s:%s" % (address, node["port"]))
        return "运行环境可用: %s" % target

    def _marker_path(self, node_name):
        return Path(self.config.node(node_name)["data_dir"]) / MARKER

    def _write_marker(self, node_name):
        data_dir = Path(self.config.node(node_name)["data_dir"])
        payload = {"version": 1, "node": node_name, "data_dir": str(data_dir.resolve()), "created_by": "pgcluster"}
        temporary = data_dir / (MARKER + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        os.replace(temporary, data_dir / MARKER)

    def _require_marker(self, node_name):
        data_dir = Path(self.config.node(node_name)["data_dir"])
        marker = data_dir / MARKER
        if marker.is_symlink() or not marker.is_file():
            raise SafetyError("拒绝操作没有 pgcluster marker 的目录: %s" % data_dir)
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise SafetyError("marker 无效: %s" % exc)
        expected = {"version": 1, "node": node_name, "data_dir": str(data_dir.resolve()), "created_by": "pgcluster"}
        if payload != expected:
            raise SafetyError("marker 与节点配置不匹配: %s" % data_dir)

    def _nodes_for_streaming(self, cluster_name):
        cluster = self.config.cluster(cluster_name)
        return [cluster["primary"]] + [item["node"] for item in cluster.get("standbys") or []]

    def _write_files(self, node_name, publisher=False, standby_slot=None, primary_name=None):
        node = self.config.node(node_name)
        data_dir = Path(node["data_dir"])
        settings = {
            "port": int(node["port"]), "listen_addresses": "'*'", "logging_collector": "on",
            "log_destination": "'stderr'", "log_directory": "'log'", "log_filename": "'postgresql-%Y-%m-%d_%H%M%S.log'",
            "log_file_mode": "0600", "log_rotation_age": "'1d'", "log_rotation_size": "'100MB'",
            "log_line_prefix": "'%m [%p] user=%u db=%d app=%a client=%r '", "log_error_verbosity": "verbose",
            "log_statement": "'all'", "log_duration": "on", "log_connections": "on", "log_disconnections": "on",
            "log_checkpoints": "on", "log_replication_commands": "on", "max_wal_senders": 10, "max_replication_slots": 10,
            "max_logical_replication_workers": 4,
        }
        if publisher:
            settings.update({"wal_level": "logical", "hot_standby_feedback": "on"})
        if standby_slot:
            settings.update({"sync_replication_slots": "on", "hot_standby_feedback": "on"})
        if primary_name:
            primary = self.config.node(primary_name)
            settings["primary_conninfo"] = quote_literal(
                "host=%s port=%s user=postgres dbname=postgres application_name=%s" %
                (self._host_address(primary_name), primary["port"], node_name))
            settings["primary_slot_name"] = quote_literal(standby_slot)
        lines = ["# Managed by pgcluster"] + ["%s = %s" % item for item in sorted(settings.items())]
        (data_dir / "pgcluster.conf").write_text("\n".join(lines) + "\n", encoding="utf-8")
        postgres_conf = data_dir / "postgresql.conf"
        include = "include_if_exists = 'pgcluster.conf'"
        existing = postgres_conf.read_text(encoding="utf-8")
        if include not in existing:
            with postgres_conf.open("a", encoding="utf-8") as stream:
                stream.write("\n%s\n" % include)
        (data_dir / "pg_hba.conf").write_text(
            "local all all trust\n"
            "host all all 0.0.0.0/0 trust\n"
            "host all all ::/0 trust\n"
            "host replication all 0.0.0.0/0 trust\n"
            "host replication all ::/0 trust\n", encoding="utf-8")

    def _init_primary(self, node_name, publisher):
        node = self.config.node(node_name)
        data_dir = Path(node["data_dir"])
        if data_dir.exists() and any(data_dir.iterdir()):
            raise SafetyError("data_dir 已存在且非空，先 clean: %s" % data_dir)
        self._ensure_data_parent(node_name)
        self.run([self.config.binary("initdb"), "-D", str(data_dir), "-U", "postgres", "--auth-local=trust", "--auth-host=trust"])
        self._write_marker(node_name)
        self._write_files(node_name, publisher=publisher)
        self._start(node_name)

    def _write_recovery_settings(self, standby_name, primary_name, slot):
        """Override pg_basebackup -R's conninfo with dbname required by slot sync."""
        data_dir = Path(self.config.node(standby_name)["data_dir"])
        primary = self.config.node(primary_name)
        conninfo = "host=%s port=%s user=postgres dbname=postgres application_name=%s" % (
            self._host_address(primary_name), primary["port"], standby_name)
        with (data_dir / "postgresql.auto.conf").open("a", encoding="utf-8") as stream:
            stream.write("primary_conninfo = %s\n" % quote_literal(conninfo))
            stream.write("primary_slot_name = %s\n" % quote_literal(slot))

    def _start(self, node_name):
        node = self.config.node(node_name)
        self.run([self.config.binary("pg_ctl"), "-D", node["data_dir"], "-l",
                  str(Path(node["data_dir"]) / "pg_ctl.log"), "-w", "start"],
                 host=self._host_address(node_name))
        self._wait(node_name)

    def _wait(self, node_name, seconds=30):
        until = time.monotonic() + seconds
        error = ""
        while time.monotonic() < until:
            result = self.run([self.config.binary("psql"), "-X", "-h", self._host_address(node_name), "-p",
                               str(self.config.node(node_name)["port"]), "-U", "postgres", "-d", "postgres", "-Atqc", "SELECT 1"],
                              host=self._host_address(node_name), check=False)
            if result.returncode == 0 and result.stdout.strip() == "1":
                return
            error = result.stderr
            time.sleep(0.5)
        raise OperationError("PostgreSQL 未在 %s 秒内启动: %s" % (seconds, error.strip()))

    def _create_streaming(self, cluster_name, publisher):
        cluster = self.config.cluster(cluster_name)
        primary = cluster["primary"]
        self._init_primary(primary, publisher)
        for standby in cluster.get("standbys") or []:
            standby_name = standby["node"]
            slot = self.config.physical_slot(cluster_name, standby)
            self.psql(primary, "SELECT pg_create_physical_replication_slot(%s)" % quote_literal(slot))
            self.psql(primary, "ALTER SYSTEM SET synchronized_standby_slots = %s" % quote_literal(slot))
            self.psql(primary, "SELECT pg_reload_conf()")
            data_dir = Path(self.config.node(standby_name)["data_dir"])
            if data_dir.exists() and any(data_dir.iterdir()):
                raise SafetyError("data_dir 已存在且非空，先 clean: %s" % data_dir)
            self._ensure_data_parent(standby_name)
            self.run([self.config.binary("pg_basebackup"), "-h", self._host_address(primary), "-p",
                      str(self.config.node(primary)["port"]), "-U", "postgres", "-D", str(data_dir), "-R", "-X", "stream",
                      "-c", "fast", "-S", slot])
            self._write_recovery_settings(standby_name, primary, slot)
            self._write_marker(standby_name)
            self._write_files(standby_name, publisher=publisher, standby_slot=slot, primary_name=primary)
            self._start(standby_name)

    def _create_logical(self, cluster_name):
        cluster = self.config.cluster(cluster_name)
        names = self.config.logical_names(cluster_name)
        publisher_cluster = self.config.cluster(cluster["publisher"])
        subscriber_cluster = self.config.cluster(cluster["subscriber"])
        publisher = publisher_cluster["primary"]
        subscriber = subscriber_cluster["primary"]
        database = cluster.get("database", "postgres")
        self.psql(publisher, "CREATE PUBLICATION %s FOR ALL TABLES" % quote_ident(names["publication"]), database)
        self.psql(publisher, "SELECT pg_create_logical_replication_slot(%s, 'pgoutput', false, false, true)" %
                  quote_literal(names["slot"]), database)
        publisher_node = self.config.node(publisher)
        connection = "host=%s port=%s user=postgres dbname=%s" % (
            self._host_address(publisher), publisher_node["port"], database)
        sql = ("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "
               "WITH (connect = true, create_slot = false, enabled = true, slot_name = %s, copy_data = %s, failover = true)") % (
                   quote_ident(names["subscription"]), quote_literal(connection), quote_ident(names["publication"]),
                   quote_literal(names["slot"]), "true" if cluster.get("copy_data", True) else "false")
        self.psql(subscriber, sql, database)
        self._wait_for_slot_sync(cluster_name)

    def _wait_for_slot_sync(self, logical_name, seconds=30):
        cluster = self.config.cluster(logical_name)
        names = self.config.logical_names(logical_name)
        publisher = self.config.cluster(cluster["publisher"])
        standby_nodes = [item["node"] for item in publisher.get("standbys") or []]
        if not standby_nodes:
            return
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            ready = True
            for standby in standby_nodes:
                slot = self.psql(standby, "SELECT coalesce(synced::text, 'false') FROM pg_replication_slots WHERE slot_name = %s" %
                                 quote_literal(names["slot"]), tuples=True)
                if slot != "true":
                    ready = False
            if ready:
                return
            time.sleep(1)
        raise OperationError("逻辑故障槽未在发布端备库同步: %s" % names["slot"])

    def create(self, target):
        if self.config.extensions:
            raise OperationError("FBase 等保插件 provider 尚未实现")
        order = self.config.closure(target)
        for name in order:
            cluster = self.config.cluster(name)
            if cluster["type"] == "streaming":
                publisher = any(
                    logical.get("type") == "logical" and logical.get("publisher") == name
                    for logical in self.config.clusters.values())
                self._create_streaming(name, publisher)
            elif cluster["type"] == "logical":
                self._create_logical(name)
            else:
                raise OperationError("MMR provider 尚未实现")
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
        if self._is_local(address):
            elevated = self.run(["sudo", "-n"] + install, host=address, check=False)
            if elevated.returncode == 0:
                return
        raise OperationError(
            "无法创建 PGDATA 父目录 %s。请先执行: sudo install -d -o postgres -g postgres -m 0700 %s" %
            (parent, parent))

    def status(self, target):
        cluster_status = {}
        details = {}
        streaming_names = []
        for name in self.config.closure(target):
            if self.config.cluster(name)["type"] == "streaming":
                streaming_names.append(name)
                ok = True
                node_states = []
                for node_name in self._nodes_for_streaming(name):
                    result = self.run([self.config.binary("pg_isready"), "-h", self._host_address(node_name), "-p",
                                       str(self.config.node(node_name)["port"])], host=self._host_address(node_name), check=False)
                    state = "OK" if result.returncode == 0 else "FAILED"
                    ok = ok and state == "OK"
                    node_states.append((node_name, state))
                cluster_status[name] = "OK" if ok else "FAILED"
                details[name] = node_states
        root_ok = all(cluster_status[name] == "OK" for name in streaming_names)
        root = self.config.cluster(target)
        if root["type"] == "logical" and root_ok:
            try:
                names = self.config.logical_names(target)
                subscriber = self.config.cluster(root["subscriber"])["primary"]
                publisher = self.config.cluster(root["publisher"])
                active = self.psql(subscriber, "SELECT count(*) FROM pg_stat_subscription WHERE subname = %s AND pid IS NOT NULL" %
                                  quote_literal(names["subscription"]), tuples=True)
                synced = "true"
                for standby in publisher.get("standbys") or []:
                    slot = self.psql(standby["node"], "SELECT coalesce(synced::text, 'false') FROM pg_replication_slots WHERE slot_name = %s" %
                                     quote_literal(names["slot"]), tuples=True)
                    synced = slot if slot else "false"
                    if synced != "true":
                        break
                root_ok = active == "1" and synced == "true"
            except OperationError:
                root_ok = False
        lines = ["CLUSTER %s %s" % (target, "OK" if root_ok else "FAILED")]
        for name in streaming_names:
            lines.append("%s %s" % (name, cluster_status[name]))
            lines.extend("  %s %s" % item for item in details[name])
        return "\n".join(lines)

    def start(self, target):
        for cluster_name in self.config.closure(target):
            cluster = self.config.cluster(cluster_name)
            if cluster["type"] != "streaming":
                continue
            for node_name in self._nodes_for_streaming(cluster_name):
                self._require_marker(node_name)
                ready = self.run([self.config.binary("pg_isready"), "-h", self._host_address(node_name), "-p",
                                  str(self.config.node(node_name)["port"])], check=False)
                if ready.returncode:
                    self._start(node_name)
        return "已启动: %s" % target

    def stop(self, target):
        for cluster_name in reversed(self.config.closure(target)):
            cluster = self.config.cluster(cluster_name)
            if cluster["type"] != "streaming":
                continue
            for node_name in reversed(self._nodes_for_streaming(cluster_name)):
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
        cluster = self.config.cluster(target)
        if cluster["type"] != "logical":
            raise OperationError("verify 当前要求 logical 集群")
        publisher = self.config.cluster(cluster["publisher"])["primary"]
        subscriber_stream = self.config.cluster(cluster["subscriber"])
        subscriber = subscriber_stream["primary"]
        subscriber_standby = (subscriber_stream.get("standbys") or [{}])[0].get("node")
        schema = "CREATE TABLE IF NOT EXISTS public.test_tbl (id integer NOT NULL, name text)"
        self.psql(publisher, schema)
        self.psql(subscriber, schema)
        subscription = self.config.logical_names(target)["subscription"]
        self.psql(subscriber, "ALTER SUBSCRIPTION %s REFRESH PUBLICATION WITH (copy_data = false)" %
                  quote_ident(subscription))
        self.psql(publisher,
                  "INSERT INTO public.test_tbl(id, name) "
                  "SELECT 1, 'c1-probe' "
                  "WHERE NOT EXISTS (SELECT 1 FROM public.test_tbl WHERE id = 1)")
        expected = "1|c1-probe"
        until = time.monotonic() + 30
        nodes = [subscriber] + ([subscriber_standby] if subscriber_standby else [])
        while time.monotonic() < until:
            values = [self.psql(node, "SELECT id || '|' || name FROM public.test_tbl WHERE id = 1", tuples=True) for node in nodes]
            if all(value == expected for value in values):
                return "验证通过: test_tbl 已从 %s 逻辑复制到 %s，并物理复制到 %s" % (
                    publisher, subscriber, subscriber_standby or subscriber)
            time.sleep(1)
        raise OperationError("逻辑复制测试失败: test_tbl 未在订阅端收敛")

    def clean(self, target, yes=False):
        if not yes:
            raise SafetyError("clean 会删除 PGDATA；请使用 --yes")
        clusters = self.config.closure(target)
        nodes = []
        for cluster_name in clusters:
            cluster = self.config.cluster(cluster_name)
            if cluster["type"] == "streaming":
                nodes.extend(self._nodes_for_streaming(cluster_name))
        existing_nodes = [node_name for node_name in nodes if Path(self.config.node(node_name)["data_dir"]).exists()]
        for node_name in existing_nodes:
            self._require_marker(node_name)
        for node_name in reversed(existing_nodes):
            node = self.config.node(node_name)
            self.run([self.config.binary("pg_ctl"), "-D", node["data_dir"], "-m", "fast", "-w", "stop"],
                     host=self._host_address(node_name), check=False)
        for node_name in existing_nodes:
            shutil.rmtree(self.config.node(node_name)["data_dir"])
        return "已清理: %s" % target
