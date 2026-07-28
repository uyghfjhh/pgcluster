from dataclasses import dataclass

from .errors import OperationError


@dataclass
class NodeHealth:
    node: str
    role: str
    ok: bool
    reason: str = ""


@dataclass
class GroupHealth:
    group: object
    nodes: list
    ok: bool


class HealthChecker:
    def __init__(self, config, psql, provider):
        self.config = config
        self.psql = psql
        self.provider = provider

    def _node_role(self, node_name):
        value = self.psql(
            node_name,
            "SELECT pg_is_in_recovery()::text || '|' || current_setting('data_directory')",
            tuples=True,
        )
        recovery, data_dir = value.split("|", 1)
        expected = self.config.node(node_name)["data_dir"]
        return recovery == "true", data_dir == expected

    def group(self, group):
        states = []
        try:
            recovery, correct_data_dir = self._node_role(group.primary)
            ok = not recovery and correct_data_dir
            reason = "" if ok else "主库角色或 PGDATA 不匹配"
            states.append(NodeHealth(group.primary, "primary", ok, reason))
        except (OperationError, ValueError) as exc:
            states.append(NodeHealth(group.primary, "primary", False, str(exc)))

        for standby in group.standbys:
            ok = True
            reason = ""
            try:
                recovery, correct_data_dir = self._node_role(standby.node)
                if not recovery or not correct_data_dir:
                    ok = False
                    reason = "备库角色或 PGDATA 不匹配"
                receiver = self.psql(
                    standby.node,
                    "SELECT status || '|' || coalesce(slot_name, '') || '|' || sender_port "
                    "FROM pg_stat_wal_receiver",
                    tuples=True,
                )
                expected_port = str(self.config.node(group.primary)["port"])
                if receiver != "streaming|%s|%s" % (standby.slot, expected_port):
                    ok = False
                    reason = "WAL receiver 未连接到配置的主库/复制槽"
                sender = self.psql(
                    group.primary,
                    "SELECT count(*) FROM pg_stat_replication "
                    "WHERE application_name = %s AND state = 'streaming'" %
                    self._literal(standby.node),
                    tuples=True,
                )
                slot = self.psql(
                    group.primary,
                    "SELECT count(*) FROM pg_replication_slots "
                    "WHERE slot_name = %s AND slot_type = 'physical' AND active" %
                    self._literal(standby.slot),
                    tuples=True,
                )
                if sender != "1" or slot != "1":
                    ok = False
                    reason = "主库 sender 或物理复制槽不正常"
            except (OperationError, ValueError) as exc:
                ok = False
                reason = str(exc)
            states.append(NodeHealth(standby.node, "standby", ok, reason))
        return GroupHealth(group, states, all(item.ok for item in states))

    def logical(self, logical_name, group_health):
        names = self.config.logical_names(logical_name)
        publisher = self.config.topology.logical_group(logical_name, "publisher")
        subscriber = self.config.topology.logical_group(logical_name, "subscriber")
        if not group_health[publisher.key].ok or not group_health[subscriber.key].ok:
            return False
        try:
            publication = self.psql(
                publisher.primary,
                "SELECT count(*) FROM pg_publication WHERE pubname = %s AND puballtables" %
                self._literal(names["publication"]),
                tuples=True,
            )
            subscription = self.psql(
                subscriber.primary,
                "SELECT s.subenabled::text || '|' || coalesce(s.subslotname, '') || '|' || "
                "(st.pid IS NOT NULL)::text "
                "FROM pg_subscription s LEFT JOIN pg_stat_subscription st ON st.subid = s.oid "
                "WHERE s.subname = %s" % self._literal(names["subscription"]),
                tuples=True,
            )
            if publication != "1":
                return False
            if not self.provider.logical_slot_ok(
                self.config, self._provider_psql, logical_name, publisher
            ):
                return False
            if subscription != "true|%s|true" % names["slot"]:
                return False
            return True
        except OperationError:
            return False

    def _provider_psql(self, node, sql, database="postgres", tuples=False):
        return self.psql(node, sql, database=database, tuples=tuples)

    @staticmethod
    def _literal(value):
        return "'%s'" % str(value).replace("'", "''")
