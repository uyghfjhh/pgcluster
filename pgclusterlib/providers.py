import time

from .errors import OperationError
from .sql import quote_literal


class Provider:
    name = ""
    available = False

    def require_available(self):
        if not self.available:
            raise OperationError("%s provider 尚未实现" % self.name)

    def ensure_logical_slot(self, config, psql, logical_name, publisher, database):
        raise NotImplementedError

    def wait_failover_slot(self, config, psql, logical_name, seconds=30):
        raise NotImplementedError

    def logical_slot_ok(self, config, psql, logical_name, publisher):
        raise NotImplementedError


class NativePostgresProvider(Provider):
    name = "postgres"
    available = True

    def ensure_logical_slot(self, config, psql, logical_name, publisher, database):
        cluster = config.cluster(logical_name)
        names = config.logical_names(logical_name)
        failover = "true" if cluster.get("failover", False) else "false"
        slot_row = psql(
            publisher,
            "SELECT slot_type || '|' || failover::text "
            "FROM pg_replication_slots WHERE slot_name = %s" %
            quote_literal(names["slot"]),
            database,
            True,
        )
        expected = "logical|%s" % failover
        if slot_row and slot_row != expected:
            raise OperationError(
                "逻辑槽 %s 已存在但属性不匹配，实际 %s，期望 %s" %
                (names["slot"], slot_row, expected)
            )
        if not slot_row:
            psql(
                publisher,
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput', false, false, %s)" %
                (quote_literal(names["slot"]), failover),
                database,
                False,
            )

    def wait_failover_slot(self, config, psql, logical_name, seconds=30):
        cluster = config.cluster(logical_name)
        if not cluster.get("failover", False):
            return
        names = config.logical_names(logical_name)
        publisher = config.topology.logical_group(logical_name, "publisher")
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            ready = True
            for standby in publisher.standbys:
                synced = psql(
                    standby.node,
                    "SELECT count(*) FROM pg_replication_slots "
                    "WHERE slot_name = %s AND slot_type = 'logical' AND synced" %
                    quote_literal(names["slot"]),
                    "postgres",
                    True,
                )
                if synced != "1":
                    ready = False
            if ready:
                return
            time.sleep(1)
        raise OperationError("逻辑故障槽未在发布端备库同步: %s" % names["slot"])

    def logical_slot_ok(self, config, psql, logical_name, publisher):
        cluster = config.cluster(logical_name)
        names = config.logical_names(logical_name)
        slot = psql(
            publisher.primary,
            "SELECT slot_type || '|' || active::text || '|' || failover::text "
            "FROM pg_replication_slots WHERE slot_name = %s" %
            quote_literal(names["slot"]),
            "postgres",
            True,
        )
        expected_failover = "true" if cluster.get("failover", False) else "false"
        if slot != "logical|true|%s" % expected_failover:
            return False
        if cluster.get("failover", False):
            for standby in publisher.standbys:
                synced = psql(
                    standby.node,
                    "SELECT count(*) FROM pg_replication_slots "
                    "WHERE slot_name = %s AND slot_type = 'logical' AND synced" %
                    quote_literal(names["slot"]),
                    "postgres",
                    True,
                )
                if synced != "1":
                    return False
        return True


class FBaseProvider(Provider):
    """Reserved integration point for MAC detection and FBase slot APIs."""

    name = "fbase"


PROVIDERS = {
    "postgres": NativePostgresProvider(),
    "fbase": FBaseProvider(),
}


def provider_for(config):
    return PROVIDERS[config.postgres.get("provider", "postgres")]
