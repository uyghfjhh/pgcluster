"""Dependency-free full-screen dashboard for pgcluster."""

import os
import select
import sys
import time

from .errors import OperationError
from .sql import quote_literal


def _topology_targets(config, target=None):
    if target:
        config._target(config._collections(), target)
        return [target]
    referenced = set()
    for link in config.logical_replications.values():
        referenced.update((link["pub"]["streaming_cluster"], link["sub"]["streaming_cluster"]))
    for cluster in config.citus_clusters.values():
        referenced.add(cluster["coordinator"]["streaming_cluster"])
        referenced.update(item["streaming_cluster"] for item in cluster["workers"].values())
    for cluster in config.mmr_clusters.values():
        referenced.update(item["streaming_cluster"] for item in cluster["members"].values())
    return (["streaming.%s" % name for name in config.streaming_clusters if name not in referenced] +
            ["logical.%s" % name for name in config.logical_replications] +
            ["citus.%s" % name for name in config.citus_clusters] +
            ["mmr.%s" % name for name in config.mmr_clusters])


def _instance_states(config, runtime, names=None):
    states = {}
    for name in names or config.instances:
        try:
            state = runtime.status_instance(name)
            if not state["known"]:
                state["running"] = None
            states[name] = state
        except OperationError as exc:
            states[name] = {"running": None, "message": str(exc)}
    return states


def _query(runtime, instance, sql, database="postgres"):
    try:
        return runtime._psql(instance, sql, database, True)
    except OperationError:
        return "未知"


def _metric(config, runtime, target):
    kind, name = target.split(".", 1)
    if kind == "streaming":
        primary = runtime._primary(name)
        expected = len(config.streaming_clusters[name].get("standbys") or [])
        value = _query(runtime, primary,
                       "SELECT count(*) FILTER (WHERE state='streaming')||'|'||"
                       "COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)),0) "
                       "FROM pg_stat_replication")
        if "|" in value:
            connected, lag = value.split("|", 1)
            return "physical %s/%s streaming, lag=%s B" % (connected, expected, lag)
        return value
    if kind == "logical":
        link = config.logical_replications[name]
        pub = runtime._primary(link["pub"]["streaming_cluster"])
        sub = runtime._primary(link["sub"]["streaming_cluster"])
        database = link["sub"].get("database", link["pub"].get("database", "postgres"))
        publication = "%s_pub" % name
        subscription = "%s_sub" % name
        pub_state = _query(runtime, pub,
                           "SELECT count(*) FROM pg_publication WHERE pubname=%s" % quote_literal(publication),
                           database)
        sub_state = _query(runtime, sub,
                           "SELECT subenabled::text||'|'||COALESCE(latest_end_lsn::text,'') "
                           "FROM pg_subscription s LEFT JOIN pg_stat_subscription g USING (subname) "
                           "WHERE s.subname=%s" % quote_literal(subscription), database)
        if not pub_state.isdigit() or sub_state.startswith("未知"):
            return "publication=%s subscription=%s" % (pub_state, sub_state)
        if not sub_state:
            return "publication=%s subscription=missing" % pub_state
        enabled, lsn = sub_state.split("|", 1)
        return "publication=%s subscription=%s lsn=%s" % (pub_state, "enabled" if enabled == "t" else "disabled", lsn or "-")
    if kind == "citus":
        cluster = config.citus_clusters[name]
        coordinator = runtime._primary(cluster["coordinator"]["streaming_cluster"])
        value = _query(runtime, coordinator,
                        "SELECT count(*)||'|'||count(*) FILTER (WHERE isactive) "
                        "FROM pg_dist_node WHERE groupid > 0")
        return "workers=%s" % value
    cluster = config.mmr_clusters[name]
    values = []
    for member_name, member in cluster["members"].items():
        node = runtime._primary(member["streaming_cluster"])
        value = _query(runtime, node,
                       "SELECT count(*) FROM fdd.mmr_node WHERE node_state='ACTIVE'",
                       cluster.get("database", "postgres"))
        values.append("%s=%s" % (member_name, value))
    return " ".join(values)


def _metrics(config, runtime, target):
    """Collect compact operational metrics used by both text and Textual UIs."""
    kind, name = target.split(".", 1)
    if kind == "streaming":
        primary = runtime._primary(name)
        row = _query(runtime, primary,
                     "SELECT count(*)||'|'||pg_current_wal_lsn()::text||'|'||"
                     "COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)),0)||'|'||"
                     "COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn)),0)||'|'||"
                     "COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)),0)||'|'||"
                     "COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)),0)||'|'||"
                     "COALESCE(max(EXTRACT(EPOCH FROM replay_lag)),0) "
                     "FROM pg_stat_replication WHERE state='streaming'")
        fields = row.split("|") if "|" in row else []
        if len(fields) == 7:
            connected, current_lsn, sent_lag, write_lag, flush_lag, replay_lag, replay_seconds = fields
        else:
            connected, current_lsn, sent_lag, write_lag, flush_lag, replay_lag, replay_seconds = ("未知",) * 7
        expected = len(config.streaming_clusters[name].get("standbys") or [])
        return {"replication": "%s/%s streaming" % (connected, expected),
                "standbys": connected, "expected": expected, "primary_current_lsn": current_lsn,
                "sent_lag_bytes": sent_lag, "write_lag_bytes": write_lag,
                "flush_lag_bytes": flush_lag, "replay_lag_bytes": replay_lag,
                "replay_lag_seconds": replay_seconds}
    if kind == "logical":
        link = config.logical_replications[name]
        pub = runtime._primary(link["pub"]["streaming_cluster"])
        sub = runtime._primary(link["sub"]["streaming_cluster"])
        database = link["sub"].get("database", link["pub"].get("database", "postgres"))
        slot = (link["sub"].get("slot") or {}).get("name") or "%s_slot" % name
        publisher_row = _query(runtime, pub,
                               "SELECT pg_current_wal_lsn()::text||'|'||"
                               "COALESCE((SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name=%s),'')||'|'||"
                               "COALESCE((SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s),'')||'|'||"
                               "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),(SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=%s))::text,'')||'|'||"
                               "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),(SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=%s))::text,'')" %
                               (quote_literal(slot), quote_literal(slot), quote_literal(slot), quote_literal(slot)), database)
        publisher_fields = publisher_row.split("|", 4) if "|" in publisher_row else []
        if len(publisher_fields) == 5:
            publisher_lsn, restart_lsn, confirmed_lsn, retained_bytes, confirmed_lag_bytes = publisher_fields
        else:
            publisher_lsn, restart_lsn, confirmed_lsn, retained_bytes, confirmed_lag_bytes = ("未知",) * 5
        row = _query(runtime, sub,
                     "SELECT subenabled::text||'|'||COALESCE(received_lsn::text,'')||'|'||"
                     "COALESCE(latest_end_lsn::text,'')||'|'||"
                     "COALESCE(EXTRACT(EPOCH FROM now()-latest_end_time)::text,'') "
                     "FROM pg_subscription s LEFT JOIN pg_stat_subscription g USING (subname) "
                     "WHERE s.subname=%s" % quote_literal("%s_sub" % name), database)
        fields = row.split("|", 3) if "|" in row else []
        if len(fields) == 4:
            enabled, received_lsn, latest_lsn, apply_seconds = fields
            subscription = "enabled" if enabled == "t" else "disabled"
        elif row == "未知":
            subscription, received_lsn, latest_lsn, apply_seconds = "未知", "未知", "未知", "未知"
        else:
            subscription, received_lsn, latest_lsn, apply_seconds = "missing", "-", "-", "-"
        lsn_lag = "未知"
        if publisher_lsn not in {"未知", ""} and latest_lsn not in {"未知", "", "-"}:
            lsn_lag = _query(runtime, sub,
                             "SELECT COALESCE(pg_wal_lsn_diff(%s,%s),0)" %
                             (quote_literal(publisher_lsn), quote_literal(latest_lsn)), database)
        return {"replication": "subscription=%s" % subscription, "subscription": subscription,
                "publisher_current_lsn": publisher_lsn, "slot_restart_lsn": restart_lsn or "-",
                "slot_confirmed_flush_lsn": confirmed_lsn or "-", "slot_retained_bytes": retained_bytes or "-",
                "slot_confirmed_lag_bytes": confirmed_lag_bytes or "-",
                "subscriber_received_lsn": received_lsn or "-",
                "subscriber_latest_lsn": latest_lsn or "-", "logical_lsn_lag_bytes": lsn_lag,
                "apply_lag_seconds": apply_seconds or "-"}
    if kind == "citus":
        cluster = config.citus_clusters[name]
        coordinator = runtime._primary(cluster["coordinator"]["streaming_cluster"])
        row = _query(runtime, coordinator,
                     "SELECT count(*)||'|'||count(*) FILTER (WHERE isactive) "
                     "FROM pg_dist_node WHERE groupid > 0")
        total, active = row.split("|", 1) if "|" in row else ("未知", "未知")
        return {"replication": "workers=%s/%s active" % (active, total),
                "workers_active": active, "workers_total": total}
    cluster = config.mmr_clusters[name]
    values = []
    for member in cluster["members"].values():
        node = runtime._primary(member["streaming_cluster"])
        values.append(_query(runtime, node,
                             "SELECT count(*) FROM fdd.mmr_node WHERE node_state='ACTIVE'",
                             cluster.get("database", "postgres")))
    return {"replication": "members active=" + ",".join(values), "members": values}


def _spark(values, width=18):
    if not values:
        return "·" * width
    values = list(values)[-width:]
    low, high = min(values), max(values)
    bars = "▁▂▃▄▅▆▇█"
    if high == low:
        return bars[3] * len(values)
    return "".join(bars[min(len(bars) - 1, int((v - low) * (len(bars) - 1) / (high - low)))] for v in values)


def format_bytes(value):
    """Render a byte counter in the compact form suitable for a dashboard."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "未知"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return "%d %s" % (value, unit) if unit == "B" else "%.1f %s" % (value, unit)
        value /= 1024.0


def _operational_metrics(config, runtime, states):
    """Aggregate database health counters for the live monitoring overview."""
    primaries = set()
    for cluster in config.streaming_clusters:
        primaries.add(runtime._primary(cluster))
    connections = 0
    transactions = 0
    cache_samples = []
    connection_limits = []
    for name in primaries:
        if states.get(name, {}).get("running") is not True:
            continue
        row = _query(runtime, name,
                     "SELECT (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend')||'|'||"
                     "(SELECT setting FROM pg_settings WHERE name='max_connections')||'|'||"
                     "(SELECT COALESCE(sum(xact_commit+xact_rollback),0) FROM pg_stat_database "
                     "WHERE datname NOT IN ('template0','template1'))||'|'||"
                     "COALESCE(round(100.0*(SELECT sum(blks_hit) FROM pg_stat_database) / "
                     "NULLIF((SELECT sum(blks_hit+blks_read) FROM pg_stat_database),0),1),0)")
        try:
            sessions, max_connections, xacts, cache_hit = row.split("|", 3)
            connections += int(sessions)
            transactions += int(xacts)
            cache_samples.append(float(cache_hit))
            connection_limits.append(int(max_connections))
        except (TypeError, ValueError):
            continue
    return {
        "connections": connections,
        "max_connections": max(connection_limits) if connection_limits else None,
        "transactions": transactions,
        "cache_hit": round(sum(cache_samples) / len(cache_samples), 1) if cache_samples else None,
    }


def _summary(states):
    values = [state["running"] for state in states]
    if any(value is None for value in values):
        return "未知"
    if all(values):
        return "运行中"
    if any(values):
        return "部分停止"
    return "已停止"


def _paint(value, color, enabled):
    return "\033[%sm%s\033[0m" % (color, value) if enabled else value


def snapshot(config, runtime, target=None):
    targets = _topology_targets(config, target)
    names = []
    for item in targets:
        for name in runtime.target_instances(item):
            if name not in names:
                names.append(name)
    states = _instance_states(config, runtime, names)
    clusters = []
    for item in targets:
        kind = item.split(".", 1)[0]
        members = [states[name] for name in runtime.target_instances(item)]
        try:
            metric = _metric(config, runtime, item)
            detail = _metrics(config, runtime, item)
        except (KeyError, OperationError):
            metric = "未知"
            detail = {"replication": "未知"}
        clusters.append((kind, item, _summary(members), metric, detail))
    operational = _operational_metrics(config, runtime, states)
    lags = []
    for kind, _item, _cluster_summary, _metric_text, detail in clusters:
        if kind == "streaming":
            try:
                lags.append(float(detail.get("lag_bytes", 0)))
            except (TypeError, ValueError):
                pass
    operational["max_lag_bytes"] = max(lags) if lags else None
    return states, clusters, operational


def render(config, runtime, color=False, target=None):
    states, clusters, operational = snapshot(config, runtime, target)
    lines = [
        "pgcluster TUI | 配置: %s | 刷新: %s" % (config.path, time.strftime("%Y-%m-%d %H:%M:%S")),
        "按 q 退出，Ctrl-C 也可退出",
        "connections=%s  transactions=%s  cache_hit=%s  max_lag=%s" %
        (operational["connections"], operational["transactions"],
         "%s%%" % operational["cache_hit"] if operational["cache_hit"] is not None else "未知",
         operational["max_lag_bytes"] if operational["max_lag_bytes"] is not None else "未知"),
        "",
        "集群状态",
        "类型      集群                         状态       复制指标",
        "--------  ---------------------------  ---------  ----------------------------------------",
    ]
    type_names = {"streaming": "流复制", "logical": "逻辑复制", "citus": "Citus", "mmr": "MMR"}
    for kind, target, summary, metric, detail in clusters:
        tone = {"运行中": "32", "部分停止": "33", "已停止": "31", "未知": "90"}[summary]
        lines.append("%-8s  %-27s  %-9s  %s" %
                     (type_names[kind], target, _paint(summary, tone, color), metric[:48]))
    lines.extend(("", "实例状态", "实例                         状态       endpoint" ,
                  "---------------------------  ---------  -----------------------"))
    for name, state in states.items():
        label = "运行中" if state["running"] is True else "已停止" if state["running"] is False else "未知"
        tone = {"运行中": "32", "已停止": "31", "未知": "90"}[label]
        instance = config.instance(name)
        endpoint = "%s:%s" % (instance["host_config"]["address"], instance["port"])
        lines.append("%-27s  %-9s  %s" % (name, _paint(label, tone, color), endpoint))
    return "\n".join(lines)


def run_text(config, runtime, refresh_seconds=3, once=False, target=None):
    refresh_seconds = max(1, int(refresh_seconds))
    interactive = sys.stdout.isatty()
    color = interactive and not os.environ.get("NO_COLOR")
    try:
        while True:
            if interactive:
                sys.stdout.write("\033[2J\033[H")
            sys.stdout.write(render(config, runtime, color=color, target=target) + "\n")
            sys.stdout.flush()
            if once:
                return 0
            if interactive:
                ready, _, _ = select.select([sys.stdin], [], [], refresh_seconds)
                if ready and sys.stdin.readline().strip().lower() == "q":
                    return 0
            else:
                time.sleep(refresh_seconds)
    except KeyboardInterrupt:
        return 0


def run(config, runtime, refresh_seconds=3, once=False, text=False, target=None):
    if text:
        return run_text(config, runtime, refresh_seconds, once, target)
    try:
        from .tui_textual import run as run_textual
    except ImportError:
        sys.stderr.write(
            "ERROR: 全屏 TUI 需要可选依赖 Textual。安装: pip install -e '.[tui]'\n"
            "可使用 ./pgcluster tui --text 运行无依赖文本监控。\n"
        )
        return 1
    return run_textual(config, runtime, refresh_seconds, once, target)
