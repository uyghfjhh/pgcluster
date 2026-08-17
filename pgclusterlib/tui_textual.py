"""Textual dashboard for live pgcluster monitoring."""

import time
from collections import defaultdict, deque

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Static

from .tui import _spark, format_bytes, snapshot


class PgClusterApp(App):
    CSS = """
    Screen { background: #10161c; color: #d8e2ea; }
    Header { background: #102a36; color: #dff8ff; }
    #title { height: 2; padding: 0 2; background: #17232d; color: #9de7f5; text-style: bold; }
    #overview { height: 6; margin: 0 2; padding: 1; border: round #8d6d2e; color: #f6d889; }
    #body { height: 1fr; padding: 1 2; }
    #upper { height: 1fr; }
    #topology { width: 62%; border: round #2b7189; padding: 1; color: #c7d7e4; overflow-y: auto; }
    #right { width: 38%; margin-left: 1; }
    #cluster_cards { height: 1fr; border: round #2b7189; padding: 1 2; overflow-y: auto; }
    #instances { height: 2fr; min-height: 8; margin: 1 2; border: round #2b7189; }
    DataTable { scrollbar-size: 1 1; }
    Footer { background: #17232d; color: #a8c5d3; }
    .ok { color: #55d98c; }
    .warn { color: #f6c453; }
    .bad { color: #ff6b6b; }
    .unknown { color: #94a3b8; }
    """
    BINDINGS = [("q", "quit", "退出"), ("r", "refresh_now", "刷新")]

    def __init__(self, config, runtime, refresh_seconds, once=False, target=None):
        super().__init__()
        self.config = config
        self.runtime = runtime
        self.refresh_seconds = max(1, int(refresh_seconds))
        self.once = once
        self.target = target
        self.history = defaultdict(lambda: deque(maxlen=24))

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("PGCLUSTER LIVE MONITOR  |  正在采样...", id="title")
        with Vertical(id="body"):
          yield Static(id="overview")
          with Horizontal(id="upper"):
            yield Static(id="topology")
            with Vertical(id="right"):
                yield Static(id="cluster_cards")
          yield DataTable(id="instances")
        yield Footer()

    def on_mount(self):
        instances = self.query_one("#instances", DataTable)
        instances.cursor_type = "none"
        instances.add_columns("实例", "角色", "状态", "Endpoint", "数据目录")
        if self.once:
            # `--once` is useful to scripts and smoke tests. Run it on the UI
            # thread so the application exits deterministically after one draw.
            self.set_timer(0.05, self.refresh_once)
        else:
            self.refresh_dashboard()
            self.set_interval(self.refresh_seconds, self.refresh_dashboard)

    def refresh_once(self):
        states, clusters, operational = snapshot(self.config, self.runtime, self.target)
        self.apply_snapshot(states, clusters, operational)
        self.set_timer(0.15, self.exit)

    def action_quit(self):
        self.exit()

    def action_refresh_now(self):
        self.refresh_dashboard()

    @work(thread=True, exclusive=True)
    def refresh_dashboard(self):
        states, clusters, operational = snapshot(self.config, self.runtime, self.target)
        self.call_from_thread(self.apply_snapshot, states, clusters, operational)

    @staticmethod
    def _tone(status):
        return {"运行中": "ok", "已部署": "ok", "部分停止": "warn", "已停止": "bad"}.get(status, "unknown")

    def apply_snapshot(self, states, clusters, operational):
        title = self.query_one("#title", Static)
        up = sum(1 for state in states.values() if state["running"] is True)
        down = sum(1 for state in states.values() if state["running"] is False)
        unknown = len(states) - up - down
        title.update("PGCLUSTER LIVE MONITOR   |   %s   |   实例: %d up  %d down  %d unknown   |   %s" %
                     (self.config.path, up, down, unknown, time.strftime("%H:%M:%S")))
        for key in ("connections", "transactions", "cache_hit", "max_lag_bytes"):
            value = operational.get(key)
            if isinstance(value, (int, float)):
                self.history[key].append(value)
        counters = list(self.history["transactions"])
        tps = (max(0, counters[-1] - counters[-2]) / float(self.refresh_seconds)
               if len(counters) > 1 else 0)
        max_connections = operational.get("max_connections")
        connections = operational.get("connections", 0)
        connection_usage = (100.0 * connections / max_connections
                            if max_connections else None)
        overview = ("[b yellow]LIVE KPI[/]  [dim]采样 %ss | 历史 24 点[/]\n"
                    "连接  [b]%s%s[/]  %s  [dim]%s[/]\n"
                    "TPS   [b]%.1f[/]       %s  [dim]%s[/]\n"
                    "缓存命中率  [b]%s[/]  %s  [dim]%s[/]\n"
                    "最大 WAL 延迟  [b]%s[/]  %s" %
                    (self.refresh_seconds, connections,
                     "/%s (%.0f%%)" % (max_connections, connection_usage) if connection_usage is not None else "",
                     _spark(self.history["connections"]), "所有运行主库",
                     tps, _spark([max(0, counters[i] - counters[i - 1]) for i in range(1, len(counters))]),
                     "事务增量 / 秒",
                     ("%s%%" % operational["cache_hit"] if operational.get("cache_hit") is not None else "未知"),
                     _spark(self.history["cache_hit"]), "数据库共享缓存",
                     format_bytes(operational.get("max_lag_bytes")),
                     _spark(self.history["max_lag_bytes"])))
        self.query_one("#overview", Static).update(Text.from_markup(overview))
        type_names = {"streaming": "流复制", "logical": "逻辑复制", "citus": "Citus", "mmr": "MMR"}
        cards = ["[b cyan]CLUSTER HEALTH[/]", "[dim]复制链路健康与当前延迟[/]", ""]
        for kind, target, status, metric, detail in clusters:
            tone = self._tone(status)
            lag_or_lsn = (format_bytes(detail.get("lag_bytes")) if detail.get("lag_bytes") is not None
                          else detail.get("latest_lsn") or "-")
            cards.extend(("[%s]▌[/] [b]%s[/]  [%s]● %s[/]" % (tone, type_names[kind], tone, status),
                          "  [cyan]%s[/]" % target,
                          "  [dim]%s | %s[/]" % (detail.get("replication", metric), lag_or_lsn), ""))
        self.query_one("#cluster_cards", Static).update(Text.from_markup("\n".join(cards)))
        topology = ["[b cyan]DEPLOYMENT GRAPH[/]", "[dim]箭头表示依赖和复制方向；颜色表示进程状态[/]", ""]
        for kind, target, status, _, _detail in clusters:
            topology.append("[dim]╭──────────────────────────────────────────────────────╮[/]")
            topology.append("[%s]● %s[/]  [dim]%s[/]" % (self._tone(status), target, status))
            kind_name, name = target.split(".", 1)
            refs = []
            if kind_name == "logical":
                link = self.config.logical_replications[name]
                refs = [("pub", link["pub"]["streaming_cluster"]), ("sub", link["sub"]["streaming_cluster"])]
            elif kind_name == "citus":
                cluster = self.config.citus_clusters[name]
                refs = [("coordinator", cluster["coordinator"]["streaming_cluster"])] + [
                    ("worker %s" % worker, value["streaming_cluster"])
                    for worker, value in cluster["workers"].items()]
            elif kind_name == "mmr":
                refs = [("member %s" % member, value["streaming_cluster"])
                        for member, value in self.config.mmr_clusters[name]["members"].items()]
            if not refs and kind_name == "streaming":
                refs = [("primary + standby", name)]
            for pos, (label, stream) in enumerate(refs):
                stream_target = "streaming.%s" % stream
                stream_states = [states[item] for item in self.runtime.target_instances(stream_target)]
                stream_status = "运行中" if all(s["running"] is True for s in stream_states) else \
                    "未知" if any(s["running"] is None for s in stream_states) else "部分停止" if any(s["running"] for s in stream_states) else "已停止"
                branch = "└─" if pos == len(refs) - 1 else "├─"
                arrow = "──►"
                topology.append("  %s %s %s [cyan]%s[/]  [%s]%s[/]" %
                                (branch, label.ljust(12), arrow, stream, self._tone(stream_status), stream_status))
                primary = self.runtime._primary(stream)
                standbys = self.runtime._streaming_standbys(stream)
                primary_endpoint = self.config.instance(primary)
                topology.append("  %s    [dim]primary[/] %s [dim]%s:%s[/]" %
                                ("│" if pos != len(refs) - 1 else " ", primary,
                                 primary_endpoint["host_config"]["address"], primary_endpoint["port"]))
                if standbys:
                    topology.append("  %s    [dim]standby[/] %s" %
                                    ("│" if pos != len(refs) - 1 else " ", ", ".join(standbys)))
            topology.append("[dim]╰──────────────────────────────────────────────────────╯[/]")
            topology.append("")
        self.query_one("#topology", Static).update(Text.from_markup("\n".join(topology)))
        instance_table = self.query_one("#instances", DataTable)
        instance_table.clear()
        roles = {}
        for target in self.runtime.config.streaming_clusters:
            cluster = self.runtime.config.streaming_clusters[target]
            roles[cluster["primary"]] = "primary"
            for standby in cluster.get("standbys") or []:
                roles[standby["instance"]] = "standby"
        for name, state in states.items():
            instance = self.config.instance(name)
            label = "运行中" if state["running"] is True else "已停止" if state["running"] is False else "未知"
            instance_table.add_row(name, roles.get(name, "-"), Text(label, style=self._tone(label)),
                                   "%s:%s" % (instance["host_config"]["address"], instance["port"]),
                                   instance["data_dir"])
        if self.once:
            self.exit()


def run(config, runtime, refresh_seconds=3, once=False, target=None):
    PgClusterApp(config, runtime, refresh_seconds, once, target).run()
    return 0
