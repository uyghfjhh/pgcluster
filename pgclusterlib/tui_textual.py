"""Textual dashboard for live pgcluster monitoring."""

import time
from collections import defaultdict, deque

from rich.align import Align
from rich.columns import Columns
from rich.console import Group
from rich.panel import Panel
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import DataTable, Footer, Header, Static

from .tui import _spark, format_bytes, snapshot


TYPE_NAMES = {"streaming": "流复制", "logical": "逻辑复制", "citus": "Citus", "mmr": "MMR"}
STATUS_STYLES = {"运行中": "green", "已部署": "green", "部分停止": "yellow",
                 "已停止": "red", "未知": "grey70"}


def _status_style(status):
    return STATUS_STYLES.get(status, "grey70")


def _stream_status(runtime, states, stream):
    values = [states[item] for item in runtime.target_instances("streaming." + stream)]
    if all(item["running"] is True for item in values):
        return "运行中"
    if any(item["running"] is None for item in values):
        return "未知"
    if any(item["running"] for item in values):
        return "部分停止"
    return "已停止"


def _node_card(config, runtime, states, stream, label, width=34):
    """Render a compact stream cluster as a web-like node card."""
    status = _stream_status(runtime, states, stream)
    primary = runtime._primary(stream)
    standbys = runtime._streaming_standbys(stream)
    def short_instance(value):
        value = value.removesuffix("_node")
        for prefix in ("logical_pub_", "logical_sub_", "citus_", "mmr_"):
            if value.startswith(prefix):
                value = value[len(prefix):]
        return value

    display_stream = stream
    for prefix, replacement in (("logical_pub_", "pub_"), ("logical_sub_", "sub_"),
                                ("citus_", ""), ("mmr_", "")):
        if display_stream.startswith(prefix):
            display_stream = replacement + display_stream[len(prefix):]
            break

    standby_text = ", ".join(short_instance(item) for item in standbys) if standbys else "-"
    lines = ["[b]%s[/]  [cyan]%s[/]" % (label, display_stream),
             "[%s]● %s[/]" % (_status_style(status), status),
             "[dim]P[/] %s  [bright_cyan]──▶[/]  [dim]S[/] %s" %
             (short_instance(primary), standby_text)]
    return Panel(Text.from_markup("\n".join(lines)), width=width,
                 border_style=_status_style(status), padding=(0, 1))


def _arrow(label="复制"):
    return Text("\n\n  ─────▶\n\n%s" % label, style="bold bright_cyan", justify="center")


def _down_arrow(label="复制"):
    return Text("     │\n     ▼  %s" % label, style="bold bright_cyan")


def _topology_renderable(config, runtime, states, clusters, width=100):
    """Build a card-and-arrow topology map using Rich renderables."""
    blocks = [Text("DEPLOYMENT MAP", style="bold bright_cyan"),
              Text("P ──▶ S = 流复制       ⇢ = 逻辑复制       颜色 = 进程状态", style="dim")]
    for kind, target, status, _metric, _detail in clusters:
        name = target.split(".", 1)[1]
        title = "%s  %s" % (TYPE_NAMES[kind], target)
        card_width = 34 if width >= 78 else 26
        if kind == "streaming":
            graph = Columns([_node_card(config, runtime, states, name, "流复制集群", card_width)], expand=False)
        elif kind == "logical":
            link = config.logical_replications[name]
            pub = link["pub"]["streaming_cluster"]
            sub = link["sub"]["streaming_cluster"]
            publisher = _node_card(config, runtime, states, pub, "PUBLISHER", card_width)
            subscriber = _node_card(config, runtime, states, sub, "SUBSCRIBER", card_width)
            graph = (Columns([publisher, _arrow("逻辑复制"), subscriber], expand=False)
                     if width >= 78 else Group(Align.left(publisher), _down_arrow("逻辑复制"), Align.left(subscriber)))
        elif kind == "citus":
            cluster = config.citus_clusters[name]
            coordinator = cluster["coordinator"]["streaming_cluster"]
            workers = [("WORKER %s" % worker, item["streaming_cluster"])
                       for worker, item in cluster["workers"].items()]
            rows = [Columns([_node_card(config, runtime, states, coordinator, "COORDINATOR", card_width)], expand=False)]
            rows.extend(Columns([Text("          │\n          ▼", style="bold bright_cyan"),
                                 _node_card(config, runtime, states, stream, label, card_width)], expand=False)
                        for label, stream in workers)
            graph = Group(*rows)
        else:
            members = [("MEMBER %s" % member, item["streaming_cluster"])
                       for member, item in config.mmr_clusters[name]["members"].items()]
            nodes = [_node_card(config, runtime, states, stream, label, card_width)
                     for label, stream in members]
            graph = (Columns(nodes, expand=False) if width >= 78
                     else Group(*[Align.left(node) for node in nodes]))
        blocks.append(Panel(graph, title=title, title_align="left",
                            border_style=_status_style(status), padding=(1, 1)))
    return Group(*blocks)


class PgClusterApp(App):
    CSS = """
    Screen { background: #10161c; color: #d8e2ea; }
    Header { background: #102a36; color: #dff8ff; }
    #title { height: 2; padding: 0 2; background: #17232d; color: #9de7f5; text-style: bold; }
    #overview { height: 6; margin: 0 2; padding: 1; border: round #8d6d2e; color: #f6d889; }
    #body { height: 1fr; padding: 1 2; }
    #upper { height: 2fr; min-height: 13; }
    #topology { width: 62%; border: round #2b7189; padding: 1; color: #c7d7e4; }
    #topology_content { padding: 0; }
    #right { width: 38%; margin-left: 1; }
    #cluster_cards { height: 1fr; border: round #2b7189; padding: 1 2; }
    #cluster_cards_content { padding: 0; }
    #instances { height: 1fr; min-height: 6; margin: 1 2; border: round #2b7189; }
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
            with VerticalScroll(id="topology"):
                yield Static(id="topology_content")
            with Vertical(id="right"):
                with VerticalScroll(id="cluster_cards"):
                    yield Static(id="cluster_cards_content")
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
        cards = ["[b cyan]CLUSTER HEALTH[/]", "[dim]复制链路健康与当前延迟[/]", ""]
        for kind, target, status, metric, detail in clusters:
            tone = self._tone(status)
            lag_or_lsn = (format_bytes(detail.get("lag_bytes")) if detail.get("lag_bytes") is not None
                          else detail.get("latest_lsn") or "-")
            cards.extend(("[%s]▌[/] [b]%s[/]  [%s]● %s[/]" % (tone, TYPE_NAMES[kind], tone, status),
                          "  [cyan]%s[/]" % target,
                          "  [dim]%s | %s[/]" % (detail.get("replication", metric), lag_or_lsn), ""))
        self.query_one("#cluster_cards_content", Static).update(Text.from_markup("\n".join(cards)))
        topology_view = self.query_one("#topology", VerticalScroll)
        self.query_one("#topology_content", Static).update(
            _topology_renderable(self.config, self.runtime, states, clusters, topology_view.size.width)
        )
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
