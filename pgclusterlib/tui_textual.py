"""Full-screen deployment map for pgcluster."""

import time

from rich.columns import Columns
from rich.console import Group
from rich.panel import Panel
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.widgets import Static

from .tui import _instance_states, _topology_targets


STATUS_STYLES = {
    "运行中": "green",
    "部分停止": "yellow",
    "已停止": "red",
    "未知": "grey70",
}


def _stream_status(runtime, states, stream):
    values = [states[item]["running"] for item in runtime.target_instances("streaming." + stream)]
    if all(value is True for value in values):
        return "运行中"
    if any(value is None for value in values):
        return "未知"
    if any(value for value in values):
        return "部分停止"
    return "已停止"


def _tone(status):
    return STATUS_STYLES.get(status, "grey70")


def _short_instance(name):
    name = name.removesuffix("_node")
    aliases = (
        ("logical_pub_", "pub."),
        ("logical_sub_", "sub."),
        ("citus_coordinator_", "coord."),
        ("citus_worker_1_", "worker1."),
        ("citus_worker_2_", "worker2."),
        ("mmr_a_", "mmr-a."),
        ("mmr_b_", "mmr-b."),
        ("basic_", "basic."),
    )
    for prefix, replacement in aliases:
        if name.startswith(prefix):
            return replacement + name[len(prefix):]
    return name


def _database_card(config, runtime, states, stream, label):
    """A compact database-shaped node for a physical streaming pair."""
    status = _stream_status(runtime, states, stream)
    primary = runtime._primary(stream)
    standbys = runtime._streaming_standbys(stream)
    primary_port = config.instance(primary)["port"]
    standby = standbys[0] if standbys else None
    standby_label = "%s :%s" % (_short_instance(standby), config.instance(standby)["port"]) if standby else "-"
    content = Text.from_markup(
        "[b bright_cyan]▣ %s[/]\n"
        "[%s]● %s[/]\n"
        "[b]P[/]  %s :%s\n"
        "[bright_cyan]└─▶[/] [b]S[/]  %s" %
        (label, _tone(status), status, _short_instance(primary), primary_port, standby_label)
    )
    return Panel(content, title="%s" % stream, title_align="left",
                 border_style=_tone(status), width=34, padding=(0, 1))


def _relationship(label, symbol="════════▶"):
    return Text("\n%s\n%s" % (symbol, label), style="bold yellow", justify="center")


def _lane(title, status, content):
    return Panel(content, title=title, title_align="left", border_style=_tone(status), padding=(0, 1))


def _deployment_map(config, runtime, states):
    """Render all configured databases and their dependency relationships."""
    blocks = [
        Text("PGCLUSTER DEPLOYMENT MAP", style="bold bright_cyan", justify="center"),
        Text("卡片内 P(primary) ──▶ S(standby) 是流复制；卡片之间的黄色箭头是上层集群关系。", style="dim", justify="center"),
        Text("", justify="center"),
    ]

    standalone_streams = [target.split(".", 1)[1] for target in _topology_targets(config)
                          if target.startswith("streaming.")]
    for stream in standalone_streams:
        status = _stream_status(runtime, states, stream)
        blocks.append(_lane("物理流复制  %s" % stream, status,
                            Columns([_database_card(config, runtime, states, stream, "STREAMING DATABASE")], expand=False)))

    for name, link in config.logical_replications.items():
        pub = link["pub"]["streaming_cluster"]
        sub = link["sub"]["streaming_cluster"]
        statuses = [_stream_status(runtime, states, pub), _stream_status(runtime, states, sub)]
        status = "运行中" if all(item == "运行中" for item in statuses) else "部分停止"
        blocks.append(_lane("逻辑复制  %s" % name, status,
                            Columns([
                                _database_card(config, runtime, states, pub, "PUBLISHER DATABASE"),
                                _relationship("LOGICAL REPLICATION"),
                                _database_card(config, runtime, states, sub, "SUBSCRIBER DATABASE"),
                            ], expand=False)))

    for name, cluster in config.citus_clusters.items():
        coordinator = cluster["coordinator"]["streaming_cluster"]
        workers = [("WORKER %s" % worker, value["streaming_cluster"])
                   for worker, value in cluster["workers"].items()]
        streams = [coordinator] + [stream for _label, stream in workers]
        statuses = [_stream_status(runtime, states, stream) for stream in streams]
        status = "运行中" if all(item == "运行中" for item in statuses) else "部分停止"
        nodes = [_database_card(config, runtime, states, coordinator, "CITUS COORDINATOR")]
        nodes.append(_relationship("CITUS DISTRIBUTION"))
        nodes.extend(_database_card(config, runtime, states, stream, label) for label, stream in workers)
        blocks.append(_lane("Citus  %s" % name, status, Columns(nodes, expand=False)))

    for name, cluster in config.mmr_clusters.items():
        members = [("MMR MEMBER %s" % member, value["streaming_cluster"])
                   for member, value in cluster["members"].items()]
        statuses = [_stream_status(runtime, states, stream) for _label, stream in members]
        status = "运行中" if all(item == "运行中" for item in statuses) else "部分停止"
        nodes = []
        for index, (label, stream) in enumerate(members):
            if index:
                nodes.append(_relationship("MMR MULTI-MASTER", "═══════↔"))
            nodes.append(_database_card(config, runtime, states, stream, label))
        blocks.append(_lane("MMR  %s" % name, status, Columns(nodes, expand=False)))

    return Group(*blocks)


class PgClusterApp(App):
    CSS = """
    Screen { background: #10161c; color: #d8e2ea; }
    #title { height: 2; padding: 0 2; background: #102a36; color: #9de7f5; text-style: bold; }
    #topology { height: 1fr; margin: 1 2; padding: 1; border: round #2b7189; overflow: hidden; }
    """
    BINDINGS = [("q", "quit", "退出"), ("r", "refresh_now", "刷新")]

    def __init__(self, config, runtime, refresh_seconds, once=False, target=None):
        super().__init__()
        self.config = config
        self.runtime = runtime
        self.refresh_seconds = max(1, int(refresh_seconds))
        self.once = once
        self.target = target

    def compose(self) -> ComposeResult:
        yield Static("PGCLUSTER DEPLOYMENT MAP | 正在读取配置...", id="title")
        yield Static(id="topology")

    def on_mount(self):
        if self.once:
            self.set_timer(0.05, self.refresh_once)
        else:
            self.refresh_dashboard()
            self.set_interval(self.refresh_seconds, self.refresh_dashboard)

    def refresh_once(self):
        states = _instance_states(self.config, self.runtime)
        self.apply_snapshot(states)
        self.set_timer(0.15, self.exit)

    def action_quit(self):
        self.exit()

    def action_refresh_now(self):
        self.refresh_dashboard()

    @work(thread=True, exclusive=True)
    def refresh_dashboard(self):
        states = _instance_states(self.config, self.runtime)
        self.call_from_thread(self.apply_snapshot, states)

    def apply_snapshot(self, states):
        up = sum(1 for item in states.values() if item["running"] is True)
        down = sum(1 for item in states.values() if item["running"] is False)
        unknown = len(states) - up - down
        self.query_one("#title", Static).update(
            "PGCLUSTER DEPLOYMENT MAP  |  全部数据库与复制关系  |  %s  |  %d up  %d down  %d unknown  |  %s" %
            (self.config.path.name, up, down, unknown, time.strftime("%H:%M:%S"))
        )
        self.query_one("#topology", Static).update(_deployment_map(self.config, self.runtime, states))
        if self.once:
            self.exit()


def run(config, runtime, refresh_seconds=3, once=False, target=None):
    PgClusterApp(config, runtime, refresh_seconds, once, target).run()
    return 0
