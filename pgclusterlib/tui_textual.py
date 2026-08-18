"""Full-screen deployment map for pgcluster."""

import re

from rich.align import Align
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


def _pid(state):
    message = state.get("message", "") if state else ""
    match = re.search(r"\bPID\s*:\s*(\d+)|\bpid\s*=\s*(\d+)", message, re.IGNORECASE)
    return (match.group(1) or match.group(2)) if match else "-"


def _database_node(config, states, name, label):
    """Render exactly one PostgreSQL instance as one box."""
    state = states.get(name, {})
    running = state.get("running")
    status = "运行中" if running is True else "已停止" if running is False else "未知"
    instance = config.instance(name)
    content = Text.from_markup(
        "[b bright_cyan]▣ %s[/]\n"
        "[%s]● %s[/]\n"
        "[cyan]%s[/]\n"
        "[dim]%s:%s  pid=%s[/]" %
        (label, _tone(status), status, _short_instance(name),
         instance["host_config"]["address"], instance["port"], _pid(state))
    )
    return Panel(content, border_style=_tone(status), width=46, padding=(0, 1))


def _physical_arrow():
    return Text("\n  ────▶\n PHYSICAL", style="bold bright_cyan", justify="center")


def _stream_pair(config, runtime, states, stream, label):
    primary = runtime._primary(stream)
    standbys = runtime._streaming_standbys(stream)
    nodes = [_database_node(config, states, primary, "%s PRIMARY" % label)]
    for standby in standbys:
        nodes.extend((_physical_arrow(), _database_node(config, states, standby, "%s STANDBY" % label)))
    return Columns(nodes, expand=False)


def _logical_arrow():
    return Text("  │\n  ▼  LOGICAL REPLICATION", style="bold yellow")


def _lane(title, status, content, width):
    return Panel(Align.center(content), title=title, title_align="left", border_style=_tone(status),
                 padding=(0, 1), width=width)


def _deployment_map(config, runtime, states, targets):
    """Render all configured databases and their dependency relationships."""
    blocks = []
    target_set = set(targets)
    standalone_streams = [target.split(".", 1)[1] for target in targets
                          if target.startswith("streaming.")]
    for stream in standalone_streams:
        status = _stream_status(runtime, states, stream)
        blocks.append(_lane("物理流复制  %s" % stream, status,
                            _stream_pair(config, runtime, states, stream, "STREAM"), 110))

    for name, link in config.logical_replications.items():
        if "logical.%s" % name not in target_set:
            continue
        pub = link["pub"]["streaming_cluster"]
        sub = link["sub"]["streaming_cluster"]
        statuses = [_stream_status(runtime, states, pub), _stream_status(runtime, states, sub)]
        status = "运行中" if all(item == "运行中" for item in statuses) else "部分停止"
        blocks.append(_lane("逻辑复制  %s" % name, status,
                            Group(_stream_pair(config, runtime, states, pub, "PUB"),
                                  _logical_arrow(),
                                  _stream_pair(config, runtime, states, sub, "SUB")), 110))

    for name, cluster in config.citus_clusters.items():
        if "citus.%s" % name not in target_set:
            continue
        coordinator = cluster["coordinator"]["streaming_cluster"]
        workers = [("WORKER %s" % worker, value["streaming_cluster"])
                   for worker, value in cluster["workers"].items()]
        streams = [coordinator] + [stream for _label, stream in workers]
        statuses = [_stream_status(runtime, states, stream) for stream in streams]
        status = "运行中" if all(item == "运行中" for item in statuses) else "部分停止"
        nodes = [_stream_pair(config, runtime, states, coordinator, "COORD")]
        nodes.append(Text("     ╰═════════ CITUS DISTRIBUTION ═════════▶", style="bold yellow"))
        nodes.extend(_stream_pair(config, runtime, states, stream, label) for label, stream in workers)
        blocks.append(_lane("Citus  %s" % name, status, Group(*nodes), 110))

    for name, cluster in config.mmr_clusters.items():
        if "mmr.%s" % name not in target_set:
            continue
        members = [("MMR MEMBER %s" % member, value["streaming_cluster"])
                   for member, value in cluster["members"].items()]
        statuses = [_stream_status(runtime, states, stream) for _label, stream in members]
        status = "运行中" if all(item == "运行中" for item in statuses) else "部分停止"
        nodes = []
        for index, (label, stream) in enumerate(members):
            if index:
                nodes.append(Text("     ╰═════════ MMR MULTI-MASTER ═════════↔", style="bold yellow"))
            nodes.append(_stream_pair(config, runtime, states, stream, label))
        blocks.append(_lane("MMR  %s" % name, status, Group(*nodes), 110))

    return Group(*blocks)


class PgClusterApp(App):
    CSS = """
    Screen { background: #10161c; color: #d8e2ea; }
    #topology { height: 1fr; margin: 1 2; padding: 1; border: round #2b7189; overflow: hidden; content-align: center middle; }
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
        yield Static(id="topology")

    def _snapshot(self):
        targets = _topology_targets(self.config, self.target)
        names = []
        for target in targets:
            for name in self.runtime.target_instances(target):
                if name not in names:
                    names.append(name)
        return _instance_states(self.config, self.runtime, names), targets

    def on_mount(self):
        if self.once:
            self.set_timer(0.05, self.refresh_once)
        else:
            self.refresh_dashboard()
            self.set_interval(self.refresh_seconds, self.refresh_dashboard)

    def refresh_once(self):
        states, targets = self._snapshot()
        self.apply_snapshot(states, targets)
        self.set_timer(0.15, self.exit)

    def action_quit(self):
        self.exit()

    def action_refresh_now(self):
        self.refresh_dashboard()

    @work(thread=True, exclusive=True)
    def refresh_dashboard(self):
        states, targets = self._snapshot()
        self.call_from_thread(self.apply_snapshot, states, targets)

    def apply_snapshot(self, states, targets):
        self.query_one("#topology", Static).update(
            Align.center(_deployment_map(self.config, self.runtime, states, targets), vertical="middle")
        )
        if self.once:
            self.exit()


def run(config, runtime, refresh_seconds=3, once=False, target=None):
    PgClusterApp(config, runtime, refresh_seconds, once, target).run()
    return 0
