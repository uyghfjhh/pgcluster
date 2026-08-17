import argparse
import os
import sys
import time
from pathlib import Path

from .config import load
from .errors import PgClusterError
from .locking import configuration_lock
from .runtime import Runtime
from . import tui


def parser():
    result = argparse.ArgumentParser(prog="pgcluster")
    result.add_argument("-f", "--file", help="配置文件（默认优先 ./pgcluster.local.yaml，再回退 ./pgcluster.yaml）")
    sub = result.add_subparsers(dest="command")
    validate = sub.add_parser("validate")
    validate.add_argument("target", help="目标，例如 citus.citus_cluster")
    graph = sub.add_parser("graph")
    graph.add_argument("target", help="目标，例如 citus.citus_cluster")
    listing = sub.add_parser("list", help="以表格列出配置集群及实例状态")
    help_command = sub.add_parser("help", help="显示全部或指定命令的帮助")
    help_command.add_argument("topic", nargs="?", help="命令名，例如 create、list、status")
    doctor = sub.add_parser("doctor")
    doctor.add_argument("instance", nargs="?", help="实例名；省略则检查全部实例")
    install = sub.add_parser("install")
    install.add_argument("installation", help="postgresql_installations 中的安装名")
    install.add_argument("--force", action="store_true", help="即使插件已安装也重新编译安装")
    status = sub.add_parser("status")
    status.add_argument("target", help="实例名或集群目标，例如 streaming.basic_cluster")
    health = sub.add_parser("health")
    health.add_argument("target", help="实例名或集群目标")
    lag = sub.add_parser("lag")
    lag.add_argument("target", help="集群目标，例如 streaming.basic_cluster")
    verify = sub.add_parser("verify")
    verify.add_argument("target", help="集群目标，例如 mmr.mmr_cluster")
    start = sub.add_parser("start")
    start.add_argument("target", help="实例名或集群目标")
    stop = sub.add_parser("stop")
    stop.add_argument("target", help="实例名或集群目标")
    restart = sub.add_parser("restart")
    restart.add_argument("target", help="实例名或集群目标")
    clean = sub.add_parser("clean")
    clean.add_argument("target", help="实例名或集群目标")
    clean.add_argument("--yes", action="store_true", help="确认删除受管数据目录")
    delete = sub.add_parser("delete")
    delete.add_argument("target", help="实例名或集群目标")
    delete.add_argument("--yes", action="store_true", help="确认删除受管数据目录")
    failover = sub.add_parser("failover")
    failover.add_argument("target", help="流复制集群，例如 streaming.basic_cluster")
    failover.add_argument("--yes", action="store_true", help="确认切换主备角色")
    failover.add_argument("--force", action="store_true", help="使用 immediate 停止旧主库")
    switchover = sub.add_parser("switchover")
    switchover.add_argument("target", help="流复制集群，例如 streaming.basic_cluster")
    switchover.add_argument("--yes", action="store_true", help="确认切换主备角色")
    rejoin = sub.add_parser("rejoin")
    rejoin.add_argument("target", help="流复制集群，例如 streaming.basic_cluster")
    rejoin.add_argument("--yes", action="store_true", help="确认重建旧主库数据目录")
    monitor = sub.add_parser("monitor")
    monitor.add_argument("target", help="实例或集群目标")
    monitor.add_argument("--once", action="store_true", help="只检查一次")
    monitor.add_argument("--interval", type=float, default=5.0, help="检查间隔秒数（默认 5）")
    tui_command = sub.add_parser("tui", help="启动实时集群监控界面")
    tui_command.add_argument("target", nargs="?", help="可选集群目标，例如 logical.pub_sub")
    tui_command.add_argument("--refresh", type=float, default=3.0, help="刷新间隔秒数（默认 3）")
    tui_command.add_argument("--once", action="store_true", help="只渲染一次后退出")
    tui_command.add_argument("--text", action="store_true", help="使用无依赖文本监控界面")
    create = sub.add_parser("create")
    create.add_argument("target", help="目标，例如 streaming.basic_cluster")
    return result


def main(argv=None):
    command_parser = parser()
    args = command_parser.parse_args(argv)
    try:
        if not args.command:
            raise PgClusterError("必须指定命令")
        if args.command == "help":
            if not args.topic:
                print(command_parser.format_help())
                return 0
            choices = next(action for action in command_parser._subparsers._group_actions).choices
            if args.topic not in choices:
                raise PgClusterError("未知命令: %s" % args.topic)
            print(choices[args.topic].format_help())
            return 0
        config_path = Path(args.file) if args.file else Path("pgcluster.local.yaml")
        if args.file is None and not config_path.exists():
            config_path = Path("pgcluster.yaml")
        config = load(config_path)
        if args.command == "validate":
            print(config.validate(args.target))
            return 0
        if args.command == "graph":
            print(config.graph(args.target))
            return 0
        if args.command == "list":
            deployment = Runtime(config).deployment_states()
            use_color = sys.stdout.isatty() and not os.environ.get("NO_COLOR")
            print("配置文件: %s" % config.path)
            print()
            print(config.list_tree(deployment=deployment, color=use_color))
            return 0
        runtime = Runtime(config, progress=lambda message: print("==> %s" % message, flush=True))
        if args.command == "tui":
            return tui.run(config, runtime, args.refresh, args.once, args.text, args.target)
        if args.command == "doctor":
            names = [args.instance] if args.instance else sorted(config.instances)
            healthy = True
            for name in names:
                checks = runtime.doctor_instance(name)
                healthy = healthy and all(checks.values())
                print("%s: %s" % (name, "ok" if all(checks.values()) else "; ".join(
                    "%s=%s" % (key, "ok" if value else "missing") for key, value in checks.items())))
            return 0 if healthy else 1
        if args.command == "install":
            with configuration_lock(config):
                print(runtime.install_installation(args.installation, args.force))
            return 0
        if args.command == "status":
            result = runtime.status_display(args.target)
            running = [state["running"] for state in result.values()]
            if any(value is None for value in running):
                summary = "未知"
            elif all(running):
                summary = "运行中"
            elif any(running):
                summary = "部分停止"
            else:
                summary = "已停止"
            use_color = sys.stdout.isatty() and not os.environ.get("NO_COLOR")
            if args.target in config.instances:
                instance = config.instance(args.target)
                state = result[args.target]
                label = "运行中" if state["running"] is True else "已停止" if state["running"] is False else "未知"
                colors = {"运行中": "\033[32m", "已停止": "\033[31m", "未知": "\033[90m"}
                paint = (lambda value: "%s%s\033[0m" % (colors[label], value)) if use_color else (lambda value: value)
                print("实例 %s [%s]" % (args.target, paint(label)))
                print("├─ endpoint: %s:%s" % (instance["host_config"]["address"], instance["port"]))
                print("└─ data: %s" % instance["data_dir"])
            else:
                print(config.list_tree(args.target, {args.target: summary}, color=use_color,
                                       instance_status=result))
            failures = [(name, state["message"]) for name, state in result.items()
                        if state["running"] is None and state["message"]]
            if failures:
                print("\n探测信息")
                for name, message in failures:
                    print("- %s: %s" % (name, message))
            return 0
        if args.command == "health":
            result = runtime.health_target(args.target)
            for name, state in result.items():
                if isinstance(state, dict):
                    print("%s: %s" % (name, "running" if state["running"] else "stopped"))
                else:
                    print("%s: %s" % (name, state))
            if result.get("health") == "failed":
                if result.get("reason"):
                    print("reason: %s" % result["reason"], file=sys.stderr)
                return 1
            return 0
        if args.command == "lag":
            for row in runtime.lag_target(args.target):
                print(" ".join("%s=%s" % item for item in sorted(row.items())))
            return 0
        if args.command == "verify":
            print(runtime.verify_target(args.target))
            return 0
        if args.command in {"failover", "switchover", "rejoin"}:
            with configuration_lock(config):
                if args.command == "rejoin":
                    print(runtime.rejoin(args.target, args.yes))
                else:
                    print(runtime.failover(args.target, args.yes, getattr(args, "force", False)))
            return 0
        if args.command == "monitor":
            if args.interval <= 0:
                raise PgClusterError("monitor.interval 必须大于 0")
            while True:
                result = runtime.health_target(args.target)
                print("%s: %s" % (args.target, result.get("health", "unknown")), flush=True)
                if result.get("reason"):
                    print("reason: %s" % result["reason"], file=sys.stderr, flush=True)
                if args.once:
                    return 0 if result.get("health") == "ok" else 1
                time.sleep(args.interval)
        if args.command in {"start", "stop", "restart", "clean", "delete"}:
            with configuration_lock(config):
                if args.command == "start":
                    print(runtime.start_target(args.target))
                elif args.command == "stop":
                    print(runtime.stop_target(args.target))
                elif args.command == "restart":
                    print(runtime.restart_target(args.target))
                else:
                    print(runtime.clean_target(args.target, args.yes))
            return 0
        if args.command == "create":
            with configuration_lock(config):
                if args.target.startswith("streaming."):
                    name = args.target.split(".", 1)[1]
                    if name not in config.streaming_clusters:
                        raise PgClusterError("未知流复制集群: %s" % name)
                    print(runtime.create_streaming(name))
                elif args.target.startswith("logical."):
                    name = args.target.split(".", 1)[1]
                    if name not in config.logical_replications:
                        raise PgClusterError("未知逻辑复制: %s" % name)
                    print(runtime.create_logical(name))
                elif args.target.startswith("citus."):
                    name = args.target.split(".", 1)[1]
                    if name not in config.citus_clusters:
                        raise PgClusterError("未知 Citus 集群: %s" % name)
                    print(runtime.create_citus(name))
                elif args.target.startswith("mmr."):
                    name = args.target.split(".", 1)[1]
                    if name not in config.mmr_clusters:
                        raise PgClusterError("未知 MMR 集群: %s" % name)
                    print(runtime.create_mmr(name))
                else:
                    raise PgClusterError("create 目标类型无效: %s" % args.target)
            return 0
        raise PgClusterError("不支持的命令: %s" % args.command)
    except PgClusterError as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return 2
    except OSError as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("ERROR: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
