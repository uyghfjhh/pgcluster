import argparse
import sys
import time

from .config import load
from .errors import PgClusterError
from .locking import configuration_lock
from .runtime import Runtime


def parser():
    result = argparse.ArgumentParser(prog="pgcluster")
    result.add_argument("-f", "--file", default="pgcluster.yaml", help="配置文件（默认 ./pgcluster.yaml）")
    sub = result.add_subparsers(dest="command")
    validate = sub.add_parser("validate")
    validate.add_argument("target", help="目标，例如 citus.citus_cluster")
    graph = sub.add_parser("graph")
    graph.add_argument("target", help="目标，例如 citus.citus_cluster")
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
    create = sub.add_parser("create")
    create.add_argument("target", help="目标，例如 streaming.basic_cluster")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if not args.command:
            raise PgClusterError("必须指定命令")
        config = load(args.file)
        if args.command == "validate":
            print(config.validate(args.target))
            return 0
        if args.command == "graph":
            print(config.graph(args.target))
            return 0
        runtime = Runtime(config)
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
            result = runtime.status_target(args.target)
            for name, state in result.items():
                print("%s: %s%s" % (name, "running" if state["running"] else "stopped",
                                    (" (%s)" % state["message"]) if state["message"] else ""))
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
