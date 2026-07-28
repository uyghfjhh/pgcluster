import argparse
import sys

from .config import load
from .errors import PgClusterError
from .manager import Manager


def parser():
    result = argparse.ArgumentParser(prog="pgcluster")
    result.add_argument("-f", "--file", default="pgcluster.yaml", help="配置文件（默认 ./pgcluster.yaml）")
    sub = result.add_subparsers(dest="command", required=True)
    for name in ("validate", "graph", "doctor", "create", "status", "verify", "start", "stop", "restart"):
        command = sub.add_parser(name)
        command.add_argument("cluster")
    clean = sub.add_parser("clean")
    clean.add_argument("cluster")
    clean.add_argument("--yes", action="store_true")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        config = load(args.file)
        manager = Manager(config)
        if args.command in {"doctor", "create", "status", "verify", "start", "stop", "restart", "clean"}:
            manager.open_log()
        try:
            if args.command == "validate":
                output = manager.validate(args.cluster)
            elif args.command == "graph":
                output = manager.graph(args.cluster)
            elif args.command == "doctor":
                output = manager.doctor(args.cluster)
            elif args.command == "create":
                output = manager.create(args.cluster)
            elif args.command == "status":
                output = manager.status(args.cluster)
            elif args.command == "verify":
                output = manager.verify(args.cluster)
            elif args.command == "start":
                output = manager.start(args.cluster)
            elif args.command == "stop":
                output = manager.stop(args.cluster)
            elif args.command == "restart":
                output = manager.restart(args.cluster)
            else:
                output = manager.clean(args.cluster, args.yes)
        finally:
            manager.close_log()
        print(output)
        return 0
    except PgClusterError as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("ERROR: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
