import shutil
import unittest
from pathlib import Path

from pgclusterlib.config import load
from pgclusterlib.errors import OperationError, SafetyError
from pgclusterlib.runtime import Runtime


class Result:
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


class RestoreExecutor:
    """Scripted executor: live roles and upstreams keyed by instance name."""

    def __init__(self, config, roles, upstreams=None, running=None, managed=True):
        self.config = config
        self.roles = dict(roles)
        self.upstreams = dict(upstreams or {})
        self.running = set(running if running is not None else roles)
        self.managed = managed
        self.commands = []
        self.removed = []

    @staticmethod
    def is_local(address):
        return address in {"127.0.0.1", "localhost"}

    def _port(self, name):
        return self.config.instance(name)["port"]

    def _name_by_dir(self, data_dir):
        for name in self.config.instances:
            if self.config.instance(name)["data_dir"] == data_dir:
                return name
        raise AssertionError("未知数据目录: %s" % data_dir)

    def run(self, args, host="local", **kwargs):
        self.commands.append(args)
        result = Result()
        exe = Path(args[0]).name
        if exe == "pg_ctl":
            name = self._name_by_dir(args[args.index("-D") + 1])
            action = args[1]
            if action == "status":
                result.returncode = 0 if name in self.running else 3
            elif action == "start":
                self.running.add(name)
            elif action == "stop":
                self.running.discard(name)
            elif action == "promote":
                self.roles[name] = "f"
        elif exe == "psql":
            port = int(args[args.index("-p") + 1])
            name = next(n for n in self.config.instances if self._port(n) == port)
            sql = args[-1]
            if "pg_is_in_recovery" in sql:
                result.stdout = self.roles.get(name, "f") + "\n"
            elif "primary_conninfo" in sql:
                result.stdout = self.upstreams.get(name, "") + "\n"
            else:
                result.stdout = "1\n"
        return result

    def is_nonempty_dir(self, host, path):
        return self.managed

    def exists(self, host, path):
        return self.managed

    def read_text(self, host, path, check=True):
        return "# existing\n"

    def write_text(self, host, path, content, mode="0600"):
        self.commands.append(["write", path])

    def append_text(self, host, path, content):
        pass

    def remove_tree(self, host, path):
        self.removed.append(path)


class RestoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self.id().split(".")[-1] + ".tmp.yaml")
        shutil.copy(str(Path(__file__).parents[1] / "pgcluster.yaml"), str(self.tmp))
        self.config = load(self.tmp)
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        for path in (self.tmp,
                     self.tmp.with_name("." + self.tmp.name + ".state.json")):
            try:
                path.unlink()
            except OSError:
                pass

    def _upstream(self, name, primary="basic_primary_node"):
        parent = self.config.instance(primary)
        return "host=%s port=%s user=postgres application_name=%s" % (
            parent["host_config"]["address"], parent["port"], name)

    def _commands(self, executor, *exe_names):
        return [args for args in executor.commands
                if args and Path(str(args[0])).name in exe_names]

    def test_no_drift_is_noop(self):
        executor = RestoreExecutor(
            self.config,
            {"basic_primary_node": "f", "basic_standby_node": "t"},
            upstreams={"basic_standby_node": self._upstream("basic_standby_node")},
        )
        result = Runtime(self.config, executor).restore("streaming.basic_cluster", yes=True)
        self.assertIn("角色与配置一致", result)
        self.assertEqual(self._commands(executor, "pg_basebackup"), [])

    def test_promoted_standby_is_rebuilt(self):
        executor = RestoreExecutor(
            self.config,
            {"basic_primary_node": "f", "basic_standby_node": "f"},
        )
        result = Runtime(self.config, executor).restore("streaming.basic_cluster", yes=True)
        self.assertIn("basic_standby_node", result)
        basebackups = self._commands(executor, "pg_basebackup")
        self.assertEqual(len(basebackups), 1)
        standby_dir = self.config.instance("basic_standby_node")["data_dir"]
        self.assertIn(standby_dir, executor.removed)

    def test_configured_primary_in_recovery_is_promoted(self):
        executor = RestoreExecutor(
            self.config,
            {"basic_primary_node": "t", "basic_standby_node": "t"},
            upstreams={"basic_standby_node": self._upstream("basic_standby_node")},
        )
        result = Runtime(self.config, executor).restore("streaming.basic_cluster", yes=True)
        self.assertIn("basic_primary_node", result)
        promotes = [a for a in self._commands(executor, "pg_ctl") if "promote" in a]
        self.assertEqual(len(promotes), 1)

    def test_wrong_upstream_is_repointed(self):
        executor = RestoreExecutor(
            self.config,
            {"basic_primary_node": "f", "basic_standby_node": "t"},
            upstreams={"basic_standby_node": "host=127.0.0.1 port=9999 user=postgres"},
        )
        result = Runtime(self.config, executor).restore("streaming.basic_cluster", yes=True)
        self.assertIn("basic_standby_node", result)
        self.assertEqual(self._commands(executor, "pg_basebackup"), [])
        sql = [a[-1] for a in self._commands(executor, "psql")]
        self.assertTrue(any("ALTER SYSTEM SET primary_conninfo" in item for item in sql))

    def test_unmanaged_diverged_node_reports_unrepairable(self):
        executor = RestoreExecutor(
            self.config,
            {"basic_primary_node": "f", "basic_standby_node": "f"},
            managed=False,
        )
        with self.assertRaises(OperationError) as ctx:
            Runtime(self.config, executor).restore("streaming.basic_cluster", yes=True)
        self.assertIn("basic_standby_node", str(ctx.exception))
        self.assertEqual(executor.removed, [])

    def test_down_standby_is_started_and_kept(self):
        executor = RestoreExecutor(
            self.config,
            {"basic_primary_node": "f", "basic_standby_node": "t"},
            upstreams={"basic_standby_node": self._upstream("basic_standby_node")},
            running={"basic_primary_node"},
        )
        Runtime(self.config, executor).restore("streaming.basic_cluster", yes=True)
        starts = [a for a in self._commands(executor, "pg_ctl") if "start" in a]
        self.assertEqual(len(starts), 1)
        self.assertEqual(self._commands(executor, "pg_basebackup"), [])

    def test_requires_yes(self):
        executor = RestoreExecutor(self.config, {"basic_primary_node": "f"})
        with self.assertRaises(SafetyError):
            Runtime(self.config, executor).restore("streaming.basic_cluster")

    def test_unknown_target_rejected(self):
        executor = RestoreExecutor(self.config, {})
        with self.assertRaises(OperationError):
            Runtime(self.config, executor).restore("streaming.missing", yes=True)
        with self.assertRaises(OperationError):
            Runtime(self.config, executor).restore("logical.pub", yes=True)


if __name__ == "__main__":
    unittest.main()
