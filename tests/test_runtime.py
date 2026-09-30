import unittest
from pathlib import Path

from pgclusterlib.config import load
from pgclusterlib.errors import OperationError
from pgclusterlib.runtime import Runtime


class Result:
    returncode = 0
    stdout = ""
    stderr = ""


class PluginExecutor:
    def __init__(self):
        self.installed = set()
        self.commands = []

    @staticmethod
    def is_local(address):
        return address == "127.0.0.1"

    def exists(self, host, path):
        if path.endswith("/bin/pg_config"):
            return True
        return path in self.installed

    @staticmethod
    def is_dir(host, path):
        return path.startswith("/home/postgres/")

    def run(self, args, host="local", **kwargs):
        self.commands.append((host, args))
        if args[:2] == ["sudo", "-n"]:
            source = args[4]
            extension = {
                "fbase_mac": "fbase_mac",
                "fdd_mmr": "fdd_mmr",
                "fb_license": "fb_license",
            }[Path(source).name]
            self.installed.add("/usr/local/fbase15.15/share/extension/%s.control" % extension)
        return Result()


class ManagedStoppedExecutor(PluginExecutor):
    def __init__(self):
        super().__init__()
        self.writes = {}

    def exists(self, host, path):
        return path.endswith(".pgcluster-managed")

    def is_nonempty_dir(self, host, path):
        return True

    def run(self, args, host="local", **kwargs):
        self.commands.append((host, args))
        result = Result()
        if len(args) > 1 and args[1] == "status":
            result.returncode = 3
        return result

    def write_text(self, host, path, content, mode="0600"):
        self.writes[path] = content

    def read_text(self, host, path, check=True):
        return ""

    def append_text(self, host, path, content):
        self.writes[path] = self.writes.get(path, "") + content


class RuntimeInstallTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load(Path(__file__).parents[1] / "pgcluster.yaml")

    def test_install_builds_missing_fbase_plugins(self):
        executor = PluginExecutor()
        result = Runtime(self.config, executor).install_installation("fbase15")
        fbase_host = self.config.hosts["fbase15_host"]["address"]
        self.assertIn("fbase_mac@%s: 已安装" % fbase_host, result)
        self.assertEqual(len([args for _, args in executor.commands if args[0] == "make"]), 3)
        self.assertEqual(len([args for _, args in executor.commands if args[:2] == ["sudo", "-n"]]), 3)

    def test_install_reports_missing_plugin_without_source(self):
        with self.assertRaisesRegex(OperationError, "未配置 source_dir"):
            Runtime(self.config, PluginExecutor()).install_installation("postgresql17")

    def test_retry_rewrites_managed_primary_config_after_start_failure(self):
        executor = ManagedStoppedExecutor()
        Runtime(self.config, executor)._init_primary("basic_primary_node")
        self.assertTrue(any(path.endswith("/pgcluster.conf") for path in executor.writes))
        self.assertTrue(any(args[1] == "start" for _, args in executor.commands))

if __name__ == "__main__":
    unittest.main()
