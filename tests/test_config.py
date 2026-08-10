import os
import unittest
from copy import deepcopy
from pathlib import Path

import yaml

from pgclusterlib.config import Config, load
from pgclusterlib.errors import ConfigError


class ConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).parents[1]
        cls.raw = yaml.safe_load((cls.root / "pgcluster.yaml").read_text(encoding="utf-8"))

    def test_complete_example_validates(self):
        root = Path(__file__).parents[1]
        config = load(root / "pgcluster.yaml")
        self.assertEqual(config.closure("c1"), ["publisher", "subscriber", "c1"])
        self.assertEqual(config.logical_names("c1")["slot"], "c1_failover_slot")
        self.assertEqual(config.closure("c3"), ["c3"])
        self.assertEqual(config.logical_node("c3", "publisher"), "c31")
        self.assertEqual(config.logical_node("c3", "subscriber"), "c32")
        self.assertEqual(config.topology.action_order("c3"), ["c31", "c32", "c3"])
        self.assertEqual(config.topology.external_dependents("publisher"), ["c1"])

    def test_environment_value_is_whole_field(self):
        original = os.environ.get("PGDATA1")
        os.environ["PGDATA1"] = "/tmp/example"
        try:
            config = load(Path(__file__).parents[1] / "pgcluster.yaml")
            self.assertEqual(config.node("pub1")["data_dir"], "/tmp/example")
        finally:
            if original is None:
                os.environ.pop("PGDATA1", None)
            else:
                os.environ["PGDATA1"] = original

    def test_logical_boolean_must_be_boolean(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c3"]["failover"] = "false"
        with self.assertRaisesRegex(ConfigError, "必须是布尔值"):
            Config("memory.yaml", raw)

    def test_replication_mode_defaults_to_async(self):
        config = Config("memory.yaml", deepcopy(self.raw))
        self.assertEqual(config.replication_mode("c1"), "async")

    def test_invalid_replication_mode_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c2"]["replication_mode"] = "automatic"
        with self.assertRaisesRegex(ConfigError, "async 或 sync"):
            Config("memory.yaml", raw)

    def test_sync_streaming_requires_standby(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c2"]["replication_mode"] = "sync"
        raw["clusters"]["c2"]["standbys"] = []
        with self.assertRaisesRegex(ConfigError, "至少需要一个备库"):
            Config("memory.yaml", raw)

    def test_synchronous_commit_requires_sync_mode(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c3"]["synchronous_commit"] = "remote_apply"
        with self.assertRaisesRegex(ConfigError, "仅能在"):
            Config("memory.yaml", raw)

    def test_failover_requires_publisher_standby(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c3"]["failover"] = True
        with self.assertRaisesRegex(ConfigError, "带备库"):
            Config("memory.yaml", raw)

    def test_node_and_cluster_names_cannot_overlap(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c31"] = {"type": "streaming", "primary": "c21"}
        with self.assertRaisesRegex(ConfigError, "不能重复"):
            Config("memory.yaml", raw)

    def test_duplicate_standby_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["clusters"]["c2"]["standbys"].append(
            {"node": "c22", "slot": "another_slot"}
        )
        with self.assertRaisesRegex(ConfigError, "重复节点"):
            Config("memory.yaml", raw)

    def test_duplicate_physical_slot_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["nodes"]["c23"] = {
            "host": "host1",
            "port": 16434,
            "data_dir": "/data/c2/c23",
        }
        raw["clusters"]["c2"]["standbys"].append(
            {"node": "c23", "slot": "c22_physical_slot"}
        )
        with self.assertRaisesRegex(ConfigError, "重复物理槽"):
            Config("memory.yaml", raw)

    def test_same_data_dir_is_allowed_on_different_hosts(self):
        raw = deepcopy(self.raw)
        raw["hosts"]["host2"] = "192.0.2.2"
        raw["nodes"]["c31"]["host"] = "host2"
        raw["nodes"]["c31"]["data_dir"] = raw["nodes"]["c21"]["data_dir"]
        config = Config("memory.yaml", raw)
        self.assertEqual(config.node("c31")["host"], "host2")
