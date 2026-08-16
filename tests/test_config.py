import unittest
from copy import deepcopy
from pathlib import Path

import yaml

from pgclusterlib.config import Config, load
from pgclusterlib.errors import ConfigError


class ConfigModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).parents[1]
        cls.raw = yaml.safe_load((cls.root / "pgcluster.yaml").read_text(encoding="utf-8"))

    def test_complete_config_validates(self):
        config = load(self.root / "pgcluster.yaml")
        self.assertIsInstance(config, Config)
        self.assertNotIn("schema_version", config.raw)
        self.assertEqual(config.validate("streaming.basic_cluster"), "配置有效: streaming.basic_cluster")
        self.assertEqual(config.validate("logical.pub_sub"), "配置有效: logical.pub_sub")
        self.assertEqual(config.validate("citus.citus_cluster"), "配置有效: citus.citus_cluster")
        self.assertEqual(config.validate("mmr.mmr_cluster"), "配置有效: mmr.mmr_cluster")

    def test_transport_is_inferred(self):
        config = load(self.root / "pgcluster.yaml")
        self.assertNotIn("transport", config.hosts["pg17_host"])
        self.assertNotIn("transport", config.hosts["fbase15_host"])

    def test_invalid_citus_factor_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["citus_clusters"]["citus_cluster"]["postgresql_config"]["parameters"][
            "citus.shard_replication_factor"
        ] = 3
        with self.assertRaisesRegex(ConfigError, "shard_replication_factor"):
            Config("memory.yaml", raw)

    def test_invalid_mmr_streaming_mode_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["mmr_clusters"]["mmr_cluster"]["members"]["node_a"]["mmr_node"]["streaming"] = "serial"
        with self.assertRaisesRegex(ConfigError, "streaming 无效"):
            Config("memory.yaml", raw)

    def test_duplicate_instance_endpoint_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["instances"]["duplicate_node"] = deepcopy(raw["instances"]["basic_primary_node"])
        with self.assertRaisesRegex(ConfigError, "重复实例端口"):
            Config("memory.yaml", raw)

    def test_relative_plugin_source_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["postgresql_installations"]["fbase15"]["plugins"]["fbase_mac"]["source_dir"] = "contrib/fbase_mac"
        with self.assertRaisesRegex(ConfigError, "source_dir 必须是绝对路径"):
            Config("memory.yaml", raw)

    def test_license_data_file_must_stay_in_pgdata(self):
        raw = deepcopy(self.raw)
        raw["postgresql_installations"]["fbase15"]["license"]["data_file"] = "../license.dat"
        with self.assertRaisesRegex(ConfigError, "PGDATA 内的文件名"):
            Config("memory.yaml", raw)


if __name__ == "__main__":
    unittest.main()
