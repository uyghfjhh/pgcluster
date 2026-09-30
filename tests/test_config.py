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

    def test_list_tree_expands_logical_replication(self):
        tree = load(self.root / "pgcluster.yaml").list_tree("logical.pub_sub")
        self.assertIn("logical.pub_sub [逻辑复制]", tree)
        self.assertIn("pub: streaming.logical_pub_cluster", tree)
        self.assertIn("sub: streaming.logical_sub_cluster", tree)

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

    def test_mmr_can_use_only_its_required_extension(self):
        raw = deepcopy(self.raw)
        raw["mmr_clusters"]["mmr_cluster"]["extensions"] = ["fdd_mmr"]
        self.assertEqual(Config("memory.yaml", raw).validate("mmr.mmr_cluster"),
                         "配置有效: mmr.mmr_cluster")
        raw["mmr_clusters"]["mmr_cluster"]["extensions"] = ["citus"]
        with self.assertRaisesRegex(ConfigError, "fdd_mmr"):
            Config("memory.yaml", raw)

    def test_streaming_application_name_is_configurable(self):
        raw = deepcopy(self.raw)
        standby = raw["streaming_clusters"]["mmr_a_cluster"]["standbys"][0]
        standby["application_name"] = "pg_240"
        self.assertEqual(Config("memory.yaml", raw).validate("mmr.mmr_cluster"),
                         "配置有效: mmr.mmr_cluster")
        standby["application_name"] = "bad name"
        with self.assertRaisesRegex(ConfigError, "application_name"):
            Config("memory.yaml", raw)

    def test_hba_rules_are_validated(self):
        raw = deepcopy(self.raw)
        raw["postgresql_config"]["hba"] = [
            {"type": "local", "database": "all", "user": "all", "auth_method": "trust"},
            {"type": "host", "database": "all", "user": "u1", "address": "0.0.0.0/0", "auth_method": "scram-sha-256"},
        ]
        self.assertEqual(Config("memory.yaml", raw).validate("mmr.mmr_cluster"),
                         "配置有效: mmr.mmr_cluster")
        raw["postgresql_config"]["hba"][1]["address"] = "invalid address"
        with self.assertRaisesRegex(ConfigError, "hba.*address"):
            Config("memory.yaml", raw)

    def test_duplicate_instance_endpoint_is_rejected(self):
        raw = deepcopy(self.raw)
        raw["instances"]["duplicate_node"] = deepcopy(raw["instances"]["basic_primary_node"])
        with self.assertRaisesRegex(ConfigError, "重复实例端口"):
            Config("memory.yaml", raw)

    def test_unknown_installation_lists_the_available_names(self):
        raw = deepcopy(self.raw)
        raw["instances"]["basic_primary_node"]["installation"] = "postgresql18"
        with self.assertRaisesRegex(ConfigError, "basic_primary_node.*postgresql18.*postgresql17"):
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

    def test_host_ssh_options_are_validated(self):
        raw = deepcopy(self.raw)
        host = raw["hosts"]["pg17_host"]
        host["ssh"] = {
            "user": "postgres",
            "port": 2222,
            "identity_file": "/keys/id",
            "connect_timeout": 3,
        }
        self.assertIn("streaming.basic_cluster", Config("memory.yaml", raw).validate("streaming.basic_cluster"))
        for bad, field in (
            ({"user": "bad name!"}, "user"),
            ({"port": 0}, "port"),
            ({"identity_file": "relative/key"}, "identity_file"),
            ({"connect_timeout": 0}, "connect_timeout"),
            ({"password": "x"}, "未知字段"),
        ):
            raw = deepcopy(self.raw)
            raw["hosts"]["pg17_host"]["ssh"] = bad
            with self.assertRaisesRegex(ConfigError, field):
                Config("memory.yaml", raw)

    def test_duplicate_host_addresses_are_rejected(self):
        raw = deepcopy(self.raw)
        raw["hosts"]["alias_host"] = {"address": raw["hosts"]["pg17_host"]["address"]}
        with self.assertRaisesRegex(ConfigError, "重复"):
            Config("memory.yaml", raw)


if __name__ == "__main__":
    unittest.main()
