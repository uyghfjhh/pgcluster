import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock

from pgclusterlib.config import Config, load
from pgclusterlib.errors import OperationError, SafetyError
from pgclusterlib.manager import Manager


class ManagerTest(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).parents[1]
        self.config = load(root / "pgcluster.yaml")
        self.manager = Manager(self.config)

    def test_graph_direct_logical_has_real_action_order(self):
        graph = self.manager.graph("c3")
        self.assertIn("CLUSTER c3 [logical, async]", graph)
        self.assertIn("├── publisher: c31", graph)
        self.assertIn("c3 -> c31", graph)
        self.assertIn("c31 -> host1", graph)
        self.assertIn("CREATE ORDER\nc31\nc32\nc3", graph)

    def test_clean_dependency_is_checked_before_filesystem(self):
        self.manager.executor.exists = Mock(side_effect=AssertionError("filesystem touched"))
        with self.assertRaisesRegex(SafetyError, "c1"):
            self.manager.clean("publisher", yes=True)
        self.manager.executor.exists.assert_not_called()

    def test_non_failover_does_not_wait_for_slot_sync(self):
        self.manager.psql = Mock(side_effect=AssertionError("psql called"))
        self.manager._wait_for_slot_sync("c3")
        self.manager.psql.assert_not_called()

    def test_sync_streaming_waits_for_configured_standby(self):
        raw = deepcopy(self.config.raw)
        raw["clusters"]["c2"]["replication_mode"] = "sync"
        manager = Manager(Config("memory.yaml", raw))
        manager.psql = Mock(return_value="")

        manager._apply_synchronous_replication("c21")

        sql = [call.args[1] for call in manager.psql.call_args_list]
        self.assertIn(
            "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (\"c22\")'",
            sql,
        )
        self.assertIn("ALTER SYSTEM SET synchronous_commit = 'remote_apply'", sql)

    def test_sync_logical_waits_for_subscription_application_name(self):
        raw = deepcopy(self.config.raw)
        raw["clusters"]["c3"]["replication_mode"] = "sync"
        manager = Manager(Config("memory.yaml", raw))
        manager.psql = Mock(return_value="1")

        manager._apply_synchronous_replication("c31")

        sql = [call.args[1] for call in manager.psql.call_args_list]
        self.assertIn(
            "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (\"c3_sub\")'",
            sql,
        )

    def test_fbase_provider_fails_explicitly(self):
        self.config.postgres["provider"] = "fbase"
        manager = Manager(self.config)
        with self.assertRaisesRegex(OperationError, "fbase provider 尚未实现"):
            manager.doctor("c1")

    def test_mmr_clean_fails_instead_of_reporting_false_success(self):
        raw = deepcopy(self.config.raw)
        raw["clusters"]["mmr1"] = {"type": "mmr", "nodes": ["c31", "c32"]}
        manager = Manager(Config("memory.yaml", raw))
        with self.assertRaisesRegex(OperationError, "MMR provider 尚未实现"):
            manager.clean("mmr1", yes=True)
