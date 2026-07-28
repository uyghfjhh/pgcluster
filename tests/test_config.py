import os
import unittest
from pathlib import Path

from pgclusterlib.config import load


class ConfigTest(unittest.TestCase):
    def test_complete_example_validates(self):
        root = Path(__file__).parents[1]
        config = load(root / "pgcluster.yaml")
        self.assertEqual(config.closure("logical1"), ["publisher", "subscriber", "logical1"])
        self.assertEqual(config.logical_names("logical1")["slot"], "logical1_failover_slot")

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
