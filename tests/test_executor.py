import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pgclusterlib.executor import Executor, OperationLog


class ExecutorTest(unittest.TestCase):
    @patch("pgclusterlib.executor.subprocess.run")
    def test_remote_write_uses_ssh_and_forwards_content(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        executor = Executor()
        executor.write_text("db.example", "/data/node/pgcluster.conf", "port = 5432\n")
        args, kwargs = run.call_args
        self.assertEqual(args[0][:3], ["ssh", "db.example", "--"])
        self.assertEqual(kwargs["input"], "port = 5432\n")

    @patch("pgclusterlib.executor.subprocess.run")
    def test_operation_log_contains_file_content(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operation.log"
            log = OperationLog(path)
            try:
                Executor(log).write_text(
                    "127.0.0.1", "/data/node/pgcluster.conf", "port = 5432\n"
                )
            finally:
                log.close()
            content = path.read_text(encoding="utf-8")
        self.assertIn("STDIN:\nport = 5432", content)
