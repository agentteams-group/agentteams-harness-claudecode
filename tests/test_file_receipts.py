import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

HARNESS = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("AGENTTEAMS_CORE_DIR", HARNESS.parent))
sys.path[:0] = [str(HARNESS), str(ROOT / "shared/lib"), str(ROOT / "deepseek-harness/scripts")]
from worker_files import stage_output, persist_terminal, load_terminal


class FileReceiptTests(unittest.TestCase):
    def test_upload_failure_recovers_exact_written_bytes_without_rerunning_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            workspace.mkdir()
            output = workspace / "report.json"
            output.write_text('{"total":42}')
            identity = {"team_id": "team", "task_id": "task", "generation": 1}
            operation = "tools:" + "a" * 64
            env = {"TEAMHARNESS_RUNTIME_CONFIG": str(root / "runtime/runtime.yaml"),
                   "AGENTTEAMS_STORAGE_PREFIX": "test/bucket", "AGENTTEAMS_WORKER_NAME": "cc"}
            with patch.dict(os.environ, env), patch("worker_files.subprocess.run") as run:
                artifact, staged = stage_output(identity, workspace, "report.json", operation)
                result = {"id": "native-call", "content": [artifact]}
                run.side_effect = subprocess.CalledProcessError(1, ["mc", "cp"])
                with self.assertRaises(subprocess.CalledProcessError):
                    persist_terminal(identity, operation, result, [artifact], files=[staged])
                # Simulate workspace rollback/replacement before recovery.
                output.write_text("older workspace snapshot")
                uploaded = {}
                def storage(args, **kwargs):
                    if args[1] == "cp":
                        uploaded[args[-1]] = Path(args[2]).read_bytes()
                    elif args[1] == "pipe":
                        uploaded[args[-1]] = kwargs["input"]
                    else:
                        self.fail(f"Unexpected recovery storage command: {args}")
                    return Mock(stdout=b"")
                run.side_effect = storage
                saved = load_terminal({**identity, "generation": 2}, operation, "native-call")
                self.assertEqual(saved["result"], result)
                self.assertEqual(uploaded["test/bucket/" + artifact["key"]], b'{"total":42}')
                self.assertEqual(output.read_text(), "older workspace snapshot")

    def test_receipt_cannot_be_reused_by_another_call_or_task(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "TEAMHARNESS_RUNTIME_CONFIG": str(Path(directory) / "runtime/runtime.yaml"),
                "AGENTTEAMS_STORAGE_PREFIX": "test/bucket", "AGENTTEAMS_WORKER_NAME": "cc"}), \
                patch("worker_files.subprocess.run", return_value=Mock(stdout=b"")):
            identity = {"team_id": "team", "task_id": "task", "generation": 2}
            operation = "tools:" + "b" * 64
            persist_terminal(identity, operation, {"id": "call-1", "content": {}}, [])
            with self.assertRaisesRegex(ValueError, "does not match"):
                load_terminal(identity, operation, "call-2")
