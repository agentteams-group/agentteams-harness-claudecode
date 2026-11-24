"""Check Claude Code's binding of verified Matrix notifications to native Task phases."""
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

HARNESS = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("AGENTTEAMS_CORE_DIR", HARNESS.parent))


class TaskPhases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Production loads TeamHarness in a separate process. Isolate its mcp
        # package from the SDK's identically named package during discovery.
        cls.enterClassContext(patch.dict(sys.modules))
        cls.enterClassContext(patch.object(sys, "path", [str(HARNESS), str(ROOT / "shared/lib"), *sys.path]))
        cls.enterClassContext(patch.dict(os.environ, AGENTTEAMS_PLUGIN_DIR=str(ROOT / "plugins/teamharness")))
        for name in list(sys.modules):
            if name == "mcp" or name.startswith("mcp."):
                del sys.modules[name]
        import team_tools
        cls.adapter = team_tools

    def setUp(self):
        self.rpc = self.enterContext(patch.object(self.adapter.task_native, "invoke"))
        self.recovery = self.enterContext(patch("task_recovery.Recovery")).return_value
        self.recovery.process = "native"
        self.entry = {"team_id": "alpha", "task": {"task_id": "child", "notifyTxn": "assignment",
                      "room_id": "!room:at", "status": "assigned"}}
        self.snapshot = {"tasks": [self.entry], "instance_id": "container"}
        self.recovery.discover.return_value = self.snapshot
        self.execution = {"state": "active", "process_id": "native"}
        self.recovery.claim.return_value = {"task": {"execution": self.execution}}

    def prepare(self, kind, role):
        packet = {"kind": kind, "id": "receipt", "task_id": "child", "delivery_id": "assignment"}
        event = {"event_id": "$input", "room_id": "!room:at", "_task_protocol_meta": {"teamharness_coordination": packet}}
        return self.adapter.prepare(event, role)

    def test_leader_phases_require_admission(self):
        for kind in ("handoff", "task_submitted", "main_task_ready"):
            with self.subTest(kind=kind):
                self.rpc.reset_mock()
                self.rpc.side_effect = [{"active": True, "pending_task_ids": ["main"]}, {"fact": "prepared"}]
                self.assertEqual(self.prepare(kind, "leader")["fact"], "prepared")
                calls = [call.args[0] for call in self.rpc.call_args_list]
                self.assertEqual([req["operation"] for req in calls], ["admit_event", "prepare_event"])
                if kind == "handoff":
                    self.assertEqual(calls[-1]["event"]["pending_task_ids"], ["main"])
        self.recovery.claim.assert_not_called()

    def test_worker_claims_exact_assignment_before_begin(self):
        def prepare_event(request):
            self.recovery.claim.assert_called_once_with(self.snapshot, self.entry)
            self.assertEqual(request["operation"], "prepare_event")
            return {"fact": "prepared"}
        self.rpc.side_effect = prepare_event
        self.assertEqual(self.prepare("task_assigned", "worker")["fact"], "prepared")

    def test_stale_or_unverified_execution_cannot_begin(self):
        self.entry["task"]["notifyTxn"] = "newer-assignment"
        self.assertIsNone(self.prepare("task_assigned", "worker"))
        self.recovery.claim.assert_not_called()
        self.entry["task"]["notifyTxn"] = "assignment"
        self.execution["state"] = "needs_verification"
        with self.assertRaisesRegex(ValueError, "requires verification"):
            self.prepare("task_assigned", "worker")
        self.rpc.assert_not_called()


if __name__ == "__main__":
    unittest.main()
