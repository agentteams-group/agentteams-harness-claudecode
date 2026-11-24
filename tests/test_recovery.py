import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

HARNESS = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("AGENTTEAMS_CORE_DIR", HARNESS.parent))
sys.path[:0] = [str(HARNESS), str(ROOT / "deepseek-harness/scripts"), str(ROOT / "shared/lib")]
import bridge
import turn


class WorkerRegression(unittest.TestCase):
    def test_equal_text_is_not_a_duplicate_input(self):
        events = [{"event_id": eid, "sender": "@human:test", "room_id": "!room:test", "body": "Create a report"}
                  for eid in ("$one", "$two", "$two")]
        self.assertEqual([e["event_id"] for e in bridge.manager_inputs(events)], ["$one", "$two"])

    def test_preparation_timeout_redelivery_uses_same_event(self):
        state, client = Mock(), Mock()
        state.data = {"events": {}, "sessions": {}}
        event = {"event_id": "$input", "room_id": "!r:test", "body": "hello"}
        member = {"member": {"role": "manager", "matrixUserId": "@manager:test"}}
        client.send_text.return_value = "$answer"
        with tempfile.TemporaryDirectory() as tmp, patch("bridge.require_unencrypted_room"), \
             patch("bridge.subprocess.run", side_effect=[subprocess.TimeoutExpired("prepare", 60),
                   Mock(stdout='{"sources": []}')]) as prepare, patch("bridge.run_turn", return_value=("native", "ok")), \
             patch.dict(os.environ, AGENTTEAMS_PLUGIN_DIR=tmp):
            state.workspace = Path(tmp)
            with self.assertRaises(subprocess.TimeoutExpired):
                bridge.execute(state, client, member, {}, event, set())
            self.assertFalse(state.data["events"]["$input"].get("delivered"))
            client.send_text.assert_not_called()
            bridge.execute(state, client, member, {}, event, set())
            bridge.execute(state, client, member, {}, event, set())
            self.assertEqual(prepare.call_count, 2)
            self.assertEqual([json.loads(c.kwargs["input"])["event"]["event_id"] for c in prepare.call_args_list],
                             ["$input", "$input"])
            client.send_text.assert_called_once()
            self.assertTrue(state.data["events"]["$input"]["delivered"])

    def test_sdk_control_timeout_retries_the_same_poll_with_a_bound(self):
        # The pinned SDK wraps TimeoutError during its initialize request.
        try:
            try:
                raise TimeoutError()
            except TimeoutError as cause:
                raise Exception("Control request timeout: initialize") from cause
        except Exception as error:
            timeout = error
        with patch.dict(os.environ, TEAMHARNESS_RUNTIME_CONFIG="/fixture"), \
             patch("bridge.MemberState") as state, patch("bridge.trace"), patch("bridge.time.sleep") as wait, \
             patch("bridge.poll", side_effect=timeout) as poll:
            with self.assertRaisesRegex(Exception, "Control request timeout"):
                bridge.main()
            self.assertEqual(poll.call_count, 3)
            self.assertTrue(all(call.args == (state.return_value, False) for call in poll.call_args_list))
            self.assertEqual(wait.call_count, 2)
            poll.reset_mock()
            poll.side_effect = Exception("unrelated SDK error")
            with self.assertRaisesRegex(Exception, "unrelated SDK error"):
                bridge.main()
            self.assertEqual(poll.call_count, 1)

    def test_only_current_prepared_inputs_are_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            task = Path(tmp) / "tasks" / "child"
            out = task / "outputs"
            batch = task / "inputs" / "batch" / "parent"
            batch.mkdir(parents=True)
            out.mkdir()
            source = batch / "ledger.csv"
            source.write_text("a,b")
            (out / "alias").symlink_to(source)
            context = {"taskDirectory": str(task), "outputDirectory": str(out), "scratchDirectory": str(out / ".scratch"),
                       "inputs": [{"readOnly": True, "directory": str(batch), "files": [{"localPath": str(source)}]}]}
            for name, path, expected in (("Read", source, True), ("Write", source, False), ("Edit", out / "alias", False),
                                        ("Read", batch.parent / "stale" / "ledger.csv", False),
                                        ("Read", task / "inputs", False), ("Write", out / "result.csv", True)):
                with self.subTest(tool=name, path=path):
                    self.assertEqual(turn.file_allowed(name, {"file_path": str(path)}, out, (), context), expected)

    def test_empty_sync_discovers_work_even_on_restart(self):
        state, client = Mock(), Mock()
        state.registered_start = False
        state.data = {"cursor": "previous", "events": {}, "sessions": {}}
        record = {"member": {"role": "worker", "matrixUserId": "@w:test"}}
        transport = {"homeserver": "local", "accessToken": "test"}
        client.sync.return_value = {"next_batch": "previous"}
        with patch.dict(os.environ, AGENTTEAMS_CONTROLLER_URL="local", AGENTTEAMS_WORKER_NAME="w"), \
             patch("bridge.WorkerMatrix"), patch("bridge.load_member", return_value=(record, transport, {})), \
             patch("bridge.MatrixClient", return_value=client), patch("bridge.managed_rooms", return_value=set()), \
             patch("bridge.subprocess.run"), patch("bridge.scan", return_value=(Mock(), [], {})), \
             patch("bridge.recover_work") as recover:
            bridge.poll(state, False)
            recover.assert_called_once()
            self.assertEqual(state.data["cursor"], "previous")

class NativeRecovery(unittest.TestCase):
    def setUp(self):
        import task_worker
        self.adapter = task_worker
        self.recovery = self.enterContext(patch("task_worker.Recovery")).return_value
        self.recovery.process = "p2"
        self.rec = {"member": {"matrixUserId": "@cc:test", "role": "worker"}}
        self.execution = {"runtime": "claudecode", "process_id": "p2", "instance_id": "i2", "generation": 4,
                          "state": "active", "checkpoint": None}
        self.task = {"task_plan_id": "plan", "task_id": "child", "team_id": "team", "status": "in_progress",
                     "notifyTxn": "assign", "main_task_id": "main", "room_id": "!room:test", "execution": self.execution}
        self.snap = {"instance_id": "i2", "tasks": [{"task": self.task, "team_id": "team", "etag": "v"}]}
        self.recovery.discover.return_value = self.snap
        self.recovery.claim.return_value = {"task": self.task, "saved_result": "stored answer"}
        self.sessions = self.enterContext(patch("task_worker.WorkerSessions")).return_value
        self.sessions.load.return_value = {"generation": 4, "checkpoint_digest": self.recovery.digest(None)}
        self.sessions.usable.return_value = False
        self.completed = self.enterContext(patch("task_worker.recovery_scanned"))
        self.go = self.enterContext(patch("task_worker.run_turn", return_value=("id", "answer")))
        self.rpc = self.enterContext(patch("task_worker.native"))
        self.client = Mock()
        self.state = Mock()

    def run_scan(self):
        self.adapter.recover_work(self.state, self.client, self.rec, {}, {})

    def test_pending_delivers_exact_saved_bytes_without_inference(self):
        self.execution["checkpoint"] = {"delivery": "pending"}
        with patch("task_worker.deliver") as delivery:
            self.run_scan()
        delivery.assert_called_once_with(self.client, self.recovery, self.rec, self.task, "team", "stored answer")
        self.go.assert_not_called()
        self.rpc.assert_not_called()

    def test_unverified_or_replaced_owner_never_enters_model(self):
        for field, value in (("runtime", "qwenpaw"), ("process_id", "stale"), ("state", "needs_verification")):
            with self.subTest(field=field), patch.dict(self.execution, {field: value}):
                self.run_scan()
        self.go.assert_not_called()
        self.rpc.assert_not_called()

    def test_replacement_reuses_only_compatible_native_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.state.projects = root / "projects"
            self.state.projects.mkdir()
            self.sessions.load.return_value["cache"] = {"native_ref": "6ab33aae-6bba-4dde-98f7-1470288419ba"}
            self.recovery.resume_text.return_value = "confirmed facts"
            self.rpc.side_effect = [{"ok": True}, {"execution": "child", "context": {"outputDirectory": directory}, "fact": "inputs ready"}] * 2
            self.recovery.current.return_value = None
            with patch.dict(os.environ, AGENTTEAMS_PLUGIN_DIR=directory), patch("task_worker.require_unencrypted_room"):
                self.run_scan()
                self.assertIsNone(self.go.await_args.args[4])
                ref = self.sessions.load.return_value["cache"]["native_ref"]
                log = self.state.projects / "scope" / (ref + ".jsonl")
                log.parent.mkdir()
                log.write_text('{"type":"user"}')
                self.sessions.usable.side_effect = lambda view, name, digest: bool(name == ref and digest)
                self.run_scan()
                self.assertEqual(self.go.await_args.args[4], ref)
            self.assertEqual(self.go.await_args.args[5]["identity"]["generation"], 4)

    def test_lost_claim_skips_obsolete_work(self):
        from task_recovery import RecoveryConflict
        self.recovery.claim.side_effect = RecoveryConflict("old etag")
        self.run_scan()
        self.completed.assert_called_once()
        self.go.assert_not_called()

    def test_handoff_from_another_harness_uses_checkpoint_not_foreign_cache(self):
        from worker_sessions import WorkerSessions
        import time
        with tempfile.TemporaryDirectory() as directory:
            self.state.projects = Path(directory) / "projects"
            self.state.projects.mkdir()
            view = self.sessions.load.return_value
            view.update(runtime="claudecode", compatible=True, checkpoint_version=3, config_generation=1,
                        cache={"runtime": "qwenpaw", "native_ref": "qwen-session", "native_digest": "foreign",
                               "checkpoint_version": 3, "checkpoint_digest": view["checkpoint_digest"],
                               "config_generation": 1, "expires_at": int(time.time()) + 300})
            self.sessions.usable.side_effect = WorkerSessions("claudecode", recovery=self.recovery).usable
            self.recovery.resume_text.return_value = "authoritative saved checkpoint"
            self.rpc.side_effect = [{"ok": True}, {"execution": "child", "context": {"outputDirectory": directory}, "fact": "prepared"}]
            self.recovery.current.return_value = None
            with patch.dict(os.environ, AGENTTEAMS_PLUGIN_DIR=directory), patch("task_worker.require_unencrypted_room"):
                self.run_scan()
            self.assertIsNone(self.go.await_args.args[4])
            self.assertIn("authoritative saved checkpoint", self.go.await_args.args[3])

class InterruptedRead(unittest.TestCase):
    def test_only_matching_native_terminal_result_reconciles(self):
        from task_worker import verified_read_result
        from task_recovery import Recovery
        args = {"file_path": "/tmp/result.csv"}
        call = {"name": "Read", "id": "tool-1", "read_only": True, "arguments_sha256": Recovery.digest(args)}
        cp = {"operation": {"state": "started", "evidence": json.dumps([call])}}
        with tempfile.TemporaryDirectory() as d:
            projects = Path(d) / "projects"
            workspace = Path(d) / "tasks/child/workspace"
            transcript = projects / "encoded-cwd" / "ref.jsonl"
            transcript.parent.mkdir(parents=True)
            def row(role, block):
                return json.dumps({"type": role, "cwd": str(workspace), "sessionId": "ref",
                                   "message": {"role": role, "content": [block]}}) + "\n"
            start = row("assistant", {"type": "tool_use", "name": "Read", "id": "tool-1", "input": args})
            done = row("user", {"type": "tool_result", "tool_use_id": "tool-1", "content": "verified bytes"})
            transcript.write_text(start)
            self.assertIsNone(verified_read_result(projects, workspace, cp))
            transcript.write_text(start + done)
            self.assertEqual(verified_read_result(projects, workspace, cp)["id"], "tool-1")
            self.assertIsNone(verified_read_result(projects, workspace.parent, cp))
            for changes in ({"arguments_sha256": "other"}, {"id": "other"}, {"read_only": False}, {"name": "Write"}):
                invalid = {"operation": {"state": "started", "evidence": json.dumps([{**call, **changes}])}}
                self.assertIsNone(verified_read_result(projects, workspace, invalid))
