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
    def test_completed_task_room_is_discovered_for_a_formal_followup(self):
        import worker_sync
        member = {"member": {"matrixUserId": "@worker:test"}, "desired": {}}
        rooms = {}
        raw = {"type": "m.room.message", "event_id": "$followup", "sender": "@human:test",
               "content": {"msgtype": "m.text", "body": "@worker:test status?", "m.mentions": {"user_ids": ["@worker:test"]}}}
        with patch("worker_sync.WorkerMatrix") as worker, patch("worker_sync.RawWorkerSync") as receiver, \
                patch("worker_sync.managed_rooms", return_value={"!completed:test"}), \
                patch("task_coordination.controller_json", return_value={"taskId": "closed", "status": "completed"}) as fetch:
            worker.return_value.pending.return_value = []
            def poll(_):
                self.assertEqual(receiver.call_args.args[4]({}), {"!completed:test"})
                self.assertEqual(receiver.call_args.args[3]("!completed:test", raw), ("message", None))
                return {"state": {"rooms": {}}}, {}
            receiver.return_value.poll.side_effect = poll
            worker_sync.scan(Mock(), member, {"groupPolicy": "open"}, rooms, set(), True, bridge.admitted, {})
            fetch.assert_called_once_with("tasks/room-context?roomId=%21completed%3Atest")

    def test_retained_assignment_is_not_executed_as_an_ordinary_message(self):
        import worker_sync
        event = {"event_id": "$assignment", "room_id": "!r", "_task_protocol_meta": {
            "teamharness_coordination": {"kind": "task_assigned", "task_id": "child"}}}
        row = {"event_id": "$assignment", "room_id": "!r", "intent": "message", "work": {"kind": "message"}, "input": {}}
        member = {"member": {"matrixUserId": "@worker:test"}, "desired": {}}
        with patch("worker_sync.WorkerMatrix") as worker, patch("worker_sync.RawWorkerSync") as receiver, \
                patch("task_coordination.controller_json", return_value={}):
            matrix = worker.return_value
            matrix.pending.return_value = [row]
            matrix.message_claim.return_value = {"recovery": "resume", "identity": {"event_key": "old"},
                                                 "execution": {"checkpoint": None}}
            def poll(_):
                receiver.call_args.args[4]({})
                return {"state": {"rooms": {}}}, {}
            receiver.return_value.poll.side_effect = poll
            with patch("worker_sync.managed_rooms", return_value={"!r"}):
                _, pending, assignments = worker_sync.scan(Mock(), member, {}, {}, set(), True, Mock(return_value=event), {})
            self.assertEqual(pending, [])
            self.assertEqual(assignments, {"child": event})
            matrix.message_cancel.assert_called_once_with({"event_key": "old"})
            matrix.message_claim.return_value["recovery"] = "verify"
            matrix.message_cancel.reset_mock()
            with patch("worker_sync.managed_rooms", return_value={"!r"}):
                _, pending, _ = worker_sync.scan(Mock(), member, {}, {}, set(), True, Mock(return_value=event), {})
            self.assertEqual(pending, [])
            matrix.message_cancel.assert_not_called()

    def test_receipts_without_a_native_project_do_not_break_startup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);config=root/"runtime.yaml"
            def storage(*args):
                if args[0]=="cp":
                    Path(args[-1]).write_text("{}")
                    return ""
                if args[0]=="ls":
                    path=args[-1]
                    if path.endswith("/runtime/"):return ""
                    if path.endswith("/claude-code/"):return '{"key":"receipts/"}\n'
                    return '{"key":"claude-code/"}\n'
                raise AssertionError("Unexpected storage mutation: "+str(args))
            with patch.dict(os.environ,TEAMHARNESS_WORKSPACE=str(root/"workspace"),CLAUDE_CONFIG_DIR=str(root/".claude"),
                            AGENTTEAMS_STORAGE_PREFIX="test/bucket",AGENTTEAMS_WORKER_NAME="cc"), patch("bridge.mc",side_effect=storage):
                state=bridge.MemberState(config)
            self.assertEqual(state.data["events"],{})


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

    def test_sdk_control_timeout_keeps_the_process_and_recovers_with_bounded_backoff(self):
        # The pinned SDK wraps TimeoutError during its initialize request.
        try:
            try:
                raise TimeoutError()
            except TimeoutError as cause:
                raise Exception("Control request timeout: initialize") from cause
        except Exception as error:
            timeout = error
        for failure in (timeout, Exception("unrelated SDK error")):
            with self.subTest(error=str(failure)), \
                 patch.dict(os.environ, TEAMHARNESS_RUNTIME_CONFIG="/fixture"), \
                 patch("telemetry.configure"), patch("bridge.MemberState") as state, \
                 patch("bridge.trace"), patch("bridge.time.sleep") as wait, \
                 patch("bridge.poll", side_effect=[failure] * 4 + [None, KeyboardInterrupt()]) as poll:
                with self.assertRaises(KeyboardInterrupt):
                    bridge.main()
                self.assertEqual(poll.call_count, 6)
                self.assertTrue(all(call.args == (state.return_value, False) for call in poll.call_args_list[:5]))
                self.assertEqual(poll.call_args.args, (state.return_value, True))
                self.assertEqual(wait.call_count, 4)
                self.assertTrue(all(0 <= call.args[0] <= 30 for call in wait.call_args_list))

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

    def test_team_leader_uses_durable_worker_inbox_after_a_runtime_switch(self):
        state = Mock()
        state.data = {"cursor": None, "events": {}}
        record = {"member": {"role": "team_leader", "matrixUserId": "@lead:test"}}
        transport = {"homeserver": "local", "accessToken": "test"}
        event = {"event_id": "$new"}
        claim = {"identity": {"event_key": "new"}}
        matrix = Mock()
        with patch.dict(os.environ, AGENTTEAMS_CONTROLLER_URL="local"), \
             patch("bridge.load_member", return_value=(record, transport, {})), \
             patch("bridge.MatrixClient") as client, \
             patch("bridge.scan", return_value=(matrix, [(event, claim)], {})) as scan, \
             patch("bridge.recover_work"), patch("bridge.execute") as execute:
            bridge.poll(state, True)
            scan.assert_called_once()
            client.return_value.sync.assert_not_called()
            self.assertIs(execute.call_args.kwargs["matrix"], matrix)
            self.assertIs(execute.call_args.kwargs["claim"], claim)

    def test_restart_after_saving_a_quiet_receipt_does_not_publish_it(self):
        import worker_sync
        event = {"event_id": "$plan", "room_id": "!r"}
        claim = {"identity": {"event_key": "key"}, "recovery": "deliver", "saved_result": "receipt fact"}
        row = {**event, "intent": "message", "input": {}}
        member = {"member": {"matrixUserId": "@lead:test"}, "desired": {}}
        with patch("worker_sync.WorkerMatrix") as worker, patch("worker_sync.RawWorkerSync") as receiver, \
             patch("worker_sync.managed_rooms", return_value={"!r"}), patch("worker_sync.deliver_message") as deliver, \
             patch("task_coordination.controller_json", return_value={}):
            matrix = worker.return_value
            matrix.pending.return_value = [row]
            matrix.message_claim.return_value = claim
            def poll(_):
                receiver.call_args.args[4]({})
                return {"state": {"rooms": {}}}, {}
            receiver.return_value.poll.side_effect = poll
            args = (Mock(), member, {}, {}, set(), True, Mock(return_value=event))
            worker_sync.scan(*args, {"$plan": {"silent_receipt": "verified", "answer": "receipt fact"}})
            matrix.message_complete.assert_called_once_with(claim["identity"])
            deliver.assert_not_called()
            worker_sync.scan(*args, {})
            deliver.assert_called_once()

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
            self.rpc.side_effect = [{"ok": True}, {"execution": "child", "context": {"outputDirectory": directory}, "fact": "inputs ready"}, {"outputDirectory": directory}] * 2
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

    def test_direct_completed_submission_still_delivers_immutable_result(self):
        with tempfile.TemporaryDirectory() as directory:
            self.rpc.side_effect = [{"ok": True}, {"execution": "child", "context": {"outputDirectory": directory}, "fact": "ready"}, {"outputDirectory": directory}]
            self.recovery.resume_text.return_value = "confirmed facts"
            saved = {**self.task, "status": "completed"}
            self.recovery.current.return_value = saved
            completed = {**saved, "execution": {**self.execution, "checkpoint": {"delivery": "pending"}}}
            self.recovery.prepare_reply.return_value = (completed, "immutable business report")
            with patch.dict(os.environ, AGENTTEAMS_PLUGIN_DIR=directory), patch("task_worker.require_unencrypted_room"), patch("task_worker.deliver") as delivery:
                self.run_scan()
            self.recovery.prepare_reply.assert_called_once_with("child", "team", "answer")
            self.assertEqual(delivery.call_args.args[-1], "immutable business report")
            interaction = self.go.await_args.kwargs["interaction"]
            self.assertEqual(interaction.run, "task:team:child:4")
            self.assertEqual(self.go.await_args.args[5]["runId"], interaction.run)
            with patch("task_worker.require_unencrypted_room"), patch("task_worker.publish_raw_saved", return_value="$sent") as publish:
                self.adapter.deliver(self.client, self.recovery, self.rec, completed, "team", "immutable business report")
            self.assertEqual(publish.call_args.kwargs["content"]["org.agentteams.run"]["run_id"], interaction.run)

    def test_own_running_tool_and_terminal_feedback_are_not_lost_execution(self):
        for status,state in (("in_progress","needs_verification"),("completed","released")):
            with self.subTest(status=status),tempfile.TemporaryDirectory() as directory:
                self.rpc.side_effect = [{"ok": True}, {"execution": "child", "context": {"outputDirectory": directory}, "fact": "ready"}, {"outputDirectory": directory}]
                self.recovery.resume_text.return_value = "checkpoint"
                pending={**self.task,"status":status,"execution":{**self.execution,"state":state}}
                self.recovery.current.side_effect=[pending,None]
                async def actual_scope(*_args,**kwargs):
                    self.assertTrue(kwargs["interaction"].current(),"The same native process is still running or settling its terminal tool")
                    return "session","answer"
                self.go.side_effect=actual_scope
                with patch.dict(os.environ,AGENTTEAMS_PLUGIN_DIR=directory),patch("task_worker.require_unencrypted_room"):
                    self.run_scan()

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
            self.rpc.side_effect = [{"ok": True}, {"execution": "child", "context": {"outputDirectory": directory}, "fact": "prepared"}, {"outputDirectory": directory}]
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


class OrdinaryConfirmedResumeTests(unittest.TestCase):
    def test_confirmed_tools_resume_instead_of_publishing_an_interruption_fallback(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = Mock(workspace=root, projects=root/'projects')
            state.data = {'events':{'$input':{'stage':'inference','replayable':False,'attempts':1}}, 'sessions':{}}
            event = {'event_id':'$input','room_id':'!room:test','body':'Continue the Task'}
            record = {'member':{'role':'worker','matrixUserId':'@worker:test'}}
            claim = {'identity':{'event_key':'input','generation':1,'instance_id':'fixture','process_id':'fixture'},
                     'execution':{'checkpoint':{'completed_steps':['tools:verified'],'operation':{'state':'verified'}}},
                     'resume_text':'Authoritative checkpoint: previous write already completed.'}
            matrix, client = Mock(), Mock()
            matrix.message_current.return_value = claim
            with patch.dict(os.environ, AGENTTEAMS_PLUGIN_DIR=temporary), \
                    patch('bridge.require_unencrypted_room'), patch('bridge.deliver_message') as deliver, \
                    patch('bridge.subprocess.run', return_value=SimpleNamespace(stdout=json.dumps({'context':{'outputDirectory':temporary}}))), \
                    patch('bridge.native', return_value={'spec':'current Task spec'}), \
                    patch('bridge.WorkerSessions', create=True) as sessions, \
                    patch('bridge.run_turn', return_value=('native-session','Resumed successfully')) as run:
                sessions.return_value.load.return_value = {'cache':None}
                bridge.execute_input(state, client, record, {}, event, set(), matrix, claim)
            run.assert_called_once()
            self.assertIn('previous write already completed', run.call_args.args[3])
            self.assertIsNone(run.call_args.args[4])
            self.assertEqual(matrix.message_save_reply.call_args.args[1], 'Resumed successfully')
            self.assertEqual(deliver.call_count,1)
            self.assertNotIn('interrupted', state.data['events']['$input']['answer'])

    def test_changed_or_unconfirmed_execution_cannot_resume_native_inference(self):
        from task_recovery import RecoveryConflict
        state=Mock(workspace=Path('/tmp/fixture'),data={'events':{'$input':{'stage':'inference','replayable':False}},'sessions':{}})
        matrix=Mock()
        matrix.message_current.side_effect=RecoveryConflict('unconfirmed tool operation')
        event={'event_id':'$input','room_id':'!room:test','body':'continue'}
        record={'member':{'role':'worker','matrixUserId':'@worker:test'}}
        with patch('bridge.require_unencrypted_room'), patch('bridge.run_turn') as run:
            with self.assertRaises(RecoveryConflict):
                bridge.execute_input(state,Mock(),record,{},event,set(),matrix,{'identity':{'event_key':'input'}})
        run.assert_not_called()
        matrix.message_save_reply.assert_not_called()
