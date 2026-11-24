"""Resume assigned task/v1 work from the Controller, independently of Matrix sync."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import UUID

from task_operation import admit
from task_recovery import Recovery, RecoveryConflict
from worker_matrix_delivery import confirm_bound
from worker_matrix_raw import publish_raw_saved, require_unencrypted_room
from worker_handoff import recovery_scanned
from worker_sessions import WorkerSessions
from runtime_observability import event as trace

from turn import run_turn
from worker_files import load_terminal, restore
from interaction import MatrixTurn, TurnStopped


def native(request):
    child = subprocess.run([sys.executable, str(Path(__file__).with_name("team_tools.py")), "rpc"],
                           input=json.dumps(request), capture_output=True, text=True, timeout=60)
    if child.returncode:
        detail = child.stderr.strip().splitlines()
        raise RuntimeError("Task bridge failed: " + (detail[-1][:300] if detail else "no receipt"))
    return json.loads(child.stdout)


def transcript_digest(projects, ref):
    try:
        if str(UUID(ref)) != ref:
            return None
    except (ValueError, TypeError, AttributeError):
        return None
    logs = list(projects.glob("*/" + ref + ".jsonl"))
    if len(logs) == 1:
        with logs[0].open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    return None


def verified_read_result(projects, workspace, checkpoint):
    """Find the exact SDK tool result in this Task's native transcript."""
    operation = (checkpoint or {}).get("operation") or {}
    if operation.get("state") != "started":
        return None
    try:
        calls = json.loads(operation.get("evidence") or "[]")
    except ValueError:
        return None
    if not isinstance(calls, list) or len(calls) != 1 or not isinstance(calls[0], dict):
        return None
    call = calls[0]
    if call.get("read_only") is not True or call.get("name") not in {"Read", "Glob", "Grep"}:
        return None
    for log in projects.glob("*/*.jsonl"):
        matched = False
        with log.open("rb") as stream:
            for line in stream:
                try:
                    entry = json.loads(line)
                except ValueError:
                    break  # An interrupted last JSONL record supplies no receipt.
                if entry.get("sessionId") != log.stem or entry.get("cwd") != str(workspace):
                    continue
                msg = entry.get("message") or {}
                blocks = msg.get("content")
                if not isinstance(blocks, list):
                    continue
                for block in blocks:
                    if not isinstance(block, dict):
                        continue
                    if (entry.get("type") == msg.get("role") == "assistant" and block.get("type") == "tool_use"
                            and block.get("id") == call["id"]):
                        matched = (block.get("name") == call["name"]
                                   and Recovery.digest(block.get("input")) == call.get("arguments_sha256"))
                    if (matched and entry.get("type") == msg.get("role") == "user" and block.get("type") == "tool_result"
                            and block.get("tool_use_id") == call["id"] and "content" in block):
                        result = {"id": call["id"], "content": {"native_response_sha256": Recovery.digest(block["content"])}}
                        if block.get("is_error") is True:
                            result.update(status="error", effect="none")
                        return result
    return None


def deliver(client, recovery, record, task, team, answer):
    execution = task["execution"]
    room = task["room_id"]
    require_unencrypted_room(client, room)

    def bind(checkpoint):
        saved = recovery.checkpoint(task["task_id"], team, checkpoint, expected_identity=execution)
        confirm_bound(saved["execution"], execution, "claudecode", checkpoint)

    event = publish_raw_saved(client, room, answer, execution["checkpoint"], record["member"]["matrixUserId"],
                              bind, content={"org.agentteams.progress_only": True})
    return recovery.delivered(task["task_id"], team, event)


def recover_work(state, client, record, provider, assignments):
    recovery = Recovery(device_delivery=True)
    sessions = WorkerSessions("claudecode", recovery=recovery)
    snapshot = recovery.discover()
    claims = []
    for entry in snapshot["tasks"]:
        if not entry["task"].get("task_plan_id"):
            continue
        try:
            claimed = recovery.claim(snapshot, entry)
        except RecoveryConflict:
            continue  # A concurrent claim or a completed assignment invalidated this ETag.
        claims.append((claimed, entry["team_id"]))
    recovery_scanned()
    for claimed, team in claims:
        task = claimed["task"]
        execution = task["execution"]
        if (execution.get("state") not in {"active", "needs_verification"} or execution["runtime"] != "claudecode"
                or execution["process_id"] != recovery.process or execution["instance_id"] != snapshot["instance_id"]):
            continue
        task_id = task["task_id"]
        cp = execution.get("checkpoint") or {}
        if execution["state"] == "needs_verification":
            if not os.getenv("TEAMHARNESS_SHARED_DIR"):
                continue
            workspace = Path(os.environ["TEAMHARNESS_SHARED_DIR"]) / "tasks" / task_id / "workspace"
            operation = cp.get("operation") or {}
            if operation.get("state") != "started" or not operation.get("id", "").startswith("tools:"):
                continue
            calls = json.loads(operation.get("evidence") or "[]")
            if len(calls) != 1 or not isinstance(calls[0], dict) or not calls[0].get("id"):
                continue
            identity = {**execution, "task_id": task_id, "team_id": team}
            result = verified_read_result(state.projects, workspace, cp)
            record_receipt = None if result else load_terminal(identity, operation["id"], calls[0]["id"])
            if record_receipt:
                result = record_receipt["result"]
            if result:
                recovery.finish_tools(identity, operation["id"], [result], artifacts=record_receipt["artifacts"] if record_receipt else [])
                trace("task.tool_reconciled", task_id=task_id, generation=execution["generation"])
            continue
        if cp.get("delivery") == "pending":
            deliver(client, recovery, record, task, team, claimed["saved_result"])
            continue
        if task["status"] not in {"assigned", "in_progress"}:
            continue
        admit(cp)
        work = {"kind": "task", "team_id": team, "task_id": task_id}
        identity = {**work, **{key: execution[key] for key in ("generation", "instance_id", "process_id")}}
        view = sessions.load(work)
        if view["generation"] != execution["generation"] or view["checkpoint_digest"] != recovery.digest(cp or None):
            continue
        ref = (view.get("cache") or {}).get("native_ref")
        session = ref if sessions.usable(view, ref, transcript_digest(state.projects, ref)) else None
        assignment = {"kind": "task_assigned", "task_id": task_id, "delivery_id": task["notifyTxn"],
                      "main_task_id": task["main_task_id"], "project_id": task.get("project_id", "")}
        gate = native({"operation": "canonical_admit", "context": {"execution": task_id, "team": team}})
        if gate.get("ok") is not True:
            raise RecoveryConflict("Task operation outcome requires authoritative verification")
        context = native({"operation": "prepare_event", "event": assignment})
        if context.get("execution") != task_id:
            raise RuntimeError("Task begin returned no matching execution: " + str(context.get("result", {}).get("error", "")))
        source = assignments.get(task_id) or {}
        registered = (task.get("task_notifications") or {}).get("assigned") or {}
        packet = dict(registered.get("event") or assignment)
        if registered.get("event_id"):
            packet["id"] = registered["event_id"]
        source_id = source.get("event_id")
        if not source_id and packet.get("id"):
            receipt = recovery.request("GET", "coordination/events/" + packet["id"])
            registered_native = receipt.get("native_id") or ""
            if registered_native.startswith("matrix:$"):
                source_id = registered_native.removeprefix("matrix:")
        context.update(role="worker", identity=identity, room=task["room_id"], team=team,
                       coordination=(source.get("_task_protocol_meta") or {}).get("teamharness_coordination") or packet,
                       sourceEvent=source_id, carrier=source.get("carrier") or {})

        require_unencrypted_room(client, task["room_id"])
        directory = Path(context["context"]["outputDirectory"])
        restore(identity, cp, directory)
        prompt = recovery.resume_text({**task, "team_id": team}) + "\n" + context["fact"] + "\n" + json.dumps(context["context"], ensure_ascii=False)
        if source:
            prompt += "\n[Message " + source["event_id"] + "]\n" + source["body"]
        trace("task.resume", task_id=task_id, generation=execution["generation"], reused_session=bool(session))
        def remember_checkpoint(native_ref, call_id, saved_execution):
            logs = list(state.projects.glob("*/" + native_ref + ".jsonl")) if transcript_digest(state.projects, native_ref) else []
            if len(logs) != 1:
                return False
            transcript = logs[0]
            data = transcript.read_bytes()
            try:
                rows = [json.loads(line) for line in data.splitlines() if line]
            except ValueError:
                return False
            if not any(block.get("tool_use_id") == call_id and block.get("type") == "tool_result"
                       for row in rows for content in [row.get("message", {}).get("content")]
                       if isinstance(content, list) for block in content if isinstance(block, dict)):
                return False
            view = sessions.load(work)
            if view["checkpoint_digest"] != recovery.digest(saved_execution.get("checkpoint")):
                return False
            if state.remote:
                destination = state.remote + "/claude-code/projects/" + transcript.relative_to(state.projects).as_posix()
                subprocess.run(["mc", "pipe", destination], input=data, check=True, capture_output=True, timeout=30)
            sessions.remember(work, view, saved_execution, native_ref, hashlib.sha256(data).hexdigest(), int(time.time()) + 86400)
            return True

        admin = ((record.get("team") or {}).get("admin") or {}).get("matrixUserId")
        event = {"event_id": source_id or f"task:{task_id}:{execution['generation']}", "room_id": task["room_id"],
                 "sender": source.get("sender") or admin or record["member"]["matrixUserId"],
                 "coordination_session": task_id}
        interaction = MatrixTurn(client, record, event, state.data, state.save)
        def current():
            saved = recovery.current(task_id, team)
            actual = saved.get("execution") or {} if saved else {}
            return bool(saved and saved["status"] != "cancelled" and actual.get("state") == "active"
                        and all(actual.get(k) == identity[k] for k in ("generation", "instance_id", "process_id")))
        interaction.current = current
        try:
            ref, answer = asyncio.run(run_turn(record, provider, directory, prompt, session, context,
                Path(os.environ["AGENTTEAMS_PLUGIN_DIR"]), on_checkpoint=remember_checkpoint, interaction=interaction))
        except TurnStopped:
            continue  # Cancellation is authoritative; never submit a synthetic completion.
        saved = recovery.current(task_id, team)
        if not saved or saved["status"] in {"completed", "cancelled"}:
            continue
        if saved["status"] in {"assigned", "in_progress"}:
            view = sessions.load(work)
            digest = transcript_digest(state.projects, ref)
            if digest and view["checkpoint_digest"] == recovery.digest(saved["execution"].get("checkpoint")):
                state.snapshot()
                sessions.remember(work, view, execution, ref, digest, int(time.time()) + 86400)
            raise RuntimeError("Worker turn ended with the task still pending submission")
        saved, answer = recovery.prepare_reply(task_id, team, answer)
        if saved["execution"].get("checkpoint", {}).get("delivery") == "pending":
            deliver(client, recovery, record, {**task, "execution": saved["execution"]}, team, answer)
