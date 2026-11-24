"""Managed Claude Code member. Keep Matrix delivery separate from inference."""
import asyncio
import json
import os
from pathlib import Path
import random
import tempfile
import subprocess
import sys
import time
from urllib.error import HTTPError

import yaml
from bridge_state import atomic_write
from matrix_bridge import (changed_outbox_files, managed_rooms, managed_matrix_event,
                           materialize_attachment, reply_mention_user_ids, snapshot_outbox, workspace_output_path)
from matrix_channel import MatrixClient
from runtime_observability import event as trace
from task_coordination import notification, IgnoredNotification
from worker_matrix_raw import require_unencrypted_room
from worker_matrix_state import WorkerMatrix
from worker_runtime_config import project

from turn import role_of, run_turn
from task_worker import native, recover_work
from worker_handoff import recovery_scanned
from worker_sync import scan, deliver_message
from interaction import MatrixTurn, TurnStopped


def mc(*args):
    return subprocess.run(["mc", *map(str, args)], check=True, capture_output=True, text=True, timeout=120).stdout


def retry_delay(error, attempt):
    if not isinstance(error, (OSError, ValueError, RuntimeError, subprocess.SubprocessError)):
        # The SDK wraps its control-plane timeout in a plain Exception.
        if not isinstance(error.__cause__, TimeoutError):
            return None
    if isinstance(error, HTTPError):
        if error.code != 429 and not 500 <= error.code < 600:
            return None
        try:
            milliseconds = json.load(error).get("retry_after_ms")
        except (OSError, ValueError, AttributeError):
            milliseconds = None
        if isinstance(milliseconds, int) and milliseconds >= 0:
            return milliseconds / 1000
    return 2 ** (attempt - 1) + random.uniform(0, 0.5)


class MemberState:
    def __init__(self, config):
        self.config = config
        self.registered_start = False
        self.journal = config.parent / "claude-code.json"
        self.workspace = Path(os.environ["TEAMHARNESS_WORKSPACE"])
        self.projects = Path(os.environ["CLAUDE_CONFIG_DIR"]) / "projects"
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.projects.mkdir(parents=True, exist_ok=True)
        prefix = os.getenv("AGENTTEAMS_STORAGE_PREFIX")
        self.remote = prefix.rstrip("/") + "/agents/" + os.environ["AGENTTEAMS_WORKER_NAME"] if prefix else ""
        self.refresh()
        if self.remote:
            rows = mc("ls", "--json", self.remote + "/runtime/")
            if any(json.loads(line).get("key") == self.journal.name for line in rows.splitlines()):
                mc("cp", self.remote + "/runtime/" + self.journal.name, self.journal)
        self.data = json.loads(self.journal.read_text()) if self.journal.exists() else {
            "cursor": None, "sessions": {}, "events": {}}
        if self.remote:
            keys = [json.loads(row)["key"] for row in mc("ls", "--json", self.remote + "/").splitlines()]
            if "claude-code/" in keys:
                # Preserve a newer local transcript if the previous process died before its upload.
                with tempfile.TemporaryDirectory(dir=self.projects.parent) as incoming:
                    mc("mirror", "--overwrite", self.remote + "/claude-code/projects/", incoming)
                    for source in Path(incoming).rglob("*.jsonl"):
                        target = self.projects / source.relative_to(incoming)
                        if not target.exists() or source.read_bytes().startswith(target.read_bytes()):
                            target.parent.mkdir(parents=True, exist_ok=True)
                            source.replace(target)
            if "workspace/" in keys:
                mc("mirror", "--overwrite", self.remote + "/workspace/", self.workspace)

    def refresh(self):
        if self.remote:
            temp = self.config.with_suffix(".pending")
            mc("cp", self.remote + "/runtime/runtime.yaml", temp)
            temp.replace(self.config)

    def save(self):
        atomic_write(self.journal, json.dumps(self.data, ensure_ascii=False))
        if self.remote:
            mc("cp", self.journal, self.remote + "/runtime/" + self.journal.name)

    def snapshot(self):
        if self.remote:
            mc("mirror", "--overwrite", self.projects, self.remote + "/claude-code/projects/")
            mc("mirror", "--overwrite", self.workspace, self.remote + "/workspace/")


def matrix_ids(values, domain):
    return [value if value == "*" or (value.startswith("@") and ":" in value)
            else "@" + value.lstrip("@") + ":" + domain for value in values]


def load_member(path):
    record = yaml.safe_load(path.read_text())
    worker = os.environ["AGENTTEAMS_WORKER_NAME"]
    if role_of(record) == "manager":
        if worker != "manager" or os.environ.get("AGENTTEAMS_AGENT_ROLE") != "manager":
            raise ValueError("Unexpected Manager identity")
        domain = os.environ["AGENTTEAMS_MATRIX_DOMAIN"]
        admin = os.getenv("AGENTTEAMS_ADMIN_USER", "admin")
        policy = record["desired"].get("channelPolicy") or {}
        record.setdefault("matrix", {})["transport"] = {
            "homeserver": os.environ["AGENTTEAMS_MATRIX_URL"], "groupPolicy": "allowlist",
            "groupAllowFrom": matrix_ids([admin, *(policy.get("groupAllowExtra") or [])], domain),
            "dm": {"policy": "allowlist", "allowFrom": matrix_ids([admin, *(policy.get("dmAllowExtra") or [])], domain)}}
    config = project(record, "claudecode", worker)
    transport = config["channels"]["matrix"]
    if str(transport.get("encryption")).lower() in {"true", "1"} or os.getenv("AGENTTEAMS_MATRIX_E2EE", "").lower() in {"true", "1"}:
        raise ValueError("Claude Code requires unencrypted Matrix rooms")
    return record, transport, config["models"]["providers"]["agentteams-gateway"]


def admitted(raw, room, rooms, member, agents, transport, deny):
    own = member["matrixUserId"]
    sender, event_id = raw.get("sender"), raw.get("event_id")
    if not sender or not event_id or raw.get("type") != "m.room.message" or raw.get("unsigned", {}).get("redacted_because"):
        return None
    blocked = deny.get("dmDenyExtra" if rooms.get(room) == "personal" else "groupDenyExtra") or []
    denied = matrix_ids(blocked, own.split(":", 1)[1])
    if "*" in denied or sender in denied:
        return None
    content = raw.get("content") or {}
    if "org.agentteams.coordination" in content:
        try:
            packet = notification(content, room, sender, own, event_id=event_id)
        except IgnoredNotification:
            return None
        event = {"body": packet["text"], "coordination_session": packet["session_id"],
                 "_task_protocol_meta": packet["meta"]}
    else:
        personal = rooms.get(room) == "personal"
        policy = transport.get("dm", {}).get("policy") if personal else transport.get("groupPolicy")
        if policy not in {"allowlist", "open"}:
            return None
        parsed = managed_matrix_event({"room_id": room, "event_id": event_id, "input": raw},
                                      rooms, own, agents, transport)
        if not parsed:
            return None
        event = parsed[2]
        if parsed[0] == "control" or event.get("body", "").split(" ", 1)[0] in {"/approve", "/reject", "/answer", "/cancel", "/steer"}:
            event["control"] = True
    return {**event, "event_id": event_id, "room_id": room, "sender": sender,
            "carrier": content.get("com.agentteams.trace") or {}}


def manager_inputs(events):
    seen = set()
    for event in events:
        # An inference result acknowledges only its actual source event. Equal
        # text is not evidence that independent Human requests are duplicates.
        if event["event_id"] not in seen:
            seen.add(event["event_id"])
            yield event


def execute(state, client, record, provider, event, agents, *, matrix=None, claim=None):
    while execute_input(state, client, record, provider, event, agents, matrix, claim):
        pass  # The Controller supplied a newer verified input frontier.


def execute_input(state, client, record, provider, event, agents, matrix, claim):
    events = state.data["events"]
    eid, room = event["event_id"], event["room_id"]
    slot = events.get(eid)
    if slot and (slot.get("stage") == "failed" or (slot.get("delivered") and not claim)):
        return
    require_unencrypted_room(client, room)
    scope = json.dumps([record["member"]["matrixUserId"], role_of(record),
                        (record.get("team") or {}).get("name"), room, event.get("coordination_session")])
    packet = (event.get("_task_protocol_meta") or {}).get("teamharness_coordination") or {}
    phase = packet.get("kind") in {"handoff", "task_submitted", "main_task_ready"}
    if slot is None:
        slot = events[eid] = {"stage": "prepare"}
    if slot.get("stage") == "inference" and slot.get("replayable") and not phase:
        slot["stage"] = "refresh" if slot.get("refreshedInputs") else "prepare"
    if slot.get("stage") in {"prepare", "refresh"}:
        if slot.get("attempts", 0) >= 3:
            slot["stage"] = "failed"
            state.save()
            raise RuntimeError("This Matrix input exhausted its three inference attempts")
        # Preparation uses the same authoritative event and idempotent Task begin on redelivery.
        prepared = subprocess.run([sys.executable, str(Path(__file__).with_name("team_tools.py")), "prepare"],
            input=json.dumps({"event": event, "role": role_of(record)}), capture_output=True,
            text=True, check=True, timeout=60)
        task = json.loads(prepared.stdout)
        if slot.get("stage") == "refresh":
            task.update(slot["updatedContext"])
            task.pop("inputRefreshPending", None)
        if task is None:
            slot["delivered"] = True
            state.save()
            return
        workspace = Path((task.get("context") or {}).get("outputDirectory") or state.workspace)
        before = snapshot_outbox(workspace)
        body = event["body"]
        if slot.get("stage") == "refresh":
            body = "\n".join(f"[Message {f['eventId']}] [{f['inputId']}]\n{f['body']}" for f in slot["refreshedInputs"])
        if event.get("kind") in {"file", "image"}:
            body, _ = materialize_attachment(event, client, workspace, 25 * 1024 * 1024)
        middle = state.data.get("deliveryContext", {})
        delivery = {}
        if role_of(record) == "manager" and os.getenv("AGENTTEAMS_CONTROLLER_URL") and not packet:
            delivery = native({"operation": "delivery_context", "room": room, "middle": middle, "inputText": body})
        facts = {k: task[k] for k in ("fact", "context", "sources", "roster") if task.get(k)}
        prompt = "\n".join([*delivery.get("facts", []), *([json.dumps(facts, ensure_ascii=False)] if facts else []), body])
        task.update(room=room, coordination=packet, carrier=event.get("carrier", {}))
        slot.update(stage="inference", replayable=True, attempts=slot.get("attempts", 0) + 1)
        state.save()

        def before_effect():
            if slot.get("replayable"):
                slot["replayable"] = False
                state.save()

        try:
            session, answer = asyncio.run(run_turn(record, provider, workspace, prompt,
                state.data["sessions"].get(scope), task, Path(os.environ["AGENTTEAMS_PLUGIN_DIR"]), on_effect=before_effect,
                interaction=MatrixTurn(client, record, event, state.data, state.save)))
        except TurnStopped as stopped:
            if claim:
                matrix.message_cancel(claim["identity"])
            slot.update(stage="stopped", delivered=True)
            if stopped.session:
                state.data["sessions"][scope] = stopped.session
            state.snapshot()
            state.save()
            return
        except Exception:
            if phase:
                slot["stage"] = "retry_phase"
                state.save()
            raise
        state.data["sessions"][scope] = session
        if task.get("refresh"):
            update = task["refresh"]
            if not update["refreshedInputs"] or update["refreshedInputs"] == slot.get("refreshedInputs"):
                raise RuntimeError("Input refresh did not advance the verified frontier")
            state.snapshot()
            slot.update(stage="refresh", attempts=0, updatedContext=update["contextUpdate"], refreshedInputs=update["refreshedInputs"])
            state.save()
            return True
        slot["answer"] = answer
        if workspace == state.workspace:
            slot["outputs"] = changed_outbox_files(workspace, before)
        if delivery.get("acknowledgements"):
            state.data["deliveryContext"] = native({"operation": "ack_delivery_context", "middle": middle,
                                                   "acknowledgements": delivery["acknowledgements"]})
        slot["stage"] = "snapshot"
        state.save()
    if slot.get("stage") == "snapshot":
        state.snapshot()
        slot["stage"] = "reply"
        state.save()
    if phase and slot.get("stage") in {"inference", "retry_phase"}:
        status = native({"operation": "admit_event", "eventId": packet["id"], "nativeEventId": eid, "action": "retry"})
        if not (status.get("settled") or status.get("retry_queued") or status.get("replaced")):
            raise RuntimeError("Coordination phase has no confirmed retry or completion")
        slot.update(stage="reconciled", delivered=True)
        state.save()
        return
    answer = slot.get("answer") or "This turn was interrupted. Verify any tool effects before sending a new request."
    require_unencrypted_room(client, room)
    if claim is None:
        reply_id = client.send_text(room, answer, eid, "claude-code:" + eid, reply_mention_user_ids(
            event, answer, agents, record["member"]["matrixUserId"]), progress_only=bool(event.get("coordination_session")))
        if not reply_id.startswith("$"):
            raise RuntimeError("Matrix did not acknowledge the saved reply")
    else:
        saved = matrix.message_save_reply(claim["identity"], answer)
        deliver_message(client, matrix, saved, event, record["member"]["matrixUserId"], answer)
    for output in slot.get("outputs", []):
        file_id = client.send_file(room, workspace_output_path(state.workspace, output), eid, eid + ":file:" + output)
        if not file_id.startswith("$"):
            raise RuntimeError("Matrix did not acknowledge the saved file")
    slot["delivered"] = True
    for frame in event.get("frames") or []:
        if frame["event_id"] != eid:
            events[frame["event_id"]] = {"delivered": True, "inferenceEvent": eid}
    for frame in slot.get("refreshedInputs") or []:
        if frame["eventId"] != eid:
            events[frame["eventId"]] = {"delivered": True, "inferenceEvent": eid}
    state.save()


def poll(state, ready):
    state.refresh()
    record, transport, provider = load_member(state.config)
    member = record["member"]
    if not state.registered_start and role_of(record) != "manager" and os.getenv("AGENTTEAMS_CONTROLLER_URL"):
        WorkerMatrix("claudecode").start()
        state.registered_start = True
    team = record.get("team") or {}
    rooms = {room: kind for room, kind in ((member.get("personalRoomId"), "personal"),
                                           (team.get("teamRoomId"), "team")) if room}
    agents = {m["matrixUserId"] for m in team.get("members") or []
              if m.get("role") != "human" and m.get("matrixUserId")}
    client = MatrixClient(transport["homeserver"], transport["accessToken"])
    if not ready and (state.data.get("interaction_pending") or state.data.get("interaction_receipts")):
        pending = next(iter(state.data.get("interaction_pending", {}).values()), {})
        event = {"event_id": pending.get("run_id", "restart"), "room_id": pending.get("room_id", ""), "sender": member["matrixUserId"]}
        asyncio.run(MatrixTurn(client, record, event, state.data, state.save).retire_previous())
    worker = role_of(record) in {"worker", "remote-member"} and bool(os.getenv("AGENTTEAMS_CONTROLLER_URL"))
    if worker:
        matrix, pending, assignments = scan(client, record, transport, rooms, agents, ready, admitted)
        recover_work(state, client, record, provider, assignments)
        if not ready:
            subprocess.run(["agt", "worker", "report-ready", "--name", os.getenv("AGENTTEAMS_WORKER_CR_NAME")
                            or os.environ["AGENTTEAMS_WORKER_NAME"]], check=True, timeout=20)
            recovery_scanned()
            trace("runtime.ready", evidence="matrix_sync_and_task_scan")
        for event, claim in pending:
            execute(state, client, record, provider, event, agents, matrix=matrix, claim=claim)
        return
    since = state.data["cursor"]
    batch = client.sync(since, 30000 if ready else 0)
    joined = managed_rooms(client, batch, rooms, member["matrixUserId"], transport)
    if role_of(record) == "manager":
        for room in joined:
            rooms.setdefault(room, "team")
    client.fill_timeline_gaps(batch, since, {room: rooms.get(room, "team") for room in joined})
    if not ready:
        if os.getenv("AGENTTEAMS_CONTROLLER_URL"):
            subprocess.run(["agt", "worker", "report-ready", "--name", os.getenv("AGENTTEAMS_WORKER_CR_NAME")
                            or os.environ["AGENTTEAMS_WORKER_NAME"]], check=True, timeout=20)
        trace("runtime.ready", evidence="matrix_sync")
    for room, view in batch.get("rooms", {}).get("join", {}).items():
        if room not in joined:
            continue
        inputs = []
        for raw in view.get("timeline", {}).get("events", []):
            if state.data["events"].get(raw.get("event_id"), {}).get("delivered"):
                continue
            event = admitted(raw, room, rooms, member, agents, transport, record["desired"].get("channelPolicy") or {})
            if event and not event.get("control"):
                inputs.append(event)
        for event in manager_inputs(inputs) if role_of(record) == "manager" else inputs:
            execute(state, client, record, provider, event, agents)
    if role_of(record) != "manager":
        recovery_scanned()
    if since != batch["next_batch"]:
        state.data["cursor"] = batch["next_batch"]
        state.data["events"] = dict(list(state.data["events"].items())[-512:])
        state.save()


def main():
    from telemetry import configure
    configure()
    state = MemberState(Path(os.environ["TEAMHARNESS_RUNTIME_CONFIG"]))
    ready = False
    failures = 0
    while True:
        try:
            poll(state, ready)
            ready = True
            failures = 0
        except Exception as error:
            failures += 1
            trace("matrix.failed", error_type=type(error).__name__, error=str(error)[:1024])
            delay = retry_delay(error, failures)
            if delay is None or failures >= 3:
                raise
            time.sleep(delay)


if __name__ == "__main__":
    main()
