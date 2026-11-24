"""Map native Claude interaction to AT's existing Matrix wire contracts."""
import asyncio
import hashlib
import json
import os
import time
import uuid
from urllib.parse import quote

from runtime_observability import redact


class TurnStopped(RuntimeError):
    def __init__(self, session=None):
        super().__init__("Claude execution was cancelled")
        self.session = session


class MatrixTurn:
    def __init__(self, matrix, record, event, data, save):
        self.matrix, self.record, self.event = matrix, record, event
        self.data, self.save = data, save
        self.own = record["member"]["matrixUserId"]
        self.room, self.run = event["room_id"], event["event_id"]
        self.context = uuid.uuid4().hex
        self.cursor = data.get("cursor")
        self.pending, self.messages, self.fragments = {}, {}, {}
        self.tools = {}
        self.segment = 0
        self.serial = 0
        self.permission_sequence = 0
        self.direct = False
        self.stopped = False
        self.stop_started = None
        self.seen = set()
        self.steers = {}
        self.session = None
        self.current = None
        self.hidden = bool(event.get("coordination_session"))
        channel = record.get("desired", {}).get("channels", {}).get("matrix", {})
        self.tools_visible = not channel.get("filter_tool_messages", record.get("matrix", {}).get("transport", {}).get("filter_tool_messages", False))
        team = record.get("team") or {}
        self.agents = {m["matrixUserId"] for m in team.get("members", [])
                       if m.get("role") != "human" and m.get("matrixUserId")}
        admin = (team.get("admin") or {}).get("matrixUserId")
        if record["member"].get("role") == "manager":
            admin = "@" + os.getenv("AGENTTEAMS_ADMIN_USER", "admin").lstrip("@").split(":")[0] + ":" + self.own.split(":", 1)[1]
        self.humans = {s for s in (event.get("sender"), admin) if s and s not in self.agents and s != self.own}

    async def send(self, content, transaction):
        content = {"msgtype": "m.notice", "org.agentteams.progress_only": True, **content}
        result = await asyncio.to_thread(self.matrix.send_content, self.room, content, transaction)
        if not isinstance(result, str) or not result.startswith("$"):
            raise RuntimeError("Matrix did not acknowledge the native interaction")
        return result

    async def block(self, key, block, body):
        content = {"body": redact(body)[:16000], "org.agentteams.run": {
            "version": "1", "run_id": self.run, "blocks": [block]}}
        original = self.messages.get(key)
        self.serial += 1
        if original:
            content = {**content, "m.new_content": {"msgtype": "m.notice", "org.agentteams.progress_only": True, **content},
                       "m.relates_to": {"rel_type": "m.replace", "event_id": original}}
        result = await self.send(content, f"claudecode:{self.context}:{key}:{self.serial}")
        self.messages.setdefault(key, result)

    async def status(self, status):
        if self.direct:
            await self.send({"body": status, "com.agentteams.execution": {
                "run_id": self.run, "status": status, "capabilities": ["stop", "steer"]}},
                f"claudecode:{self.context}:execution:{status}")

    async def start(self):
        await self.retire_previous()
        if self.room == self.record["member"].get("personalRoomId") and self.event.get("sender") in self.humans:
            members = await asyncio.to_thread(self.matrix.request, "GET",
                "/_matrix/client/v3/rooms/" + quote(self.room, safe="") + "/joined_members")
            self.direct = set(members.get("joined", {})) == {self.own, self.event["sender"]}
        await self.status("running")

    async def receipt(self, request, status):
        if request["kind"] == "tool_approval":
            content = {"body": f"{request['tool_name']}: {status}", "org.agentteams.approval": {
                **request["wire"], "status": status}}
        else:
            content = {"body": f"Question {status}", "org.agentteams.run": {"version": "1", "run_id": request["run_id"],
                "blocks": [{**request["wire"], "status": status}]}}
        # Keep the terminal receipt durable until Matrix confirms delivery.
        key = f"claudecode:{request['context_id']}:{request['id']}:{status}"
        outbox = self.data.setdefault("interaction_receipts", {})
        outbox[key] = {"room": request["room_id"], "content": content}
        self.save()
        await self.flush_receipts()

    async def flush_receipts(self):
        for key, entry in list(self.data.get("interaction_receipts", {}).items()):
            result = await asyncio.to_thread(self.matrix.send_content, entry["room"],
                {"msgtype": "m.notice", "org.agentteams.progress_only": True, **entry["content"]}, key)
            if not isinstance(result, str) or not result.startswith("$"):
                raise RuntimeError("Matrix did not acknowledge the interaction receipt")
            self.data["interaction_receipts"].pop(key, None)
            self.save()

    async def retire_previous(self):
        await self.flush_receipts()
        for key, request in list(self.data.get("interaction_pending", {}).items()):
            if request["context_id"] != self.context:
                await self.receipt(request, "expired")
                self.data["interaction_pending"].pop(key, None)
                self.save()

    async def permission(self, name, fields, call_id):
        if self.stopped:
            return False
        self.permission_sequence += 1
        request_id = "claude-" + hashlib.sha256(f"{self.context}:{call_id}:{self.permission_sequence}".encode()).hexdigest()
        expires = int(time.time() * 1000) + int(float(os.getenv("AGENTTEAMS_HITL_TIMEOUT_SECONDS", "300")) * 1000)
        question = name == "AskUserQuestion"
        request = {"id": request_id, "kind": "ask_user" if question else "tool_approval", "tool_name": name, "call_id": call_id,
                   "context_id": self.context, "room_id": self.room, "run_id": self.run, "expires_at": expires}
        if question:
            questions = fields.get("questions") or []
            if not questions or len({q.get("question") for q in questions}) != len(questions):
                return False
            request["questions"] = questions
            wire_questions = [{"id": str(i), "question": q["question"], "multi_select": q.get("multiSelect") is True,
                "options": [{"id": o["label"], "label": o["label"], "description": o.get("description", "")}
                            for o in q.get("options", [])]} for i, q in enumerate(questions)]
            wire = {"type": "confirmation", "tool_name": "ask_human", "confirmation_id": request_id,
                    "reply_format": "com.agentteams.answer", "status": "pending", "expires_at": expires,
                    "questions": wire_questions, "requested_of": self.event["sender"] if self.event["sender"] in self.humans else next(iter(sorted(self.humans)), "")}
            content = {"body": "\n".join(q["question"] + " " + ", ".join(o["label"] for o in q.get("options", [])) for q in questions)
                       + f"\n/answer {request_id} {self.context} <answer>",
                       "org.agentteams.run": {"version": "1", "run_id": self.run, "blocks": [wire]}}
        else:
            wire = {"version": 1, "runtime": "claudecode", "request_id": request_id,
                    "context_id": self.context, "reply_format": "com.agentteams.answer", "status": "pending",
                    "expires_at": expires, "tool_name": name, "parameters": redact(json.dumps(fields, ensure_ascii=False))[:16000]}
            content = {"body": f"Approval required: {name}\n/approve {request_id} {self.context}\n/reject {request_id} {self.context}",
                       "org.agentteams.approval": wire}
        request["wire"] = wire
        self.data.setdefault("interaction_pending", {})[request_id] = request
        self.save()
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = {**request, "future": future}
        try:
            await self.send(content, f"claudecode:{self.context}:request:{request_id}")
            await self.tool(call_id, name, fields, "pending")
            # Polling remains active while native permission callbacks are paused.
            return await asyncio.wait_for(asyncio.shield(future), max(0, (expires / 1000) - time.time()))
        except TimeoutError:
            await self.resolve(request_id, "expired", False)
            return False
        except BaseException:
            await self.resolve(request_id, "cancelled", False)
            raise

    async def resolve(self, key, status, result):
        request = self.pending.get(key)
        if not request:
            return
        await self.receipt(request, status)
        await self.tool(request["call_id"], request["tool_name"], {},
                        "running" if status in {"approved", "answered"} else "failed", status)
        self.pending.pop(key, None)
        self.data.get("interaction_pending", {}).pop(key, None)
        self.save()
        if not request["future"].done():
            request["future"].set_result(result)

    async def expire(self):
        for key, request in list(self.pending.items()):
            if request["expires_at"] <= time.time() * 1000:
                await self.resolve(key, "expired", False)

    def answers(self, request, responses):
        questions = request["questions"]
        if not isinstance(responses, list) or len(responses) != len(questions):
            return None
        answers = {}
        for i, question in enumerate(questions):
            matches = [r for r in responses if isinstance(r, dict) and r.get("id") == str(i)]
            if len(matches) != 1:
                return None
            selected, custom = matches[0].get("selected", []), matches[0].get("custom", "")
            options = {o["label"] for o in question.get("options", [])}
            if (not isinstance(selected, list) or not isinstance(custom, str)
                    or any(not isinstance(s, str) or s not in options for s in selected)
                    or len(set(selected)) != len(selected) or (not question.get("multiSelect") and len(selected) > 1)
                    or (not selected and not custom.strip())):
                return None
            values = selected + ([custom.strip()] if custom.strip() else [])
            answers[question["question"]] = values if question.get("multiSelect") else values[0]
        return {"questions": questions, "answers": answers}

    async def handle(self, room, raw, native):
        eid, sender = raw.get("event_id"), raw.get("sender")
        if (room != self.room or raw.get("type") != "m.room.message" or not eid or eid in self.seen
                or sender not in self.humans or raw.get("unsigned", {}).get("redacted_because")):
            return
        content = raw.get("content") or {}
        answer, control = content.get("com.agentteams.answer"), content.get("com.agentteams.control")
        body = content.get("body") or ""
        if answer is None and control is None and isinstance(body, str):
            verb, _, supplement = body.partition(" ")
            if verb == "/cancel" and not supplement:
                control = {"action": "stop", "agent_id": self.own, "run_id": self.run}
            elif verb == "/steer":
                control = {"action": "steer", "agent_id": self.own, "run_id": self.run}
                body = supplement
        if answer is not None:
            if not isinstance(answer, dict) or answer.get("agent_id") != self.own:
                return
            key = answer.get("confirmation_id")
            request = self.pending.get(key) if isinstance(key, str) else None
            if not request or request["expires_at"] <= time.time() * 1000:
                return
            if request["kind"] == "tool_approval":
                if (answer.get("kind") != "tool_approval" or answer.get("context_id") != self.context
                        or answer.get("answer") not in {"approved", "rejected"}):
                    return
                approved = answer["answer"] == "approved"
                await self.resolve(key, "approved" if approved else "rejected", approved)
            else:
                if answer.get("kind") not in {None, "ask_user"}:
                    return
                result = self.answers(request, answer.get("responses"))
                if result is None:
                    return
                await self.resolve(key, "answered", result)
        elif control is not None:
            if (not self.direct or not isinstance(control, dict) or sender != self.event["sender"]
                    or control.get("agent_id") != self.own or control.get("run_id") != self.run
                    or control.get("action") not in {"stop", "steer"}):
                return
            if control["action"] == "stop":
                self.stopped = True
                self.stop_started = time.monotonic()
                await self.status("stopping")
                await self.close("cancelled")
                await native.interrupt()
            elif isinstance(body, str) and body.strip():
                # Save the supplementary input before handing it to the native process.
                native_id = str(uuid.uuid5(uuid.NAMESPACE_URL, self.room + ":" + eid))
                self.steers[eid] = native_id
                self.data.setdefault("steers", {})[eid] = {"run_id": self.run, "body": body, "room_id": room,
                    "sender": sender, "native_uuid": native_id, "status": "pending"}
                self.save()
                async def supplementary():
                    yield {"type": "user", "uuid": native_id, "message": {"role": "user",
                        "content": f"[AgentTeams supplementary input {eid}]\n{body}"}, "parent_tool_use_id": None}
                await native.query(supplementary())
                self.seen.add(eid)
                return  # A stdin write is not the native input-consumption receipt.
            else:
                return
        elif isinstance(body, str) and body.split(" ", 1)[0] in {"/approve", "/reject"}:
            parts = body.split()
            if len(parts) != 3 or parts[2] != self.context:
                return
            request = self.pending.get(parts[1])
            if not request or request["kind"] != "tool_approval" or request["expires_at"] <= time.time() * 1000:
                return
            approved = parts[0] == "/approve"
            await self.resolve(parts[1], "approved" if approved else "rejected", approved)
        elif isinstance(body, str) and body.startswith("/answer "):
            parts = body.split(" ", 3)
            if len(parts) != 4 or parts[2] != self.context:
                return
            request = self.pending.get(parts[1])
            if not request or request["kind"] != "ask_user" or request["expires_at"] <= time.time() * 1000:
                return
            try:
                result = self.answers(request, json.loads(parts[3])["responses"]) if parts[3].startswith("{") else (
                    {"questions": request["questions"], "answers": {request["questions"][0]["question"]: parts[3]}}
                    if len(request["questions"]) == 1 and parts[3].strip() else None)
            except (ValueError, KeyError, TypeError):
                return
            if result is None:
                return
            await self.resolve(parts[1], "answered", result)
        else:
            return
        self.seen.add(eid)
        self.data["events"][eid] = {"delivered": True, "control": True}
        self.save()

    def confirm_inputs(self, message):
        consumed = set(message.user_message_uuids or [])
        if message.user_message_uuid:
            consumed.add(message.user_message_uuid)
        for eid, native_id in list(self.steers.items()):
            if native_id in consumed:
                self.data["steers"][eid]["status"] = "consumed"
                self.data["events"][eid] = {"delivered": True, "control": True}
                self.steers.pop(eid)
                self.save()
        return not self.steers

    async def poll(self, native):
        batch = await asyncio.to_thread(self.matrix.sync, self.cursor, 1000)
        await asyncio.to_thread(self.matrix.fill_timeline_gaps, batch, self.cursor, {self.room: "personal"})
        for room, view in batch.get("rooms", {}).get("join", {}).items():
            for raw in view.get("timeline", {}).get("events", []):
                await self.handle(room, raw, native)
        self.cursor = batch["next_batch"]
        await self.expire()

    async def watch(self, native):
        next_check = 0
        while True:
            await self.poll(native)
            if self.current and time.monotonic() >= next_check:
                next_check = time.monotonic() + 2
                if not await asyncio.to_thread(self.current):
                    self.stopped = True
                    self.stop_started = time.monotonic()
                    await self.close("cancelled")
                    await native.interrupt()
                    # Keep receiving until native termination is acknowledged.
                    await asyncio.Event().wait()
            await asyncio.sleep(0.5)

    async def tool(self, key, name, fields, status, result=None):
        if not self.tools_visible:
            return
        block = {**self.tools.get(key, {}), "type": "tool_call", "tool_name": name, "tool_call_id": f"{self.context}:{key}",
                 "status": status}
        if fields or "arguments" not in block:
            block["arguments"] = json.loads(redact(json.dumps(fields, default=str)))
        block.setdefault("started_at", int(time.time() * 1000))
        if status in {"succeeded", "failed"}:
            block["finished_at"] = int(time.time() * 1000)
        if result is not None:
            block["result"] = redact(json.dumps(result, ensure_ascii=False, default=str))[:16000]
        self.tools[key] = block
        await self.block("tool:" + key, block, f"{name}: {status}")

    async def stream(self, event):
        kind, index = event.get("type"), event.get("index")
        if kind == "message_start":
            self.segment += 1
        elif kind == "content_block_start":
            block = event.get("content_block") or {}
            if block.get("type") == "thinking" and not self.hidden:
                self.fragments[index] = {"type": "thinking", "content": block.get("thinking", ""), "isStreaming": True}
        elif kind == "content_block_delta" and index in self.fragments:
            delta = event.get("delta") or {}
            if delta.get("type") == "thinking_delta":
                block = self.fragments[index]
                block["content"] = (block["content"] + delta.get("thinking", ""))[-16000:]
                last = block.get("_last", 0)
                if time.monotonic() - last > 1:
                    block["_last"] = time.monotonic()
                    await self.block(f"thinking:{self.segment}:{index}", {k: redact(v) if k == "content" else v for k, v in block.items() if not k.startswith("_")}, block["content"])
        elif kind == "content_block_stop" and index in self.fragments:
            block = self.fragments.pop(index)
            block.pop("_last", None)
            block.update(content=redact(block["content"]), isStreaming=False)
            await self.block(f"thinking:{self.segment}:{index}", block, block["content"])

    async def close(self, status, *, terminal=False):
        for key in list(self.pending):
            await self.resolve(key, status, False)
        if not terminal:
            return
        for index, fragment in list(self.fragments.items()):
            fragment.pop("_last", None)
            fragment.update(content=redact(fragment["content"]), isStreaming=False)
            await self.block(f"thinking:{self.segment}:{index}", fragment, fragment["content"])
            self.fragments.pop(index)
        if self.stopped:
            for key, tool in list(self.tools.items()):
                if tool["status"] in {"running", "pending"}:
                    await self.tool(key, tool["tool_name"], tool["arguments"], "failed", "Interrupted; any earlier effects require verification")
            await self.block("ending", {"type": "error", "kind": "cancelled", "title": "Execution stopped"}, "Execution stopped")
