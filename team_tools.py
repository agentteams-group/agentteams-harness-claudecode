"""Expose the role's TeamHarness MCP tools with transport-bound Task inputs."""
import json
import os
from pathlib import Path
import sys

# TeamHarness and the SDK each ship a Python package named mcp. The stdio
# process keeps their import namespaces independent.
sys.path.insert(0, os.environ["AGENTTEAMS_PLUGIN_DIR"])
from mcp import server
from mcp.common import task_native
from mcp.common.task_context import COORDINATION_PHASES


def prepare(event, role):
    context = {"role": role, "sourceEvent": event["event_id"], "sourceRoom": event["room_id"], "sources": [],
               "actor": os.environ.get("AGENTTEAMS_MATRIX_USER_ID", "")}
    packet = (event.get("_task_protocol_meta") or {}).get("teamharness_coordination")
    if packet:
        if packet["kind"] == "task_assigned":
            from task_recovery import Recovery
            recovery = Recovery()
            snapshot = recovery.discover()
            entry = next((row for row in snapshot["tasks"]
                          if row["task"]["task_id"] == packet["task_id"]
                          and row["task"].get("notifyTxn") == packet["delivery_id"]
                          and row["task"].get("room_id") == event["room_id"]
                          and row["task"]["status"] in {"assigned", "in_progress"}), None)
            if entry is None:
                return None
            execution = recovery.claim(snapshot, entry)["task"]["execution"]
            if execution["state"] != "active" or execution["process_id"] != recovery.process:
                raise ValueError("Task execution requires verification before it can begin")
        elif packet["kind"] in COORDINATION_PHASES:
            admission = task_native.invoke({"operation": "admit_event", "eventId": packet["id"],
                                            "nativeEventId": event["event_id"]})
            if admission.get("active") is not True:
                return None
            if packet["kind"] == "handoff":
                packet = dict(packet, pending_task_ids=admission.get("pending_task_ids"))
        prepared = task_native.invoke({"operation": "prepare_event", "event": packet})
        if prepared.get("ok") is False:
            raise ValueError("Task preparation failed")
        context.update(prepared)
    else:
        context["sources"] = task_native.invoke({"operation": "sources", "meta": {
            "event_id": event["event_id"], "room_id": event["room_id"], "sender_id": event["sender"],
            **({"matrix_input_sources": event["frames"]} if event.get("frames") else {})}})
    if not packet and os.getenv("AGENTTEAMS_CONTROLLER_URL"):
        context.update(task_native.invoke({"operation": "room_context", "room": event["room_id"]}))
    if os.getenv("AGENTTEAMS_CONTROLLER_URL") and (role == "manager" or context.get("planning")):
        context["roster"] = task_native.invoke({"operation": "roster", "role": role,
            "team": os.environ.get("AGENTTEAMS_TEAM_NAME") if role == "leader" else None})
    return context


def pending_handoffs(result, confirmed):
    sent = {json.loads(row["receiptId"])[1] for row in confirmed}
    assignments = [{"status": "queued", "eventId": item["eventId"], "taskId": item["taskId"]}
                   for item in result.get("assignments", []) if item.get("status") == "queued"
                   and item.get("eventId") and item["eventId"] not in sent]
    notice = result.get("notification") or {}
    queued = notice.get("status") == "queued" and notice.get("eventId") and notice["eventId"] not in sent
    if not assignments and not queued:
        return None
    task = result["task"]
    pending = {"task": {key: task[key] for key in ("id", "mainTaskId", "submissionId")}, "assignments": assignments}
    if queued:
        pending["notification"] = {"status": "queued", "eventId": notice["eventId"]}
    return pending


def serve(context):
    closed = False
    task_only = any(context.get(key) for key in ("review", "planning", "completion", "execution"))
    for line in sys.stdin:
        req = json.loads(line)
        params = req.get("params") or {}
        try:
            if req.get("method") != "tools/call":
                response = server.handle_request(req)
                if req.get("method") == "tools/list" and task_only:
                    response["result"]["tools"] = [tool for tool in response["result"]["tools"] if tool["name"] == "task"]
            elif closed:
                raise ValueError("This Task phase has ended")
            elif params.get("name") == "task":
                arguments = params["arguments"]
                if arguments.get("action") == "plan" and context.get("planning") and not (
                    isinstance(arguments.get("overview"), str) and arguments["overview"].strip()
                ):
                    raise ValueError("请填写规划 overview，说明任务目标、验收标准、分工及交付物。")
                outcome = task_native.invoke({"operation": "task", "arguments": arguments, "context": context})
                context.update(outcome.get("contextUpdate") or {})
                if outcome.get("requiresInputCheckpoint"):
                    # This private file carries the verified bridge result across the stdio process boundary.
                    target = Path(os.environ["AT_CLAUDE_REFRESH_FILE"])
                    temporary = target.with_suffix(".pending")
                    updates = {k: context[k] for k in ("sources", "registeredInputs", "inputRefreshPending") if k in context}
                    temporary.write_text(json.dumps(dict(outcome, contextUpdate=updates), ensure_ascii=False))
                    temporary.replace(target)
                closed = bool(outcome.get("phaseComplete") or outcome.get("requiresInputCheckpoint"))
                result = outcome.get("result", outcome)
                if outcome.get("phaseComplete") and outcome.get("receiptId") and outcome.get("fact"):
                    task = result.get("task") or {}
                    outcome = {**{key: outcome[key] for key in ("phaseComplete", "receiptId", "fact", "displayText", "extraReceipts") if key in outcome},
                               "task": {key: task[key] for key in ("id", "status", "submissionId", "completionId", "planId") if key in task}}
                    if arguments.get("action") == "submit":
                        handoffs = pending_handoffs(result, outcome.get("extraReceipts") or [])
                        if handoffs:
                            outcome["pendingHandoffs"] = handoffs
                response = {"jsonrpc": "2.0", "id": req["id"], "result": {
                    "isError": result.get("ok") is False,
                    "content": [{"type": "text", "text": json.dumps(outcome, ensure_ascii=False)}]}}
            elif task_only:
                raise ValueError("Only the Task tool is available in this phase")
            else:
                response = server.handle_request(req)
        except (KeyError, OSError, ValueError) as error:
            response = {"jsonrpc": "2.0", "id": req.get("id"), "error": {"code": -32603, "message": str(error)}}
        if response is not None:
            print(json.dumps(response), flush=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["rpc"]:
        request = json.load(sys.stdin)
        if request["operation"] == "confirmed_handoffs":
            result = task_native.dispatched_receipts(request["result"], request["actor"], task_native.controller_json)
        else:
            result = task_native.invoke(request)
        print(json.dumps(result, default=str))
    elif sys.argv[1:] == ["prepare"]:
        data = json.load(sys.stdin)
        print(json.dumps(prepare(data["event"], data["role"]), default=str))
    else:
        serve(json.loads(Path(os.environ["AT_CLAUDE_TASK_CONTEXT_FILE"]).read_text()))
