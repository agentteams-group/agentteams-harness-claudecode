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
    context = {"role": role, "sourceEvent": event["event_id"], "sources": []}
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
    if os.getenv("AGENTTEAMS_CONTROLLER_URL") and (role == "manager" or context.get("planning")):
        context["roster"] = task_native.invoke({"operation": "roster", "role": role,
            "team": os.environ.get("AGENTTEAMS_TEAM_NAME") if role == "leader" else None})
    return context


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
                outcome = task_native.invoke({"operation": "task", "arguments": params["arguments"], "context": context})
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
        print(json.dumps(task_native.invoke(json.load(sys.stdin)), default=str))
    elif sys.argv[1:] == ["prepare"]:
        data = json.load(sys.stdin)
        print(json.dumps(prepare(data["event"], data["role"]), default=str))
    else:
        serve(json.loads(os.environ["AT_CLAUDE_TASK_CONTEXT"]))
