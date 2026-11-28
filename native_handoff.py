"""Attach confirmed Matrix handoffs to the submitting Claude Code session."""
import asyncio
import json
import os
import hashlib
import subprocess
from pathlib import Path

from claude_agent_sdk import ClaudeSDKError, ClaudeAgentOptions, ClaudeSDKClient, HookMatcher, ResultMessage

from runtime_observability import event as trace
from turn import task_completion


async def append_handoff(transcript, workspace, fact):
    session = transcript.stem

    def recorded():
        # The SDK conversation projection can omit tool branches. Verify the
        # user's exact committed input against the submitting native transcript.
        with transcript.open() as stream:
            for line in stream:
                row = json.loads(line)
                if (row.get("sessionId") == session and row.get("type") == "user"
                        and not row.get("isMeta") and not row.get("isSidechain")
                        and (row.get("message") or {}).get("content") == fact):
                    return True
        return False

    if recorded():
        return

    async def record_only(data, _call_id, _context):
        if data["prompt"] != fact or data["session_id"] != session:
            raise ValueError("Unexpected input to the sender receipt session")
        return {"continue_": False, "stopReason": "Handoff recorded"}

    # The SDK saves the user input before this hook ends the turn. This local
    # operation has no model credentials, tools, skills, or Matrix publisher.
    workspace.mkdir(parents=True, exist_ok=True)
    options = ClaudeAgentOptions(cwd=str(workspace), resume=session, tools=[], setting_sources=[],
            strict_mcp_config=True, hooks={"UserPromptSubmit": [HookMatcher(hooks=[record_only])]},
            env={"ANTHROPIC_BASE_URL": "http://127.0.0.1:1", "ANTHROPIC_AUTH_TOKEN": "local-only",
                 "ANTHROPIC_API_KEY": "", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"})
    async with ClaudeSDKClient(options=options) as client:
        await client.query(fact)
        async for message in client.receive_response():
            if isinstance(message, ResultMessage):
                if message.is_error or message.session_id != session:
                    raise RuntimeError("Claude Code did not record the handoff in the submitting session")
    if not recorded():
        raise RuntimeError("Sender handoff is missing from the native transcript")


def outgoing_receipts(path):
    calls = set()
    workspace = None
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            if row.get("sessionId") != path.stem:
                continue
            # Bash can change cwd mid-turn. The SDK indexes this transcript under
            # the directory of the original session, including after a resume.
            if workspace is None and row.get("type") in {"user", "assistant"} and row.get("cwd"):
                workspace = Path(row["cwd"])
            blocks = (row.get("message") or {}).get("content")
            if not isinstance(blocks, list):
                continue
            for block in blocks:
                if (row["type"] == "assistant" and block.get("type") == "tool_use" and
                        block.get("name") == "mcp__teamharness__task" and (block.get("input") or {}).get("action") == "submit"):
                    calls.add(block["id"])
                if (row["type"] != "user" or block.get("type") != "tool_result" or
                        block.get("tool_use_id") not in calls or block.get("is_error")):
                    continue
                content = block.get("content")
                receipt = task_completion([{"text": content}] if isinstance(content, str) else content)
                if workspace is not None and receipt and receipt.get("pendingHandoffs"):
                    yield workspace, receipt["pendingHandoffs"]


def flush_handoffs(projects, checked, actor, confirm, backup):
    for path in projects.glob("*/*.jsonl"):
        try:
            stamp = path.stat().st_mtime_ns
            if checked.get(path) == stamp:
                continue
            settled = True
            for workspace, result in outgoing_receipts(path):
                confirmed = confirm({"operation": "confirmed_handoffs", "result": result, "actor": actor})
                for record in confirmed:
                    asyncio.run(append_handoff(path, workspace, record["fact"]))
                if confirmed:
                    backup(path)
                count = len(result.get("assignments", [])) + bool(result.get("notification"))
                settled &= len(confirmed) == count
            if settled:
                checked[path] = path.stat().st_mtime_ns
        except (OSError, ValueError, RuntimeError, ClaudeSDKError, subprocess.SubprocessError) as error:
            # A partial transcript or a temporarily unavailable receipt can be retried independently.
            trace("task.handoff.deferred", session_id=path.stem, error=str(error)[:256])


def commit_supplement(session, native_id, event_id, item):
    """Prove the SDK input row before retiring Core's ordinary pending input."""
    from worker_matrix_state import WorkerMatrix, matrix_event_key
    from runtime_control import retire_native_supplement
    projects = Path(os.getenv('CLAUDE_CONFIG_DIR') or Path.home() / '.claude') / 'projects'
    expected = f"[AgentTeams supplementary input {event_id}]\n{item['body']}"
    for path in projects.glob('*/' + session + '.jsonl'):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if row.get('sessionId') != session or row.get('isSidechain') or row.get('isMeta'):
                continue
            attachment = row.get('attachment') or {}
            if row.get('type') == 'user' and row.get('uuid') == native_id:
                content = (row.get('message') or {}).get('content')
            elif (row.get('type') == 'attachment' and attachment.get('type') == 'queued_command'
                  and attachment.get('source_uuid') == native_id and attachment.get('commandMode') == 'prompt'):
                # An input received during a tool call is persisted as a queued command.
                content = attachment.get('prompt')
            else:
                continue
            if content != expected and content != [{'type': 'text', 'text': expected}]:
                raise RuntimeError('Claude native supplementary input differs from the admitted source')
            matrix = WorkerMatrix('claudecode')
            key = matrix_event_key(item['room_id'], event_id)
            snapshot = matrix.native_input(key)
            digest = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
            if not matrix.native_current(snapshot, session, digest):
                matrix.native_committed(key, snapshot, session, digest)
            retire_native_supplement(matrix, key)
            return
    raise RuntimeError('Claude native transcript has not persisted the supplementary input')
