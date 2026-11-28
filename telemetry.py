"""Claude Code native spans with opt-in, sanitized model and tool content."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
from itertools import pairwise
import json
import hashlib
import logging
import os
from pathlib import Path
import sys
import time
import uuid

from claude_agent_sdk import project_key_for_directory
from opentelemetry import propagate, trace

# Load shared sanitization without enabling sitecustomize instrumentation.
sys.path.insert(0, "/opt/agentteams/scripts/lib/telemetry")
from at_telemetry import SanitizingExporter, set_io


def configure():
    if os.getenv("AGENTTEAMS_OTEL_ENABLED", "").lower() not in {"true", "1"}:
        return
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    if isinstance(trace.get_tracer_provider(), TracerProvider):
        return
    provider = TracerProvider(resource=Resource.create({
        "service.name": os.getenv("OTEL_SERVICE_NAME", "agentteams-claude-code"),
        "agentteams.runtime": "claudecode", "agentteams.worker.name": os.getenv("AGENTTEAMS_WORKER_NAME", "")}))
    provider.add_span_processor(BatchSpanProcessor(SanitizingExporter(OTLPSpanExporter())))
    trace.set_tracer_provider(provider)


@contextmanager
def turn_span(role, task, model, input=None):
    packet = task.get("coordination") or {}
    identity = task.get("identity") or {}
    ids = [s["eventId"] for s in task.get("sources") or []]
    if not ids and task.get("sourceEvent"):
        ids = [task["sourceEvent"]]
    worker = os.getenv("AGENTTEAMS_WORKER_NAME", "unknown")
    team = identity.get("team_id") or os.getenv("AGENTTEAMS_TEAM_NAME")
    snapshot = (task.get("context") or {}).get("task") or {}
    planning = task.get("planning") or []
    main = task.get("completion") or (planning[0] if len(planning) == 1 else None)
    task_id = identity.get("task_id") or packet.get("task_id") or main or snapshot.get("id") or (packet.get("main_task_id") if packet.get("kind") == "main_task_ready" else None)
    main_task_id = packet.get("main_task_id") or snapshot.get("mainTaskId") or (snapshot.get("id") if snapshot.get("kind") == "main" else main)
    tags = ["agentteams", "worker-turn", "claudecode"]
    tags.extend(label + ":" + value for label,value in (("task",task_id),("main_task",main_task_id),("team",team),("room",task.get("room"))) if value)
    fields = {"langfuse.user.id": worker, "langfuse.trace.tags": tags, "langfuse.observation.type":"agent",
              "agentteams.worker.name":worker,"agentteams.runtime":"claudecode","agentteams.team.id":team,
              "agentteams.main_task.id": main_task_id, "gen_ai.agent.id": worker, "gen_ai.agent.name": worker,
              "agentteams.harness": "claudecode", "agentteams.role": role, "gen_ai.request.model": model,
              "agentteams.input.event_ids": ids, "agentteams.event.id": task.get("sourceEvent"),
              "agentteams.room.id": task.get("room"), "agentteams.task.id": task_id,
              "agentteams.coordination.id": packet.get("id"), "agentteams.coordination.kind": packet.get("kind"),
              "agentteams.execution.generation": identity.get("generation")}
    for key, value in tuple(fields.items()):
        if value is not None and key.startswith(("agentteams.", "gen_ai.agent.")):
            fields["langfuse.trace.metadata." + key] = value
            fields["langfuse.observation.metadata." + key] = value
    with trace.get_tracer("agentteams.claude-code").start_as_current_span("claude-code.turn",
            context=propagate.extract(task.get("carrier") or {}), attributes={k: v for k, v in fields.items() if v},
            record_exception=False, set_status_on_exception=False) as span:
        span.set_attribute("agentteams.telemetry.capture_content", os.getenv("AGENTTEAMS_OTEL_CAPTURE_CONTENT", "true"))
        set_io(span, input=input)
        yield span


def identity_attributes():
    return {key: value for key, value in (getattr(trace.get_current_span(), "attributes", None) or {}).items()
            if key.startswith(("agentteams.", "gen_ai.agent.", "langfuse.trace.metadata.", "langfuse.observation.metadata."))}


def _replay_text(value):
    if isinstance(value, dict):
        return {key: (item.rstrip() if isinstance(item, str) and key in {"text", "content"}
                      else _replay_text(item)) for key, item in value.items()}
    return [_replay_text(item) for item in value] if isinstance(value, list) else value


def _export_requests(path, offset, started_ms, session_id, turn_error):
    turn_end = time.time_ns()
    requests = {}
    responses = {}
    outputs = {}
    inputs = {}
    shapes, blobs, histories, assistant_blocks = {}, {}, {}, {}
    user_blocks = []
    capture_content = os.getenv("AGENTTEAMS_OTEL_CAPTURE_CONTENT", "true").lower() in {"true", "1"}
    with path.open("rb") as journal:
        # Resumed requests refer to shapes, blobs and prefixes from prior turns.
        journal.seek(0 if capture_content else offset)
        for line in journal:
            if b'"type":"api-request' not in line and b'"requestRef":' not in line and b'"type":"user"' not in line:
                continue
            try:
                row = json.loads(line)
            except (ValueError, UnicodeDecodeError):
                continue  # The CLI can be interrupted during its last append.
            if row.get("sessionId") != session_id:
                continue
            if capture_content:
                if row.get("type") == "user":
                    message = row.get("message") or {}
                    content = message.get("content")
                    if isinstance(content, str):
                        content = [{"type": "text", "text": content}]
                    if isinstance(content, list):
                        user_blocks.extend(content)
                        # The CLI can coalesce parallel MCP results and trim
                        # their trailing text in its content-addressed replay.
                        # Only a matching native hash can select these blobs.
                        for blocks in (content, user_blocks):
                            for normalized in (blocks, _replay_text(blocks)):
                                native_message = {"role": "user", "content": normalized}
                                encoded = json.dumps(native_message, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
                                blobs[hashlib.sha256(encoded).hexdigest()] = native_message
                if row.get("type") == "assistant":
                    user_blocks = []
                    message = row.get("message") or {}
                    if message.get("id") and isinstance(message.get("content"), list):
                        blocks = assistant_blocks.setdefault(message["id"], [])
                        blocks.extend(message["content"])
                        native_message = {"role": "assistant", "content": list(blocks)}
                        encoded = json.dumps(native_message, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
                        blobs[hashlib.sha256(encoded).hexdigest()] = native_message
                if row.get("type") == "api-request-shape":
                    shapes[row["shapeHash"]] = row["shape"]
                    continue
                if row.get("type") == "api-request-blob":
                    blobs[row["hash"]] = row["message"]
                    continue
                if row.get("type") == "api-request":
                    base = histories.get(row.get("base"), [])
                    keep = row.get("keep", 0)
                    # An absent prefix is incomplete evidence, never an empty prompt.
                    if keep <= len(base):
                        histories[row["id"]] = base[:keep] + row.get("tail", [])
            try:
                stamp = round(datetime.fromisoformat(row["timestamp"]).timestamp() * 1000)
            except (ValueError, KeyError, TypeError):
                continue
            if stamp < started_ms:
                continue
            timestamp = stamp * 1_000_000
            if row.get("type") == "api-request" and row.get("id"):
                requests[row["id"]] = (timestamp, (row.get("params") or {}).get("model"))
                if capture_content and row["id"] in histories:
                    try:
                        body = deepcopy({**row.get("params", {}), **shapes[row["shapeHash"]],
                                         "messages": [blobs[key] for key in histories[row["id"]]]})
                        for control in row.get("cacheControl", []):
                            block = body
                            for part in control["path"]:
                                block = block[part]
                            block["cache_control"] = control["value"]
                        inputs[row["id"]] = body
                    except (KeyError, IndexError, TypeError):
                        pass  # Missing native journal references cannot prove model input.
            elif row.get("type") == "assistant" and row.get("requestRef") in requests:
                message = row.get("message") or {}
                if capture_content and message.get("content"):
                    outputs.setdefault(row["requestRef"], []).extend(message["content"])
                if message.get("stop_reason") or row.get("isApiErrorMessage"):
                    responses[row["requestRef"]] = (timestamp, message.get("id"), bool(row.get("isApiErrorMessage")), message.get("usage") or {})

    tracer = trace.get_tracer("agentteams.claude-code")
    # For unanswered attempts only the next dispatch or turn exit is known.
    # Neither timestamp is an upstream response time.
    error_type = type(turn_error).__name__ if turn_error is not None else "NoTerminalResponse"
    for request_id, next_id in pairwise([*requests, None]):
        start, model = requests[request_id]
        fields = {"agentteams.model.request.id": request_id, "agentteams.session.id": session_id, "langfuse.observation.type":"generation", "langfuse.session.id":session_id}
        if model:
            fields["gen_ai.request.model"] = model
        response = responses.get(request_id)
        if response:
            end, response_id, failed, usage = response
            for key in ("input_tokens", "output_tokens"):
                if type(usage.get(key)) is int:
                    fields["gen_ai.usage." + key] = usage[key]
            if response_id:
                fields["gen_ai.response.id"] = response_id
            fields["agentteams.model.end_observation"] = "assistant_message"
            if failed:
                fields["error.type"] = "ModelError"
        else:
            if next_id is None:
                end = turn_end
                fields.update({"agentteams.model.end_observation": "turn_exit", "error.type": error_type})
            else:
                end, _ = requests[next_id]
                fields.update({"agentteams.model.end_observation": "next_request", "error.type": "NoTerminalResponse"})
        request_span = tracer.start_span("claude-code.model.request", start_time=start, attributes={**identity_attributes(), **fields})
        set_io(request_span, input=inputs.get(request_id), output=outputs.get(request_id))
        if request_id in inputs:
            request_span.set_attribute("agentteams.model.input_source", "native_replay_journal")
        if capture_content and request_id not in inputs:
            request_span.set_attribute("agentteams.model.input_unavailable", "native_journal_references_missing")
        request_span.set_status(trace.StatusCode.ERROR if "error.type" in fields else trace.StatusCode.OK)
        request_span.end(end_time=max(start, end))


@contextmanager
def model_requests(turn, workspace, resume):
    if not turn.is_recording():
        yield None
        return
    try:
        session_id = str(uuid.UUID(resume) if resume else uuid.uuid4())
    except ValueError:
        yield None  # Let the SDK validate its resume argument.
        return
    projects = Path(os.getenv("CLAUDE_CONFIG_DIR") or Path.home() / ".claude") / "projects"
    path = projects / project_key_for_directory(workspace) / f"{session_id}.jsonl"
    original = None
    started_ms = time.time_ns() // 1_000_000
    # Capture the append boundary before the SDK opens a resumed session.
    try:
        if resume:
            original = path.stat()
    except OSError:
        pass
    error = None
    try:
        yield None if resume else session_id
    except BaseException as exc:
        error = exc
        raise
    finally:
        # SDK teardown flushes its native request journal, including requests
        # that never produced an AssistantMessage. Content follows the shared capture policy.
        try:
            current = path.stat()
            offset = 0
            if original and current.st_ino == original.st_ino and current.st_size >= original.st_size:
                offset = original.st_size
            _export_requests(path, offset, started_ms, session_id, error)
        except FileNotFoundError:
            pass  # The SDK can fail during initialization, before dispatching any request.
        except (OSError, ValueError, TypeError) as exc:
            logging.getLogger(__name__).warning("Could not read Claude request metadata (%s)", type(exc).__name__)
