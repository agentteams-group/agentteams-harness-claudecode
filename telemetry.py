"""Content-free Claude Code turn, model request and tool spans."""
from contextlib import contextmanager
from datetime import datetime
from itertools import pairwise
import json
import logging
import os
from pathlib import Path
import time
import uuid

from claude_agent_sdk import project_key_for_directory
from opentelemetry import propagate, trace


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
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


@contextmanager
def turn_span(role, task, model):
    packet = task.get("coordination") or {}
    identity = task.get("identity") or {}
    ids = [s["eventId"] for s in task.get("sources") or []]
    if not ids and task.get("sourceEvent"):
        ids = [task["sourceEvent"]]
    fields = {"agentteams.harness": "claudecode", "agentteams.role": role, "gen_ai.request.model": model,
              "agentteams.input.event_ids": ids, "agentteams.event.id": task.get("sourceEvent"),
              "agentteams.room.id": task.get("room"), "agentteams.task.id": identity.get("task_id") or packet.get("task_id"),
              "agentteams.coordination.id": packet.get("id"), "agentteams.coordination.kind": packet.get("kind"),
              "agentteams.execution.generation": identity.get("generation")}
    with trace.get_tracer("agentteams.claude-code").start_as_current_span("claude-code.turn",
            context=propagate.extract(task.get("carrier") or {}), attributes={k: v for k, v in fields.items() if v},
            record_exception=False, set_status_on_exception=False) as span:
        yield span


def _export_requests(path, offset, started_ms, session_id, turn_error):
    turn_end = time.time_ns()
    requests = {}
    responses = {}
    with path.open("rb") as journal:
        journal.seek(offset)
        for line in journal:
            # Skip large prompt/attachment records without decoding them.
            if b'"type":"api-request"' not in line and b'"requestRef":' not in line:
                continue
            try:
                row = json.loads(line)
                stamp = round(datetime.fromisoformat(row["timestamp"]).timestamp() * 1000)
            except (ValueError, KeyError, TypeError, UnicodeDecodeError):
                continue  # A CLI interrupted during its last append can leave a partial line.
            if stamp < started_ms or row.get("sessionId") != session_id:
                continue
            timestamp = stamp * 1_000_000
            if row.get("type") == "api-request" and row.get("id"):
                requests[row["id"]] = (timestamp, (row.get("params") or {}).get("model"))
            elif row.get("type") == "assistant" and row.get("requestRef") in requests:
                message = row.get("message") or {}
                if message.get("stop_reason") or row.get("isApiErrorMessage"):
                    responses[row["requestRef"]] = (timestamp, message.get("id"), bool(row.get("isApiErrorMessage")))

    tracer = trace.get_tracer("agentteams.claude-code")
    # For unanswered attempts only the next dispatch or turn exit is known.
    # Neither timestamp is an upstream response time.
    error_type = type(turn_error).__name__ if turn_error is not None else "NoTerminalResponse"
    for request_id, next_id in pairwise([*requests, None]):
        start, model = requests[request_id]
        fields = {"agentteams.model.request.id": request_id, "agentteams.session.id": session_id}
        if model:
            fields["gen_ai.request.model"] = model
        response = responses.get(request_id)
        if response:
            end, response_id, failed = response
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
        request_span = tracer.start_span("claude-code.model.request", start_time=start, attributes=fields)
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
        # that never produced an AssistantMessage. Export only safe metadata.
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
