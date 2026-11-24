"""Compare exported attempts to HTTP requests received from the bundled Claude CLI."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import TestCase, mock

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from test_member import ROOT
from turn import run_turn


class Messages(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(request.get("stream", False))
        if self.server.reject_first and len(self.server.requests) == 1:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"type":"error","error":{"type":"overloaded_error","message":"fixture"}}')
            return
        if self.server.stall or request.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            start = {"type": "message_start", "message": {"id": "msg_partial", "type": "message",
                     "role": "assistant", "model": request["model"], "content": [],
                     "usage": {"input_tokens": 2, "output_tokens": 0}}}
            self.wfile.write(b"event: message_start\ndata: " + json.dumps(start).encode() + b"\n\n")
            self.wfile.flush()
            if self.server.stall:
                self.server.release.wait(20)
            return

        body = json.dumps({"id": "msg_local", "type": "message", "role": "assistant",
                           "model": request["model"], "content": [{"type": "text", "text": "done"}],
                           "stop_reason": "end_turn", "usage": {"input_tokens": 2, "output_tokens": 1}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ModelRequests(TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.http = self.enterContext(ThreadingHTTPServer(("127.0.0.1", 0), Messages))
        self.http.requests = []
        self.http.stall = False
        self.http.reject_first = False
        self.http.release = threading.Event()
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.addCleanup(self.http.shutdown)
        self.addCleanup(self.http.release.set)
        self.exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.addCleanup(provider.shutdown)
        self.enterContext(mock.patch("telemetry.trace.get_tracer", provider.get_tracer))
        self.enterContext(mock.patch.dict(os.environ, {
            "PATH": os.environ["PATH"], "HOME": str(self.directory),
            "CLAUDE_CONFIG_DIR": str(self.directory / ".claude"),
            "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness"),
            "AGENTTEAMS_LLM_STREAM_TIMEOUT_S": "15",
        }, clear=True))

    def run_native(self, session=None):
        record = {"member": {"role": "manager"}, "desired": {}}
        provider = {"baseUrl": f"http://127.0.0.1:{self.http.server_port}", "apiKey": "fixture-secret",
                    "models": [{"id": "claude-sonnet-4-6"}]}
        return asyncio.run(run_turn(record, provider, self.directory, "private-test-prompt", session,
                                    {}, ROOT / "plugins/teamharness"))

    def attempts(self):
        return [s for s in self.exporter.get_finished_spans() if s.name == "claude-code.model.request"]

    def test_stream_disconnect_then_success_and_resume(self):
        session, answer = self.run_native()
        self.assertEqual(answer, "done")
        self.assertEqual(self.http.requests, [True, False])
        attempts = self.attempts()
        self.assertEqual(len(attempts), 2)
        self.assertEqual([s.status.status_code.name for s in attempts], ["ERROR", "OK"])
        self.assertEqual(attempts[1].attributes["gen_ai.response.id"], "msg_local")
        self.assertNotIn("gen_ai.response.id", attempts[0].attributes)
        self.assertEqual(len({s.attributes["agentteams.model.request.id"] for s in attempts}), 2)
        turn = next(s for s in self.exporter.get_finished_spans() if s.name == "claude-code.turn")
        self.assertTrue(all(s.parent.span_id == turn.context.span_id for s in attempts))
        self.assertNotIn("private-test-prompt", str([dict(s.attributes) for s in attempts]))
        self.assertNotIn("fixture-secret", str([dict(s.attributes) for s in attempts]))
        old_ids = {s.attributes["agentteams.model.request.id"] for s in attempts}
        self.exporter.clear()
        self.http.requests.clear()
        self.assertEqual(self.run_native(session), (session, "done"))
        self.assertEqual(len(self.attempts()), len(self.http.requests))
        self.assertTrue(old_ids.isdisjoint(s.attributes["agentteams.model.request.id"] for s in self.attempts()))

    def test_http_retry_and_stream_fallback_are_separate_attempts(self):
        self.http.reject_first = True
        _, answer = self.run_native()
        self.assertEqual(answer, "done")
        self.assertEqual(self.http.requests, [True, True, False])
        self.assertEqual([s.status.status_code.name for s in self.attempts()], ["ERROR", "ERROR", "OK"])

    def test_idle_timeout_keeps_the_only_unanswered_request(self):
        self.http.stall = True
        os.environ["AGENTTEAMS_LLM_STREAM_TIMEOUT_S"] = "4"
        with self.assertRaises(TimeoutError):
            self.run_native()
        self.assertEqual(self.http.requests, [True])
        attempts = self.attempts()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].status.status_code.name, "ERROR")
        self.assertEqual(attempts[0].attributes["error.type"], "TimeoutError")
        self.assertNotIn("gen_ai.response.id", attempts[0].attributes)
