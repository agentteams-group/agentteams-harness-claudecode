"""Compare exported attempts to HTTP requests received from the bundled Claude CLI."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import TestCase, mock

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from test_member import ROOT
from turn import run_turn
import telemetry


class NativeRequestReplay(TestCase):
    def test_initial_string_message_is_recovered_only_by_its_native_hash(self):
        for valid in [True, False]:
            with self.subTest(valid=valid), tempfile.TemporaryDirectory() as temporary:
                session = 'native-session'
                text = '原始任务上下文' * 1000
                message = {'role': 'user', 'content': [{'type': 'text', 'text': text}]}
                digest = hashlib.sha256(json.dumps(message, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
                rows = [
                    {'type': 'user', 'sessionId': session, 'message': {'role': 'user', 'content': text if valid else text + 'changed'}},
                    {'type': 'api-request-shape', 'sessionId': session, 'shapeHash': 'shape', 'shape': {'system': []}},
                    {'type': 'api-request', 'sessionId': session, 'id': 'request', 'timestamp': '2026-11-28T00:00:00+00:00',
                     'shapeHash': 'shape', 'params': {'model': 'fixture'}, 'keep': 0, 'tail': [digest]},
                ]
                journal = Path(temporary) / 'session.jsonl'
                journal.write_text(''.join(json.dumps(row, separators=(',', ':'), ensure_ascii=False) + '\n' for row in rows))
                exporter = InMemorySpanExporter()
                provider = TracerProvider()
                provider.add_span_processor(SimpleSpanProcessor(exporter))
                try:
                    with mock.patch.dict(os.environ, {'AGENTTEAMS_OTEL_CAPTURE_CONTENT': 'true'}), mock.patch('telemetry.trace.get_tracer', provider.get_tracer):
                        telemetry._export_requests(journal, 0, 0, session, None)
                    span, = exporter.get_finished_spans()
                    if valid:
                        captured = json.loads(span.attributes['langfuse.observation.input'])
                        self.assertEqual(captured['messages'], [message])
                    else:
                        self.assertNotIn('langfuse.observation.input', span.attributes)
                        self.assertEqual(span.attributes['agentteams.model.input_unavailable'], 'native_journal_references_missing')
                finally:
                    provider.shutdown()


class Messages(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(request.get("stream", False))
        self.server.bodies.append(request)
        self.server.release.wait(getattr(self.server, "response_delay", 0))
        if getattr(self.server, "delay_first_headers", False) and len(self.server.requests) == 1:
            self.server.release.wait(20)
            return
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
                           "model": request["model"], "content": ([{"type": "thinking", "thinking": "fixture reasoning", "signature": "fixture"}] if getattr(self.server, "thinking", False) else []) + [{"type": "text", "text": "done"}],
                           "stop_reason": "end_turn", "usage": {"input_tokens": 2, "output_tokens": 1}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ModelRequests(TestCase):
    maxDiff = 800
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.http = self.enterContext(ThreadingHTTPServer(("127.0.0.1", 0), Messages))
        self.http.requests = []
        self.http.bodies = []
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

    def run_native(self, session=None, interaction=None, approval_mode=None, prompt="private-test-prompt"):
        record = {"member": {"role": "manager"}, "desired": {"approvalMode": approval_mode} if approval_mode else {}}
        provider = {"baseUrl": f"http://127.0.0.1:{self.http.server_port}", "apiKey": "fixture-secret",
                    "models": [{"id": "claude-sonnet-4-6"}]}
        return asyncio.run(run_turn(record, provider, self.directory, prompt, session,
                                    {}, ROOT / "plugins/teamharness", interaction=interaction))

    def attempts(self):
        return [s for s in self.exporter.get_finished_spans() if s.name == "claude-code.model.request"]

    def test_native_cli_accepts_adapter_pings_while_upstream_is_silent(self):
        requests = []
        async def frames(model):
            await asyncio.sleep(0.08)
            yield {"type": "message_start", "message": {"id": "msg_ping", "type": "message",
                   "role": "assistant", "model": model, "content": [],
                   "usage": {"input_tokens": 2, "output_tokens": 0}}}
            yield {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}
            yield {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "done"}}
            yield {"type": "content_block_stop", "index": 0}
            yield {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                   "usage": {"output_tokens": 1}}
            yield {"type": "message_stop"}
        async def convert(body, _provider):
            requests.append(body)
            await asyncio.sleep(0.08)
            return frames(body['model'])
        os.environ['AGENTTEAMS_CLAUDE_UPSTREAM_API'] = 'responses'
        with mock.patch('responses_compat.convert_messages', convert), mock.patch('responses_compat._PING_SECONDS', 0.01):
            _, answer = self.run_native()
        self.assertEqual(answer, 'done')
        self.assertEqual(len(requests), 1)
        self.assertTrue(requests[0]['stream'])

    def test_native_stream_drains_buffered_thinking_after_a_slow_matrix_send(self):
        from interaction import MatrixTurn
        updates=[]
        matrix=mock.Mock()
        def send(_room, content, _transaction):
            blocks=content.get('org.agentteams.run',{}).get('blocks',[])
            if blocks and blocks[0].get('type')=='thinking' and blocks[0].get('isStreaming'):
                updates.append(blocks[0]); time.sleep(1.1)
            return '$progress'
        matrix.send_content.side_effect=send
        turn=MatrixTurn(matrix, {'member':{'matrixUserId':'@manager:test'},'desired':{}},
            {'room_id':'!test','event_id':'$test','sender':'@human:test'}, {}, lambda:None)
        turn.start=mock.AsyncMock()
        async def watch(_client):
            await asyncio.Event().wait()
        turn.watch=watch
        async def frames(model):
            yield {'type':'message_start','message':{'id':'slow-progress','type':'message',
                'role':'assistant','model':model,'content':[],'usage':{'input_tokens':2,'output_tokens':0}}}
            yield {'type':'content_block_start','index':0,'content_block':{'type':'thinking','thinking':'','signature':''}}
            for _ in range(20):
                yield {'type':'content_block_delta','index':0,'delta':{'type':'thinking_delta','thinking':'checking '}}
            yield {'type':'content_block_delta','index':0,'delta':{'type':'signature_delta','signature':'fixture'}}
            yield {'type':'content_block_stop','index':0}
            yield {'type':'content_block_start','index':1,'content_block':{'type':'text','text':''}}
            yield {'type':'content_block_delta','index':1,'delta':{'type':'text_delta','text':'done'}}
            yield {'type':'content_block_stop','index':1}
            yield {'type':'message_delta','delta':{'stop_reason':'end_turn','stop_sequence':None},'usage':{'output_tokens':21}}
            yield {'type':'message_stop'}
        async def convert(body,_provider):
            return frames(body['model'])
        os.environ['AGENTTEAMS_CLAUDE_UPSTREAM_API']='responses'
        with mock.patch('responses_compat.convert_messages',convert):
            _,answer=self.run_native(interaction=turn)
        self.assertEqual(answer,'done')
        self.assertLessEqual(len(updates),2)
        self.assertEqual(turn.fragments,{})

    def test_stream_disconnect_then_success_and_resume(self):
        os.environ["AGENTTEAMS_OTEL_CAPTURE_CONTENT"] = "false"
        session, answer = self.run_native()
        self.assertEqual(answer, "done")
        self.assertEqual(self.http.requests, [True, False])
        attempts = self.attempts()
        self.assertEqual(len(attempts), 2)
        self.assertEqual([s.status.status_code.name for s in attempts], ["ERROR", "OK"])
        self.assertEqual(attempts[1].attributes["gen_ai.response.id"], "msg_local")
        self.assertEqual(attempts[1].attributes["gen_ai.usage.input_tokens"],2)
        self.assertEqual(attempts[1].attributes["gen_ai.usage.output_tokens"],1)
        self.assertNotIn("gen_ai.response.id", attempts[0].attributes)
        self.assertEqual(len({s.attributes["agentteams.model.request.id"] for s in attempts}), 2)
        turn = next(s for s in self.exporter.get_finished_spans() if s.name == "claude-code.turn")
        self.assertTrue(all(s.parent.span_id == turn.context.span_id for s in attempts))
        self.assertIn("worker-turn",turn.attributes["langfuse.trace.tags"])
        self.assertTrue(turn.attributes["langfuse.user.id"])
        self.assertEqual(turn.attributes["langfuse.session.id"],session)
        self.assertNotIn("private-test-prompt", str([dict(s.attributes) for s in attempts]))
        self.assertNotIn("fixture-secret", str([dict(s.attributes) for s in attempts]))
        old_ids = {s.attributes["agentteams.model.request.id"] for s in attempts}
        self.exporter.clear()
        self.http.requests.clear()
        self.assertEqual(self.run_native(session), (session, "done"))
        self.assertEqual(len(self.attempts()), len(self.http.requests))
        self.assertTrue(old_ids.isdisjoint(s.attributes["agentteams.model.request.id"] for s in self.attempts()))

    def test_input_matches_native_wire_on_first_turn_and_resume(self):
        self.http.thinking = True
        os.environ["AGENTTEAMS_OTEL_CAPTURE_CONTENT"] = "true"
        session, _ = self.run_native()
        for resumed in [False, True]:
            if resumed:
                self.exporter.clear()
                self.http.requests.clear()
                self.http.bodies.clear()
                self.run_native(session)
            for span, body in zip(self.attempts(), self.http.bodies, strict=True):
                captured = json.loads(span.attributes["langfuse.observation.input"])
                self.assertEqual(captured["messages"], body["messages"])
                # Native replay excludes the SDK's transient billing/version header.
                system = [block for block in body["system"] if not block.get("text", "").startswith("x-anthropic-billing-header:")]
                self.assertEqual(captured["system"], system)
                self.assertEqual(captured.get("tools"), body.get("tools"))
                self.assertNotIn("fixture-secret", str(captured))

    def test_large_initial_input_matches_actual_native_wire_without_preview(self):
        os.environ['AGENTTEAMS_OTEL_CAPTURE_CONTENT'] = 'true'
        prompt = '完整的长任务上下文' * 15000 + ' END-OF-INPUT'
        self.run_native(prompt=prompt)
        for span, body in zip(self.attempts(), self.http.bodies, strict=True):
            captured = json.loads(span.attributes['langfuse.observation.input'])
            self.assertEqual(captured['messages'], body['messages'])
            self.assertIn('END-OF-INPUT', json.dumps(captured, ensure_ascii=False))
            self.assertGreater(len(json.dumps(captured, ensure_ascii=False).encode()), 256 * 1024)

    def test_model_input_keeps_native_tool_results(self):
        from test_member import Wire
        class ToolWire(Wire):
            def do_POST(self):
                self.path = "/route" + self.path
                self.headers.replace_header("Authorization", "Bearer fixture-gateway")
                super().do_POST()
        self.http.RequestHandlerClass = ToolWire
        self.http.bodies = self.http.requests
        self.http.tool_name = "mcp__teamharness__health"
        self.http.tool_input = {}
        os.environ["AGENTTEAMS_OTEL_CAPTURE_CONTENT"] = "true"
        self.assertEqual(self.run_native(approval_mode="allow")[1], "first-ok")
        self.assertGreaterEqual(len(self.http.bodies), 2)
        self.assertIn("tool_result", json.dumps(self.http.bodies[-1]["messages"]))
        for span, body in zip(self.attempts(), self.http.bodies, strict=True):
            self.assertIn("langfuse.observation.input", span.attributes)
            captured = json.loads(span.attributes["langfuse.observation.input"])
            # Native replay normalizes trailing MCP result whitespace; it
            # remains content-addressed, rather than fabricated missing input.
            from telemetry import _replay_text
            self.assertEqual(_replay_text(captured["messages"]), _replay_text(body["messages"]))

    def test_returned_thinking_follows_global_content_switch(self):
        self.http.thinking = True
        with mock.patch.dict(os.environ, {"AGENTTEAMS_OTEL_CAPTURE_CONTENT": "true"}):
            self.assertEqual(self.run_native()[1], "done")
        outputs = [s.attributes.get("langfuse.observation.output", "") for s in self.attempts()]
        self.assertTrue(any("fixture reasoning" in value for value in outputs), outputs)
        self.exporter.clear()
        with mock.patch.dict(os.environ, {"AGENTTEAMS_OTEL_CAPTURE_CONTENT": "false"}):
            self.assertEqual(self.run_native()[1], "done")
        self.assertTrue(all("langfuse.observation.output" not in s.attributes for s in self.attempts()))
        self.assertTrue(all("langfuse.observation.input" not in s.attributes for s in self.attempts()))

    def test_http_retry_and_stream_fallback_are_separate_attempts(self):
        self.http.reject_first = True
        _, answer = self.run_native()
        self.assertEqual(answer, "done")
        self.assertEqual(self.http.requests, [True, True, False])
        self.assertEqual([s.status.status_code.name for s in self.attempts()], ["ERROR", "ERROR", "OK"])

    def test_native_timeout_then_success_records_each_request(self):
        self.http.delay_first_headers = True
        os.environ["API_TIMEOUT_MS"] = "1000"
        self.assertEqual(self.run_native()[1], "done")
        attempts = self.attempts()
        self.assertGreaterEqual(len(attempts), 3)
        self.assertEqual(len(attempts), len(self.http.requests))
        self.assertEqual(attempts[0].status.status_code.name, "ERROR")
        self.assertNotIn("gen_ai.response.id", attempts[0].attributes)
        self.assertEqual(attempts[-1].status.status_code.name, "OK")

    def test_native_request_can_outlast_the_previous_message_deadline(self):
        self.http.response_delay = 3
        os.environ["AGENTTEAMS_LLM_STREAM_TIMEOUT_S"] = "2"
        os.environ["API_TIMEOUT_MS"] = "15000"
        self.assertEqual(self.run_native()[1], "done")
        self.assertEqual(self.http.requests, [True, False])
        self.assertEqual([span.status.status_code.name for span in self.attempts()], ["ERROR", "OK"])
