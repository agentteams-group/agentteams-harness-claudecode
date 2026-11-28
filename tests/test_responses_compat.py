"""The local SDK boundary keeps credentials, upstream and stream semantics fixed."""
import asyncio
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest
from unittest.mock import AsyncMock, patch

from starlette.testclient import TestClient
from responses_compat import convert_messages, responses_app, model_endpoint, stream_events


PROVIDER = {"baseUrl":"https://gateway.example/prefix/v1", "apiKey":"consumer-fixture", "models":[{"id":"selected-model"}]}
HEADERS = {"Authorization":"Bearer consumer-fixture"}


class ResponsesCompatibilityTests(unittest.TestCase):
    def test_stream_keeps_native_connection_alive_during_conversion_and_model_silence(self):
        async def run():
            async def delayed_events():
                await asyncio.sleep(0.04)
                yield {"type": "message_stop"}
            async def convert(*_):
                await asyncio.sleep(0.04)
                return delayed_events()
            with patch('responses_compat.convert_messages', convert), patch('responses_compat._PING_SECONDS', 0.01):
                frames = [frame async for frame in stream_events({}, PROVIDER)]
            assert frames.count('event: ping\ndata: {"type":"ping"}\n\n') >= 2
            assert json.loads(frames[-1].split('data: ')[1]) == {"type": "message_stop"}
        asyncio.run(run())

    def test_stream_failure_keeps_auth_semantics_and_redacts_credentials(self):
        async def run():
            error = RuntimeError('Rejected ' + PROVIDER['apiKey'])
            error.status_code = 401
            with patch('responses_compat.convert_messages', AsyncMock(side_effect=error)):
                frames = [frame async for frame in stream_events({}, PROVIDER)]
            event = json.loads(frames[0].split('data: ')[1])
            assert event['error']['type'] == 'authentication_error'
            assert PROVIDER['apiKey'] not in frames[0]
        asyncio.run(run())

    def test_disconnecting_a_silent_stream_cancels_its_upstream_request(self):
        async def run():
            cancelled = asyncio.Event()
            async def convert(*_):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            with patch('responses_compat.convert_messages', convert), patch('responses_compat._PING_SECONDS', 0.01):
                stream = stream_events({}, PROVIDER)
                assert 'event: ping' in await anext(stream)
                await stream.aclose()
            assert cancelled.is_set()
        asyncio.run(run())

    def test_native_converter_keeps_system_blocks_in_one_leading_message(self):
        received = []

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                received.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'id': 'resp-system', 'object': 'response', 'created_at': 1,
                    'status': 'completed', 'model': 'selected-model',
                    'output': [{'id': 'msg-system', 'type': 'message', 'role': 'assistant',
                        'status': 'completed', 'content': [{'type': 'output_text', 'text': 'OK', 'annotations': []}]}],
                    'usage': {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2},
                }).encode())

        server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        body = {'model': 'selected-model', 'max_tokens': 128,
                'system': [{'type': 'text', 'text': 'First instruction'},
                           {'type': 'text', 'text': 'Second instruction', 'cache_control': {'type': 'ephemeral'}}],
                'messages': [{'role': 'user', 'content': 'Hello'},
                             {'role': 'system', 'content': 'Updated instruction'}]}
        try:
            provider = {**PROVIDER, 'baseUrl': f'http://127.0.0.1:{server.server_port}/v1'}
            asyncio.run(convert_messages(body, provider))
        finally:
            server.shutdown()
            thread.join()
            server.server_close()
        self.assertEqual([message['role'] for message in received[0]['input']], ['developer', 'user'])
        self.assertEqual(received[0]['input'][0]['content'][0]['text'], 'First instruction\n\nSecond instruction\n\nUpdated instruction')
        self.assertIsInstance(body['system'], list)
        self.assertEqual(body['messages'][-1]['role'], 'system')

    def test_unknown_gateway_model_keeps_requested_upstream_streaming(self):
        import httpx
        from unittest.mock import patch
        sent = []
        async def capture(_client, request, **kwargs):
            sent.append(json.loads(request.content))
            raise RuntimeError("fixture stops before any upstream effect")
        body = {"model": "gateway-native-stream-fixture", "max_tokens": 32000, "stream": True,
                "thinking": {"type": "adaptive"}, "output_config": {"effort": "high"},
                "messages": [{"role": "user", "content": "Hello"}]}
        async def run():
            with patch.object(httpx.AsyncClient, "send", capture):
                with self.assertRaises(Exception):
                    await convert_messages(body, PROVIDER)
        asyncio.run(run())
        self.assertEqual(len(sent), 1)
        self.assertIs(sent[0].get("stream"), True)
        self.assertEqual(sent[0].get("max_output_tokens"), 32000)
        self.assertEqual(sent[0].get("reasoning", {}).get("effort"), "high")

    def test_converter_preserves_gateway_reasoning_text_events(self):
        reasoning = {"id": "rs-fixture", "type": "reasoning", "summary": []}
        text = {"id": "msg-fixture", "type": "message", "role": "assistant", "status": "completed",
                "content": [{"type": "output_text", "text": "done", "annotations": []}]}
        response = {"id": "resp-fixture", "object": "response", "created_at": 1,
                    "model": "gateway-reasoning-fixture", "status": "completed", "output": [reasoning, text],
                    "usage": {"input_tokens": 3, "output_tokens": 5, "total_tokens": 8}}
        events = [
            {"type": "response.created", "response": {**response, "status": "in_progress", "output": []}},
            {"type": "response.output_item.added", "output_index": 0, "item": reasoning},
            {"type": "response.reasoning_text.delta", "item_id": reasoning["id"], "output_index": 0, "content_index": 0, "delta": "fixture thinking"},
            {"type": "response.output_item.done", "output_index": 0, "item": reasoning},
            {"type": "response.output_item.added", "output_index": 1, "item": {**text, "status": "in_progress", "content": []}},
            {"type": "response.output_text.delta", "item_id": text["id"], "output_index": 1, "content_index": 0, "delta": "done"},
            {"type": "response.output_item.done", "output_index": 1, "item": text},
            {"type": "response.completed", "response": response},
        ]
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                self.server.streaming = request.get("stream")
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for number, event in enumerate(events):
                    self.wfile.write(("data:" + json.dumps({**event, "sequence_number": number}) + "\n\n").encode())
                    self.wfile.flush()
        with ThreadingHTTPServer(("127.0.0.1", 0), Upstream) as server:
            threading.Thread(target=server.serve_forever, daemon=True).start()
            try:
                async def run():
                    stream = await convert_messages({"model": response["model"], "stream": True, "max_tokens": 128,
                        "messages": [{"role": "user", "content": "Hello"}]},
                        {**PROVIDER, "baseUrl": f"http://127.0.0.1:{server.server_port}/v1"})
                    return [chunk.decode() if isinstance(chunk, bytes) else str(chunk) async for chunk in stream]
                frames = asyncio.run(run())
                import tempfile
                from pathlib import Path
                from test_member import ROOT
                from turn import run_turn
                with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {
                        "HOME": temporary, "CLAUDE_CONFIG_DIR": str(Path(temporary) / ".claude"),
                        "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness"),
                        "AGENTTEAMS_CLAUDE_UPSTREAM_API": "responses"}, clear=True):
                    _, answer = asyncio.run(run_turn({"member": {"role": "manager"}, "desired": {}},
                        {**PROVIDER, "models": [{"id": response["model"]}],
                         "baseUrl": f"http://127.0.0.1:{server.server_port}/v1"},
                        Path(temporary), "Return the fixture result.", None, {}, ROOT / "plugins/teamharness"))
                    self.assertEqual(answer, "done")
            finally:
                server.shutdown()
        self.assertIs(server.streaming, True)
        self.assertIn("fixture thinking", "".join(frames))
        self.assertIn("thinking_delta", "".join(frames))
        self.assertIn("done", "".join(frames))

    def test_known_model_capabilities_are_not_overwritten(self):
        import litellm
        body = {"model": "known-fixture", "max_tokens": 128, "stream": True, "messages": []}
        with patch.object(litellm, "get_model_info", return_value={"supports_native_streaming": False}), \
             patch.object(litellm, "register_model") as register, \
             patch("litellm.anthropic.messages.acreate", AsyncMock(return_value={})):
            asyncio.run(convert_messages(body, PROVIDER))
        register.assert_not_called()

    def test_only_authenticated_anthropic_requests_reach_the_saved_upstream(self):
        convert = AsyncMock(return_value={"id":"msg-fixture","type":"message","role":"assistant","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn"})
        with patch("responses_compat.convert_messages", convert), TestClient(responses_app(PROVIDER)) as client:
            self.assertEqual(client.post('/v1/messages',json={"model":"selected-model","messages":[]}).status_code,401)
            self.assertEqual(client.post('/v1/messages',headers=HEADERS,json={"model":"other-model","messages":[]}).status_code,400)
            self.assertEqual(client.post('/v1/messages',headers=HEADERS,json={"model":"selected-model","messages":[],"api_base":"https://other.example"}).status_code,400)
            body={"model":"selected-model","max_tokens":128,"messages":[{"role":"user","content":"Hello"}]}
            response=client.post('/v1/messages',headers=HEADERS,json=body)
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.json()['content'][0]['text'],'OK')
            convert.assert_awaited_once_with(body,PROVIDER)

    def test_stream_delivers_typed_anthropic_events_without_buffering_the_answer(self):
        frames=[{"type":"message_start","message":{"id":"msg","type":"message","role":"assistant","content":[]}},
                {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call-one","name":"Read","input":{}}},
                {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"report.md\"}"}},
                {"type":"message_stop"}]
        async def events():
            for frame in frames:
                yield frame
        with patch("responses_compat.convert_messages", AsyncMock(return_value=events())), TestClient(responses_app(PROVIDER)) as client:
            response=client.post('/v1/messages',headers=HEADERS,json={"model":"selected-model","messages":[],"max_tokens":128,"stream":True})
            self.assertEqual(response.headers['content-type'].split(';')[0],'text/event-stream')
            received=[json.loads(line[6:]) for line in response.text.splitlines() if line.startswith('data: ')]
            self.assertEqual(received,frames)

    def test_existing_anthropic_endpoint_does_not_start_a_local_adapter(self):
        async def run():
            with patch.dict(os.environ,AGENTTEAMS_CLAUDE_UPSTREAM_API='anthropic'):
                async with model_endpoint(PROVIDER) as base:
                    self.assertEqual(base,'https://gateway.example/prefix')
        asyncio.run(run())

    def test_converter_native_sse_bytes_are_forwarded_without_reencoding(self):
        frames = [b'event: message_start\ndata: {"type":"message_start"}\n\n',
                  b'event: message_stop\ndata: {"type":"message_stop"}\n\n']
        async def events():
            for frame in frames:
                yield frame
        with patch('responses_compat.convert_messages', AsyncMock(return_value=events())), TestClient(responses_app(PROVIDER)) as client:
            response = client.post('/v1/messages', headers=HEADERS, json={
                'model':'selected-model', 'messages':[], 'max_tokens':128, 'stream':True})
            self.assertEqual(response.content, b''.join(frames))
