"""Run the real bundled Claude Code executable against local protocol fixtures."""
import json
import io
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch
from urllib.parse import urlsplit
from urllib.error import HTTPError

import yaml

HARNESS = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("AGENTTEAMS_CORE_DIR", HARNESS.parent))
sys.path[:0] = [str(HARNESS), str(ROOT / "deepseek-harness/scripts"), str(ROOT / "shared/lib")]
from bridge import MemberState, admitted, load_member, poll, retry_delay


def message(text, event_id, sender="@human:at"):
    return {"type": "m.room.message", "event_id": event_id, "sender": sender,
            "content": {"msgtype": "m.text", "body": text}}


class Wire(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, value, code=200):
        data = json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/_matrix/client/v3/sync?"):
            self.reply({"next_batch": self.server.cursor, "rooms": {"join": {
                "!dm:at": {"timeline": {"events": self.server.batch}}}}})
        elif self.path.endswith("joined_rooms"):
            self.reply({"joined_rooms": ["!dm:at"]})
        elif self.path.endswith("joined_members"):
            self.reply({"joined": {"@cc:at": {}, "@human:at": {}}})
        elif "/state/m.room.encryption" in self.path:
            self.reply({"errcode": "M_NOT_FOUND"}, 404)
        else:
            self.reply({}, 404)

    def do_PUT(self):
        content = json.load(self.rfile_body())
        self.server.puts.append(self.path)
        self.server.sent[self.path] = content
        if self.server.lose_ack and content.get("body") == "first-ok":
            self.server.lose_ack = False
            self.reply({"errcode": "M_UNAVAILABLE"}, 503)
            return
        self.reply({"event_id": "$receipt"})

    def rfile_body(self):
        return io.BytesIO(self.rfile.read(int(self.headers["Content-Length"])))

    def do_POST(self):
        if urlsplit(self.path).path != "/route/v1/messages":
            self.reply({}, 404)
            return
        if self.headers.get("Authorization") != "Bearer fixture-gateway" or not self.headers.get("anthropic-version"):
            self.reply({}, 401)
            return
        request = json.load(self.rfile_body())
        self.server.requests.append(request)
        history = json.dumps(request["messages"])
        resume = "second-turn" in history
        first = not resume and "tool_result" not in history
        model = request["model"]
        usage = {"input_tokens": 5, "output_tokens": 3}
        start = {"id": "msg_fixture_" + str(len(self.server.requests)), "type": "message", "role": "assistant",
                 "model": model, "content": [], "stop_reason": None, "usage": usage}
        tool_name = getattr(self.server, "tool_name", "mcp__teamharness__health")
        tool_input = getattr(self.server, "tool_input", {})
        block = {"type": "tool_use", "id": "toolu_local", "name": tool_name, "input": {}} if first else {
            "type": "text", "text": ""}
        delta = {"type": "input_json_delta", "partial_json": json.dumps(tool_input)} if first else {
            "type": "text_delta", "text": "second-ok" if resume else "first-ok"}
        frames = [
            ("message_start", {"message": start}),
            ("content_block_start", {"index": 0, "content_block": block}),
            ("content_block_delta", {"index": 0, "delta": delta}),
            ("content_block_stop", {"index": 0}),
            ("message_delta", {"delta": {"stop_reason": "tool_use" if first else "end_turn", "stop_sequence": None},
                               "usage": usage}),
            ("message_stop", {}),
        ]
        if first and getattr(self.server, "thinking", None):
            for _, value in frames:
                if "index" in value:
                    value["index"] += 1
            frames[1:1] = [
                ("content_block_start", {"index": 0, "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
                ("content_block_delta", {"index": 0, "delta": {"type": "thinking_delta", "thinking": self.server.thinking}}),
                ("content_block_delta", {"index": 0, "delta": {"type": "signature_delta", "signature": "fixture-signature"}}),
                ("content_block_stop", {"index": 0}),
            ]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for kind, value in frames:
            self.wfile.write(("event: " + kind + "\ndata: " + json.dumps(dict(value, type=kind)) + "\n\n").encode())
        self.wfile.flush()


class MemberTest(unittest.TestCase):
    def test_registers_worker_process_before_matrix_sync(self):
        env = {"AGENTTEAMS_CONTROLLER_URL": "http://controller", "AGENTTEAMS_WORKER_NAME": "cc",
               "AGENTTEAMS_PROCESS_ID": "supervisor-start"}
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, env, clear=True):
            home = Path(directory)
            os.environ.update(TEAMHARNESS_WORKSPACE=str(home / "workspace"), CLAUDE_CONFIG_DIR=str(home / "claude"))
            runtime = home / "runtime.yaml"
            for role in ("leader", "worker", "manager"):
                state = MemberState(runtime)
                calls = []
                record = {"member": {"role": role, "matrixUserId": "@cc:at"}}
                transport = {"homeserver": "http://matrix", "accessToken": "fixture"}
                client = Mock()
                def sync(*_):
                    calls.append("sync")
                    return {"next_batch": "c1"}
                client.sync.side_effect = sync
                with patch("bridge.load_member", return_value=(record, transport, {})), \
                     patch("bridge.MatrixClient", return_value=client), patch("bridge.managed_rooms", return_value=set()), \
                     patch("bridge.subprocess.run"), patch("bridge.trace"), patch("bridge.recover_work"), \
                     patch("bridge.scan", side_effect=lambda *_: (calls.append("sync"), [], {})), \
                     patch("worker_matrix_state.WorkerMatrix.start", side_effect=lambda: calls.append("start")):
                    poll(state, False)
                    poll(state, True)
                self.assertEqual(calls, ["sync", "sync"] if role == "manager" else ["start", "sync", "sync"])

    def test_matrix_retry_obeys_server_delay_and_permanent_failure(self):
        def error(status, body):
            return HTTPError("http://matrix", status, "fixture", {}, io.BytesIO(body))
        self.assertEqual(retry_delay(error(429, b'{"retry_after_ms": 125}'), 1), 0.125)
        self.assertIsNone(retry_delay(error(403, b""), 1))
        self.assertTrue(2 <= retry_delay(error(503, b""), 2) <= 2.5)

    def test_manager_sender_policy_resolves_local_names(self):
        env = {"AGENTTEAMS_WORKER_NAME": "manager", "AGENTTEAMS_AGENT_ROLE": "manager",
               "AGENTTEAMS_MATRIX_DOMAIN": "at", "AGENTTEAMS_MATRIX_URL": "http://matrix",
               "AGENTTEAMS_WORKER_MATRIX_TOKEN": "fixture", "AGENTTEAMS_WORKER_GATEWAY_KEY": "fixture"}
        policy = {"groupAllowExtra": ["lead"], "dmAllowExtra": ["alice", "@alice:other"], "dmDenyExtra": ["alice"]}
        record = {"kind": "MemberRuntimeConfig", "apiVersion": "agentteams.io/v1beta1",
                  "member": {"runtime": "claudecode", "role": "manager", "runtimeName": "manager", "matrixUserId": "@manager:at"},
                  "storage": {"memberPrefix": "agents/manager"},
                  "desired": {"channelPolicy": policy, "model": {"model": "fixture", "gatewayUrl": "http://gateway"}}}
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, env, clear=True):
            config = Path(directory) / "runtime.yaml"
            config.write_text(yaml.safe_dump(record))
            _, transport, _ = load_member(config)
            self.assertEqual(transport["groupAllowFrom"], ["@admin:at", "@lead:at"])
            rooms = {"!dm:at": "personal"}
            def accept(sender):
                return admitted(message("hello", "$input", sender), "!dm:at", rooms, record["member"], set(), transport, policy)
            self.assertIsNone(accept("@alice:at"))
            self.assertIsNotNone(accept("@alice:other"))
            self.assertIsNone(accept("@lead:at"))

    def test_matrix_tool_resume_and_duplicate(self):
        with tempfile.TemporaryDirectory() as directory, ThreadingHTTPServer(("127.0.0.1", 0), Wire) as http:
            threading.Thread(target=http.serve_forever, daemon=True).start()
            self.addCleanup(http.shutdown)
            http.requests, http.puts, http.sent, http.lose_ack = [], [], {}, True
            rejected = "AT-unauthorized-message-fixture-819d"
            http.cursor, http.batch = "c1", [message(rejected, "$bad", "@stranger:at"), message("first-turn", "$one")]
            url = "http://127.0.0.1:" + str(http.server_port)
            home = Path(directory)
            runtime = home / "runtime/runtime.yaml"
            runtime.parent.mkdir()
            record = {"kind": "MemberRuntimeConfig", "apiVersion": "agentteams.io/v1beta1",
                      "member": {"runtime": "claudecode", "runtimeName": "cc", "role": "standalone",
                                 "personalRoomId": "!dm:at", "matrixUserId": "@cc:at"},
                      "storage": {"memberPrefix": "agents/cc"},
                      "matrix": {"transport": {"homeserver": url, "groupPolicy": "disabled",
                                               "dm": {"policy": "allowlist", "allowFrom": ["@human:at"]}}},
                      "desired": {"model": {"model": "claude-sonnet-4-6", "gatewayUrl": url + "/route/v1"}}}
            runtime.write_text(yaml.safe_dump(record))
            env = {"PATH": os.environ["PATH"], "HOME": directory, "CLAUDE_CONFIG_DIR": str(home / ".claude"),
                   "AGENTTEAMS_WORKER_NAME": "cc", "AGENTTEAMS_WORKER_MATRIX_TOKEN": "fixture-matrix",
                   "AGENTTEAMS_WORKER_GATEWAY_KEY": "fixture-gateway", "AGENTTEAMS_LOG_DIR": str(home / "logs"),
                   "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness"),
                   "TEAMHARNESS_RUNTIME_CONFIG": str(runtime), "TEAMHARNESS_WORKSPACE": str(home / "workspace"),
                   "AGENTTEAMS_LLM_STREAM_TIMEOUT_S": "45"}
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaises(HTTPError):
                    poll(MemberState(runtime), False)
                calls = len(http.requests)
                poll(MemberState(runtime), True)
                self.assertEqual(len(http.requests), calls)
                replies = [out for out in http.sent.values() if not out.get("org.agentteams.progress_only")]
                final_puts = [path for path in http.puts if http.sent[path].get("body") == "first-ok"]
                self.assertEqual(len(final_puts), 2)
                self.assertEqual(final_puts[0], final_puts[1])
                self.assertEqual([out["body"] for out in replies], ["first-ok"])
                results = [b for m in http.requests[-1]["messages"] if isinstance(m["content"], list)
                           for b in m["content"] if b.get("type") == "tool_result"]
                self.assertEqual(len(results), 1)
                self.assertFalse(results[0].get("is_error"), results)
                self.assertIn("health", json.dumps(results).lower())
                http.cursor, http.batch = "c2", [http.batch[1], message("second-turn", "$two")]
                previous_count = len(http.requests)
                poll(MemberState(runtime), True)
                replies = [out for out in http.sent.values() if not out.get("org.agentteams.progress_only")]
                self.assertEqual([out["body"] for out in replies], ["first-ok", "second-ok"])
                self.assertEqual(len(http.requests), previous_count + 1)
                self.assertIn("first-turn", json.dumps(http.requests[-1]["messages"]))
                self.assertFalse(any(rejected in json.dumps(req["messages"]) for req in http.requests))
                self.assertEqual(replies[-1]["m.relates_to"]["m.in_reply_to"]["event_id"], "$two")


if __name__ == "__main__":
    unittest.main()
