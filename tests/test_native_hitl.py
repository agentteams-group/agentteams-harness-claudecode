"""Real bundled CLI permission callbacks against a local Messages fixture."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import Mock, patch

from test_member import ROOT, Wire, message
from interaction import MatrixTurn, TurnStopped
from turn import run_turn


class NativeHitlTests(unittest.TestCase):
    def native(self, name, fields, approve=True, control=None, thinking=None):
        with tempfile.TemporaryDirectory() as directory, ThreadingHTTPServer(("127.0.0.1", 0), Wire) as http:
            threading.Thread(target=http.serve_forever, daemon=True).start()
            self.addCleanup(http.shutdown)
            http.requests, http.tool_name, http.tool_input = [], name, fields
            http.thinking = thinking
            root = Path(directory)
            record = {"member": {"role": "worker", "matrixUserId": "@cc:at", "personalRoomId": "!dm:at"}, "desired": {}}
            event = {"event_id": "$task", "sender": "@human:at", "room_id": "!dm:at"}
            sent = []
            matrix = Mock()
            matrix.request.return_value = {"joined": {"@cc:at": {}, "@human:at": {}}}
            def send(room, content, txn):
                sent.append(content)
                return "$card-" + str(len(sent))
            matrix.send_content.side_effect = send
            def sync(*_):
                events = []
                for content in sent:
                    approval = content.get("org.agentteams.approval")
                    if approval and approval["status"] == "pending":
                        events.append(message("Decision", "$permission"))
                        events[-1]["content"]["com.agentteams.answer"] = {
                            "kind": "tool_approval", "agent_id": "@cc:at", "confirmation_id": approval["request_id"],
                            "context_id": approval["context_id"], "answer": "approved" if approve else "rejected"}
                    for block in content.get("org.agentteams.run", {}).get("blocks", []):
                        if block["type"] == "confirmation" and block.get("status") == "pending":
                            events.append(message("Answer", "$question"))
                            events[-1]["content"]["com.agentteams.answer"] = {
                                "kind": "ask_user", "agent_id": "@cc:at", "confirmation_id": block["confirmation_id"],
                                "responses": [{"id": "0", "selected": ["JSON"]}]}
                if control and any(c.get("org.agentteams.approval", {}).get("status") == "approved" for c in sent):
                    events.append(message("second-turn: use the additional requirement", "$supplement"))
                    events[-1]["content"]["com.agentteams.control"] = {
                        "action": control, "agent_id": "@cc:at", "run_id": "$task"}
                return {"next_batch": "cursor", "rooms": {"join": {"!dm:at": {"timeline": {"events": events}}}}}
            matrix.sync.side_effect = sync
            env = {"PATH": os.environ["PATH"], "HOME": directory, "CLAUDE_CONFIG_DIR": str(root / ".claude"),
                   "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness"), "AGENTTEAMS_LLM_STREAM_TIMEOUT_S": "15",
                   "AGENTTEAMS_HITL_TIMEOUT_SECONDS": "5"}
            with patch.dict(os.environ, env, clear=True):
                self.last_data = {"events": {}}
                interaction = MatrixTurn(matrix, record, event, self.last_data, Mock())
                try:
                    answer = asyncio.run(run_turn(record, {"models": [{"id": "claude-sonnet-4-6"}],
                        "baseUrl": f"http://127.0.0.1:{http.server_port}/route", "apiKey": "fixture-gateway"},
                        root, "Complete the task", None, {}, ROOT / "plugins/teamharness", interaction=interaction))
                except TurnStopped:
                    time.sleep(3.5)
                    self.late_file_exists = (root / "approved.txt").exists()
                    raise
            return answer, sent, json.dumps(http.requests), (root / "approved.txt").exists()

    def test_real_native_question_receives_matrix_selection(self):
        answer, sent, history, _ = self.native("AskUserQuestion", {"questions": [{"question": "Output format?", "header": "Format",
            "options": [{"label": "JSON", "description": "Structured"}, {"label": "CSV", "description": "Table"}]}]})
        self.assertEqual(answer[1], "first-ok")
        self.assertIn("JSON", history)
        receipts = [b for c in sent for b in c.get("org.agentteams.run", {}).get("blocks", []) if b["type"] == "confirmation"]
        self.assertEqual([r["status"] for r in receipts], ["pending", "answered"])

    def test_real_native_bash_approval_and_rejection(self):
        for approved in (True, False):
            with self.subTest(approved=approved):
                answer, sent, _, exists = self.native("Bash", {"command": "printf native-approval-ok > approved.txt"}, approved)
                self.assertEqual(answer[1], "first-ok")
                self.assertEqual(exists, approved)
                receipts = [c["org.agentteams.approval"] for c in sent if "org.agentteams.approval" in c]
                self.assertEqual([r["status"] for r in receipts], ["pending", "approved" if approved else "rejected"])

    def test_real_native_steer_consumes_the_supplementary_uuid(self):
        answer, _, history, exists = self.native("Bash", {"command": "sleep 1; printf ok > approved.txt"}, control="steer")
        self.assertTrue(exists)
        self.assertEqual(answer[1], "second-ok")
        self.assertIn("second-turn", history)
        self.assertEqual(self.last_data["steers"]["$supplement"]["status"], "consumed")

    def test_real_native_stop_terminates_the_running_shell(self):
        with self.assertRaises(TurnStopped):
            self.native("Bash", {"command": "sleep 3; printf late > approved.txt"}, control="stop")
        self.assertFalse(self.late_file_exists)

    def test_real_native_thinking_is_a_structured_matrix_block(self):
        _, sent, _, _ = self.native("AskUserQuestion", {"questions": [{"question": "Output?", "header": "Format",
            "options": [{"label": "JSON", "description": "Structured"}, {"label": "CSV", "description": "Table"}]}]},
            thinking="Checking the supplied input")
        blocks = [block for content in sent for block in content.get("org.agentteams.run", {}).get("blocks", [])
                  if block["type"] == "thinking"]
        self.assertEqual(blocks[-1]["content"], "Checking the supplied input")
        self.assertFalse(blocks[-1]["isStreaming"])
