"""Silent sender receipts must survive a real bundled CLI resume."""
import asyncio
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from uuid import uuid4
from unittest.mock import patch

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, ResultMessage, get_session_messages
from test_member import Wire
from native_handoff import append_handoff


class NativeHandoffTest(unittest.TestCase):
    def test_replay_finds_native_input_omitted_by_projected_history(self):
        session, root_id = str(uuid4()), str(uuid4())
        fact = "[Task handoff receipt]\nMatrix event $confirmed."
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"CLAUDE_CONFIG_DIR": tmp}):
            project = Path(tmp) / "projects" / "-work"
            project.mkdir(parents=True)
            transcript = project / (session + ".jsonl")
            entries = [
                (root_id, None, "Begin"),
                (str(uuid4()), root_id, fact),
                (str(uuid4()), root_id, "Continue a parallel branch"),
            ]
            transcript.write_text("".join(json.dumps({"uuid": uid, "parentUuid": parent,
                    "sessionId": session, "type": "user", "cwd": "/work",
                    "message": {"role": "user", "content": content}}) + "\n"
                    for uid, parent, content in entries))
            projected = [row.message["content"] for row in get_session_messages(session, directory="/work")]
            self.assertIn("Continue a parallel branch", projected)
            self.assertNotIn(fact, projected)
            with patch("native_handoff.ClaudeSDKClient") as client:
                asyncio.run(append_handoff(transcript, Path("/work"), fact))
                client.assert_not_called()

    def test_receipt_survives_resume_and_replay_has_no_model_call(self):
        async def conversation(root, endpoint):
            async def chat(text, session=None):
                options = ClaudeAgentOptions(cwd=str(root), resume=session, tools=[], model="claude-sonnet-4-6",
                        env={"ANTHROPIC_BASE_URL": endpoint, "ANTHROPIC_AUTH_TOKEN": "fixture-gateway"})
                async with ClaudeSDKClient(options=options) as client:
                    await client.query(text)
                    async for message in client.receive_response():
                        if isinstance(message, ResultMessage):
                            self.assertFalse(message.is_error)
                            return message.session_id

            root.mkdir()
            session = await chat("second-turn: begin the fixture.")
            # A recreated Worker restores Claude projects before per-task directories.
            root.rmdir()
            fact = "[Task handoff receipt]\nMatrix event $actually-sent. No action is needed."
            transcript = next((root.parent / ".claude" / "projects").glob("*/" + session + ".jsonl"))
            await append_handoff(transcript, root, fact)
            await append_handoff(transcript, root, fact)
            self.assertEqual(len(http.requests), 1)
            messages = get_session_messages(session, directory=str(root))
            self.assertEqual(sum(m.message.get("content") == fact for m in messages), 1)
            await chat("second-turn: continue normal work.", session)
            self.assertEqual(len(http.requests), 2)
            self.assertEqual(json.dumps(http.requests[-1]["messages"]).count("$actually-sent"), 1)

        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(("127.0.0.1", 0), Wire) as http:
            threading.Thread(target=http.serve_forever, daemon=True).start()
            http.requests = []
            try:
                with patch.dict(os.environ, {"HOME": tmp, "CLAUDE_CONFIG_DIR": str(Path(tmp) / ".claude"),
                        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"}, clear=True):
                    asyncio.run(conversation(Path(tmp) / "work", f"http://127.0.0.1:{http.server_port}/route"))
            finally:
                http.shutdown()
