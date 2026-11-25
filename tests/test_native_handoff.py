"""Silent sender receipts must survive a real bundled CLI resume."""
import asyncio
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, ResultMessage, get_session_messages
from test_member import Wire
from native_handoff import append_handoff


class NativeHandoffTest(unittest.TestCase):
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

            session = await chat("second-turn: begin the fixture.")
            fact = "[Task handoff receipt]\nMatrix event $actually-sent. No action is needed."
            await append_handoff(session, root, fact)
            await append_handoff(session, root, fact)
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
                    asyncio.run(conversation(Path(tmp), f"http://127.0.0.1:{http.server_port}/route"))
            finally:
                http.shutdown()
