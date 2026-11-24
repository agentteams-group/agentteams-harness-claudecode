"""Bind SDK permission and input receipts to the Matrix adapter."""
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

HARNESS = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("AGENTTEAMS_CORE_DIR", HARNESS.parent))
sys.path[:0] = [str(HARNESS), str(ROOT / "shared/lib"), str(ROOT / "deepseek-harness/scripts")]
from claude_agent_sdk import ResultMessage, SystemMessage
from claude_agent_sdk.types import ToolPermissionContext
from interaction import MatrixTurn, TurnStopped
from turn import run_turn


def result(text="done", **extra):
    return ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1,
                         is_error=False, num_turns=1, session_id="native-session", result=text, **extra)


class TurnInteractionTests(unittest.IsolatedAsyncioTestCase):
    async def drive(self, stream, watch):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            record = {"member": {"role": "worker", "matrixUserId": "@cc:at"}, "desired": {}}
            event = {"event_id": "$input", "sender": "@human:at", "room_id": "!room:at"}
            matrix = Mock()
            matrix.send_content.return_value = "$card"
            interaction = MatrixTurn(matrix, record, event, {"events": {}, "sessions": {}}, Mock())
            interaction.start = AsyncMock()
            client = Mock()
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            client.query = AsyncMock()
            client.interrupt = AsyncMock()
            factory = Mock(return_value=client)
            client.receive_response.side_effect = lambda: stream(factory.call_args.kwargs["options"], client, interaction)
            interaction.watch = lambda native: watch(native, interaction)
            with patch("turn.ClaudeSDKClient", factory), patch("turn.prepare_assets", return_value="You are a Worker"), \
                    patch("turn.mcp_servers", return_value={"teamharness": {"command": "unused", "env": {}}}), \
                    patch.dict(os.environ, AGENTTEAMS_LLM_STREAM_TIMEOUT_S="2"):
                return await run_turn(record, {"models": [{"id": "fixture"}], "baseUrl": "http://fixture", "apiKey": "fixture"},
                                      root, "Task", None, {}, root, interaction=interaction)

    async def test_native_question_callback_waits_for_verified_matrix_answer(self):
        async def stream(options, client, interaction):
            yield SystemMessage(subtype="init", data={"session_id": "native-session"})
            self.assertIn("AskUserQuestion", options.tools)
            self.assertNotIn("AskUserQuestion", options.allowed_tools)
            answer = await options.can_use_tool("AskUserQuestion", {"questions": [{
                "question": "Output?", "options": [{"label": "JSON"}, {"label": "CSV"}]}]},
                ToolPermissionContext(tool_use_id="native-question"))
            self.assertEqual(answer.behavior, "allow")
            self.assertEqual(answer.updated_input["answers"], {"Output?": "JSON"})
            yield result()
        async def watch(native, interaction):
            while not interaction.pending:
                await asyncio.sleep(0)
            key = next(iter(interaction.pending))
            await interaction.handle("!room:at", {"type": "m.room.message", "event_id": "$answer", "sender": "@human:at",
                "content": {"com.agentteams.answer": {"kind": "ask_user", "agent_id": "@cc:at", "confirmation_id": key,
                    "responses": [{"id": "0", "selected": ["JSON"]}]}}}, native)
            await asyncio.Event().wait()
        self.assertEqual(await self.drive(stream, watch), ("native-session", "done"))

    async def test_steer_racing_with_completion_waits_for_its_native_input_receipt(self):
        calls = 0
        async def stream(options, client, interaction):
            nonlocal calls
            calls += 1
            if calls == 1:
                yield SystemMessage(subtype="init", data={"session_id": "native-session"})
                while not interaction.steers:
                    await asyncio.sleep(0)
                yield result("old answer", user_message_uuids=["original-uuid"])
            else:
                yield result("updated answer", user_message_uuids=list(interaction.steers.values()))
        async def watch(native, interaction):
            interaction.direct = True
            await interaction.handle("!room:at", {"type": "m.room.message", "event_id": "$steer", "sender": "@human:at",
                "content": {"body": "Use CSV instead", "com.agentteams.control": {
                    "action": "steer", "agent_id": "@cc:at", "run_id": "$input"}}}, native)
            await asyncio.Event().wait()
        self.assertEqual(await self.drive(stream, watch), ("native-session", "updated answer"))
        self.assertEqual(calls, 2)

    async def test_stop_never_returns_a_completed_answer(self):
        async def stream(options, client, interaction):
            yield SystemMessage(subtype="init", data={"session_id": "native-session"})
            while not interaction.stopped:
                await asyncio.sleep(0)
            yield result("late answer")
        async def watch(native, interaction):
            interaction.direct = True
            await interaction.handle("!room:at", {"type": "m.room.message", "event_id": "$stop", "sender": "@human:at",
                "content": {"com.agentteams.control": {"action": "stop", "agent_id": "@cc:at", "run_id": "$input"}}}, native)
            await asyncio.Event().wait()
        with self.assertRaises(TurnStopped):
            await self.drive(stream, watch)

    async def test_native_mcp_error_is_a_failed_tool_card(self):
        async def stream(options, client, interaction):
            data = {"tool_use_id": "native-tool", "tool_name": "mcp__teamharness__health", "tool_input": {}}
            await options.hooks["PreToolUse"][0].hooks[0](data, "native-tool", {})
            await options.hooks["PostToolUse"][0].hooks[0]({**data, "hook_event_name": "PostToolUse",
                "tool_response": {"isError": True, "content": [{"type": "text", "text": "service unavailable"}]}}, "native-tool", {})
            self.assertEqual(interaction.tools["native-tool"]["status"], "failed")
            yield result()
        async def watch(native, interaction):
            await asyncio.Event().wait()
        await self.drive(stream, watch)


if __name__ == "__main__":
    unittest.main()
