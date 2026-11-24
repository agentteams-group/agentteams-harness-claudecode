"""Exercise the Matrix wire contract without a model or a live homeserver."""
import asyncio
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, Mock

ROOT = Path(os.environ.get("AGENTTEAMS_CORE_DIR", Path(__file__).resolve().parents[2]))
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(ROOT / "shared/lib")]
from interaction import MatrixTurn


class InteractionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sent = []
        self.matrix = Mock()
        def send(room, content, transaction):
            self.sent.append((room, content, transaction))
            return "$wire-" + str(len(self.sent))
        self.matrix.send_content.side_effect = send
        self.record = {"member": {"matrixUserId": "@cc:at", "personalRoomId": "!dm:at"},
                       "team": {"admin": {"matrixUserId": "@admin:at"},
                                "members": [{"matrixUserId": "@leader:at", "role": "team_leader"}]}, "desired": {}}
        self.event = {"event_id": "$source", "room_id": "!dm:at", "sender": "@human:at"}
        self.data = {"events": {}, "sessions": {}}
        self.save = Mock()
        self.turn = MatrixTurn(self.matrix, self.record, self.event, self.data, self.save)
        self.native = Mock(interrupt=AsyncMock(), query=AsyncMock())

    def raw(self, content, sender="@human:at", event_id="$answer"):
        return {"type": "m.room.message", "event_id": event_id, "sender": sender, "content": content}

    async def pending(self, name="Bash", fields=None):
        waiter = asyncio.create_task(self.turn.permission(name, fields or {"command": "pwd"}, "call-1"))
        for _ in range(30):
            if self.turn.pending:
                return waiter, next(iter(self.turn.pending))
            await asyncio.sleep(0.01)
        self.fail("Permission was not published")

    def approval(self):
        return next(content["org.agentteams.approval"] for _, content, _ in reversed(self.sent)
                    if "org.agentteams.approval" in content)

    def question(self):
        return next(block for _, content, _ in reversed(self.sent)
                    for block in content.get("org.agentteams.run", {}).get("blocks", [])
                    if block["type"] == "confirmation")

    async def test_approval_is_one_shot_and_bound_to_room_actor_and_run(self):
        waiter, request = await self.pending()
        pending = self.approval()
        self.assertEqual(pending["reply_format"], "com.agentteams.answer")
        content = {"body": "/approve forged", "com.agentteams.answer": {
            "kind": "tool_approval", "agent_id": "@cc:at", "confirmation_id": request,
            "context_id": pending["context_id"], "answer": "approved"}}
        for room, sender, context in (("!other:at", "@human:at", pending["context_id"]),
                                      ("!dm:at", "@leader:at", pending["context_id"]),
                                      ("!dm:at", "@human:at", "old-run")):
            invalid = {**content, "com.agentteams.answer": {**content["com.agentteams.answer"], "context_id": context}}
            await self.turn.handle(room, self.raw(invalid, sender), self.native)
            self.assertFalse(waiter.done())
        await self.turn.handle("!dm:at", self.raw(content, event_id="$valid"), self.native)
        self.assertTrue(await waiter)
        self.assertEqual(self.approval()["status"], "approved")
        self.assertFalse(self.turn.pending)
        await self.turn.handle("!dm:at", self.raw(content, event_id="$duplicate"), self.native)

    async def test_question_preserves_multiselect_and_custom_answers(self):
        fields = {"questions": [{"question": "Which outputs?", "header": "Outputs", "multiSelect": True,
                    "options": [{"label": "CSV", "description": "Table"}, {"label": "JSON"}]}]}
        waiter, request = await self.pending("AskUserQuestion", fields)
        block = self.question()
        question = block["questions"][0]
        self.assertTrue(question["multi_select"])
        answer = {"kind": "ask_user", "agent_id": "@cc:at", "confirmation_id": request,
                  "responses": [{"id": question["id"], "selected": ["CSV", "JSON"], "custom": "also XML"}]}
        await self.turn.handle("!dm:at", self.raw({"com.agentteams.answer": answer}), self.native)
        self.assertEqual(await waiter, {"questions": fields["questions"],
                                       "answers": {"Which outputs?": ["CSV", "JSON", "also XML"]}})
        self.assertEqual(self.question()["status"], "answered")

    async def test_rejects_unknown_question_options_without_resolving(self):
        waiter, request = await self.pending("AskUserQuestion", {"questions": [
            {"question": "Choose", "options": [{"label": "A"}, {"label": "B"}]}]})
        question = self.question()["questions"][0]
        answer = {"kind": "ask_user", "agent_id": "@cc:at", "confirmation_id": request,
                  "responses": [{"id": question["id"], "selected": ["forged"]}]}
        await self.turn.handle("!dm:at", self.raw({"com.agentteams.answer": answer}), self.native)
        self.assertFalse(waiter.done())
        await self.turn.close("cancelled")
        self.assertFalse(await waiter)

    async def test_expiry_and_restart_retire_cards_without_approving(self):
        waiter, request = await self.pending()
        self.turn.pending[request]["expires_at"] = 0
        await self.turn.expire()
        self.assertFalse(await waiter)
        self.assertEqual(self.approval()["status"], "expired")
        waiter, _ = await self.pending()
        replacement = MatrixTurn(self.matrix, self.record, self.event, self.data, self.save)
        await replacement.retire_previous()
        self.assertEqual(self.approval()["status"], "expired")
        await self.turn.close("cancelled")
        self.assertFalse(await waiter)

    async def test_reused_native_call_id_cannot_reuse_an_old_approval(self):
        waiter, old = await self.pending()
        old_context = self.approval()["context_id"]
        await self.turn.resolve(old, "rejected", False)
        self.assertFalse(await waiter)
        waiter, new = await self.pending()
        self.assertNotEqual(old, new)
        await self.turn.handle("!dm:at", self.raw({"com.agentteams.answer": {"kind": "tool_approval", "agent_id": "@cc:at",
            "confirmation_id": old, "context_id": old_context, "answer": "approved"}}), self.native)
        self.assertFalse(waiter.done())
        await self.turn.close("cancelled")
        self.assertFalse(await waiter)

    async def test_stream_and_tools_edit_existing_blocks_with_stable_ids(self):
        await self.turn.stream({"type": "content_block_start", "index": 0,
                               "content_block": {"type": "thinking", "thinking": ""}})
        await self.turn.stream({"type": "content_block_delta", "index": 0,
                               "delta": {"type": "thinking_delta", "thinking": "Checking inputs"}})
        await self.turn.stream({"type": "content_block_stop", "index": 0})
        thinking = [content for _, content, _ in self.sent if "org.agentteams.run" in content]
        self.assertEqual(thinking[-1]["org.agentteams.run"]["blocks"][0]["content"], "Checking inputs")
        await self.turn.tool("call-1", "Read", {"file_path": "input.csv"}, "running")
        original = self.sent[-1][1]
        await self.turn.tool("call-1", "Read", {"file_path": "input.csv"}, "succeeded", "data")
        update = self.sent[-1][1]
        self.assertEqual(update["m.relates_to"]["rel_type"], "m.replace")
        self.assertEqual(original["org.agentteams.run"]["blocks"][0]["tool_call_id"],
                         update["m.new_content"]["org.agentteams.run"]["blocks"][0]["tool_call_id"])
        self.assertTrue(update["org.agentteams.progress_only"])

    async def test_controls_use_native_interrupt_and_steer_and_check_run(self):
        self.turn.direct = True
        def control(action, run="$source"):
            return {"body": "Additional requirement", "com.agentteams.control": {
                "action": action, "run_id": run, "agent_id": "@cc:at"}}
        await self.turn.handle("!dm:at", self.raw(control("stop", "$old")), self.native)
        self.native.interrupt.assert_not_called()
        await self.turn.handle("!dm:at", self.raw(control("steer"), event_id="$steer"), self.native)
        self.native.query.assert_awaited_once()
        source = self.native.query.await_args.args[0]
        message = await anext(source)
        self.assertIn("Additional requirement", message["message"]["content"])
        self.assertNotIn("$steer", self.data["events"])
        self.turn.confirm_inputs(Mock(user_message_uuids=[message["uuid"]], user_message_uuid=None))
        self.assertTrue(self.data["events"]["$steer"]["delivered"])
        await self.turn.handle("!dm:at", self.raw(control("stop"), event_id="$stop"), self.native)
        self.native.interrupt.assert_awaited_once()
        self.assertTrue(self.turn.stopped)

    async def test_control_scan_does_not_advance_the_main_inbox(self):
        self.data["cursor"] = "main-cursor"
        self.matrix.sync.return_value = {"next_batch": "control-cursor", "rooms": {"join": {"!dm:at": {
            "timeline": {"events": [self.raw({"body": "next independent task"})]}}}}}
        await self.turn.poll(self.native)
        self.assertEqual(self.data["cursor"], "main-cursor")
        self.assertNotIn("$answer", self.data["events"])

    async def test_plain_matrix_clients_can_answer_and_stop(self):
        waiter, request = await self.pending("AskUserQuestion", {"questions": [
            {"question": "Output?", "options": [{"label": "A"}, {"label": "B"}]}]})
        await self.turn.handle("!dm:at", self.raw({"body": f"/answer {request} {self.turn.context} custom answer"}), self.native)
        self.assertEqual((await waiter)["answers"], {"Output?": "custom answer"})
        self.turn.direct = True
        await self.turn.handle("!dm:at", self.raw({"body": "/cancel"}, event_id="$stop"), self.native)
        self.native.interrupt.assert_awaited_once()

    async def test_replaced_task_owner_interrupts_without_another_tool_call(self):
        self.matrix.sync.return_value = {"next_batch": "cursor", "rooms": {}}
        self.turn.current = lambda: False
        interrupted = asyncio.Event()
        self.native.interrupt.side_effect = lambda: interrupted.set()
        watcher = asyncio.create_task(self.turn.watch(self.native))
        try:
            await asyncio.wait_for(interrupted.wait(), 1)
            self.assertTrue(self.turn.stopped)
            self.native.query.assert_not_called()
        finally:
            watcher.cancel()
            try:
                await watcher
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    unittest.main()
