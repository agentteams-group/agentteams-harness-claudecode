"""Exercise the actual native Bash result shape inside an assigned Task."""
import asyncio,json,os,tempfile,threading,unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest.mock import Mock,patch
from test_member import ROOT,Wire
from turn import run_turn

class TaskBashTests(unittest.TestCase):
    def test_manager_native_shell_uses_standard_approval_without_legacy_yolo(self):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(('127.0.0.1',0),Wire) as http:
            threading.Thread(target=http.serve_forever,daemon=True).start();self.addCleanup(http.shutdown)
            http.requests=[];http.tool_name='Bash';http.tool_input={'command':'printf native-manager-test'}
            root=Path(tmp)
            import turn
            actual_client=turn.ClaudeSDKClient
            seen=[]
            def client(*,options):
                original=options.hooks['PostToolUse'][0].hooks[0]
                async def observe(data,*args):
                    seen.append(data.get('tool_response'))
                    return await original(data,*args)
                options.hooks['PostToolUse'][0].hooks[0]=observe
                return actual_client(options=options)
            with patch('turn.ClaudeSDKClient',side_effect=client), patch.dict(os.environ,{'HOME':tmp,'CLAUDE_CONFIG_DIR':str(root/'.claude'),
                    'AGENTTEAMS_PLUGIN_DIR':str(ROOT/'plugins/teamharness')},clear=True), patch('turn.Recovery') as recovery:
                _,answer=asyncio.run(run_turn({'member':{'role':'manager','matrixUserId':'@manager:at'},
                    'desired':{'approvalMode':'allow'}},
                    {'models':[{'id':'claude-sonnet-4-6'}],'baseUrl':f'http://127.0.0.1:{http.server_port}/route','apiKey':'fixture-gateway'},
                    root,'Run the native check.',None,{},ROOT/'plugins/teamharness'))
            self.assertEqual(answer,'first-ok')
            recovery.assert_not_called()
            self.assertTrue(seen)
            self.assertEqual(seen[0]['stdout'],'native-manager-test')
            self.assertIs(seen[0]['interrupted'],False)
            self.assertTrue(any('native-manager-test' in json.dumps(req.get('messages')) for req in http.requests))
            self.assertTrue({'Bash','Write','Edit'} <= {tool['name'] for tool in http.requests[0]['tools']})

    def test_task_bash_has_journaled_terminal_receipt(self):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(('127.0.0.1',0),Wire) as http:
            threading.Thread(target=http.serve_forever,daemon=True).start();self.addCleanup(http.shutdown)
            http.requests=[];http.tool_name='Bash';http.tool_input={'command':'printf native-task-test'}
            root=Path(tmp);recovery=Mock();recovery.begin_tools.return_value='tools:test'
            recovery.finish_tools.return_value={'execution':{}}
            task={'role':'worker','execution':'child','identity':{'task_id':'child','team_id':'team'},'context':{'outputDirectory':str(root)}}
            import turn
            actual_client=turn.ClaudeSDKClient
            seen=[]
            def client(*,options):
                self.assertEqual(options.env.get("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS"), "1")
                original=options.hooks['PostToolUse'][0].hooks[0]
                async def observe(data,*args):
                    seen.append(data.get('tool_response'))
                    return await original(data,*args)
                options.hooks['PostToolUse'][0].hooks[0]=observe
                return actual_client(options=options)
            with patch('turn.ClaudeSDKClient',side_effect=client), patch.dict(os.environ,{'HOME':tmp,'CLAUDE_CONFIG_DIR':str(root/'.claude'),
                    'AGENTTEAMS_YOLO':'1','AGENTTEAMS_PLUGIN_DIR':str(ROOT/'plugins/teamharness')},clear=True), \
                    patch('turn.Recovery',return_value=recovery),patch('turn.persist_terminal') as persist:
                session,answer=asyncio.run(run_turn({'member':{'role':'worker','matrixUserId':'@cc:at'},'desired':{}},
                    {'models':[{'id':'claude-sonnet-4-6'}],'baseUrl':f'http://127.0.0.1:{http.server_port}/route','apiKey':'fixture-gateway'},
                    root,'Run the native check.',None,task,ROOT/'plugins/teamharness'))
            self.assertEqual(answer,'first-ok')
            call=recovery.begin_tools.call_args.args[1][0]
            self.assertEqual(call['name'],'Bash')
            self.assertTrue(persist.called)
            recovery.finish_tools.assert_called_once()

            self.assertTrue(seen)
            self.assertIs(seen[0]["interrupted"],False)
            self.assertEqual(seen[0]["stdout"],"native-task-test")


    def test_failed_foreground_command_records_error_without_claiming_no_effect(self):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(("127.0.0.1", 0), Wire) as http:
            threading.Thread(target=http.serve_forever, daemon=True).start(); self.addCleanup(http.shutdown)
            http.requests = []; http.tool_name = "Bash"
            http.tool_input = {"command": "printf written > partial.txt && exit 1"}
            root = Path(tmp); recovery = Mock(); recovery.begin_tools.return_value = "tools:failed"
            recovery.finish_tools.return_value = {"execution": {}}
            task = {"execution": "child", "identity": {"task_id": "child", "team_id": "team"}, "context": {"outputDirectory": str(root)}}
            with patch.dict(os.environ, {"HOME": tmp, "CLAUDE_CONFIG_DIR": str(root / ".claude"),
                    "AGENTTEAMS_YOLO": "1", "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness")}, clear=True), \
                    patch("turn.Recovery", return_value=recovery), patch("turn.persist_terminal") as persist:
                _, answer = asyncio.run(run_turn({"member": {"role": "worker", "matrixUserId": "@cc:at"}, "desired": {}},
                    {"models": [{"id": "claude-sonnet-4-6"}], "baseUrl": f"http://127.0.0.1:{http.server_port}/route",
                     "apiKey": "fixture-gateway"}, root, "Run the check.", None, task, ROOT / "plugins/teamharness"))
            self.assertEqual(answer, "first-ok")
            self.assertEqual((root / "partial.txt").read_text(), "written")
            recovery.finish_tools.assert_called_once(); persist.assert_called_once()
            receipt = recovery.finish_tools.call_args.args[2][0]
            self.assertEqual(receipt["status"], "error")
            self.assertNotIn("effect", receipt)  # A failed command may already have changed files.
            self.assertTrue(any("tool_result" in json.dumps(req["messages"]) for req in http.requests))

    def test_interrupted_failed_command_remains_unverified(self):
        self.check_unverified_command({"command": "printf written > partial.txt && exit 1"}, failure_interrupted=True)

    def test_native_write_failure_is_terminal_without_claiming_no_effect(self):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(("127.0.0.1", 0), Wire) as http:
            threading.Thread(target=http.serve_forever, daemon=True).start(); self.addCleanup(http.shutdown)
            root = Path(tmp); parent = root / "directory"; parent.mkdir(); target = parent / "file.txt"
            http.requests = []; http.tool_name = "Write"
            http.tool_input = {"file_path": str(target), "content": "cannot replace a directory"}
            recovery = Mock(); recovery.begin_tools.return_value = "tools:write-failed"
            recovery.finish_tools.return_value = {"execution": {}}
            task = {"execution": "child", "identity": {"task_id": "child", "team_id": "team"},
                    "context": {"outputDirectory": str(root)}}
            import turn
            actual_client = turn.ClaudeSDKClient
            def client(*, options):
                before = options.hooks['PreToolUse'][0].hooks[0]
                async def remove_parent_after_admission(data, *args):
                    result = await before(data, *args)
                    if data['tool_name'] == 'Write':
                        # A real filesystem race after admission, not a fabricated SDK failure event.
                        parent.rmdir(); parent.write_text('parent became a file')
                    return result
                options.hooks['PreToolUse'][0].hooks[0] = remove_parent_after_admission
                return actual_client(options=options)
            with patch.dict(os.environ, {"HOME": tmp, "CLAUDE_CONFIG_DIR": str(root / ".claude"),
                    "AGENTTEAMS_YOLO": "1", "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness")}, clear=True), \
                    patch("turn.Recovery", return_value=recovery), patch("turn.persist_terminal") as persist, \
                    patch('turn.ClaudeSDKClient', side_effect=client):
                _, answer = asyncio.run(run_turn({"member": {"role": "worker", "matrixUserId": "@cc:at"}, "desired": {}},
                    {"models": [{"id": "claude-sonnet-4-6"}], "baseUrl": f"http://127.0.0.1:{http.server_port}/route",
                     "apiKey": "fixture-gateway"}, root, "Run the check.", None, task, ROOT / "plugins/teamharness"))
            self.assertEqual(answer, "first-ok")
            self.assertTrue(parent.is_file()); self.assertFalse(target.exists())
            recovery.finish_tools.assert_called_once(); persist.assert_called_once()
            receipt = recovery.finish_tools.call_args.args[2][0]
            self.assertEqual(receipt["status"], "error")
            self.assertNotIn("effect", receipt)

    def test_timed_out_foreground_command_stops_before_another_model_request(self):
        # Recorded native PostToolUse shape when the CLI backgrounds a timed-out foreground command.
        self.check_unverified_command({"command": "printf written > partial.txt"},
                                      {"stdout": "", "stderr": "", "interrupted": False,
                                       "backgroundTaskId": "test-job", "timedOutAfterMs": 120000})

    def check_unverified_command(self, tool_input, native_response=None, failure_interrupted=False):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(("127.0.0.1", 0), Wire) as http:
            threading.Thread(target=http.serve_forever, daemon=True).start()
            self.addCleanup(http.shutdown)
            http.requests = []
            http.tool_name = "Bash"
            http.tool_input = tool_input
            root = Path(tmp)
            import turn
            actual_client = turn.ClaudeSDKClient
            def client(*, options):
                if failure_interrupted:
                    failed = options.hooks["PostToolUseFailure"][0].hooks[0]
                    async def interrupted(data, *args):
                        return await failed({**data, "is_interrupt": True}, *args)
                    options.hooks["PostToolUseFailure"][0].hooks[0] = interrupted
                if native_response is not None:
                    hook = options.hooks["PostToolUse"][0].hooks[0]
                    async def timed_out(data, *args):
                        return await hook({**data, "tool_response": native_response}, *args)
                    options.hooks["PostToolUse"][0].hooks[0] = timed_out
                return actual_client(options=options)
            recovery = Mock()
            task = {"role": "worker", "execution": "child", "identity": {"task_id": "child", "team_id": "team"},
                    "context": {"outputDirectory": str(root)}}
            with patch.dict(os.environ, {"HOME": tmp, "CLAUDE_CONFIG_DIR": str(root / ".claude"),
                    "AGENTTEAMS_YOLO": "1", "AGENTTEAMS_PLUGIN_DIR": str(ROOT / "plugins/teamharness")}, clear=True), \
                    patch("turn.Recovery", return_value=recovery), patch("turn.ClaudeSDKClient", side_effect=client):
                with self.assertRaisesRegex(RuntimeError, "需要核对"):
                    asyncio.run(run_turn({"member": {"role": "worker", "matrixUserId": "@cc:at"}, "desired": {}},
                        {"models": [{"id": "claude-sonnet-4-6"}], "baseUrl": f"http://127.0.0.1:{http.server_port}/route",
                         "apiKey": "fixture-gateway"}, root, "Run the native check.", None, task, ROOT / "plugins/teamharness"))
            self.assertEqual((root / "partial.txt").read_text(), "written")
            recovery.finish_tools.assert_not_called()
            self.assertEqual(sum("tool_result" in json.dumps(req["messages"]) for req in http.requests), 0)
