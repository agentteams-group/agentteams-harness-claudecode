"""Real bundled CLI completion boundary with a synthetic Task CLI authority."""
import asyncio,json,os,tempfile,threading,unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from test_member import ROOT,Wire
from turn import run_turn
from native_handoff import flush_handoffs, outgoing_receipts
from unittest.mock import Mock

class TaskCompletionTests(unittest.TestCase):
    def test_terminal_task_receipt_stops_without_another_model_request(self):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(('127.0.0.1',0),Wire) as http:
            threading.Thread(target=http.serve_forever,daemon=True).start();self.addCleanup(http.shutdown)
            http.requests=[];http.tool_name='mcp__teamharness__task';http.tool_input={'action':'submit','taskId':'child','summary':'Done'}
            root=Path(tmp);binary=root/'bin';binary.mkdir()
            terminal={'task':{'id':'child','mainTaskId':'main','status':'completed','submissionId':'s1',
                'handoff':{'submissionId':'s1','actorRole':'worker','actorName':'worker'}},
                'mainProjection':'applied','dispatchStatus':'ready','assignments':[{'status':'queued',
                    'taskId':'next','recipient':'tester','eventId':'e'*32}]}
            exe=binary/'agt';exe.write_text('#!/bin/sh\nprintf %s '+"'"+json.dumps(terminal)+"'"+'\n');exe.chmod(0o700)
            record={'member':{'role':'worker','matrixUserId':'@cc:at'},'desired':{}}
            task={'role':'worker','sourceEvent':'$source','execution':'child','context':{'outputDirectory':str(root)}}
            env={'PATH':str(binary)+os.pathsep+os.environ['PATH'],'HOME':tmp,'CLAUDE_CONFIG_DIR':str(root/'.claude'),
                 'AGENTTEAMS_PLUGIN_DIR':str(ROOT/'plugins/teamharness'),'TEAMHARNESS_SHARED_DIR':str(root/'shared'),
                 'AGENTTEAMS_LLM_STREAM_TIMEOUT_S':'20'}
            with patch.dict(os.environ,env,clear=True):
                session,answer=asyncio.run(run_turn(record,{'models':[{'id':'claude-sonnet-4-6'}],
                    'baseUrl':f'http://127.0.0.1:{http.server_port}/route','apiKey':'fixture-gateway'},
                    root,'Submit the prepared Task.',None,task,ROOT/'plugins/teamharness'))
                projects = root / '.claude/projects'
                path = next(projects.glob('*/' + session + '.jsonl'))
                # Claude records the current shell cwd after a Bash cd, while the
                # native session still belongs to its original project directory.
                scratch = root / ".scratch"
                scratch.mkdir(exist_ok=True)
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                for row in rows:
                    blocks = (row.get("message") or {}).get("content")
                    if isinstance(blocks, list) and any(b.get("type") == "tool_result" for b in blocks):
                        row["cwd"] = str(scratch)
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                items = list(outgoing_receipts(path))
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0][0], root)
                pending = items[0][1]
                self.assertEqual(pending["assignments"], [{"taskId": "next", "eventId": "e"*32, "status": "queued"}])
                self.assertNotIn("handoff", pending["task"])
                fact = '[Task handoff receipt] Matrix sent $real-event'
                confirm = Mock(side_effect=[[], [{"receiptId": "dispatch1", "fact": fact}]])
                backup, checked = Mock(), {}
                flush_handoffs(projects, checked, '@cc:at', confirm, backup)
                self.assertNotIn(fact, path.read_text())
                flush_handoffs(projects, checked, '@cc:at', confirm, backup)
                self.assertIn(fact, path.read_text())
                flush_handoffs(projects, checked, '@cc:at', confirm, backup)
                self.assertEqual(confirm.call_count, 2)
                backup.assert_called_once_with(path)
            self.assertEqual(len(http.requests),1,'A model request followed terminal Task submission')
            self.assertIn('[Task system receipt]',answer)
            self.assertTrue(task.get('_completed'))
            self.assertEqual(task['_completed']['sessionId'],session)
