"""Real bundled CLI completion boundary with a synthetic Task CLI authority."""
import asyncio,json,os,tempfile,threading,unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from test_member import ROOT,Wire
from turn import run_turn

class TaskCompletionTests(unittest.TestCase):
    def test_terminal_task_receipt_stops_without_another_model_request(self):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(('127.0.0.1',0),Wire) as http:
            threading.Thread(target=http.serve_forever,daemon=True).start();self.addCleanup(http.shutdown)
            http.requests=[];http.tool_name='mcp__teamharness__task';http.tool_input={'action':'submit','taskId':'child','summary':'Done'}
            root=Path(tmp);binary=root/'bin';binary.mkdir()
            terminal={'task':{'id':'child','mainTaskId':'main','status':'completed','submissionId':'s1',
                'handoff':{'submissionId':'s1','actorRole':'worker','actorName':'worker'}},
                'mainProjection':'applied','dispatchStatus':'ready','assignments':[]}
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
            self.assertEqual(len(http.requests),1,'A model request followed terminal Task submission')
            self.assertIn('[Task system receipt]',answer)
            self.assertTrue(task.get('_completed'))
            self.assertEqual(task['_completed']['sessionId'],session)
