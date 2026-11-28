"""Real bundled CLI and HTTP MCP server: preserve actual remote terminal receipts."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import socket
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from test_member import ROOT, Wire
from turn import run_turn, UnverifiedToolEffect
from task_recovery import RecoveryUnavailable


class MCP(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        if os.environ.get('MCP_FIXTURE_OUTCOME') == 'deferred_error' and self.server.pending.wait(3):
            self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
            payload=json.dumps({'jsonrpc':'2.0','id':self.server.pending_id,'result':{'isError':True,'content':[{'type':'text','text':'A partial change failed'}]}}).encode()
            self.wfile.write(b'event: message\r\ndata:'+payload+b'\r\n\r\n');self.wfile.flush()
            self.close_connection=True
            return
        self.send_error(405)

    def do_DELETE(self):
        self.send_response(200); self.send_header('Content-Length', '0'); self.end_headers()

    def do_POST(self):
        if self.headers.get('Authorization') != 'Bearer fixture-mcp-private':
            self.send_response(401);self.send_header('Content-Length','0');self.end_headers();return
        message = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        method = message['method']
        if method == 'initialize':
            result = {'protocolVersion': message['params']['protocolVersion'], 'capabilities': {'tools': {}},
                      'serverInfo': {'name': 'receipt-fixture', 'version': '1'}}
        elif method == 'tools/list':
            result = {'tools': [{'name': 'record', 'description': 'Fixture operation', 'inputSchema': {'type': 'object'}}]}
            if os.environ.get('MCP_FIXTURE_OUTCOME') == 'alternative':
                result['tools'].append({'name':'alternative', 'description':'Another available approach', 'inputSchema':{'type':'object'}})
        elif method == 'tools/call':
            self.server.calls += 1
            outcome = os.environ.get('MCP_FIXTURE_OUTCOME', 'server_error')
            if outcome == 'deferred_error':
                self.server.pending_id=message['id'];self.server.pending.set()
                self.send_response(202);self.send_header('Content-Length','0');self.end_headers();return
            if outcome == 'http':
                body = b'fixture unavailable'; self.send_response(503); self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body); return
            if outcome == 'disconnect':
                self.connection.shutdown(socket.SHUT_RDWR); self.connection.close(); return
            result = {'isError': True, 'content': [{'type': 'text', 'text': 'A partial change failed'}]}
            if outcome in {'success', 'idempotent'} or (outcome == 'alternative' and message['params']['name'] == 'alternative'):
                result = {'content': [{'type': 'text', 'text': 'Requested state achieved'}],
                          'structuredContent': {'ok': True, 'idempotent_hit': outcome == 'idempotent'}}
        else:
            self.send_response(202); self.send_header('Content-Length', '0'); self.end_headers(); return
        reply={'jsonrpc':'2.0','id':message['id'],'result':result}
        if method == 'tools/call' and os.environ.get('MCP_FIXTURE_OUTCOME') == 'rpc_error':
            reply={'jsonrpc':'2.0','id':message['id'],'error':{'code':-32602,'message':'Invalid fixture arguments'}}
        if method == 'tools/call' and os.environ.get('MCP_FIXTURE_OUTCOME') == 'sse_error':
            body=b': heartbeat\r\nevent: message\r\ndata:'+json.dumps(reply,ensure_ascii=False).encode()+b'\r\n\r\n'
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
        body = json.dumps(reply).encode()
        self.send_response(200); self.send_header('Content-Type', 'application/json')
        if os.environ.get('MCP_FIXTURE_OUTCOME') == 'deferred_error':self.send_header('Mcp-Session-Id','fixture-session')
        self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)


class MCPReceiptTests(unittest.TestCase):
    def test_successful_remote_return_is_not_retried(self):
        self.check('success')

    def test_successful_idempotent_match_is_not_a_failure_or_retried(self):
        self.check('idempotent')

    def test_returned_remote_error_has_terminal_receipt_without_no_effect_claim(self):
        self.check('server_error')

    def test_returned_http_error_has_terminal_receipt(self):
        self.check('http')

    def test_sse_error_has_terminal_receipt(self):
        self.check('sse_error')

    def test_deferred_202_and_get_sse_keep_the_same_call_binding(self):
        self.check('deferred_error')

    def test_jsonrpc_error_has_terminal_receipt(self):
        self.check('rpc_error')

    def test_disconnect_remains_unknown_without_another_model_request(self):
        self.check('disconnect')

    def test_receipt_outage_does_not_replace_the_native_error_or_repeat_the_tool(self):
        self.check('server_error', receipt_outage=True)

    def test_denied_remote_permission_never_dispatches_the_tool(self):
        self.check('server_error', denied=True)

    def test_ordinary_message_uses_the_shared_message_tool_journal(self):
        self.check('server_error', ordinary=True)

    def test_ordinary_message_disconnect_cannot_commit_a_result(self):
        self.check('disconnect', ordinary=True)

    def test_three_failures_allow_worker_to_use_another_available_tool(self):
        self.check('alternative')

    def test_ordinary_worker_can_change_tools_after_three_failures(self):
        self.check('alternative', ordinary=True)

    def test_confirmed_mcp_return_is_serialized_before_native_cache_callback(self):
        self.check('success', ordinary=True)

    def test_model_failure_after_a_tool_still_preserves_its_native_cache(self):
        self.check('success', ordinary=True, model_failure=True)

    def test_cache_storage_failure_does_not_prevent_alternative_tools(self):
        self.check('alternative', ordinary=True, cache_failure=True)

    def check(self, outcome, receipt_outage=False, denied=False, ordinary=False, model_failure=False, cache_failure=False):
        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(('127.0.0.1', 0), Wire) as model, \
                ThreadingHTTPServer(('127.0.0.1', 0), MCP) as mcp:
            for server in [model, mcp]:
                threading.Thread(target=server.serve_forever, daemon=True).start(); self.addCleanup(server.shutdown)
            model.requests = []; model.tool_name = 'mcp__fixture__record'; model.tool_input = {}; mcp.calls = 0
            if model_failure:model.fail_after=1
            if outcome == 'alternative':
                model.tool_sequence = ['mcp__fixture__record'] * 3 + ['mcp__fixture__alternative']
            mcp.pending=threading.Event();mcp.pending_id=None
            root = Path(tmp); recovery = Mock(); recovery.begin_tools.return_value = 'tools:remote'
            recovery.finish_tools.return_value = {'execution': {}}
            if receipt_outage:recovery.finish_tools.side_effect=RecoveryUnavailable('receipt commit unavailable')
            matrix=Mock()
            matrix.message_begin_tools=recovery.begin_tools
            matrix.message_finish_tools=recovery.finish_tools
            matrix.message_current=recovery.tools_current
            task = {'execution': 'child', 'identity': {'task_id': 'child', 'team_id': 'team'},
                    'context': {'outputDirectory': str(root)}}
            record = {'member': {'role': 'worker', 'matrixUserId': '@cc:at'}, 'desired': {
                'approvalMode': 'ask' if denied else 'allow', 'mcpServers': [{'name': 'fixture', 'url': f'http://127.0.0.1:{mcp.server_port}/mcp'}]}}
            import turn
            actual_client = turn.ClaudeSDKClient
            original_servers = turn.mcp_servers
            def configured(*args, **kwargs):
                servers=original_servers(*args, **kwargs)
                servers['fixture']['headers']={'Authorization':'Bearer fixture-mcp-private'}
                return servers
            seen, cached = [], []
            def remember(ref, call, execution):
                logs=list((root/'.claude/projects').glob('*/'+ref+'.jsonl'))
                if len(logs)!=1:return False
                rows=[json.loads(line)for line in logs[0].read_text().splitlines()if line]
                ready=any(row.get('type')=='user' and any(
                    b.get('type')=='tool_result' and b.get('tool_use_id')==call
                    for b in (row.get('message')or{}).get('content',[])if isinstance(b,dict)) for row in rows)
                if not ready:return False
                cached.append(call)
                if cache_failure:raise RecoveryUnavailable('native cache storage unavailable')
                return True
            def client(*, options):
                for event in ['PostToolUse', 'PostToolUseFailure']:
                    original = options.hooks[event][0].hooks[0]
                    async def observe(data, *args, original=original):
                        seen.append(data)
                        return await original(data, *args)
                    options.hooks[event][0].hooks[0] = observe
                return actual_client(options=options)
            with patch.dict(os.environ, {'HOME': tmp, 'CLAUDE_CONFIG_DIR': str(root / '.claude'),
                    'MCP_TIMEOUT':'30000', 'MCP_TOOL_TIMEOUT':'3000', 'MCP_FIXTURE_OUTCOME':outcome, 'AGENTTEAMS_YOLO': '1', 'AGENTTEAMS_PLUGIN_DIR': str(ROOT / 'plugins/teamharness')}, clear=True), \
                    patch('turn.Recovery', return_value=recovery), patch('turn.persist_terminal') as persist, \
                    patch('turn.ClaudeSDKClient', side_effect=client), patch('turn.mcp_servers',side_effect=configured), patch('worker_matrix_state.WorkerMatrix',return_value=matrix):
                def run():
                    return asyncio.run(run_turn(record, {'models': [{'id': 'claude-sonnet-4-6'}],
                    'baseUrl': f'http://127.0.0.1:{model.server_port}/route', 'apiKey': 'fixture-gateway'},
                    root, 'Run the fixture operation.', None, task, ROOT / 'plugins/teamharness',
                    on_checkpoint=remember,
                    **({'message_identity':{'event_key':'fixture-message','generation':1,'instance_id':'fixture','process_id':'fixture'}} if ordinary else {})))
                if model_failure:
                    with self.assertRaisesRegex(RuntimeError, 'no usable result'):run()
                elif outcome == 'disconnect' or receipt_outage:
                    with self.assertRaises(UnverifiedToolEffect):run()
                else:
                    _, answer=run()
                    self.assertEqual(answer,'first-ok')
            print('Native fixture return:', json.dumps({'outcome':os.environ.get('MCP_FIXTURE_OUTCOME'),'calls':mcp.calls,'hooks':seen}), flush=True)
            self.assertNotIn('fixture-mcp-private',json.dumps(model.requests))
            if denied:
                self.assertEqual(mcp.calls,0)
                recovery.finish_tools.assert_called_once()
                self.assertEqual(recovery.finish_tools.call_args.args[2][0]['effect'],'none')
                return
            if outcome == 'alternative':
                self.assertEqual(mcp.calls, 4)
                self.assertEqual(recovery.begin_tools.call_count, 4)
                receipts = [call.args[2][0] for call in recovery.finish_tools.call_args_list]
                self.assertEqual([r.get('status') for r in receipts], ['error'] * 3 + [None])
                self.assertEqual(len(model.requests), 5)
                return
            if outcome in {'success','idempotent'}:
                self.assertEqual(len(cached),1)
            self.assertEqual(mcp.calls,1)
            recovery.begin_tools.assert_called_once()
            if outcome == 'disconnect':
                self.assertIn('RemoteProtocolError', seen[-1]['error'])
                recovery.finish_tools.assert_not_called()
                self.assertEqual(len(model.requests),1)
                return
            recovery.finish_tools.assert_called_once()
            if receipt_outage:
                self.assertIn('A partial change failed',seen[-1]['error'])
                self.assertEqual(len(model.requests),1)
            receipt = recovery.finish_tools.call_args.args[2][0]
            if outcome in {'success', 'idempotent'}:
                self.assertNotEqual(receipt.get('status'), 'error')
                self.assertEqual(receipt['content']['result']['structuredContent'],
                                 {'ok': True, 'idempotent_hit': outcome == 'idempotent'})
            else:
                self.assertEqual(receipt['status'], 'error')
            self.assertNotIn('effect', receipt)
