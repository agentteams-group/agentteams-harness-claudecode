"""An execution may use assigned MCP services; coordination stays task-only."""
import json
import os
import tempfile
from pathlib import Path
import sys
import unittest

ROOT = Path(os.environ.get('AGENTTEAMS_CORE_DIR', Path(__file__).resolve().parents[2]))
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(ROOT / 'shared/lib')]
from turn import mcp_servers


class TaskMCPTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.plugin = Path(directory.name)

    def test_execution_keeps_assigned_gateway_server_without_remote_token(self):
        record = {'member': {'role': 'worker'}, 'desired': {'mcpServers': [
            {'name': 'github', 'url': 'http://gateway/mcp-servers/github', 'transport': 'http'}]}}
        servers = mcp_servers(record, ROOT / 'plugins/teamharness',
                              {'baseUrl': 'http://gateway/v1', 'apiKey': 'consumer-key'}, {'execution': 'child'}, self.plugin)
        self.assertEqual(servers['github']['headers'], {'Authorization': 'Bearer consumer-key'})
        for phase in ('planning', 'review', 'completion'):
            self.assertEqual(list(mcp_servers(record, ROOT / 'plugins/teamharness',
                {'baseUrl': 'http://gateway/v1', 'apiKey': 'consumer-key'}, {phase: 'main'}, self.plugin)), ['teamharness'])

    def test_large_context_is_preserved_in_private_file_not_cli_environment(self):
        context = {'completion': 'main', 'acceptedResults': ['原始记录' * 20000]}
        servers = mcp_servers({'member': {'role': 'team_leader'}, 'desired': {}},
                              ROOT / 'plugins/teamharness', {}, context, self.plugin)
        env = servers['teamharness']['env']
        self.assertNotIn('AT_CLAUDE_TASK_CONTEXT', env)
        self.assertEqual(json.loads(Path(env['AT_CLAUDE_TASK_CONTEXT_FILE']).read_text()), context)
        self.assertLess(len(json.dumps(servers)), 10000)
