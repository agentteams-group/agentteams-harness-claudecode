"""A cache descriptor needs replayable native bytes for the matching checkpoint."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from task_recovery import Recovery
from task_worker import remember_checkpoint


class NativeSessionCacheTests(unittest.TestCase):
    def check(self, *, row_type='user', matching=True):
        temporary = self.enterContext(TemporaryDirectory())
        root = Path(temporary)
        ref = '6ab33aae-6bba-4dde-98f7-1470288419ba'
        log = root/'projects'/'project'/(ref+'.jsonl')
        log.parent.mkdir(parents=True)
        log.write_text(json.dumps({'type':row_type,'message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'call','content':'confirmed'}]}})+'\n')
        cp = {'version':3,'operation':{'id':'tool','state':'verified'}}
        sessions = Mock()
        sessions.load.return_value = {'checkpoint_digest':Recovery.digest(cp) if matching else 'newer'}
        state=SimpleNamespace(projects=root/'projects', remote='fixture/private-agent')
        with patch('task_worker.subprocess.run') as upload:
            result=remember_checkpoint(state,sessions,{'kind':'message','event_key':'event'},Recovery,
                                       ref,'call',{'checkpoint':cp})
        return result,sessions,upload,log

    def test_matching_serialized_tool_result_is_backed_up_before_remember(self):
        result,sessions,upload,log=self.check()
        self.assertTrue(result)
        self.assertEqual(upload.call_args.kwargs['input'],log.read_bytes())
        sessions.remember.assert_called_once()

    def test_api_preview_is_not_a_replayable_tool_result(self):
        result,sessions,upload,_=self.check(row_type='api-request-blob')
        self.assertFalse(result)
        upload.assert_not_called()
        sessions.remember.assert_not_called()

    def test_changed_checkpoint_cannot_be_relabelled_with_old_native_bytes(self):
        result,sessions,upload,_=self.check(matching=False)
        self.assertFalse(result)
        upload.assert_not_called()
        sessions.remember.assert_not_called()
