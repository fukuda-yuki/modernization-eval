"""Synthetic native JSON pipe checks; no Docker, model or saved message content."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import support
from harness import staged_runtime


class NativeApiEncodingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'run';(self.root/'workspace').mkdir(parents=True)
        self.control=self.root/'controller';self.control.mkdir()
        self.owner=Mock()
        self.owner._owned_container.return_value={'State':{'Running':True},
            'Mounts':[{'Destination':'/workspace','Source':str(self.root/'workspace')}]}
        self.controller=SimpleNamespace(root=self.control,remaining=Mock(return_value=30.))
        self.transport=staged_runtime.DockerTransport(self.root,{'worker':'synthetic-worker'},
            self.controller,self.owner,'synthetic-model')

    def api_output(self, raw, body=None):
        actual_run=subprocess.run
        command=[sys.executable,'-c','import sys;sys.stdout.buffer.write(bytes.fromhex('+repr(raw.hex())+'))']
        invocations=[]
        def synthetic_child(args,**kwargs):
            invocations.append((args,kwargs))
            return actual_run(command,**kwargs)
        with patch.object(staged_runtime.subprocess,'run',side_effect=synthetic_child):
            result=self.transport.api('worker','/session/synthetic/message',body)
        return result,invocations

    def test_unescaped_japanese_and_non_ascii_json_survive_the_real_pipe(self):
        expected=[{'info':{'id':'synthetic'},'parts':[{'text':'日本語の合成記録 ✓ 🐈 café'}]}]
        raw=json.dumps(expected,ensure_ascii=False).encode('utf-8')
        result,calls=self.api_output(raw)
        self.assertEqual(expected,result)
        self.assertIsNone(json.loads(calls[0][1]['input'])['body'])

    def test_non_ascii_input_stays_exact_and_only_on_stdin(self):
        text='合成の追加入力 ✓ 🐈'
        body={'parts':[{'type':'text','text':text}]}
        result,calls=self.api_output(b'{"accepted":true}',body)
        self.assertEqual({'accepted':True},result)
        args,kwargs=calls[0]
        self.assertEqual(body,json.loads(kwargs['input'].decode('utf-8'))['body'])
        self.assertNotIn(text,str(args));self.assertNotIn(text,str(kwargs['env']))

    def test_invalid_utf8_is_rejected_without_replacement_or_ignoring_bytes(self):
        with self.assertRaises(UnicodeDecodeError):self.api_output(b'{"text":"\xff"}')

    def test_invalid_json_is_not_normalized_to_an_empty_inventory(self):
        with self.assertRaises(json.JSONDecodeError):self.api_output(b'{')

    def test_both_stop_guards_block_reads_and_writes_before_any_child(self):
        for directory in (self.root,self.control):
            marker=directory/'stop-request.json';marker.write_text('{}',encoding='utf-8')
            try:
                for body in (None,{'parts':[{'text':'Synthetic input must not be sent.'}]}):
                    with self.subTest(scope=directory.name,read_only=body is None), \
                            patch.object(staged_runtime.subprocess,'run') as child:
                        self.owner.reset_mock();self.controller.remaining.reset_mock()
                        with self.assertRaisesRegex(RuntimeError,'Staged transport stopped'):
                            self.transport.api('worker','/session/synthetic/message',body)
                        child.assert_not_called();self.owner._owned_container.assert_not_called()
                        self.controller.remaining.assert_not_called()
            finally:marker.unlink()


if __name__=='__main__':unittest.main()
