"""Opt-in real Docker closure using an already installed Python image only.

No model/provider, pull, credential, original Run, or external network. The two
toy containers exercise the ordinary fence/stop/network-cleanup API after an
actual owner process dies. SAMPLE2_PIPELINE_DOCKER_FIXTURE=1 enables this test.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

from test_pipeline_owner import fixture,REPO
sys.path.insert(0,str(REPO/'outer/tests'))
import test_recording_repair as recording
from outer.harness import util, runtime
from research import pipeline_guard as guard


@unittest.skipUnless(os.environ.get('SAMPLE2_PIPELINE_DOCKER_FIXTURE')=='1',
                     'Opt-in installed-image Docker stop fixture')
class OwnedDockerStopTests(unittest.TestCase):
    def test_abrupt_controller_exit_fences_stops_and_removes_only_owned_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);config,record,source,manifest=fixture(base)
            (source/'manifest.json').unlink();raw=recording.RecordingRepairTests().fixture(source)
            for name in ('started.jsonl','events.jsonl'):
                rows=util.read_lines(raw/name);(raw/name).unlink()
                for row in rows:
                    row.update(session_id='1'*32,usage_complete=True,send_evidence='observed_send',http_status=200)
                    util.append_line(raw/name,row)
            value=util.read_json(source/'manifest.json')
            value.update(run_instance_id='1'*32,stop_confirmed=False,duration_seconds=None,ended_at=None,started_at=config['started_at'])
            util.write_json_atomic(source/'manifest.json',value)
            suffix=uuid.uuid4().hex[:16]
            network='s2-net-'+suffix;names={role:'s2-'+role+'-'+suffix for role in ('worker','gateway')}
            # inspect never pulls; a missing installed fixture image is a skip.
            image=runtime.docker('image','inspect','python:3.12-slim-bookworm','--format','{{.Id}}',check=False,timeout=10)
            if image.returncode:self.skipTest('Installed Python fixture image unavailable; no pull permitted')
            image_id=image.stdout.strip();created=[];controller=None
            labels=['--label','sample2.run=R','--label','sample2.instance='+'1'*32]
            try:
                network_id=runtime.docker('network','create','--internal',*labels,network).stdout.strip()
                gateway_code="""import json
from http.server import BaseHTTPRequestHandler,HTTPServer
class Handler(BaseHTTPRequestHandler):
 def do_POST(self):
  payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
  body=json.dumps(dict(payload,admission_closed=True,closed_at='fixture-stop')).encode()
  self.send_response(200);self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
HTTPServer(('127.0.0.1',8080),Handler).serve_forever()
"""
                for role in ('worker','gateway'):
                    code=gateway_code if role=='gateway' else 'import time;time.sleep(300)'
                    runtime.docker('create','--name',names[role],*labels,'--network',network,
                        '--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
                        '--pids-limit','32','--memory','128m','--user','1000:1000',
                        image_id,'python','-B','-c',code)
                    created.append(names[role]);runtime.docker('start',names[role])
                util.write_new_json(source/'runtime.json',dict(run_id='R',run_instance_id='1'*32,
                    network=network,network_id=network_id,**names))
                ready=base/'controller-ready'
                code=("import sys,time;from pathlib import Path;"
                      f"sys.path.insert(0,{str(REPO)!r});from outer.harness.ownership import lease;"
                      f"owner=lease(Path({str(source)!r}));owner.__enter__();Path({str(ready)!r}).write_text('ready');time.sleep(300)")
                controller=subprocess.Popen([sys.executable,'-c',code])
                deadline=time.monotonic()+5
                while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
                self.assertTrue(ready.exists())
                cached=guard.owned_rows(config)
                controller.kill();controller.wait(5)
                closure=guard.close_owned(config,REPO,base/'closure','synthetic_controller_kill',controller,cached)
                self.assertTrue(closure['owned_closure_confirmed'],closure)
                self.assertTrue((Path(config['root'])/'STOP').exists())
                for role in names:
                    state=runtime.inspect_container(names[role])
                    self.assertFalse(state['State']['Running'])
                final=util.read_json(source/'manifest.json')
                self.assertTrue(final['stop_confirmed']);self.assertTrue(final['network_cleanup']['confirmed'])
                self.assertIsNone(final['duration_seconds'])
                self.assertTrue((base/'closure/stops'/('1'*32)/'physical-stop.json').exists())
                self.assertTrue((base/'closure/stops'/('1'*32)/'result.json').exists())
                self.assertTrue(any((source/'evidence/admission-stop').glob('*.json')))
                self.assertTrue(any((source/'evidence/network-cleanup').glob('*-result.json')))
            finally:
                if controller and controller.poll() is None:controller.kill();controller.wait(5)
                for name in created:runtime.docker('rm','-f',name,check=False,timeout=15)
                runtime.docker('network','rm',network,check=False,timeout=15)


if __name__=='__main__':unittest.main()
