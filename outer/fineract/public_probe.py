"""Fresh public source build and real API test in the identical 5GiB worker."""
import argparse
import json
from pathlib import Path
import time
import uuid

from outer.harness import runtime,util,run
from . import box
from .artifacts import junit_result


PROGRAM=r'''import hashlib,json,os,subprocess,time,ssl,urllib.request,xml.etree.ElementTree as ET
from pathlib import Path
source=Path('/workspace/source');out=Path('/state/public-proof');out.mkdir()
result={'model_calls':0,'private_inputs':False,'fresh_outputs':not (source/'fineract-provider/build').exists(),'steps':[]}
def command(name,args,env=None):
 with (out/(name+'.log')).open('w') as log:
  status=subprocess.call(args,cwd=source,stdout=log,stderr=subprocess.STDOUT,env=env)
 result['steps'].append({'name':name,'command':args,'exit_code':status})
 if status:raise RuntimeError(name+' failed')
app=None
try:
 command('fresh-build',['fineract-gradle','--offline','--no-build-cache','-PcargoDisabled',':fineract-provider:bootJar',':integration-tests:compileTestJava'])
 result['sdk_jar_sha256']=hashlib.sha256(Path('/state/public-sdk/client.jar').read_bytes()).hexdigest()
 result['production_classes']=sum(1 for p in (source/'fineract-provider/build/classes').rglob('*.class'))
 result['test_classes']=sum(1 for p in (source/'integration-tests/build/classes').rglob('*.class'))
 args=['java','-Xmx1024m','--add-exports=java.naming/com.sun.jndi.ldap=ALL-UNNAMED']
 args+=['--add-opens='+v+'=ALL-UNNAMED' for v in ['java.base/java.lang','java.base/java.lang.invoke','java.base/java.io','java.base/java.security','java.base/java.util','java.management/javax.management','java.naming/javax.naming']]
 args+=['-jar',str(source/'fineract-provider/build/libs/fineract-provider.jar')]
 with (out/'application.log').open('w') as log:app=subprocess.Popen(args,cwd='/tmp',stdout=log,stderr=subprocess.STDOUT)
 deadline=time.monotonic()+300
 while True:
  try:
   with urllib.request.urlopen('https://localhost:8443/fineract-provider/actuator/health',context=ssl._create_unverified_context(),timeout=5) as response:
    if response.status==200:break
  except Exception:pass
  if app.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Application health failed')
  time.sleep(3)
 command('public-api',['fineract-gradle','--offline','--no-build-cache','-PcargoDisabled',':integration-tests:test','--tests','org.apache.fineract.integrationtests.DelinquencyActionIntegrationTests.testCreateAndReadPauseDelinquencyActionUsingExternalId'],{**os.environ,'BACKEND_HOST':'localhost','FINERACT_TEST_EXTERNAL_HOST':'localhost'})
 cases=[]
 for path in (source/'integration-tests/build/test-results/test').glob('TEST-*.xml'):
  for c in ET.parse(path).getroot().iter('testcase'):
   cases.append({'suite':c.get('classname'),'case':c.get('name'),'passed':all(c.find(t) is None for t in ('failure','error','skipped'))})
 result['cases']=cases
 result['success']=len(cases)==1 and all(c['passed'] for c in cases) and result['production_classes']>0 and result['test_classes']>0 and result['fresh_outputs']
except Exception as error:
 result['success']=False;result['error_type']=type(error).__name__;result['error']=str(error)
finally:
 if app and app.poll() is None:
  app.terminate()
  try:app.wait(timeout=20)
  except subprocess.TimeoutExpired:app.kill();app.wait()
 result['memory_peak_bytes']=int(Path('/sys/fs/cgroup/memory.peak').read_text())
 result['memory_max_bytes']=int(Path('/sys/fs/cgroup/memory.max').read_text())
 result['memory_events']=Path('/sys/fs/cgroup/memory.events').read_text()
 (out/'result.json').write_text(json.dumps(result))
raise SystemExit(0 if result['success'] else 1)
'''


def begin(settings_path,plan_path,output):
    settings=util.read_json(Path(settings_path));settings['synthetic']=True
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=False)
    ident=uuid.uuid4().hex
    settings['budget_volume']='fe-public-proof-budget-'+ident
    box.volume(settings['budget_volume'],'modernization.public_proof='+ident,settings['worker_image'])
    box.budget_action(settings,'create',{'campaign_id':'public-proof-'+ident,'plan_sha256':util.sha256_file(Path(plan_path)),
        'max_requests':1,'max_tokens':100,'deadline':time.time()+2400})
    util.write_new_json(root/'settings.json',settings)
    data=box.allocate(root/'run',settings,util.read_json(Path(plan_path))['assignments'][0],ident)
    box.start_gateway(root/'run',settings)
    box.launch(root/'run',settings,public_build_probe=PROGRAM)
    util.write_new_json(root/'started.json',{'at':run.now(),'id':ident,'model_calls':0,'private_inputs':False})
    return {'record':str(root),'container':data['names']['worker']}


def collect(record):
    root=Path(record).resolve();settings=util.read_json(root/'settings.json')
    if (root/'result.json').exists():return util.read_json(root/'result.json')
    data=util.read_json(root/'run/box.json');item=box.owned(data,'worker')
    if item['State']['Running']:return {'running':True}
    box.budget_action(settings,'close',{'reason':'public_proof_complete'})
    stopped=box.stop(root/'run','public_proof_complete')
    runtime.docker('cp',data['names']['worker']+':/state/public-proof',root/'evidence')
    proof=util.read_json(root/'evidence/result.json') if (root/'evidence/result.json').is_file() else None
    result={'at':run.now(),'state':item['State'],'proof':proof,'stop_confirmed':stopped['stop_confirmed'],
        'success':bool(proof and proof['success'] and item['State']['ExitCode']==0 and not item['State']['OOMKilled'] and stopped['stop_confirmed']),
        'evidence_files':util.tree_hashes(root/'evidence'),'model_calls':0,'private_inputs':False}
    util.write_new_json(root/'result.json',result)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    q=sub.add_parser('begin')
    for name in ('settings','plan','output'):q.add_argument('--'+name,required=True,type=Path)
    sub.add_parser('collect').add_argument('--record',required=True,type=Path);a=p.parse_args()
    print(json.dumps(begin(a.settings,a.plan,a.output) if a.command=='begin' else collect(a.record)))
