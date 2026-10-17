"""Owned isolated PostgreSQL/Fineract service pair for baseline and evaluation."""
import argparse
import json
from pathlib import Path
import tarfile
import uuid

from outer.harness import runtime,util,run
from .prepare import POSTGRES
from .network import create_internal


def start_baseline(preparation):
    root=Path(preparation).resolve()
    prep=util.read_json(root/'result.json')
    if not prep['success']:raise ValueError('Successful real baseline build required')
    return start(root,prep,root.parent/'source.tar.gz','pinned_baseline')


def start_evaluation(record):
    root=Path(record).resolve();result=util.read_json(root/'result.json')
    request=util.read_json(root/'request.json')
    groups=(result.get('stage') or {}).get('groups',[])
    if result['environment_failure'] or not any(g['id']=='bootJar' and g['exit_code']==0 for g in groups):
        raise ValueError('Fresh canonical bootJar success required')
    prep={k:request[k] for k in ('id','source_commit','volume','java_image_id','archive_sha256')}
    return start(root,prep,root.parent.parent/'fineract-preparation/source.tar.gz','canonical_evaluation')


def start(root,prep,archive,basis):
    ident=uuid.uuid4().hex
    target=root/'services'/ident;target.mkdir(parents=True)
    names={r:'fe-'+r+'-'+ident for r in ('network','database','app','data')}
    labels=['--label','modernization.service='+ident]
    receipt={'kind':'fineract-service-v1','id':ident,'names':names,'started_at':run.now(),
             'build_basis':basis,
             'preparation_id':prep['id'],'source_commit':prep['source_commit'],
             'source_volume':prep['volume'],'java_image_id':prep['java_image_id'],
             'postgres_image':POSTGRES,'ports_published':False,'model_calls':0}
    util.write_new_json(target/'intent.json',receipt)
    # These are pinned upstream disposable-development DB values, never host/Go
    # credentials. Read directly from the verified archive and do not print them.
    if util.sha256_file(archive)!=prep['archive_sha256']:raise ValueError('Archive changed')
    def env_from(name):
        with tarfile.open(archive) as tar:
            raw=tar.extractfile('fineract-'+prep['source_commit']+'/config/docker/env/'+name).read().decode()
        return {k:v for line in raw.splitlines() if line and not line.startswith('#') and '=' in line
                for k,v in [line.split('=',1)]}
    db=env_from('postgresql.env')
    app={**env_from('fineract-common.env'),**env_from('fineract.env'),**env_from('fineract-postgresql.env')}
    # Limit JVM memory without enabling the upstream sample debug listener.
    app.pop('JAVA_TOOL_OPTIONS',None)
    for name,values in (('database.env',db),('app.env',app)):
        (target/name).write_text(''.join(k+'='+v+'\n' for k,v in values.items()),encoding='utf-8')
    receipt['network_allocation']=create_internal(names['network'],labels)
    runtime.docker('volume','create',*labels,names['data'])
    runtime.docker('run','-d','--name',names['database'],*labels,'--network',names['network'],
        '--network-alias','db','--memory=1g','--cpus=2','--pids-limit=256',
        '--security-opt=no-new-privileges','--env-file',target/'database.env',
        '--mount',f'type=volume,source={names["data"]},target=/var/lib/postgresql',
        '--mount',f'type=volume,source={prep["volume"]},target=/docker-entrypoint-initdb.d,volume-subpath=source/config/docker/postgresql/docker-entrypoint-initdb.d,readonly',
        POSTGRES)
    # Docker DNS stays internal. No host port or unrelated container is touched.
    runtime.docker('run','-d','--name',names['app'],*labels,'--network',names['network'],
        '--network-alias','fineract','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges',
        '--user','1000:1000','--memory=4g','--cpus=4','--pids-limit=512',
        '--tmpfs','/tmp:rw,exec,size=1g','--env-file',target/'app.env',
        '--mount',f'type=volume,source={prep["volume"]},target=/app,volume-subpath=source/fineract-provider/build/libs,readonly',
        '-w','/tmp',prep['java_image_id'],'java','-Xmx2g',
        '--add-exports=java.naming/com.sun.jndi.ldap=ALL-UNNAMED',
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED',
        '--add-opens=java.base/java.io=ALL-UNNAMED',
        '--add-opens=java.base/java.security=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-opens=java.management/javax.management=ALL-UNNAMED',
        '--add-opens=java.naming/javax.naming=ALL-UNNAMED',
        '-jar','/app/fineract-provider.jar')
    util.write_new_json(target/'started.json',receipt)
    return {'record':str(target),'containers':{k:names[k] for k in ('database','app')}}


def inspect(record):
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    states={}
    for role in ('database','app'):
        item=json.loads(runtime.docker('inspect',data['names'][role]).stdout)[0]
        if item['Config']['Labels'].get('modernization.service')!=data['id']:
            raise ValueError('Service ownership mismatch')
        states[role]=item['State']
    probe = """import json,ssl,urllib.request
try:
 r=urllib.request.urlopen('https://127.0.0.1:8443/fineract-provider/actuator/health',context=ssl._create_unverified_context(),timeout=5)
 print(json.dumps({'http_status':r.status,'body':json.loads(r.read())}))
except Exception as e:print(json.dumps({'error_type':type(e).__name__}))
"""
    result=runtime.docker('exec',data['names']['app'],'python3','-c',probe,check=False,timeout=15)
    evidence={'at':run.now(),'states':states,
              'health':json.loads(result.stdout) if result.returncode==0 else {'container_not_running':True}}
    util.append_line(root/'health.jsonl',evidence)
    return evidence


def api_probe(record):
    """Read-only calls with the pinned upstream disposable test account.

    Credentials stay inside a trusted probe container. Neither response bodies nor
    authorization headers enter host logs; this is infrastructure evidence only.
    """
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    item=json.loads(runtime.docker('inspect',data['names']['app']).stdout)[0]
    if item['Config']['Labels'].get('modernization.service')!=data['id']:
        raise ValueError('Service ownership mismatch')
    script=r'''import base64,hashlib,json,re,ssl,urllib.request,urllib.error
from pathlib import Path
source=Path('/baseline/source/integration-tests/src/test/java/org/apache/fineract/integrationtests/ConfigProperties.java').read_text()
def default(name):
 match=re.search(r'String '+name+r' = getValueFromEnvOrDefault\("[^"\n]+", "([^"\n]+)"\)',source)
 if not match:raise ValueError('Pinned upstream test configuration changed')
 return match.group(1)
headers={'Authorization':'Basic '+base64.b64encode((default('USERNAME')+':'+default('PASSWORD')).encode()).decode(),
 'Fineract-Platform-TenantId':default('TENANT')}
results=[]
for path in ['charges/template','charges/template?chargeAppliesTo=1','charges/template?chargeAppliesTo=2','charges','offices']:
 req=urllib.request.Request('https://127.0.0.1:8443/fineract-provider/api/v1/'+path,headers=headers)
 try:
  with urllib.request.urlopen(req,context=ssl._create_unverified_context(),timeout=20) as response:
   raw=response.read();body=json.loads(raw)
   results.append({'path':path,'http_status':response.status,'body_sha256':hashlib.sha256(raw).hexdigest(),
    'option_counts':{k:len(v) for k,v in body.items() if isinstance(v,list) and k.endswith('Options')} if isinstance(body,dict) else {},
    'item_count':len(body) if isinstance(body,list) else None})
 except urllib.error.HTTPError as error:results.append({'path':path,'http_status':error.code})
 except Exception as error:results.append({'path':path,'error_type':type(error).__name__})
print(json.dumps({'checks':results,'all_http_200':all(r.get('http_status')==200 for r in results)}))
'''
    # The application receives only its JAR directory. Private evaluator files
    # in the compilation volume are never exposed to submitted application code.
    output=runtime.docker('run','--rm','--network','container:'+data['names']['app'],
        '--read-only','--user','1000:1000','--cap-drop=ALL','--security-opt=no-new-privileges',
        '--mount',f'type=volume,source={data["source_volume"]},target=/baseline,readonly',
        data['java_image_id'],'python3','-c',script,timeout=120)
    result={'at':run.now(),'service_id':data['id'],**json.loads(output.stdout),'model_calls':0}
    util.write_new_json(root/('api-probe-'+uuid.uuid4().hex+'.json'),result)
    return result


def stop(record):
    """Retain owned containers and data after collecting service logs."""
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    evidence=root/'shutdown'/uuid.uuid4().hex;evidence.mkdir(parents=True)
    result={'at':run.now(),'service_id':data['id'],'containers':{}}
    for role in ('app','database'):
        name=data['names'][role]
        item=json.loads(runtime.docker('inspect',name).stdout)[0]
        if item['Config']['Labels'].get('modernization.service')!=data['id']:
            raise ValueError('Service ownership mismatch')
        runtime.docker('stop','--time','20',name,timeout=45)
        logs=runtime.docker('logs',name,check=False)
        (evidence/(role+'.stdout.log')).write_text(logs.stdout,encoding='utf-8')
        (evidence/(role+'.stderr.log')).write_text(logs.stderr,encoding='utf-8')
        item=json.loads(runtime.docker('inspect',name).stdout)[0]
        result['containers'][role]={'image_id':item['Image'],'state':item['State']}
    result['stop_confirmed']=all(not x['state']['Running'] for x in result['containers'].values())
    util.write_new_json(evidence/'result.json',result)
    return result


def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    s=sub.add_parser('start-baseline');s.add_argument('--preparation',required=True,type=Path)
    s=sub.add_parser('start-evaluation');s.add_argument('--record',required=True,type=Path)
    s=sub.add_parser('inspect');s.add_argument('--record',required=True,type=Path)
    for command in ('api-probe','stop'):
        sub.add_parser(command).add_argument('--record',required=True,type=Path)
    args=p.parse_args()
    result=start_baseline(args.preparation) if args.command=='start-baseline' else {
        'start-evaluation':start_evaluation,'inspect':inspect,'api-probe':api_probe,'stop':stop}[args.command](args.record)
    print(json.dumps(result))


if __name__=='__main__':main()
