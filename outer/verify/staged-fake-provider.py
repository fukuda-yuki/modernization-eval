"""Docker-only canned provider for the staged-input probe; no external endpoint."""
import argparse
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import sys
import threading
from gateway import Gateway

p=argparse.ArgumentParser()
p.add_argument('--run-id',required=True);p.add_argument('--model',required=True)
p.add_argument('--session-id',required=True);p.add_argument('--upstream-timeout',type=int,required=True)
p.add_argument('--expected-prompt',required=True);p.add_argument('--staged-input',required=True)
a=p.parse_args();secret=sys.stdin.readline().rstrip('\r\n')
count=0
class Provider(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def do_POST(self):
        global count
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));count+=1
        scenario=a.run_id.lower()
        ambiguous_at=2 if 'post-' in scenario else 1
        def tool(index,name,arguments):
            return {'index':index,'id':'synthetic-'+str(count)+'-'+str(index),'type':'function',
                'function':{'name':name,'arguments':json.dumps(arguments)}}
        calls=[]
        if count==ambiguous_at and 'child' in scenario:
            program="import subprocess;from pathlib import Path;Path('/workspace/Program.cs').write_text('// first');subprocess.Popen(['sh','-c','sleep 60; printf late > /workspace/Late.cs'],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)"
            import shlex
            calls=[tool(0,'bash',{'command':'python3 -c '+shlex.quote(program),'description':'Synthetic detached child lifetime fixture'})]
        elif ('early' not in scenario and count <= (1 if 'no-post-change' in scenario else 3 if 'continue' in scenario else 2)):
            calls=[tool(0,'write',{'filePath':'/workspace/Program.cs','content':'// synthetic stage '+str(count)+'\n'})]
            if 'archive-failure' in scenario:
                command=("printf synthetic > /workspace/example.sqlite; printf unsealed > /workspace/example.sqlite-wal"
                         if count==1 else 'rm /workspace/example.sqlite-wal')
                calls.append(tool(1,'bash',{'command':command,'description':'Synthetic static DB sidecar observation'}))
            if count==ambiguous_at and 'parallel' in scenario:
                calls.append(tool(1,'write',{'filePath':'/workspace/Other.cs','content':'// simultaneous synthetic mutation\n'}))
        delta={'role':'assistant','tool_calls':calls} if calls else {'role':'assistant','content':'Synthetic completion.'}
        item={'id':'synthetic-'+str(count),'model':body['model'],'object':'chat.completion.chunk','created':1,
            'choices':[{'index':0,'delta':delta,'finish_reason':None}]}
        self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
        self.wfile.write(('data: '+json.dumps(item)+'\n\n').encode())
        item['choices']=[{'index':0,'delta':{},'finish_reason':'tool_calls' if calls else 'stop'}]
        item['usage']={'prompt_tokens':10,'completion_tokens':2,'total_tokens':12}
        self.wfile.write(('data: '+json.dumps(item)+'\n\n: synthetic padding for redaction buffer\n\ndata: [DONE]\n\n').encode());self.wfile.flush()
upstream=ThreadingHTTPServer(('127.0.0.1',0),Provider)
threading.Thread(target=upstream.serve_forever,daemon=True).start()
server=Gateway(('0.0.0.0',8080),'/records',a.run_id,a.model,secret,upstream_host='127.0.0.1',
    upstream_port=upstream.server_port,tls=False,session_id=a.session_id,upstream_timeout=a.upstream_timeout,
    expected_prompt=a.expected_prompt,staged_input=a.staged_input)
server.serve_with_signals()
