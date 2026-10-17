"""Network-isolated synthetic transport for runtime proof; never reads a key."""
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from pathlib import Path

from gateway import Gateway
from campaign_budget import Budget


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        item={'id':'synthetic-local-only','model':'deepseek-v4.1-flash',
              'choices':[{'index':0,'delta':{'content':'synthetic probe'},'finish_reason':'stop'}],
              'usage':{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10}}
        self.wfile.write(('data: '+json.dumps(item)+'\n\ndata: [DONE]\n\n').encode())


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--run-id',required=True)
    p.add_argument('--campaign-budget',required=True)
    p.add_argument('--expected-prompt',required=True)
    a=p.parse_args()
    Path('/records/transport.json').write_text(json.dumps({'transport':'synthetic_loopback','provider_calls':0,'credential_source':None}))
    upstream=ThreadingHTTPServer(('127.0.0.1',0),Upstream)
    Thread(target=upstream.serve_forever,daemon=True).start()
    gateway=Gateway(('0.0.0.0',8080),'/records',a.run_id,'deepseek-v4.1-flash',
                    'synthetic-no-provider-credential',upstream_host='127.0.0.1',
                    upstream_port=upstream.server_port,tls=False,session_id=a.run_id,
                    expected_prompt=a.expected_prompt,
                    upstream_timeout=600, upstream_deadline=600,
                    campaign_budget=Budget(a.campaign_budget))
    gateway.serve_with_signals()


if __name__=='__main__':
    main()
