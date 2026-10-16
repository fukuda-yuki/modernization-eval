"""Absolute request limits against loopback streams; no provider/model calls."""
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import socketserver
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import support
from harness.gateway import Gateway, RequestDeadline


class Trickle(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        try:
            if self.server.slow_headers:
                for byte in b'HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n':
                    self.wfile.write(bytes([byte])); self.wfile.flush(); time.sleep(.05)
            else:
                self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
            first={'model':'synthetic','usage':{'prompt_tokens':7,'completion_tokens':3},'choices':[]}
            self.wfile.write(('data: '+json.dumps(first)+'\n\n').encode()); self.wfile.flush()
            for _ in range(40):
                self.wfile.write(b'data: {"model":"synthetic","choices":[]}\n\n'); self.wfile.flush(); time.sleep(.05)
            self.wfile.write(b'data: [DONE]\n\n'); self.wfile.flush()
        except OSError:
            self.server.closed_by_gateway.set()


class GatewayDeadlineTests(unittest.TestCase):
    def exercise(self, disconnect=False, slow_headers=False):
        with tempfile.TemporaryDirectory() as directory:
            upstream=ThreadingHTTPServer(('127.0.0.1',0),Trickle)
            upstream.daemon_threads=True
            upstream.slow_headers=slow_headers; upstream.closed_by_gateway=threading.Event()
            server=Gateway(('127.0.0.1',0),directory,'synthetic-run','synthetic','dummy',
                           upstream_host='127.0.0.1',upstream_port=upstream.server_port,
                           tls=False,upstream_timeout=.5)
            # An opt-in contract; unchanged historical callers keep their socket timeout.
            server.upstream_deadline=.5
            threads=[threading.Thread(target=s.serve_forever,daemon=True) for s in (upstream,server)]
            for thread in threads:thread.start()
            client=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
            started=time.monotonic()
            try:
                body=json.dumps({'model':'synthetic','stream':True,'messages':[{'role':'user','content':'synthetic'}]})
                client.request('POST','/v1/chat/completions',body,{'Content-Type':'application/json'})
                if disconnect:
                    client.sock.shutdown(socket.SHUT_RDWR); client.close()
                else:
                    try:client.getresponse().read()
                    except (OSError,http.client.HTTPException):pass
                path=Path(directory)/'events.jsonl'
                while not path.exists() and time.monotonic()-started<5:time.sleep(.01)
                event=json.loads(path.read_text().splitlines()[-1])
                elapsed=time.monotonic()-started
                self.assertLess(elapsed,1.4)
                self.assertEqual('provider_deadline_exceeded',event['status'])
                self.assertFalse(event['usage_complete'])
                self.assertFalse(event['stream_done'])
                self.assertTrue(upstream.closed_by_gateway.wait(.5))
                if not slow_headers:
                    self.assertEqual(7,event['usage']['input_tokens'])
                if disconnect:self.assertTrue(event['client_disconnected'])
                return {'elapsed_seconds':elapsed,'event':event,'upstream_closed':True,
                        'provider_calls':0,'model_calls':0}
            finally:
                client.close()
                for s in (server,upstream):s.shutdown();s.server_close()
                for thread in threads:thread.join(2)

    def test_continuous_stream_has_absolute_deadline(self):self.exercise()
    def test_disconnect_does_not_bypass_deadline(self):self.exercise(disconnect=True)
    def test_trickled_headers_have_absolute_deadline(self):self.exercise(slow_headers=True)

    def test_stalled_tls_handshake_is_closed_at_deadline(self):
        class Stall(socketserver.BaseRequestHandler):
            def handle(self):
                while self.request.recv(4096): pass
                self.server.peer_closed.set()
        with tempfile.TemporaryDirectory() as directory:
            upstream=socketserver.ThreadingTCPServer(('127.0.0.1',0),Stall)
            upstream.daemon_threads=True; upstream.peer_closed=threading.Event()
            server=Gateway(('127.0.0.1',0),directory,'synthetic-run','synthetic','dummy',
                upstream_host='127.0.0.1',upstream_port=upstream.server_address[1],tls=True,
                upstream_timeout=5,upstream_deadline=.5)
            for s in (server,upstream):threading.Thread(target=s.serve_forever,daemon=True).start()
            client=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
            started=time.monotonic()
            try:
                client.request('POST','/v1/chat/completions',json.dumps({'model':'synthetic','stream':True,'messages':[]}))
                try:client.getresponse().read()
                except (OSError,http.client.HTTPException):pass
                eventfile=Path(directory)/'events.jsonl'
                while not eventfile.exists() and time.monotonic()-started<3:time.sleep(.01)
                event=json.loads(eventfile.read_text().splitlines()[-1])
                self.assertLess(time.monotonic()-started,1.4)
                self.assertEqual('provider_deadline_exceeded',event['status'])
                self.assertEqual('known_no_send',event['send_evidence'])
                self.assertFalse(event['usage_complete'])
                self.assertTrue(upstream.peer_closed.wait(.5))
            finally:
                client.close()
                for s in (server,upstream):s.shutdown();s.server_close()

    def test_dns_wait_cannot_create_a_socket_after_deadline(self):
        released=threading.Event(); finished=threading.Event()
        class Client:
            def shutdown(self,*args): pass
            def close(self): pass
        def resolve(*args):
            released.wait(2); finished.set()
            return [(socket.AF_INET,socket.SOCK_STREAM,0,'',('127.0.0.1',1))]
        deadline=RequestDeadline(.15,Client())
        started=time.monotonic()
        try:
            with patch('socket.getaddrinfo',side_effect=resolve), patch('socket.socket') as create:
                with self.assertRaises(TimeoutError):deadline.connect_socket(('synthetic.invalid',1),5)
                self.assertLess(time.monotonic()-started,.8)
                released.set();self.assertTrue(finished.wait(1))
                create.assert_not_called()
            self.assertTrue(deadline.expired.is_set())
        finally:released.set();deadline.finish()


if __name__=='__main__':unittest.main()
