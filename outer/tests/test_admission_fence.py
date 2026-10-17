"""Deterministic local HTTP/fault tests. No credential, Docker or model service."""
from concurrent.futures import ThreadPoolExecutor
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import support
from harness import gateway, runtime, util
from harness.campaign_budget import Budget


HTTPConnection = http.client.HTTPConnection
BODY = json.dumps({'model': 'test-model', 'stream': True,
                   'messages': [{'role': 'user', 'content': 'synthetic'}]})
SSE = b'data: {"model":"test-model","usage":{"prompt_tokens":11,"completion_tokens":3}}\n\ndata: [DONE]\n\n'


class AdmissionFenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.received = []
        self.entered, self.release = threading.Event(), threading.Event()
        self.release.set()
        owner = self
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                owner.received.append(self.rfile.read(int(self.headers['Content-Length'])))
                owner.entered.set()
                owner.release.wait(5)
                self.send_response(200); self.end_headers()
                self.wfile.write(SSE)
        self.upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        self.proxy = gateway.Gateway(('127.0.0.1', 0), self.root, 'A', 'test-model', 'synthetic-canary',
            session_id='instance-A', upstream_host='127.0.0.1',
            upstream_port=self.upstream.server_port, tls=False)
        for server in (self.upstream, self.proxy):
            threading.Thread(target=server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.release.set()
        for server in (self.proxy, self.upstream): server.shutdown(); server.server_close()

    def request(self, path='/v1/chat/completions', body=BODY):
        connection = HTTPConnection('127.0.0.1', self.proxy.server_port, timeout=5)
        try:
            connection.request('POST', path, body)
            response = connection.getresponse()
            return response.status, response.read()
        finally: connection.close()

    def test_connected_requests_waiting_before_send_cannot_cross_fence(self):
        # Both have passed parsing and connected, before send-intent/POST. No
        # sleep determines ordering; barriers force the previously failing race.
        connected = threading.Barrier(3)
        release = threading.Event()
        class PausedConnection(HTTPConnection):
            def connect(inner):
                super(PausedConnection, inner).connect()
                connected.wait(5); release.wait(5)
        with patch.object(gateway.http.client, 'HTTPConnection', PausedConnection), ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(self.request) for _ in range(2)]
            connected.wait(5)
            receipt = self.proxy.close_admission()
            release.set()
            self.assertTrue(all(f.result(5)[0] == 502 for f in futures))
        self.assertEqual(self.received, [])
        self.proxy.shutdown(); self.proxy.server_close()
        ends = util.read_lines(self.root/'events.jsonl')
        self.assertEqual(len(ends), 2)
        self.assertTrue(all(e['send_evidence'] == 'known_no_send' and e['usage'] is None for e in ends))
        self.assertEqual(util.read_lines(self.root/'transmission.jsonl'), [])
        self.assertEqual(self.proxy.close_admission(), receipt)

    def test_send_in_progress_linearizes_before_ack_and_is_not_duplicated(self):
        sending, release_send = threading.Event(), threading.Event()
        class PausedSend(HTTPConnection):
            def request(inner, *args, **kwargs):
                sending.set(); release_send.wait(5)
                return super(PausedSend, inner).request(*args, **kwargs)
        with patch.object(gateway.http.client, 'HTTPConnection', PausedSend), ThreadPoolExecutor(2) as pool:
            response = pool.submit(self.request)
            self.assertTrue(sending.wait(5))
            fence_entered = threading.Event()
            def fence():
                fence_entered.set()
                return self.proxy.close_admission()
            closed = pool.submit(fence)
            self.assertTrue(fence_entered.wait(5))
            self.assertFalse(closed.done())
            release_send.set()
            ack = closed.result(5)
            self.assertEqual(response.result(5)[0], 200)
        self.assertEqual(self.request()[0], 403)
        self.proxy.shutdown(); self.proxy.server_close()
        self.assertEqual(len(self.received), 1)
        ends = util.read_lines(self.root/'events.jsonl')
        self.assertEqual(len(ends), 1)
        self.assertLessEqual(ends[0]['transmitted_at'], ack['closed_at'])
        self.assertEqual(ends[0]['usage']['input_tokens'], 11)

    def test_inflight_stream_finishes_after_ack_without_usage_or_original_loss(self):
        self.release.clear()
        with ThreadPoolExecutor(1) as pool:
            response = pool.submit(self.request)
            self.assertTrue(self.entered.wait(5))
            status, raw_ack = self.request('/control/stop-admission', json.dumps({
                'run_id': 'A', 'session_id': 'instance-A'}))
            self.assertEqual(status, 200)
            ack = json.loads(raw_ack)
            self.assertEqual(self.request()[0], 403)
            self.release.set()
            self.assertEqual(response.result(5)[1], SSE)
        # server_close joins its handler threads; terminal fsync can follow the
        # client's EOF and must finish before examining the retained journal.
        self.proxy.shutdown(); self.proxy.server_close()
        ends = util.read_lines(self.root/'events.jsonl')
        self.assertEqual(len(self.received), 1)
        self.assertEqual(len(ends), 1)
        self.assertTrue(ends[0]['usage_complete'])
        self.assertGreaterEqual(ends[0]['ended_at'], ack['closed_at'])
        self.assertEqual((self.root/ends[0]['response_file']).read_bytes(), SSE)
        self.assertEqual(ends[0]['response_sha256'], util.sha256_file(self.root/ends[0]['response_file']))

    def test_wrong_instance_or_nonloopback_control_cannot_close_admission(self):
        self.assertEqual(self.request('/control/stop-admission', json.dumps({
            'run_id': 'A', 'session_id': 'wrong'}))[0], 403)
        # Handler unit boundary: spoofing Host is irrelevant; peer address wins.
        handler = object.__new__(gateway.Handler)
        handler.server, handler.path = self.proxy, '/control/stop-admission'
        handler.client_address = ('192.0.2.1', 1234)
        handler.connection = type('Connection', (), {'settimeout': lambda *_: None})()
        handler.headers = {'Content-Length': '1'}
        handler.send_error = lambda status, *_: self.assertEqual(status, 403)
        handler.do_POST()
        self.assertFalse(self.proxy.admission_closed.is_set())

    def test_emergency_cancellation_does_not_wait_for_dispatch_lock_or_journal(self):
        with self.proxy.admission_lock, patch.object(self.proxy, 'record', side_effect=OSError('synthetic')):
            with ThreadPoolExecutor(1) as pool:
                stopped = pool.submit(self.proxy.cancel_and_shutdown)
                stopped.result(5)
            self.assertTrue(self.proxy.stopping.is_set())
            self.assertTrue(self.proxy.admission_closed.is_set())
            self.assertIsNone(self.proxy.admission_receipt)

    def test_campaign_cap_stops_before_any_extra_upstream_send(self):
        budget = Budget.create(self.root/'campaign.sqlite', campaign_id='test',
            plan_sha256='a'*64, max_requests=1, max_tokens=100, deadline=time.time()+60)
        self.proxy.campaign_budget = budget
        self.assertEqual(self.request()[0], 200)
        self.assertEqual(self.request()[0], 502)
        self.proxy.shutdown(); self.proxy.server_close()
        self.assertEqual(len(self.received), 1)
        summary = budget.snapshot()
        self.assertEqual(summary['reserved_requests'], 1)
        self.assertEqual(summary['observed_tokens'], 14)
        events = util.read_lines(self.root/'events.jsonl')
        denied = next(e for e in events if e['campaign_admission']=='denied_or_unavailable')
        self.assertEqual(denied['send_evidence'], 'known_no_send')
        self.assertIsNone(denied['usage'])

    def test_missing_campaign_policy_never_connects_upstream(self):
        self.proxy.campaign_budget = Budget(self.root/'missing.sqlite')
        self.assertEqual(self.request()[0], 502)
        self.assertEqual(self.received, [])
        self.assertTrue(self.proxy.admission_closed.is_set())


class OwnedFenceFaultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = {'run_id': 'A', 'run_instance_id': 'instance-A', 'worker': 'w', 'gateway': 'g'}
        util.write_new_json(self.root/'runtime.json', self.state)
        self.item = {'Config': {'Labels': {'sample2.run': 'A', 'sample2.instance': 'instance-A'}},
                     'State': {'Running': True}}

    def test_control_timeout_lost_ack_and_wrong_ack_stop_gateway_before_worker(self):
        for result in (TimeoutExpired('synthetic-control', 10), CompletedProcess([], 1, '', ''),
                       CompletedProcess([], 0, '{"admission_closed":true,"session_id":"wrong"}', '')):
            with self.subTest(result=type(result).__name__):
                stopped=[]
                def stop(state, role): stopped.append(role); return True
                with patch.object(runtime, '_owned_container', return_value=self.item), \
                     patch.object(runtime, '_stop_owned_role', side_effect=stop), \
                     patch.object(runtime, 'docker', side_effect=result if isinstance(result, Exception) else None,
                                  return_value=result):
                    self.assertTrue(runtime.stop_owned(self.root))
                self.assertEqual(stopped, ['gateway', 'worker', 'gateway'])
                latest=list((self.root/'evidence/admission-stop').glob('*.json'))[-1]
                self.assertEqual(util.read_json(latest)['method'], 'gateway_stop')

    def test_failed_fallback_does_not_confirm_stop(self):
        with patch.object(runtime, '_owned_container', return_value=self.item), \
             patch.object(runtime, 'docker', return_value=CompletedProcess([], 1, '', '')), \
             patch.object(runtime, '_stop_owned_role', return_value=False):
            self.assertFalse(runtime.stop_owned(self.root))

    def test_fence_or_worker_timeout_cannot_skip_other_owned_role(self):
        for fence_ok in (False, True):
            attempted=[]
            def stop(state, role):
                attempted.append(role)
                if role == 'worker': raise TimeoutExpired('synthetic-worker', 40)
                return True
            with patch.object(runtime, 'fence_owned', side_effect=None if fence_ok else TimeoutExpired('synthetic-fence', 10),
                              return_value={'confirmed': True}), \
                 patch.object(runtime, '_stop_owned_role', side_effect=stop):
                self.assertFalse(runtime.stop_owned(self.root))
            self.assertEqual(set(attempted), {'worker', 'gateway'})

    def test_foreign_role_is_untouched_but_owned_peer_is_attempted(self):
        with patch.object(runtime, 'fence_owned', side_effect=runtime.ContainerOwnershipError('foreign gateway')), \
             patch.object(runtime, '_stop_owned_role', side_effect=lambda state, role:
                (_ for _ in ()).throw(runtime.ContainerOwnershipError('foreign gateway')) if role == 'gateway' else True) as stop:
            with self.assertRaises(runtime.ContainerOwnershipError): runtime.stop_owned(self.root)
        stop.assert_any_call(self.state, 'worker')

    def test_ownership_mismatch_never_executes_or_stops(self):
        item = {**self.item, 'Config': {'Labels': {'sample2.run': 'other'}}}
        with patch.object(runtime, 'inspect_container', return_value=item), patch.object(runtime, 'docker') as docker:
            with self.assertRaisesRegex(RuntimeError, 'ownership mismatch'): runtime.fence_owned(self.root)
            docker.assert_not_called()

    def test_receipt_storage_failure_cannot_skip_owned_shutdown(self):
        ack = {'run_id': 'A', 'session_id': 'instance-A', 'closed_at': 'synthetic', 'admission_closed': True}
        stopped=[]
        with patch.object(runtime, '_owned_container', return_value=self.item), \
             patch.object(runtime, 'docker', return_value=CompletedProcess([], 0, json.dumps(ack), '')), \
             patch.object(runtime, '_stop_owned_role', side_effect=lambda state, role: stopped.append(role) or True), \
             patch.object(runtime.util, 'write_new_json', side_effect=OSError('synthetic')):
            self.assertTrue(runtime.stop_owned(self.root))
            receipt=runtime.fence_owned(self.root)
        self.assertEqual(stopped, ['worker', 'gateway'])
        self.assertFalse(receipt['evidence_persisted'])

    def test_stop_marker_storage_failure_still_attempts_owned_shutdown(self):
        util.write_new_json(self.root/'manifest.json', {'schema_version': 2, 'run_id': 'A'})
        with patch.object(runtime.util, 'write_json_atomic', side_effect=OSError('synthetic')), \
             patch.object(runtime, 'stop_owned', return_value=True) as stop:
            with self.assertRaisesRegex(RuntimeError, 'shutdown attempted'):
                runtime.request_stop(self.root)
            stop.assert_called_once_with(self.root)

    def test_request_stop_fences_before_controller_lease_wait(self):
        path = self.root/'manifest.json'
        util.write_new_json(path, {'schema_version': 2, 'run_id': 'A'})
        order=[]
        def fence(root):
            self.assertTrue((root/'stop-request.json').exists())
            order.append('fence')
            util.write_json_atomic(path, {'schema_version': 2, 'run_id': 'A', 'ended_at': 'synthetic',
                                        'stop_confirmed': True, 'network_cleanup': {'confirmed': True}})
        def lease(root):
            order.append('lease')
            raise BlockingIOError()
        with patch.object(runtime, 'fence_owned', side_effect=fence), \
             patch.object(runtime.ownership, 'lease', side_effect=lease):
            self.assertTrue(runtime.request_stop(self.root)['stop_confirmed'])
        self.assertEqual(order, ['fence', 'lease'])

    def test_request_stop_early_fence_fault_still_attempts_owned_peer_shutdown(self):
        util.write_new_json(self.root/'manifest.json', {'schema_version': 2, 'run_id': 'A'})
        for fault in (TimeoutExpired('synthetic', 10), runtime.ContainerOwnershipError('foreign')):
            with self.subTest(fault=type(fault).__name__), \
                 patch.object(runtime, 'fence_owned', side_effect=fault), \
                 patch.object(runtime, 'stop_owned', return_value=False) as stop:
                with self.assertRaises(type(fault)): runtime.request_stop(self.root)
                stop.assert_called_once_with(self.root)


if __name__ == '__main__': unittest.main()
