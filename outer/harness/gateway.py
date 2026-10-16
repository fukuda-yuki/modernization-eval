"""Fixed-upstream gateway. Credential enters on stdin, never argv/env/files.

Runs in a separate container. Only this process has both upstream access and
the credential. Request bodies and redacted response streams are researcher-only.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import signal
import threading
import uuid


def now():
    return datetime.now(timezone.utc).isoformat()


def usage_from_native(native):
    if not isinstance(native, dict):
        return None
    details = native.get('prompt_tokens_details') or {}
    out_details = native.get('completion_tokens_details') or {}
    return {'input_tokens': native.get('prompt_tokens'),
            'output_tokens': native.get('completion_tokens'),
            'total_tokens': native.get('total_tokens'),
            'cache_read_tokens': details.get('cached_tokens', native.get('prompt_cache_hit_tokens')),
            'cache_write_tokens': details.get('cache_write_tokens'),
            'reasoning_tokens': out_details.get('reasoning_tokens')}


def parse_sse(raw):
    """Retain the last cumulative report even when the stream is interrupted."""
    result = {'stream_done': False, 'usage': None, 'parse_errors': []}
    for number, line in enumerate(raw.splitlines(), 1):
        if not line.startswith(b'data:'):
            continue
        payload = line[5:].strip()
        if payload == b'[DONE]':
            result['stream_done'] = True
            continue
        try:
            item = json.loads(payload)
            if not isinstance(item, dict):
                raise ValueError()
        except (ValueError, UnicodeError):
            result['parse_errors'].append(number)
            continue
        for source, dest in (('model', 'response_model_id'), ('id', 'provider_response_id')):
            if item.get(source):
                result[dest] = item[source]
        if item.get('usage') is not None:
            result['native_usage'] = item['usage']
            result['usage'] = usage_from_native(item['usage'])
    return result


class SecretFilter:
    """Hold enough bytes to redact a credential split across transport chunks."""
    def __init__(self, secret):
        self.secret, self.pending, self.detected = secret.encode(), b'', False

    def feed(self, chunk, final=False):
        self.pending += chunk
        if self.secret in self.pending:
            self.detected = True
            self.pending = self.pending.replace(self.secret, b'[REDACTED]')
        size = len(self.pending) if final else max(0, len(self.pending) - len(self.secret) + 1)
        result, self.pending = self.pending[:size], self.pending[size:]
        return result


class ResponseEndFence:
    """Withhold the terminal SSE line, across arbitrary network chunk splits."""
    def __init__(self):
        self.pending = b''
        self.tail = b''
        self.ended = False

    def feed(self, chunk, final=False):
        if self.ended:
            self.tail += chunk; return b''
        self.pending += chunk
        output = bytearray()
        while b'\n' in self.pending or final and self.pending:
            if b'\n' in self.pending:
                line, self.pending = self.pending.split(b'\n', 1); line += b'\n'
            else: line, self.pending = self.pending, b''
            if line.startswith(b'data:') and line[5:].strip() == b'[DONE]':
                self.ended = True; self.tail = line+self.pending; self.pending = b''; break
            output.extend(line)
        return bytes(output)


def response_tool_ids(raw):
    ids = []
    for line in raw.splitlines():
        if not line.startswith(b'data:') or line[5:].strip() == b'[DONE]': continue
        item = json.loads(line[5:])
        if not isinstance(item,dict) or not isinstance(item.get('choices',[]),list):
            raise ValueError('Unrecognized SSE choices at tool boundary')
        for choice in item.get('choices', []):
            if not isinstance(choice,dict) or not isinstance(choice.get('delta',{}),dict):
                raise ValueError('Unrecognized SSE delta at tool boundary')
            calls = choice.get('delta', {}).get('tool_calls')
            # A null or absent field contributes no tool-call delta. Do not
            # coerce other malformed collection types into an empty group.
            if calls is None: continue
            if not isinstance(calls, list):
                raise ValueError('Unrecognized SSE tool calls at tool boundary')
            for call in calls:
                if not isinstance(call,dict): raise ValueError('Unrecognized SSE tool call')
                if call.get('id') and call['id'] not in ids: ids.append(call['id'])
    return ids


class Gateway(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, address, root, run_id, model, secret, *, upstream_host='opencode.ai',
                 upstream_port=443, tls=True, session_id=None, upstream_timeout=120, expected_prompt=None,
                 staged_input=None, campaign_budget=None):
        super().__init__(address, Handler)
        self.root, self.run_id, self.model, self.secret = Path(root), run_id, model, secret
        self.session_id = session_id or run_id
        self.upstream_host, self.upstream_port, self.tls = upstream_host, upstream_port, tls
        self.upstream_timeout = upstream_timeout
        self.expected_prompt = Path(expected_prompt).read_bytes().decode('utf-8') if expected_prompt else None
        self.input_checked = False
        self.campaign_budget = campaign_budget
        self.lock = threading.Lock()
        self.failed = threading.Event()
        self.stopping = threading.Event()
        self.admission_lock = threading.Lock()
        self.admission_closed = threading.Event()
        self.admission_receipt = None
        self.socket_lock = threading.Lock()
        self.active_sockets = set()
        self.root.mkdir(parents=True, exist_ok=True)
        self.stage = None
        self.stage_arm = None
        self.stage_first_request = None
        self.stage_barrier = None
        self.stage_barrier_event = threading.Event()
        self.stage_busy_request = None
        self.stage_capture_complete = False
        self.stage_native_message_id = None
        self.stage_resume_action = None
        self.stage_first_post_resume_request = None
        if staged_input:
            directory = Path(staged_input)
            contract_raw = (directory/'contract.json').read_bytes()
            contract = json.loads(contract_raw)
            observed = contract.get('kind') == 'observed_presentation_v1'
            control = observed and contract.get('presentation_mode') == 'full_initial_no_additional'
            additional = None if control else (directory/'additional.txt').read_bytes().decode('utf-8')
            if (contract.get('kind') not in ('one_additional_input_v1', 'observed_presentation_v1')
                    or (observed and (contract.get('presentation_mode') not in ('staged_once','full_initial_no_additional')
                        or contract.get('planned_additional_count') != (0 if control else 1)
                        or contract['boundary_contract'].get('observation_policy') != 'resumption_first_change_v1'
                        or contract['boundary_contract'].get('boundary_policy') != 'response_tool_group_pause_v1'))
                    or (contract['run_id'], contract['run_instance_id']) != (run_id, self.session_id)
                    or not self.expected_prompt
                    or (control and (contract['additional_sha256'] is not None or (directory/'additional.txt').exists()))
                    or (not control and (not additional or hashlib.sha256(additional.encode()).hexdigest() != contract['additional_sha256']))
                    or hashlib.sha256(self.expected_prompt.encode()).hexdigest() != contract['initial_sha256']):
                raise ValueError('Staged input identity mismatch')
            self.stage = {**contract, 'contract_sha256': hashlib.sha256(contract_raw).hexdigest(), 'text': additional}
            self.record('staged-input.jsonl', {'kind': 'planned', 'run_id': run_id,
                'run_instance_id': self.session_id, 'delivery_id': contract['delivery_id'],
                'contract_sha256': self.stage['contract_sha256'], 'additional_sha256': contract['additional_sha256']})

    def uses_response_barrier(self):
        return self.stage and self.stage['boundary_contract'].get('transport') == 'opencode-server-response-barrier-v1'

    def observes_resumption(self):
        return self.stage and self.stage.get('kind') == 'observed_presentation_v1'

    def inspect_stage_barrier(self):
        with self.admission_lock:
            return dict(self.stage_barrier) if self.stage_barrier else None

    def release_stage_barrier(self, barrier_id, native_message_id=None, capture_complete=False, resume_action=None):
        with self.admission_lock:
            if (not self.stage_barrier or self.stage_barrier['barrier_id'] != barrier_id
                    or self.stage_barrier_event.is_set()
                    or self.admission_closed.is_set() or self.stopping.is_set() or self.failed.is_set()):
                raise ValueError('No matching open response barrier')
            if self.stage_arm and not native_message_id:
                raise ValueError('Native input acknowledgement required before releasing an armed barrier')
            if self.stage.get('presentation_mode') == 'full_initial_no_additional' and native_message_id is not None:
                raise ValueError('Zero-input control cannot supply a native input acknowledgement')
            if self.stage_native_message_id and native_message_id != self.stage_native_message_id:
                raise ValueError('Native acknowledgement changed at later boundary')
            if resume_action is not None:
                control = self.stage.get('presentation_mode') == 'full_initial_no_additional'
                expected = {'run_id':self.run_id, 'run_instance_id':self.session_id,
                    'contract_sha256':self.stage['contract_sha256'],
                    'barrier_id':barrier_id, 'request_id':self.stage_barrier['request_id'],
                    'action':'no_input_by_design' if control else 'additional_input_acknowledged',
                    'native_message_id':None if control else native_message_id}
                if (not self.observes_resumption() or self.stage_resume_action is not None
                        or set(resume_action) != set(expected)|{'native_session_id','checkpoint_receipt_sha256'}
                        or any(resume_action.get(k)!=v for k,v in expected.items())
                        or not resume_action['native_session_id'] or not resume_action['checkpoint_receipt_sha256']
                        or (control and (native_message_id is not None or self.stage_arm is not None))
                        or (not control and (not self.stage_arm or not native_message_id
                            or resume_action['native_session_id'] != self.stage_arm['native_session_id']))):
                    raise ValueError('Invalid or repeated resumption action')
            if self.observes_resumption() and self.stage_arm and not self.stage_resume_action and resume_action is None:
                raise ValueError('Acknowledged input requires an explicit resumption action')
            if capture_complete and self.stage_barrier.get('phase') != ('after_resume' if self.observes_resumption() else 'after_additional'):
                raise ValueError('Post-input checkpoint cannot precede an observed additional request')
            self.record('response-barriers.jsonl', {'kind':'released', 'at':now(),
                **self.stage_barrier, 'native_message_id':native_message_id,'capture_complete':capture_complete,
                **({'resume_action':resume_action} if self.observes_resumption() else {})})
            if resume_action is not None:self.stage_resume_action=dict(resume_action)
            if native_message_id:self.stage_native_message_id=native_message_id
            if capture_complete:self.stage_capture_complete=True
            self.stage_busy_request = None
            self.stage_barrier_event.set()

    def hold_response_end(self, event, raw):
        ids = response_tool_ids(raw)
        if not ids: return
        with self.admission_lock:
            if self.stage_barrier or self.admission_closed.is_set() or self.failed.is_set():
                raise ValueError('Response barrier unavailable')
            self.stage_barrier_event.clear()
            self.stage_barrier = {'barrier_id':uuid.uuid4().hex, 'request_id':event['request_id'],
                'request_sha256':event['request_sha256'], 'tool_call_ids':ids,
                'run_id':self.run_id, 'run_instance_id':self.session_id,
                'phase':'after_additional' if self.stage_first_request else 'before_additional',
                'first_additional_request_id':self.stage_first_request}
            if self.observes_resumption():
                self.stage_barrier.update(phase='after_resume' if self.stage_first_post_resume_request else 'before_resume',
                    first_post_resume_request_id=self.stage_first_post_resume_request)
            self.record('response-barriers.jsonl', {'kind':'held','at':now(),**self.stage_barrier})
        if (not self.stage_barrier_event.wait(self.stage['budget_seconds'])
                or self.stopping.is_set() or self.admission_closed.is_set()):
            raise TimeoutError('Response barrier stopped or expired')
        with self.admission_lock: self.stage_barrier = None

    def arm_stage(self, body):
        """Controller-only intent; this does not prove native or provider receipt."""
        with self.admission_lock:
            if (not self.stage or self.stage.get('presentation_mode') == 'full_initial_no_additional'
                    or self.admission_closed.is_set() or self.stopping.is_set() or self.failed.is_set()):
                raise ValueError('Staged input closed')
            expected = {k: self.stage[k] for k in ('run_id', 'run_instance_id', 'delivery_id', 'contract_sha256', 'additional_sha256')}
            if (set(body) != set(expected) | {'native_session_id', 'barrier_id'}
                    or any(body.get(k) != v for k, v in expected.items())
                    or not body['native_session_id'] or not body['barrier_id']):
                raise ValueError('Staged input arm binding mismatch')
            if self.uses_response_barrier() and (not self.stage_barrier or self.stage_barrier['barrier_id'] != body['barrier_id']):
                raise ValueError('Additional input requires the live response barrier')
            if self.stage_arm is not None:
                if self.stage_arm != body: raise ValueError('Conflicting staged input writer')
                return dict(self.stage_arm)
            self.record('staged-input.jsonl', {'kind': 'armed', 'at': now(), **body})
            self.stage_arm = dict(body)
            return dict(body)

    def record(self, file, obj):
        with self.lock:
            with (self.root / file).open('a', encoding='utf-8', newline='\n') as f:
                f.write(json.dumps(obj, ensure_ascii=False) + '\n')
                f.flush()
                os.fsync(f.fileno())

    def track_socket(self, connection):
        with self.socket_lock:
            if self.stopping.is_set():
                connection.close()
                raise ConnectionAbortedError()
            self.active_sockets.add(connection)

    def cancel_and_shutdown(self):
        """Stop upstream traffic and let each handler durably close its journal."""
        # Emergency cancellation must not wait behind a blocked POST/fsync.
        # It supplies no admission ACK: close sockets now; incomplete sends
        # retain unknown evidence. Runtime verifies container exit separately.
        self.admission_closed.set()
        self.stopping.set()
        self.failed.set()
        self.stage_barrier_event.set()
        with self.socket_lock:
            sockets = list(self.active_sockets)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        self.shutdown()

    def close_admission(self):
        """Linearize the fence with the actual upstream POST, not request parsing.

        Existing requests can finish; closing admission alone never cancels a
        socket. An acknowledgement follows every earlier local send return.
        """
        with self.admission_lock:
            self.admission_closed.set()
            self.stage_barrier_event.set()
            if self.admission_receipt is None:
                receipt = {'run_id': self.run_id, 'session_id': self.session_id,
                           'closed_at': now(), 'admission_closed': True}
                self.record('control.jsonl', {'reason': 'admission_closed', **receipt})
                self.admission_receipt = receipt
            return dict(self.admission_receipt)

    def serve_with_signals(self):
        def stop(*_):
            # shutdown must run outside the serve_forever thread.
            threading.Thread(target=self.cancel_and_shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            self.serve_forever()
        finally:
            self.server_close()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ready":true}')
        else:
            self.send_error(403, 'Endpoint denied')

    def do_POST(self):
        g = self.server
        if self.path == '/control/staged-barrier':
            try:
                self.connection.settimeout(5); size=int(self.headers.get('Content-Length','0'))
                if self.client_address[0]!='127.0.0.1' or not 0<size<=4096: raise ValueError()
                body=json.loads(self.rfile.read(size))
                if body.get('run_id')!=g.run_id or body.get('run_instance_id')!=g.session_id: raise ValueError()
                if body.get('action')=='inspect': result=g.inspect_stage_barrier()
                elif body.get('action')=='release':
                    g.release_stage_barrier(body['barrier_id'],body.get('native_message_id'),body.get('capture_complete',False),body.get('resume_action')); result={'released':True}
                else: raise ValueError()
            except (ValueError,TypeError,OSError,KeyError):
                self.send_error(403,'Staged barrier unavailable');return
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(result).encode());return
        if self.path == '/control/staged-input':
            try:
                self.connection.settimeout(5)
                size = int(self.headers.get('Content-Length', '0'))
                if self.client_address[0] != '127.0.0.1' or not 0 < size <= 4096:
                    raise ValueError()
                receipt = g.arm_stage(json.loads(self.rfile.read(size)))
            except (ValueError, TypeError, OSError, KeyError):
                self.send_error(403, 'Staged input acknowledgement unavailable'); return
            self.send_response(200); self.end_headers()
            self.wfile.write(json.dumps(receipt).encode()); return
        if self.path == '/control/stop-admission':
            # The worker cannot reach the gateway's loopback interface. This
            # endpoint is exercised only by ownership-checked docker exec.
            try:
                self.connection.settimeout(5)
                size = int(self.headers.get('Content-Length', '0'))
                if self.client_address[0] != '127.0.0.1' or not 0 < size <= 1024:
                    raise ValueError()
                body = json.loads(self.rfile.read(size))
                if body != {'run_id': g.run_id, 'session_id': g.session_id}:
                    raise ValueError()
                receipt = g.close_admission()
            except (ValueError, TypeError, OSError):
                self.send_error(403, 'Stop acknowledgement unavailable')
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(receipt).encode())
            return
        if (self.path not in ('/v1/chat/completions', '/chat/completions')
                or g.failed.is_set() or g.admission_closed.is_set()):
            self.send_error(403, 'Endpoint denied or Run stopped')
            return
        try:
            self.connection.settimeout(30)
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 64 * 1024 * 1024:
                raise ValueError()
            raw = self.rfile.read(size)
            body = json.loads(raw)
            if g.secret.encode() in raw or body.get('model') != g.model or not body.get('stream'):
                raise ValueError()
            if (not isinstance(body.get('messages'), list) or body.get('store')
                    or not isinstance(body.get('stream_options', {}), dict)):
                raise ValueError()
            # include_usage changes telemetry only; capture the exact transmitted body.
            body.setdefault('stream_options', {})['include_usage'] = True
            raw = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
            with g.lock:
                if g.expected_prompt is not None and not g.input_checked:
                    receipt = check_initial_input(body, g.expected_prompt)
                    receipt['request_sha256'] = hashlib.sha256(raw).hexdigest()
                    with (g.root / 'first-request-contract.json').open('x', encoding='utf-8') as f:
                        json.dump(receipt, f, indent=2)
                    if not receipt['verified']:
                        raise ValueError('Initial input mismatch')
                    g.input_checked = True
        except (ValueError, TypeError, OSError, AttributeError):
            g.failed.set()
            g.record('control.jsonl', {'run_id': g.run_id, 'at': now(), 'reason': 'request_rejected'})
            g.record('failure.jsonl', {'run_id': g.run_id, 'at': now(), 'status': 'request_rejected'})
            self.send_error(403, 'Request policy denied')
            return
        rid = uuid.uuid4().hex
        event = {'run_id': g.run_id, 'session_id': g.session_id, 'event_id': rid, 'request_id': rid,
                 'mode': 'request', 'includes_children': False, 'model_id': g.model,
                 'provider': 'opencode-go', 'started_at': now(), 'usage': None,
                 'status': 'started', 'evidence_version': 3, 'send_evidence': 'known_no_send',
                 'provider_acknowledged': False, 'request_file': rid + '.request.json',
                 'response_file': rid + '.response.sse',
                 'request_sha256': hashlib.sha256(raw).hexdigest()}
        try:
            with (g.root / event['request_file']).open('xb') as original:
                original.write(raw)
                original.flush()
                os.fsync(original.fileno())
            g.record('started.jsonl', event)
            original = (g.root / event['response_file']).open('xb')
        except OSError:
            # Storage failure before any transmission stops this gateway. Do not
            # send a request whose identity/original could not be preserved.
            g.failed.set()
            self.send_error(507, 'Evidence storage unavailable')
            return
        filt = SecretFilter(g.secret)
        response_bytes = bytearray()
        connection = None
        upstream_socket = None
        sent_headers = False
        response_fence = None
        campaign_reserved = False
        try:
            if g.campaign_budget is not None:
                try:
                    g.campaign_budget.reserve(rid, g.session_id)
                    campaign_reserved = True
                    event['campaign_admission'] = 'reserved'
                except Exception:
                    event['campaign_admission'] = 'denied_or_unavailable'
                    g.admission_closed.set()
                    raise
            cls = http.client.HTTPSConnection if g.tls else http.client.HTTPConnection
            connection = cls(g.upstream_host, g.upstream_port, timeout=g.upstream_timeout)
            connection.connect()
            upstream_socket = connection.sock
            g.track_socket(upstream_socket)
            with g.admission_lock:
                if g.admission_closed.is_set() or g.stopping.is_set() or g.failed.is_set():
                    raise ConnectionAbortedError()
                if g.uses_response_barrier() and not g.stage_capture_complete:
                    if g.stage_busy_request is not None:
                        raise ValueError('Concurrent request crossed the staged response fence')
                    g.stage_busy_request = rid
                if g.stage is not None:
                    receipt = check_additional_input(body, g.stage['text']) if g.stage['text'] is not None else {
                        'verified':False,'locations':[], 'not_applicable':'no_input_by_design'}
                    reason = ('early_input' if receipt['locations'] and g.stage_arm is None else
                              'duplicate_or_wrong_role' if receipt['locations'] and not receipt['verified'] else
                              'boundary_missed' if g.stage_arm and not g.stage_first_request and not receipt['verified'] else None)
                    stage_record = {'kind': 'request_checked', 'at': now(), 'request_id': rid,
                        'request_sha256': event['request_sha256'], 'delivery_id': g.stage['delivery_id'],
                        'contract_sha256': g.stage['contract_sha256'], 'run_id': g.run_id,
                        'run_instance_id': g.session_id, 'native_session_id': (g.stage_arm or {}).get('native_session_id'),
                        'receipt': receipt, 'rejection': reason}
                    if g.observes_resumption():
                        stage_record.update(after_resume=g.stage_resume_action is not None,
                            first_post_resume=bool(g.stage_resume_action and not g.stage_first_post_resume_request))
                        if g.stage_resume_action and not g.stage_first_post_resume_request:
                            g.stage_first_post_resume_request=rid
                    g.record('staged-input.jsonl', stage_record)
                    if reason: raise ValueError('Staged input request denied')
                    if receipt['verified']:
                        event['additional_input'] = {'delivery_id': g.stage['delivery_id'],
                            'contract_sha256': g.stage['contract_sha256'], 'first_observed': g.stage_first_request is None}
                        if g.stage_first_request is None:
                            g.stage_first_request = rid
                if g.uses_response_barrier() and not g.stage_capture_complete:
                    response_fence = ResponseEndFence()
                event['send_evidence'] = 'unknown'
                g.record('transmission.jsonl', {**event, 'phase': 'send_intent', 'at': now()})
                connection.request('POST', '/zen/go/v1/chat/completions', raw, {
                    'Authorization': 'Bearer ' + g.secret, 'Content-Type': 'application/json',
                    'Accept': 'text/event-stream', 'User-Agent': 'sample2-verification-machine/1',
                    'x-opencode-session': g.session_id})
                event['send_evidence'] = 'observed_send'
                event['transmitted_at'] = now()
                g.record('transmission.jsonl', {**event, 'phase': 'local_send_returned'})
            response = connection.getresponse()
            event['provider_acknowledged'] = True
            event['http_status'] = response.status
            if response.status != 200:
                g.failed.set()
            self.send_response(response.status)
            self.send_header('Content-Type', response.getheader('Content-Type', 'text/event-stream'))
            self.send_header('Connection', 'close')
            self.end_headers()
            sent_headers = True
            connected = True
            while True:
                chunk = response.read1(16384)
                safe = filt.feed(chunk, final=not chunk)
                response_bytes.extend(safe)
                original.write(safe)
                original.flush()
                os.fsync(original.fileno())
                if len(response_bytes) > 64 * 1024 * 1024:
                    raise ValueError('response_limit')
                if connected:
                    try:
                        self.wfile.write(response_fence.feed(safe) if response_fence else safe)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionError, OSError):
                        connected = False
                        event['client_disconnected'] = True
                if not chunk:
                    break
            if response_fence:
                self.wfile.write(response_fence.feed(b'', final=True));self.wfile.flush()
                if not response_fence.ended or response.status != 200:
                    raise ValueError('Incomplete response cannot establish a staged tool boundary')
                g.hold_response_end(event, bytes(response_bytes))
                self.wfile.write(response_fence.tail);self.wfile.flush()
            event.update(parse_sse(bytes(response_bytes)))
            event['status'] = 'completed' if response.status == 200 and event['stream_done'] else 'provider_error'
        except Exception as exc:
            # Never log exception messages: upstream errors can include credentials.
            event['status'] = ('cancelled' if g.stopping.is_set() or
                g.admission_closed.is_set() and event['send_evidence'] == 'known_no_send'
                else 'transport_error')
            event['error_type'] = type(exc).__name__
            if not (g.admission_closed.is_set() and event['send_evidence'] == 'known_no_send'):
                g.failed.set()
            if not sent_headers:
                try:
                    self.send_error(502, 'Upstream transport failed')
                except OSError:
                    pass
        finally:
            if g.stopping.is_set() and not event.get('stream_done'):
                event['status'] = 'cancelled'
            if connection:
                connection.close()
            if upstream_socket:
                with g.socket_lock:
                    g.active_sockets.discard(upstream_socket)
            tail = filt.feed(b'', final=True)
            response_bytes.extend(tail)
            original.write(tail)
            original.flush()
            os.fsync(original.fileno())
            original.close()
            # Parsing also happens after cancellation/transport exceptions and
            # after SecretFilter releases its final tail. DONE alone is not usage.
            event.update(parse_sse(bytes(response_bytes)))
            if event['send_evidence'] != 'known_no_send' and event.get('response_model_id') != g.model:
                event['policy_error'] = 'response_model_mismatch'
                g.failed.set()
            event['usage_complete'] = (event['status'] == 'completed' and not event['parse_errors']
                and not event.get('policy_error') and bool(event.get('usage'))
                and all(type(event['usage'].get(k)) is int and event['usage'][k] >= 0
                        for k in ('input_tokens', 'output_tokens')))
            if filt.detected:
                event['policy_error'] = 'credential_echo_redacted'
                g.failed.set()
            event['response_sha256'] = hashlib.sha256(response_bytes).hexdigest()
            event['ended_at'] = now()
            if (event['status'] != 'completed' and not
                    (g.admission_closed.is_set() and event['send_evidence'] == 'known_no_send')):
                g.failed.set()
            g.record('events.jsonl', event)
            if campaign_reserved:
                try:
                    g.campaign_budget.finish(rid, event.get('usage'), event['status'])
                except Exception:
                    g.failed.set()
                    g.record('control.jsonl', {'run_id':g.run_id, 'at':now(),
                                             'reason':'campaign_usage_persistence_failed'})
            if g.failed.is_set():
                g.record('failure.jsonl', {'run_id': g.run_id, 'request_id': rid,
                                          'status': event['status'], 'at': now()})
            with g.admission_lock:
                if g.stage_busy_request == rid: g.stage_busy_request = None


def check_initial_input(body, expected):
    """Check semantic text after normal agent serialization, before upstream dispatch."""
    messages = body.get('messages', [])
    texts = []
    for i, message in enumerate(messages):
        content = message.get('content', '')
        if isinstance(content, list):
            content = ''.join(part.get('text', '') for part in content if isinstance(part, dict))
        if isinstance(content, str):
            texts.append((i, message.get('role'), content))
    locations = [{'message': i, 'role': role, 'occurrences': text.count(expected)}
                 for i, role, text in texts if expected in text]
    header_count = sum(text.count('CATALOG_SOURCE ') for _, _, text in texts)
    expected_headers = expected.count('CATALOG_SOURCE ')
    return {'verified': len(locations) == 1 and locations[0]['role'] == 'user'
            and locations[0]['occurrences'] == 1 and header_count == expected_headers,
            'prompt_sha256': hashlib.sha256(expected.encode()).hexdigest(),
            'prompt_utf8_bytes': len(expected.encode()), 'locations': locations,
            'source_packet_count': header_count, 'expected_source_packet_count': expected_headers}


def check_additional_input(body, expected):
    """Later input has its own role/occurrence proof, independent of preload headers."""
    if not expected: raise ValueError('Empty additional input')
    receipt = check_initial_input(body, expected)
    locations = receipt['locations']
    receipt['verified'] = (len(locations) == 1 and locations[0]['role'] == 'user'
                           and locations[0]['occurrences'] == 1)
    return receipt


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run-id', required=True)
    p.add_argument('--model', required=True)
    p.add_argument('--session-id')
    p.add_argument('--upstream-timeout', type=int, default=120)
    p.add_argument('--directory', default='/records')
    p.add_argument('--expected-prompt')
    p.add_argument('--staged-input', help='Controller-only contract directory; never a worker mount')
    p.add_argument('--campaign-budget', help='Trusted shared admission database; never a worker mount')
    args = p.parse_args()
    import sys
    secret = sys.stdin.readline().strip()
    if not secret:
        raise SystemExit('Gateway credential is missing')
    budget = None
    if args.campaign_budget:
        from campaign_budget import Budget
        budget = Budget(args.campaign_budget)
        budget.snapshot()  # Refuse missing/unreadable policy before listening.
    server = Gateway(('0.0.0.0', 8080), args.directory, args.run_id, args.model, secret,
                     session_id=args.session_id, upstream_timeout=args.upstream_timeout,
                     expected_prompt=args.expected_prompt, staged_input=args.staged_input,
                     campaign_budget=budget)
    server.serve_with_signals()


if __name__ == '__main__':
    main()
