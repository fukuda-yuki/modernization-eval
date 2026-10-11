"""One controller-owned additional input, without a scientific/transport default.

No model client or worker mount is created here. A transport must supply a
verified pre-request fence and a same-session send callback. Unknown sends are
never replayed. The ledger is private Run evidence, not a public export format.
"""
import json
import os
from pathlib import Path
import time
import uuid

from . import ownership, util

GROUP_BOUNDARY = 'response_tool_group_pause_v1'
OBSERVATION_POLICY = 'resumption_first_change_v1'
OBSERVED_KIND = 'observed_presentation_v1'


def observed(contract):
    return contract.get('observation_policy') == OBSERVATION_POLICY


def no_additional(contract):
    return contract.get('presentation_mode') == 'full_initial_no_additional'


def checkpoint_labels(contract):
    return (('initial','before-resume','after-resume-first-change','final') if observed(contract['boundary_contract'])
            else ('initial','before-additional','after-additional','final'))


def prepare_presentation(condition, plan, source):
    """The observed control uses the exact existing full explore prompt bytes."""
    from . import profiles
    initial, additional, partition = request_partition(condition['migration_request'], plan)
    control = condition['intervention']['method'] == 'explore-observed'
    if control and not observed(plan['boundary_contract']):
        raise ValueError('Observed control requires the explicit resumption observation policy')
    prompt, context = profiles.prepare_prompt({**condition, 'migration_request':
        condition['migration_request'] if control else initial}, source)
    partition = {**partition, 'request_initial_sha256':partition['initial_sha256'],
                 'initial_sha256':util.sha256_bytes(prompt.encode())}
    return prompt, context, None if control else additional, partition


def group_boundary(contract):
    return contract.get('boundary_policy') == GROUP_BOUNDARY


def digest(value):
    return util.sha256_bytes(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())


def split_sections(sections, initial_ids, additional_ids):
    """Partition frozen sections exactly once, preserving their original bytes/order."""
    ids = [s['id'] for s in sections]
    chosen = initial_ids + additional_ids
    if (not initial_ids or not additional_ids or len(set(ids)) != len(ids)
            or len(set(chosen)) != len(chosen) or set(chosen) != set(ids)
            or initial_ids != [i for i in ids if i in initial_ids]
            or additional_ids != [i for i in ids if i in additional_ids]
            or any(not isinstance(s['text'], str) or not s['text'] for s in sections)):
        raise ValueError('Exact nonempty disjoint ordered section partition required')
    text = {s['id']: s['text'] for s in sections}
    initial, additional = ''.join(text[i] for i in initial_ids), ''.join(text[i] for i in additional_ids)
    return (initial, additional, {'sections_sha256': digest(sections),
            'initial_ids': initial_ids, 'additional_ids': additional_ids,
            'initial_sha256': util.sha256_bytes(initial.encode()), 'additional_sha256': util.sha256_bytes(additional.encode())})


def request_partition(request, plan):
    if (set(plan) != {'kind','request_sha256','sections','initial_ids','additional_ids','boundary_contract'}
            or plan['kind'] != 'staged_request_partition_v1'
            or plan['request_sha256'] != util.sha256_bytes(request.encode())
            or ''.join(section['text'] for section in plan['sections']) != request
            or plan['boundary_contract'].get('transport') != 'opencode-server-response-barrier-v1'
            or (set(plan['boundary_contract']) != {'transport','snapshot_policy','snapshot_policy_sha256'}
                and (set(plan['boundary_contract']) not in (
                        {'transport','snapshot_policy','snapshot_policy_sha256','boundary_policy'},
                        {'transport','snapshot_policy','snapshot_policy_sha256','boundary_policy','observation_policy'})
                     or not group_boundary(plan['boundary_contract'])))
            or ('observation_policy' in plan['boundary_contract'] and not observed(plan['boundary_contract']))
            or digest(plan['boundary_contract']['snapshot_policy']) != plan['boundary_contract']['snapshot_policy_sha256']):
        raise ValueError('Frozen original request, exhaustive sections and explicit transport/snapshot policy required')
    validate_snapshot_policy(plan['boundary_contract']['snapshot_policy'])
    return split_sections(plan['sections'], plan['initial_ids'], plan['additional_ids'])


def validate_snapshot_policy(policy):
    if (set(policy) != {'suffixes', 'excluded_directories'}
            or not isinstance(policy['suffixes'],list) or not policy['suffixes']
            or not isinstance(policy['excluded_directories'],list)
            or any(not isinstance(s,str) or not s.startswith('.') or '/' in s or '\\' in s for s in policy['suffixes'])
            or any(not isinstance(s,str) or not s or s in ('.','..') or '/' in s or '\\' in s for s in policy['excluded_directories'])):
        raise ValueError('Explicit implementation snapshot policy required')


def snapshot(workspace, policy):
    """Hash only the caller-frozen implementation scope; no collection or scoring."""
    validate_snapshot_policy(policy)
    util.reject_links(Path(workspace))
    root = Path(workspace).resolve(strict=True)
    util.reject_links(root)
    files = {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        if (path.is_file() and path.suffix in policy['suffixes']
                and not set(relative.parts[:-1]) & set(policy['excluded_directories'])):
            files[relative.as_posix()] = util.sha256_file(path)
    return {'workspace': str(root), 'policy_sha256': digest(policy), 'files': files, 'sha256': digest(files)}


def create(directory, *, run_id, run_instance_id, task, condition, workspace, worker_roots,
           initial_prompt, additional_prompt, budget_seconds, boundary_contract, partition, artifact_collection_policy=None):
    directory, workspace = Path(directory).resolve(), Path(workspace).resolve(strict=True)
    roots = [workspace, *(Path(p).resolve(strict=True) for p in worker_roots)]
    control = condition == 'explore-observed'
    if control and (not observed(boundary_contract) or additional_prompt is not None):
        raise ValueError('Control requires explicit observation policy and zero additional input')
    if any(directory.is_relative_to(p) or p.is_relative_to(directory) for p in roots):
        raise ValueError('Additional input must be outside every worker-readable mount')
    if (type(budget_seconds) is not int or budget_seconds <= 0 or not boundary_contract.get('snapshot_policy_sha256')
            or partition.get('initial_sha256') != util.sha256_bytes(initial_prompt.encode())
            or (not control and (not additional_prompt
                or partition.get('additional_sha256') != util.sha256_bytes(additional_prompt.encode())
                or additional_prompt in initial_prompt))
            or not initial_prompt
            or not all(isinstance(v, str) and v for v in (run_id, run_instance_id, task, condition))):
        raise ValueError('Explicit identity, partition, boundary and continuous budget required')
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name, text in [('initial.txt', initial_prompt)] + ([] if control else [('additional.txt', additional_prompt)]):
        with os.fdopen(os.open(directory/name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w', encoding='utf-8', newline='') as stream:
            stream.write(text); stream.flush(); os.fsync(stream.fileno())
    contract = {'schema_version': 1, 'kind': 'one_additional_input_v1', 'run_id': run_id,
        'run_instance_id': run_instance_id, 'task': task, 'condition': condition, 'workspace': str(workspace),
        'worker_roots': [str(p) for p in roots], 'budget_seconds': budget_seconds,
        'boundary_contract': boundary_contract, 'partition': partition, 'delivery_id': uuid.uuid4().hex,
        'artifact_collection_policy':util.resolve_collection_policy(artifact_collection_policy),
        'initial_sha256': util.sha256_file(directory/'initial.txt'),
        'additional_sha256': None if control else util.sha256_file(directory/'additional.txt')}
    if observed(boundary_contract):
        contract.update(kind=OBSERVED_KIND, presentation_mode='full_initial_no_additional' if control else 'staged_once',
                        planned_additional_count=0 if control else 1)
    util.write_new_json(directory/'contract.json', contract)
    controller = Controller(directory)
    with ownership.lease(directory): controller._append('planned', {})
    return controller


class Controller:
    def __init__(self, directory, *, clock=time.time, monotonic=time.monotonic):
        self.root = Path(directory).resolve(strict=True)
        self.clock = clock
        self.monotonic = monotonic
        self.contract = util.read_json(self.root/'contract.json')
        self.contract_sha256 = util.sha256_file(self.root/'contract.json')
        if observed(self.contract['boundary_contract']) and (self.contract.get('kind')!=OBSERVED_KIND
                or self.contract.get('presentation_mode') not in ('staged_once','full_initial_no_additional')
                or self.contract.get('planned_additional_count') != (0 if no_additional(self.contract) else 1)):
            raise ValueError('Explicit observed presentation mode and count required')
        if no_additional(self.contract) and ((self.root/'additional.txt').exists()
                or self.contract.get('kind') != OBSERVED_KIND or self.contract['additional_sha256'] is not None):
            raise ValueError('Control must not contain a later prompt')
        for name in (('initial',) if no_additional(self.contract) else ('initial', 'additional')):
            if util.sha256_file(self.root/(name+'.txt')) != self.contract[name+'_sha256']:
                raise ValueError('Immutable prompt identity changed')
        if any(self.root.is_relative_to(Path(p).resolve()) or Path(p).resolve().is_relative_to(self.root)
               for p in self.contract['worker_roots']):
            raise ValueError('Controller input became worker-readable')

    def bind_run(self, root):
        """Attach a private evidence reference before any initial dispatch."""
        root = Path(root).resolve(strict=True)
        with ownership.lease(root):
            manifest = util.read_json(root/'manifest.json')
            if (manifest.get('started_at') or 'staged_input' in manifest
                    or (manifest['run_id'], manifest['run_instance_id'], manifest['task_id'], manifest['condition_id'])
                    != tuple(self.contract[k] for k in ('run_id', 'run_instance_id', 'task', 'condition'))
                    or str(root/'workspace') != self.contract['workspace']
                    or util.sha256_file(root/'inputs/prompt.txt') != self.contract['initial_sha256']):
                raise ValueError('Fresh Run, workspace and initial input binding required')
            required = {str(root/p) for p in ('inputs', 'workspace', 'state')}
            if not required <= set(self.contract['worker_roots']):
                raise ValueError('All existing worker mounts must be audited')
            self.audit_mounts()
            manifest['staged_input'] = {'contract_path': str(self.root/'contract.json'),
                                        'contract_sha256': self.contract_sha256}
            util.write_json_atomic(root/'manifest.json', manifest)

    def audit_mounts(self):
        future = None if no_additional(self.contract) else (self.root/'additional.txt').read_bytes().decode('utf-8')
        def strings(value):
            if isinstance(value, str): yield value
            elif isinstance(value, dict):
                for child in value.values(): yield from strings(child)
            elif isinstance(value, list):
                for child in value: yield from strings(child)
        for name in self.contract['worker_roots']:
            root = Path(name); util.reject_links(root)
            for path in root.rglob('*'):
                if not path.is_file(): continue
                raw = path.read_bytes()
                if future and future.encode() in raw: raise ValueError('Future input leaked into worker mount')
                if path.suffix == '.json':
                    try: data = json.loads(raw)
                    except (ValueError, UnicodeError): continue
                    if future and any(future in value for value in strings(data)):
                        raise ValueError('Future input leaked through serialized worker metadata')

    def events(self):
        path = self.root/'events.jsonl'
        raw = path.read_bytes() if path.exists() else b''
        if raw and not raw.endswith(b'\n'):
            raise ValueError('Interrupted staged-input ledger; hold, never replay')
        events = util.read_lines(path)
        previous = None
        for index, event in enumerate(events):
            if (event.get('sequence') != index or event.get('previous_sha256') != previous
                    or event.get('contract_sha256') != self.contract_sha256):
                raise ValueError('Staged-input ledger binding changed')
            previous = digest(event)
        return events

    def _append(self, kind, data):
        events = self.events()
        stamp = self.clock()
        steady = self.monotonic()
        if events and (stamp < events[-1]['at_unix'] or steady < events[-1]['at_monotonic']):
            raise ValueError('Controller clock regressed; hold')
        event = {'sequence': len(events), 'previous_sha256': digest(events[-1]) if events else None,
            'contract_sha256': self.contract_sha256, 'kind': kind, 'at_unix': stamp, 'at_monotonic': steady, **data}
        util.append_line(self.root/'events.jsonl', event)
        return event

    def remaining(self):
        events = self.events()
        starts = [e for e in events if e['kind'] == 'started']
        if len(starts) != 1 or self.clock() < events[-1]['at_unix'] or self.monotonic() < events[-1]['at_monotonic']:
            raise ValueError('One durable time origin and nonregressing clock required')
        return max(0., min(starts[0]['deadline_unix'] - self.clock(), starts[0]['deadline_monotonic'] - self.monotonic()))

    def start(self):
        with ownership.lease(self.root):
            if [e['kind'] for e in self.events()] != ['planned']:
                raise ValueError('Initial dispatch/time origin cannot be restarted')
            self._append('started', {'deadline_unix': self.clock()+self.contract['budget_seconds'],
                                    'deadline_monotonic': self.monotonic()+self.contract['budget_seconds']})

    def bind_session(self, native_session_id):
        with ownership.lease(self.root):
            events = self.events()
            if (not native_session_id or any(e['kind'] in ('native_session', 'terminal') for e in events)
                    or not self.remaining()):
                raise ValueError('Native session must be bound once within the original budget')
            self._append('native_session', {'native_session_id': native_session_id})

    def unknown_boundary(self, reason, *, phase=None):
        with ownership.lease(self.root):
            self._append('boundary_unknown', {'reason': reason,'phase':phase})

    def checkpoint(self,label,binding):
        from . import staged_artifacts
        with ownership.lease(self.root):
            receipt=staged_artifacts.capture(self,label,binding)
            self._append('checkpoint',{'label':label,'receipt_sha256':util.sha256_file(self.root/'checkpoints'/label/'receipt.json'),
                'archive':receipt['archive'],'reason':receipt['reason']})
            if receipt['archive'] is None and not group_boundary(self.contract['boundary_contract']):
                raise ValueError('Private checkpoint unavailable: '+label)
            return receipt

    def unchanged_boundary(self, before, after, fence):
        with ownership.lease(self.root):
            if before != after: raise ValueError('Read-only boundary changed implementation')
            self._append('unchanged_boundary', {'before':before,'after':after,'fence':fence})

    def boundary(self, tool, before, after, fence):
        """Consume transport-owned terminal/quiescence proof, never infer from build success."""
        with ownership.lease(self.root):
            events = self.events()
            session = next((e['native_session_id'] for e in events if e['kind'] == 'native_session'), None)
            grouped = group_boundary(self.contract['boundary_contract'])
            tools = tool if grouped else [tool]
            if (any(e['kind'] in ('boundary_observed', 'boundary_unknown', 'send_intent', 'terminal') for e in events)
                    or (self.root/'stop-request.json').exists() or not self.remaining()):
                raise ValueError('Boundary closed, stopped or out of budget')
            if (not session or not tools or any(t.get('sessionID') != session
                    or t.get('state', {}).get('status') not in ('completed', 'error')
                    or not t.get('callID') or not t.get('messageID') for t in tools)
                    or fence.get('native_session_id') != session or fence.get('run_instance_id') != self.contract['run_instance_id']
                    or fence.get('next_request_blocked') is not True or fence.get('active_tools') != []
                    or (not grouped and fence.get('children_exited') is not True) or not fence.get('barrier_id')):
                self._append('boundary_unknown', {'reason': 'terminal_or_fence_unverified'})
                raise ValueError('Owned terminal tool and pre-request quiescence fence required')
            if self.contract['boundary_contract'].get('transport') == 'opencode-server-response-barrier-v1':
                if (fence.get('tool_call_ids') != [t['callID'] for t in tools]
                        or fence.get('completed_tool_call_ids') != [t['callID'] for t in tools]
                        or len({t['callID'] for t in tools}) != len(tools)
                        or not fence.get('request_id') or not fence.get('request_sha256')):
                    self._append('boundary_unknown', {'reason': 'response_tool_identity_unverified'})
                    raise ValueError('Single attributed response tool required')
            if grouped and (fence.get('worker_pause',{}).get('paused') is not True
                    or fence['worker_pause'].get('run_instance_id') != self.contract['run_instance_id']):
                raise ValueError('Owned worker pause required for response-group observation')
            if (before['workspace'] != self.contract['workspace'] or after['workspace'] != before['workspace']
                    or before['policy_sha256'] != after['policy_sha256']
                    or before['policy_sha256'] != self.contract['boundary_contract']['snapshot_policy_sha256']
                    or any(s['sha256'] != digest(s['files']) for s in (before, after))):
                self._append('boundary_unknown', {'reason': 'snapshot_binding_mismatch'})
                raise ValueError('Implementation snapshot binding mismatch')
            changed = sorted(k for k in before['files'].keys() | after['files'].keys() if before['files'].get(k) != after['files'].get(k))
            if not changed: return False
            details = ({'tools':[{'tool_call_id':t['callID'],'message_id':t['messageID'],
                        'tool_status':t['state']['status']} for t in tools], 'attribution':'response_tool_group'}
                       if grouped else {'tool_call_id':tool['callID'],'message_id':tool['messageID'],
                                        'tool_status':tool['state']['status']})
            self._append('boundary_observed', {**details, 'native_session_id': session, 'fence': fence,
                'before': before, 'after': after, 'changed_files': changed})
            return True

    def dispatch(self, send):
        """send(payload, remaining_seconds) must be bounded; any uncertain call holds.

        It must submit to the existing native session, not start a new Run. The
        callback may arm the gateway before sending; its ACK is not provider
        delivery. A process crash after intent cannot cause an automatic retry.
        """
        with ownership.lease(self.root):
            if no_additional(self.contract): raise ValueError('Control cannot dispatch an additional message')
            events = self.events()
            boundaries = [e for e in events if e['kind'] == 'boundary_observed']
            if len(boundaries) != 1 or any(e['kind'] in ('send_intent', 'terminal') for e in events):
                raise ValueError('No replay: one reached boundary and one additional send only')
            if (self.root/'stop-request.json').exists() or not self.remaining():
                raise ValueError('Stopped or expired before additional input')
            if self.contract['boundary_contract'].get('transport') == 'opencode-server-response-barrier-v1':
                checkpoints=[e for e in events if e['kind']=='checkpoint' and e['label']==checkpoint_labels(self.contract)[1]
                    and (e['archive'] or group_boundary(self.contract['boundary_contract']))]
                if len(checkpoints)!=1:raise ValueError('Restorable pre-input checkpoint required before dispatch')
            payload = {k: self.contract[k] for k in ('run_id', 'run_instance_id', 'workspace', 'delivery_id')}
            payload.update(native_session_id=boundaries[0]['native_session_id'],
                contract_sha256=self.contract_sha256, additional_sha256=self.contract['additional_sha256'],
                barrier_id=boundaries[0]['fence']['barrier_id'],
                text=(self.root/'additional.txt').read_bytes().decode('utf-8'))
            if util.sha256_bytes(payload['text'].encode()) != payload['additional_sha256']:
                raise ValueError('Additional input changed before dispatch')
            self._append('send_intent', {k: v for k, v in payload.items() if k != 'text'})
            if (self.root/'stop-request.json').exists() or not self.remaining():
                self._append('known_no_send', {'reason': 'stopped_or_expired_after_intent'})
                return self.status()
            try:
                ack = send(payload, self.remaining())
                self._append('send_returned', {'delivery_id': payload['delivery_id']})
                if (any(ack.get(k) != payload[k] for k in ('run_instance_id', 'workspace', 'native_session_id', 'delivery_id'))
                        or not ack.get('native_message_id')):
                    raise ValueError('Native acknowledgement identity mismatch')
                self._append('acknowledged', {'delivery_id': payload['delivery_id'],
                    'native_session_id': payload['native_session_id'], 'native_message_id': ack['native_message_id']})
            except Exception as error:
                self._append('delivery_unknown', {'error_type': type(error).__name__})
            return self.status()

    def resume_action(self):
        """Durable action before releasing the fence; never a fabricated native ACK."""
        with ownership.lease(self.root):
            events = self.events()
            boundaries = [e for e in events if e['kind']=='boundary_observed']
            checkpoints = [e for e in events if e['kind']=='checkpoint' and e['label']=='before-resume']
            if (not observed(self.contract['boundary_contract']) or len(boundaries)!=1 or len(checkpoints)!=1
                    or any(e['kind'] in ('resumption_action','terminal') for e in events)
                    or (self.root/'stop-request.json').exists() or not self.remaining()):
                raise ValueError('One live observed boundary and pre-resume checkpoint required')
            control = no_additional(self.contract)
            acks = [e for e in events if e['kind']=='acknowledged']
            if (control and any(e['kind'] in ('send_intent','acknowledged') for e in events)) or (not control and len(acks)!=1):
                raise ValueError('Presentation action does not match native input evidence')
            action = dict(run_id=self.contract['run_id'], run_instance_id=self.contract['run_instance_id'],
                contract_sha256=self.contract_sha256, native_session_id=boundaries[0]['native_session_id'],
                barrier_id=boundaries[0]['fence']['barrier_id'], request_id=boundaries[0]['fence']['request_id'],
                checkpoint_receipt_sha256=checkpoints[0]['receipt_sha256'],
                action='no_input_by_design' if control else 'additional_input_acknowledged',
                native_message_id=None if control else acks[0]['native_message_id'])
            self._append('resumption_action', {'action':action,'action_sha256':digest(action)})
            return action

    def stop(self, reason):
        # Independent of a blocked send lock. Native/gateway shutdown remains
        # the existing runtime owner's job; this marker is not a stop ACK.
        try: util.write_new_json(self.root/'stop-request.json', {'reason': reason, 'at_unix': self.clock()})
        except FileExistsError: pass
        try: return self.status()
        except (ValueError, OSError):
            return {'stop_requested': True, 'delivery_state': 'unknown', 'retry_allowed': False}

    def finish(self, reason, *, stop_confirmed):
        with ownership.lease(self.root):
            if any(e['kind'] == 'terminal' for e in self.events()):
                raise ValueError('Terminal evidence is immutable')
            from . import staged_artifacts
            self._append('terminal', {'reason': reason, 'stop_confirmed': stop_confirmed is True,
                'checkpoints':staged_artifacts.summary(self,reason)})
        return self.status()

    def status(self):
        events = self.events(); kinds = {e['kind'] for e in events}
        terminal = next((e for e in events if e['kind'] == 'terminal'), None)
        start = next((e for e in events if e['kind'] == 'started'), None)
        return {'contract_sha256': self.contract_sha256, 'run_id': self.contract['run_id'],
            'run_instance_id': self.contract['run_instance_id'], 'delivery_id': self.contract['delivery_id'],
            'boundary_reached': 'boundary_observed' in kinds,
            'delivery_state': 'not_applicable' if no_additional(self.contract) else 'acknowledged' if 'acknowledged' in kinds else 'unknown' if 'send_intent' in kinds and 'known_no_send' not in kinds else 'not_sent',
            'stop_requested': (self.root/'stop-request.json').exists(), 'terminal': terminal,
            'retry_allowed': False, 'deadline_unix': start['deadline_unix'] if start else None,
            'elapsed_seconds': terminal['at_monotonic']-start['at_monotonic'] if terminal and start else None}
