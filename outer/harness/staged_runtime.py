"""Owned Docker adapter for fixed OpenCode server + gateway response boundary.

The runner is selected only by an explicit staged condition/contract. Requests
use docker-exec stdin: later text never enters argv, environment or worker files.
No retry is performed after any initial or additional dispatch intent.
"""
import json
from contextlib import contextmanager, nullcontext
from pathlib import Path
import subprocess
import threading
import time

from . import live_usage, staged_input, util
from .security import child_environment

PORT = 40961


class ObservationPending(RuntimeError):
    """Retry only the tree observation under the same held response barrier."""

API = """import json,sys,urllib.request
v=json.load(sys.stdin)
r=urllib.request.Request(v['url'], data=json.dumps(v['body']).encode() if v['body'] is not None else None,
 headers={'Content-Type':'application/json','x-opencode-directory':'/workspace'})
with urllib.request.urlopen(r,timeout=v['timeout']) as x: sys.stdout.buffer.write(x.read())
"""
CHILDREN = """import json,os,pathlib
rows=[]
for p in pathlib.Path('/proc').glob('[0-9]*/stat'):
 try:
  s=p.read_text();tail=s[s.rfind(')')+2:].split();rows.append((int(p.parent.name),int(tail[1]),tail[0],int(tail[19])))
 except (OSError,ValueError): pass
parents={1};found=set()
while True:
 new={pid for pid,ppid,state,start in rows if ppid in parents and pid!=os.getpid() and state!='Z'}-found
 if not new:break
 found|=new;parents|=new
print(json.dumps([dict(pid=pid,parent_pid=ppid,state=state,start_ticks=start,clock_ticks=os.sysconf('SC_CLK_TCK'))
 for pid,ppid,state,start in sorted(rows) if pid in found]))
"""


def validate_authorization(repo, root, manifest, controller, condition):
    """Direct Run entrypoints cannot bypass the exact approved campaign plan.

    This is the existing local receipt trust boundary, not a new authentication
    mechanism. A partition alone is an implementation choice, never permission
    to dispatch a provider request.
    """
    from research import campaign_initialization, live_pilot, pair_execution
    acquisition = manifest.get('acquisition') or {}
    if not acquisition.get('epoch') or not acquisition.get('campaign_config'):
        raise ValueError('Staged activation requires the approved campaign acquisition binding')
    epoch = campaign_initialization.verify_epoch(live_pilot.checked(acquisition['epoch']), Path(repo))
    if acquisition['campaign_config'] != epoch['campaign_config'] or epoch.get('cases_per_assignment') != 1:
        raise ValueError('Staged campaign configuration differs from approved epoch')
    binding = manifest.get('assignment') or {}
    matches = [c for a in epoch['assignments'] for c in a['cases']
               if c['run_id'] == manifest['run_id'] and c['run_instance_id'] == manifest['run_instance_id']]
    if (len(matches) != 1 or any(binding.get(k) != v for k,v in matches[0].items())
            or Path(root).resolve() != Path(epoch['batch']).resolve()/('pair-'+str(matches[0]['pair']))/manifest['run_id']
            or condition['runtime']['id'] != epoch['runtime_by_task'][manifest['task_id']]
            or condition['runtime']['model_id'] != epoch['settings']['model_id']):
        raise ValueError('Staged Run is not the approved epoch assignment')
    dispatch=pair_execution.state(Path(root).parent/'_control/pair-journal.jsonl')['dispatch'].get(manifest['run_id'])
    config=util.read_json(live_pilot.checked(epoch['campaign_config']))
    if (not dispatch or any(dispatch.get(k)!=manifest[k] for k in ('run_id','run_instance_id'))
            or dispatch.get('input_sha256')!=manifest.get('prompt_sha256')
            or dispatch.get('condition_sha256')!=manifest.get('condition_sha256')
            or (Path(config['root'])/'STOP').exists()):
        raise ValueError('Staged activation requires an admitted dispatch and an open campaign')
    partition_plan = util.read_json(live_pilot.checked(epoch['staged_inputs'][manifest['task_id']]))
    prompt, _, additional, partition = staged_input.prepare_presentation(condition, partition_plan, Path(root)/'inputs/legacy-source')
    if (controller.contract['partition'] != partition
            or (staged_input.observed(partition_plan['boundary_contract']) and controller.contract.get('presentation_mode')
                != ('full_initial_no_additional' if additional is None else 'staged_once'))
            or controller.contract['initial_sha256'] != util.sha256_bytes(prompt.encode())
            or controller.contract['additional_sha256'] != (None if additional is None else util.sha256_bytes(additional.encode()))
            or controller.contract['boundary_contract'] != partition_plan['boundary_contract']):
        raise ValueError('Staged transport/partition differs from the approved plan')


class DockerTransport:
    def __init__(self, root, state, controller, runtime, model):
        self.root, self.state, self.controller, self.runtime = Path(root), state, controller, runtime
        self.model = model

    def owned(self, role):
        item = self.runtime._owned_container(self.state, role)
        if not item or not item['State']['Running']:
            raise RuntimeError('Owned staged '+role+' is not running')
        if role == 'worker':
            mounts = [m for m in item.get('Mounts', []) if m.get('Destination') == '/workspace']
            if len(mounts) != 1 or Path(mounts[0]['Source']).resolve() != (self.root/'workspace').resolve():
                raise ValueError('Staged workspace ownership mismatch')

    def api(self, role, path, body=None):
        if (self.root/'stop-request.json').exists() or (self.controller.root/'stop-request.json').exists():
            raise RuntimeError('Staged transport stopped')
        self.owned(role)
        timeout = min(10., self.controller.remaining())
        if timeout <= 0: raise TimeoutError('Original Run budget exhausted')
        port = PORT if role == 'worker' else 8080
        payload = {'url':f'http://127.0.0.1:{port}'+path,'body':body,'timeout':timeout}
        result = subprocess.run(['docker','exec','-i',self.state[role],'python3','-c',API],
            input=json.dumps(payload),text=True,capture_output=True,env=child_environment(),timeout=timeout+.5)
        if result.returncode: raise RuntimeError('Owned staged API call failed')
        return json.loads(result.stdout)

    def barrier(self):
        return self.api('gateway','/control/staged-barrier',{'action':'inspect',
            'run_id':self.state['run_id'],'run_instance_id':self.state['run_instance_id']})

    def release(self, barrier_id, native_message_id=None, capture_complete=False, resume_action=None):
        return self.api('gateway','/control/staged-barrier',{'action':'release','barrier_id':barrier_id,
            'run_id':self.state['run_id'],'run_instance_id':self.state['run_instance_id'],
            'native_message_id':native_message_id,'capture_complete':capture_complete,
            **({'resume_action':resume_action} if resume_action is not None else {})})

    def children(self):
        self.owned('worker')
        result = self.runtime.docker('exec',self.state['worker'],'python3','-c',CHILDREN,
            timeout=max(.1,min(5.,self.controller.remaining())))
        return json.loads(result.stdout)

    @contextmanager
    def frozen(self):
        """Freeze only this owned worker, including surviving tool descendants.

        The gateway's response fence stays held. The original Run clock keeps
        running, and the native server resumes before any additional API call.
        """
        children = self.children()
        self.owned('worker')
        paused = False
        resume_done=threading.Event();resume_lock=threading.Lock();release={};watchdog=None
        def resume(reason):
            with resume_lock:
                if release:return
                item=self.runtime._owned_container(self.state,'worker')
                if item and item['State'].get('Paused'):
                    self.runtime.docker('unpause',self.state['worker'],timeout=5)
                    item=self.runtime._owned_container(self.state,'worker')
                    if item and item['State'].get('Paused'):raise RuntimeError('Owned worker resume unconfirmed')
                release.update(reason=reason,released_at_unix=self.controller.clock(),
                               released_at_monotonic=self.controller.monotonic(),paused=False)
        def limit_pause(seconds):
            if not resume_done.wait(seconds):
                try:resume('pause_limit')
                except Exception:pass  # finally retries; failure is never a resume ACK.
        try:
            self.runtime.docker('pause',self.state['worker'],timeout=max(.1,min(5.,self.controller.remaining())))
            paused = True
            item = self.runtime._owned_container(self.state,'worker')
            if not item or item['State'].get('Paused') is not True:
                raise RuntimeError('Owned worker pause unconfirmed')
            proof = {'worker':self.state['worker'],'run_instance_id':self.state['run_instance_id'],
                     'paused':True,'children_observed':children,'at_unix':self.controller.clock(),
                     'paused_at_monotonic':self.controller.monotonic(),
                     'reason':'stable_group_observation','pause_limit_seconds':min(5.,self.controller.remaining())}
            self.controller._append('worker_paused',proof)
            watchdog=threading.Thread(target=limit_pause,args=(proof['pause_limit_seconds'],),daemon=True)
            watchdog.start()
            yield proof
        finally:
            # Even a failed pause command can have taken effect before its
            # timeout. Inspect exact ownership before attempting to release it.
            resume_done.set()
            resume('observation_finished')
            if watchdog:watchdog.join(6)
            if paused:self.controller._append('worker_resumed',{'worker':self.state['worker'],
                'run_instance_id':self.state['run_instance_id'],**release})

    def send(self, payload, remaining):
        self.api('gateway','/control/staged-input',{k:payload[k] for k in
            ('run_id','run_instance_id','delivery_id','contract_sha256','additional_sha256','native_session_id','barrier_id')})
        response = self.api('worker','/session/'+payload['native_session_id']+'/message',
            {'noReply':True,'model':{'providerID':'sample2','modelID':self.model},
             'parts':[{'type':'text','text':payload['text']}]})
        session = self.api('worker','/session/'+payload['native_session_id'])
        if session.get('directory') != '/workspace' or response['info'].get('role') != 'user':
            raise ValueError('Native acknowledgement/workspace mismatch')
        return {'run_instance_id':self.state['run_instance_id'],'workspace':str((self.root/'workspace').resolve()),
            'delivery_id':payload['delivery_id'],'native_session_id':response['info']['sessionID'],
            'native_message_id':response['info']['id']}


def observe_boundary(controller, transport, barrier, events, before, *, after_delivery=False):
    """One whole model response's tools must terminate before considering mutation."""
    contract = controller.contract
    observed = staged_input.observed(contract['boundary_contract'])
    labels = staged_input.checkpoint_labels(contract)
    post_phase = 'after_resume' if observed else 'after_additional'
    request_key = 'first_post_resume_request_id' if observed else 'first_additional_request_id'
    sessions = {e['sessionID'] for e in events if 'sessionID' in e}
    session = next(e['native_session_id'] for e in controller.events() if e['kind']=='native_session')
    if not sessions:return before,False
    if sessions != {session}: raise ValueError('Native session changed at boundary')
    parts = [e['part'] for e in events if e.get('type')=='tool_use'
        and e.get('part',{}).get('callID') in barrier['tool_call_ids']
        and e['part'].get('state',{}).get('status') in ('completed','error')]
    ids = [p['callID'] for p in parts]
    if len(ids) != len(set(ids)): raise ValueError('Duplicate native terminal tool event')
    if set(ids) != set(barrier['tool_call_ids']): return before, False
    grouped = staged_input.group_boundary(contract['boundary_contract'])
    if not grouped and transport.children():
        # A completed tool with a surviving child has no attributable stable
        # terminal snapshot. Waiting for it would silently move the boundary.
        controller.unknown_boundary('live_children_at_tool_terminal',phase='after_additional' if after_delivery else 'before_additional')
        raise ValueError('Native child lifetime ambiguous at terminal tool boundary')
    parts = sorted(parts,key=lambda p:barrier['tool_call_ids'].index(p['callID']))
    ids = [p['callID'] for p in parts]
    if grouped:
        controller._append('group_terminal_observed',{'barrier_id':barrier['barrier_id'],
            'request_id':barrier['request_id'],'tools':[{'tool_call_id':p['callID'],
            'message_id':p['messageID'],'native_session_id':p['sessionID'],
            'status':p['state']['status'],'native_time':p['state'].get('time'),
            'time_reason':None if p['state'].get('time') else 'native_time_unavailable'} for p in parts]})
    with transport.frozen() if grouped else nullcontext(None) as pause:
        snapshot_started=controller.clock()
        try:after = staged_input.snapshot(contract['workspace'],contract['boundary_contract']['snapshot_policy'])
        except OSError as error:
            if not grouped:raise
            raise ObservationPending('Implementation tree temporarily unavailable') from error
        if grouped:
            # If the pause limit released the worker during this tree read,
            # re-observe in the same barrier, never infer a stable boundary.
            if controller.monotonic()-pause['paused_at_monotonic']>=pause.get('pause_limit_seconds',5.):
                raise ObservationPending('Implementation observation exceeded owned pause interval')
            controller._append('implementation_observed',{'barrier_id':barrier['barrier_id'],
                'started_at_unix':snapshot_started,'ended_at_unix':controller.clock(),
                'before_sha256':before['sha256'],'after_sha256':after['sha256']})
        changed = before['sha256'] != after['sha256']
        if changed and grouped:
            fence={**barrier,'native_session_id':session,'next_request_blocked':True,'active_tools':[],
                   'completed_tool_call_ids':ids,'worker_pause':pause}
            binding={'fence':fence,'native_session_id':session,'before':before,'after':after,
                     'tools':[{'tool_call_id':p['callID'],'message_id':p['messageID'],
                               'tool_status':p['state']['status']} for p in parts]}
            if after_delivery:
                if barrier.get('phase')!=post_phase or not barrier.get(request_key):
                    raise ValueError('Post-input checkpoint lacks observed request binding')
                controller.checkpoint(labels[2],binding)
            else:
                controller.boundary(parts,before,after,fence)
                controller.checkpoint(labels[1],binding)
    if changed and not grouped and len(parts) != 1:
        # Simultaneous tools cannot be individually attributed by a global file
        # diff. Retain this ambiguity instead of moving to a later boundary.
        controller.unknown_boundary('parallel_mutation',phase='after_additional' if after_delivery else 'before_additional')
        raise ValueError('First mutation attribution ambiguous across parallel tools')
    if changed:
        if after_delivery:
            if barrier.get('phase')!=post_phase or not barrier.get(request_key):
                raise ValueError('Post-input checkpoint lacks observed request binding')
            if not grouped:
                controller.checkpoint('after-additional',{'fence':barrier,'native_session_id':session,
                    'tool_call_id':parts[0]['callID'],'message_id':parts[0]['messageID'],
                    'before':before,'after':after,'tool_status':parts[0]['state']['status']})
            ack=next((e for e in controller.events() if e['kind']=='acknowledged'),None)
            transport.release(barrier['barrier_id'],ack['native_message_id'] if ack else None,capture_complete=True)
            return after,True
        if not grouped:
            controller.boundary(parts[0],before,after,{**barrier,'native_session_id':session,
                'next_request_blocked':True,'active_tools':[],'children_exited':True,
                'completed_tool_call_ids':ids})
            controller.checkpoint('before-additional',{'fence':barrier,'native_session_id':session,
                'tool_call_id':parts[0]['callID'],'message_id':parts[0]['messageID'],'before':before,'after':after})
        if not staged_input.no_additional(contract):controller.dispatch(transport.send)
        acks=[e for e in controller.events() if e['kind']=='acknowledged']
        if not staged_input.no_additional(contract) and len(acks)!=1:
            raise RuntimeError('Additional delivery uncertain; retain and stop without resend')
        if observed:
            action=controller.resume_action()
            transport.release(barrier['barrier_id'],action['native_message_id'],resume_action=action)
        else:transport.release(barrier['barrier_id'],acks[0]['native_message_id'])
    else:
        controller.unchanged_boundary(before,after,barrier)
        ack=next((e for e in controller.events() if e['kind']=='acknowledged'),None)
        if after_delivery:transport.release(barrier['barrier_id'],ack['native_message_id'] if ack else None)
        else:transport.release(barrier['barrier_id'])
    return after, changed


def execute(root, state, controller, condition, runtime, *, transport=None):
    """Called inside the existing Run owner; collection/scoring remain outside."""
    root=Path(root); transport=transport or DockerTransport(root,state,controller,runtime,condition['runtime']['model_id'])
    while True:
        if (root/'stop-request.json').exists(): return None,'operator_stop'
        if not controller.remaining(): return None,'timeout'
        try: transport.api('worker','/global/health');break
        except (OSError,RuntimeError,ValueError): time.sleep(.1)
    session=transport.api('worker','/session',{'title':state['run_id'],'permission':[
        {'permission':p,'pattern':'*','action':'deny'} for p in ('question','plan_enter','plan_exit')]})['id']
    controller.bind_session(session);controller.audit_mounts()
    before=staged_input.snapshot(controller.contract['workspace'],controller.contract['boundary_contract']['snapshot_policy'])
    util.write_new_json(controller.root/'initial-snapshot.json',before)
    controller.checkpoint('initial',{'native_session_id':session,'implementation':before})
    transport.owned('worker')
    command=['docker','exec','-i',state['worker'],'opencode','run','--attach',f'http://127.0.0.1:{PORT}',
        '--dir','/workspace','--session',session,'--pure','--format','json',
        '--model','sample2/'+condition['runtime']['model_id']]
    with (root/'evidence/agent.jsonl').open('xb') as output, (root/'inputs/prompt.txt').open('rb') as prompt:
        client=subprocess.Popen(command,stdin=prompt,stdout=output,stderr=subprocess.STDOUT,env=child_environment())
        try:
            delivered=False;captured=False;handled=set();waiting=set()
            while client.poll() is None:
                if (root/'stop-request.json').exists():
                    controller.stop('operator_stop');return None,'operator_stop'
                if not controller.remaining():
                    if waiting-handled:controller.unknown_boundary('terminal_or_snapshot_unconfirmed_at_deadline',
                        phase=('after_resume' if delivered else 'before_resume') if staged_input.observed(controller.contract['boundary_contract'])
                        else ('after_additional' if delivered else 'before_additional'))
                    return None,'timeout'
                if (root/'usage/raw/failure.jsonl').exists(): return None,'provider_failure'
                if not captured:
                    barrier=transport.barrier()
                    if barrier and barrier['barrier_id'] not in handled:
                        events,errors=live_usage.journal(root/'evidence/agent.jsonl')
                        # A writer may currently be halfway through the last line.
                        if not errors:
                            prior=before
                            try:before,reached=observe_boundary(controller,transport,barrier,events,before,after_delivery=delivered)
                            except ObservationPending:reached=False
                            if delivered:captured=reached
                            else:delivered=reached
                            if reached or before is not prior: handled.add(barrier['barrier_id'])
                            elif barrier['barrier_id'] not in waiting:
                                controller._append('boundary_wait',{'barrier_id':barrier['barrier_id'],
                                    'reason':'terminal_or_snapshot_pending','phase':barrier.get('phase')})
                                waiting.add(barrier['barrier_id'])
                time.sleep(.05)
            return client.returncode,'completed' if client.returncode==0 else 'agent_error'
        finally:
            if staged_input.observed(controller.contract['boundary_contract']):
                # Full unpaginated native history, read-only. A stopped/expired
                # server can make this unavailable; never infer zero sends then.
                try:
                    messages=transport.api('worker','/session/'+session+'/message')
                    util.write_new_json(controller.root/'native-messages.json',messages)
                    controller._append('native_message_inventory',{'native_session_id':session,
                        'sha256':util.sha256_file(controller.root/'native-messages.json'),
                        'client_finished':client.poll() is not None})
                except Exception as error:
                    controller._append('native_message_inventory_unavailable',{'error_type':type(error).__name__})
            # Terminate only this owned docker-exec client. The existing runtime
            # shutdown fences/stops both owned containers, including descendants.
            if client.poll() is None:
                client.terminate()
                try:client.wait(timeout=3)
                except subprocess.TimeoutExpired:client.kill();client.wait(timeout=3)
