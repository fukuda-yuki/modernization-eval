"""Explicit Windows-only, no-provider process failure probes (not CI discovery)."""
import argparse
import os
from pathlib import Path
import time
from outer.fineract import supervisor,lifecycle
from outer.harness import util


def wait_file(path,seconds=15):
    end=time.monotonic()+seconds
    while not path.exists():
        if time.monotonic()>end:raise TimeoutError(str(path.name))
        time.sleep(.1)
    return util.read_json(path)


def run(root):
    root.mkdir(parents=True,exist_ok=False);rows=[]
    os.environ['OPENCODE_GO_API_KEY']='non-secret-process-fixture-never-used-for-authentication'
    for scenario in ('normal','exception','forced','owner_lost','nonterminal'):
        record=root/scenario
        supervisor.launch_probe(record,seconds=10 if scenario=='owner_lost' else 60 if scenario=='forced' else 1,
                                outcome=scenario if scenario in ('exception','nonterminal') else 'normal')
        folder=record/'supervisor';child=wait_file(folder/'child.json')['identity']
        if scenario=='forced':lifecycle.terminate(child)
        if scenario=='owner_lost':
            owner=util.read_json(folder/'owner.json')['identity'];lifecycle.terminate(owner)
            observed=supervisor.status(record)
            assert observed['classification']=='owner_lost_exit_unknown' and observed['exit_code'] is None
            wait_file(record/'probe-result.json',20)
            assert not (folder/'exit.json').exists()
            result=observed
        else:
            result=wait_file(folder/'exit.json')
            expected={'normal':'normal','exception':'controller_exception','forced':'terminated_or_nonzero_exit','nonterminal':'nonterminal_exit'}[scenario]
            assert result['classification']==expected,(scenario,result)
            assert result['exit_code']==(0 if scenario in ('normal','nonterminal') else 143 if scenario=='forced' else 1)
            assert result['os_exit_time'] and result['exit_observed_at']
        rows.append({'scenario':scenario,'result':result})
        if (record/'probe-result.json').exists():assert util.read_json(record/'probe-result.json')['provider_credential_in_environment'] is False
    util.write_new_json(root/'results.json',{'scenarios':rows,'model_calls':0,'scorer_calls':0})
    return {'passed':len(rows),'model_calls':0}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True)
    print(run(p.parse_args().output.resolve()))
