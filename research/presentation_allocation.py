"""Reproducible blocked 80/20 allocation, independent of outcomes and runtime.

This constructs a proposed schedule only, never authorization or dispatch.
SHA-256 ranking avoids Python PRNG/version dependence. Clock-time balance is
not promised. Each of five strata contains five blocks of the same four tasks.
"""
import hashlib
from outer.harness import run, staged_input

ALGORITHM = 'sha256_rank_five_strata_25_blocks_v1'


def generate(seed, tasks):
    if (not isinstance(seed, str) or not seed or len(tasks)!=4 or len(set(tasks))!=4
            or any(not isinstance(t,str) or not t for t in tasks)):
        raise ValueError('Explicit seed and exactly four distinct task IDs required')
    tasks = sorted(tasks)
    def rank(domain, value):
        return hashlib.sha256((seed+'\0'+domain+'\0'+str(value)).encode()).hexdigest(), str(value)
    rows=[]; assignments=[]; attempts={}
    for stratum in range(1,6):
        blocks=list(range((stratum-1)*5+1,stratum*5+1))
        control=min(blocks,key=lambda b:rank('control:'+str(stratum),b))
        for block in blocks:
            condition='explore-observed' if block==control else 'staged-explore'
            for task in sorted(tasks,key=lambda t:rank('task:'+str(block),t)):
                position=len(rows)+1
                attempts[task,condition]=attempts.get((task,condition),0)+1
                attempt=attempts[task,condition]
                rows.append(dict(position=position,stratum=stratum,block=block,task=task,condition=condition))
                assignments.append(dict(pair=position,cases=[dict(task=task,condition=condition,pair=position,
                    slot=1,attempt=attempt,run_id=run.run_id_for(task,condition,attempt))]))
    allocation=dict(algorithm=ALGORITHM,seed=seed,tasks=tasks,rows=rows)
    allocation['sha256']=staged_input.digest(allocation)
    return allocation,assignments


def validate(allocation, assignments):
    expected, cases=generate(allocation.get('seed'),allocation.get('tasks',[]))
    if allocation!=expected or assignments!=cases:
        raise ValueError('Frozen block allocation/hash/order/identity mismatch')
