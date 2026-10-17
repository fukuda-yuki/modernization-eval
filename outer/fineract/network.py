"""Small explicit internal subnets, preserving historical Docker networks."""
import ipaddress
import json
from outer.harness import runtime

POOL='10.239.0.0/16'


def choose_subnet(occupied,pool=POOL):
    pool=ipaddress.ip_network(pool)
    if pool.version!=4 or not pool.is_private or pool.prefixlen>28:
        raise ValueError('Private IPv4 pool containing /28 subnets required')
    used=[ipaddress.ip_network(item) for item in occupied]
    for candidate in pool.subnets(new_prefix=28):
        if not any(candidate.overlaps(item) for item in used if item.version==4):
            return str(candidate)
    raise ValueError('Explicit test network pool exhausted; no networks deleted')


def create_internal(name,labels):
    ids=runtime.docker('network','ls','-q').stdout.split()
    networks=json.loads(runtime.docker('network','inspect',*ids).stdout) if ids else []
    occupied=[entry['Subnet'] for net in networks for entry in (net.get('IPAM',{}).get('Config') or [])
              if entry.get('Subnet')]
    subnet=choose_subnet(occupied)
    # Docker atomically rejects an overlap if another creator races this check.
    # Never remove or reconfigure an existing network to make space.
    runtime.docker('network','create','--internal','--subnet',subnet,*labels,name)
    return {'subnet':subnet,'pool':POOL,'internal':True}
