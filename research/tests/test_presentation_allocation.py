"""Allocation is planned order, not measured wall-time or authorization."""
import copy
from collections import Counter
import unittest
from research import presentation_allocation as allocation


class AllocationTests(unittest.TestCase):
    def test_exact_blocks_strata_counts_reproducibility_and_distinct_run_ids(self):
        tasks=['MusicA','MusicB','EducationC','EducationD']
        spec,assignments=allocation.generate('synthetic-seed-1',tasks)
        self.assertEqual((spec,assignments),allocation.generate('synthetic-seed-1',list(reversed(tasks))))
        allocation.validate(spec,assignments)
        rows=spec['rows'];self.assertEqual(len(rows),100)
        counts=Counter((r['task'],r['condition']) for r in rows)
        for task in tasks:
            self.assertEqual(counts[task,'staged-explore'],20)
            self.assertEqual(counts[task,'explore-observed'],5)
        for stratum in range(1,6):
            control={r['block'] for r in rows if r['stratum']==stratum and r['condition']=='explore-observed'}
            self.assertEqual(len(control),1)
        for block in range(1,26):
            blockrows=[r for r in rows if r['block']==block]
            self.assertEqual({r['task'] for r in blockrows},set(tasks))
            self.assertEqual(len({r['condition'] for r in blockrows}),1)
        self.assertEqual(len({a['cases'][0]['run_id'] for a in assignments}),100)
        self.assertNotEqual(spec,allocation.generate('different',tasks)[0])
        self.assertEqual(spec['sha256'],'abb81e44ad6b480c4e722d60673f025b87703b5b44aa63afdb25e6020cdd4a12')

    def test_reordered_or_mutated_allocation_requires_new_hash(self):
        spec,assignments=allocation.generate('toy',['a','b','c','d'])
        for key,value in [('seed','changed'),('algorithm','alias'),('sha256','0'*64)]:
            with self.assertRaises(ValueError):allocation.validate({**spec,key:value},assignments)
        changed=copy.deepcopy(assignments);changed[0]['cases'][0]['condition']='alias'
        with self.assertRaises(ValueError):allocation.validate(spec,changed)
        with self.assertRaises(ValueError):allocation.validate(spec,list(reversed(assignments)))
