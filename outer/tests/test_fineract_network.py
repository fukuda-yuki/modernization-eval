import unittest
from outer.fineract.network import choose_subnet


class FineractNetworkTests(unittest.TestCase):
    def test_skips_overlapping_allocations_and_other_ip_families(self):
        self.assertEqual(choose_subnet(['10.239.0.0/24','10.239.1.0/28','172.17.0.0/16','fd00::/64']),
                         '10.239.1.16/28')

    def test_exhaustion_fails_without_recycling(self):
        with self.assertRaises(ValueError):choose_subnet(['10.239.0.0/16'])
        with self.assertRaises(ValueError):choose_subnet([],pool='8.8.8.0/24')
