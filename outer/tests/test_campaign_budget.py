"""Global request admission must remain bounded under concurrent gateways."""
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from outer.harness.campaign_budget import Budget, BudgetDenied


def process_reserve(args):
    path, i = args
    try:
        Budget(path).reserve(str(i), 'process-'+str(i%4), now=100)
        return True
    except BudgetDenied:
        return False


class CampaignBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'budget.sqlite'
        self.budget = Budget.create(self.path, campaign_id='test', plan_sha256='a'*64,
                                    max_requests=10, max_tokens=100, deadline=200)

    def tearDown(self):
        self.tmp.cleanup()

    def test_atomic_four_gateway_admission(self):
        def attempt(i):
            try:
                Budget(self.path).reserve(str(i), 'run-'+str(i%4), now=100)
                return True
            except BudgetDenied:
                return False
        with ThreadPoolExecutor(max_workers=4) as executor:
            self.assertEqual(sum(executor.map(attempt, range(40))), 10)
        self.assertEqual(self.budget.snapshot()['reserved_requests'], 10)

    def test_partial_usage_is_not_zero_and_finish_is_idempotent(self):
        self.budget.reserve('one', 'run', now=100)
        usage = {'input_tokens': 50, 'output_tokens': None, 'cache_read_tokens': 40}
        self.budget.finish('one', usage, 'transport_error')
        self.budget.finish('one', usage, 'transport_error')
        result = self.budget.snapshot()
        self.assertEqual(result['observed_tokens_lower_bound'], 50)
        self.assertIsNone(result['observed_tokens'])
        self.assertEqual(result['missing_usage_requests'], 1)
        with self.assertRaises(ValueError):
            self.budget.finish('one', {'input_tokens': 60}, 'transport_error')

    def test_separate_processes_share_one_request_limit(self):
        with ProcessPoolExecutor(max_workers=4) as pool:
            admitted = list(pool.map(process_reserve, [(str(self.path),i) for i in range(40)]))
        self.assertEqual(sum(admitted), 10)
        self.assertEqual(self.budget.snapshot()['inflight_requests'], 10)

    def test_token_limit_deadline_and_duplicate_fail_closed(self):
        self.budget.reserve('one', 'run', now=100)
        with self.assertRaises(BudgetDenied):
            self.budget.reserve('one', 'run', now=100)
        self.budget.finish('one', {'input_tokens': 90, 'output_tokens': 11}, 'completed')
        with self.assertRaises(BudgetDenied):
            self.budget.reserve('two', 'run', now=100)
        other = Budget.create(Path(self.tmp.name)/'other.sqlite', campaign_id='test',
                              plan_sha256='b'*64, max_requests=1, max_tokens=1, deadline=100)
        with self.assertRaises(BudgetDenied):
            other.reserve('one', 'run', now=100)

    def test_crashed_reservation_retains_unknown_usage(self):
        self.budget.reserve('one', 'run', now=100)
        result = self.budget.snapshot()
        self.assertEqual(result['inflight_requests'], 1)
        self.assertIsNone(result['observed_tokens'])
        self.budget.close('operator_stop')
        with self.assertRaises(BudgetDenied):
            self.budget.reserve('two', 'run', now=100)

    def test_missing_database_is_not_created_or_admitted(self):
        with self.assertRaises(FileNotFoundError):
            Budget(Path(self.tmp.name)/'missing.sqlite').reserve('x', 'r', now=100)


if __name__ == '__main__':
    unittest.main()
