"""Synthetic SSE schema regressions; no provider data, credentials or network."""
import json
import unittest

import support
from harness import gateway


def stream(*items):
    return (''.join('data: '+json.dumps(item)+'\n\n' for item in items)
            +'data: [DONE]\n\n').encode()


def delta(value):
    return {'choices': [{'delta': value}]}


class ResponseToolIdsTests(unittest.TestCase):
    def test_missing_tool_calls_adds_no_ids(self):
        self.assertEqual([], gateway.response_tool_ids(stream(delta({'content': 'Synthetic text.'}))))

    def test_null_tool_calls_adds_no_ids_and_keeps_complete_usage(self):
        raw = stream(delta({'content': 'Synthetic text.', 'tool_calls': None}),
                     {'choices': [], 'usage': {'prompt_tokens': 7, 'completion_tokens': 3, 'total_tokens': 10}})
        self.assertEqual([], gateway.response_tool_ids(raw))
        self.assertTrue(gateway.parse_sse(raw)['stream_done'])
        self.assertEqual(10, gateway.parse_sse(raw)['usage']['total_tokens'])
        # With no tool IDs the real boundary method must return immediately,
        # leaving the terminal marker available to its caller.
        gateway.Gateway.hold_response_end(object(), {}, raw)

    def test_empty_tool_call_array_adds_no_ids(self):
        self.assertEqual([], gateway.response_tool_ids(stream(delta({'tool_calls': []}))))

    def test_valid_tool_calls_keep_order_across_null_and_argument_fragments(self):
        raw = stream(delta({'tool_calls': [{'index': 0, 'id': 'first', 'type': 'function',
                                            'function': {'name': 'synthetic', 'arguments': '{'}}]}),
                     delta({'tool_calls': None}), delta({}), delta({'tool_calls': []}),
                     delta({'tool_calls': [{'index': 0, 'function': {'arguments': '}'}}]}),
                     delta({'tool_calls': [{'index': 0, 'id': 'first'}, {'index': 1, 'id': 'second'}]}))
        self.assertEqual(['first', 'second'], gateway.response_tool_ids(raw))

    def test_non_array_tool_calls_are_rejected_even_when_falsy(self):
        for malformed in ({}, '', 0, False, {'id': 'not-an-array'}, 'invalid', 1, True):
            with self.subTest(tool_calls=malformed), self.assertRaises(ValueError):
                gateway.response_tool_ids(stream(delta({'tool_calls': malformed})))

    def test_non_object_tool_call_members_are_rejected(self):
        for malformed in (None, [], '', 0, False):
            with self.subTest(member=malformed), self.assertRaises(ValueError):
                gateway.response_tool_ids(stream(delta({'tool_calls': [malformed]})))

    def test_invalid_enclosing_schema_remains_rejected(self):
        for malformed in (None, [], {'choices': None}, {'choices': {}},
                          {'choices': [None]}, delta(None), delta([])):
            with self.subTest(item=malformed), self.assertRaises(ValueError):
                gateway.response_tool_ids(stream(malformed))


if __name__ == '__main__':
    unittest.main()
