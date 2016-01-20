#!/usr/bin/env python
"""
Unit tests for json wrapper.
"""

import json
import unittest

import WMCore.Wrappers.JsonWrapper as json_wrap


class TestWrapper(unittest.TestCase):
    """
    JSON wrapper unit tests.
    """

    def setUp(self):
        """Init parameters"""
        self.record = {"test": 1}

    def test_json(self):
        """
        Test default json implementation.
        """
        result = json_wrap.dumps(self.record)
        expect = json.dumps(self.record)
        self.assertEqual(expect, result)

        data = result

        result = json_wrap.loads(data)
        expect = json.loads(data)
        self.assertEqual(expect, result)

        with self.assertRaises(ValueError):
            json_wrap.loads("bogusbogus")


if __name__ == "__main__":
    unittest.main()
