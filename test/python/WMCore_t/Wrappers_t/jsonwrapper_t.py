#!/usr/bin/env python
"""
Unit tests for json wrapper.
"""

__revision__ = "$Id: jsonwrapper_t.py,v 1.1 2009/12/15 19:08:58 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import unittest
import json
__test = 0
try:
    import cjson
    __test = 1
except:
    print "No cjson module is found, skip the test"

import WMCore.Wrappers.jsonwrapper as json_wrap

class TestWrapper(unittest.TestCase):
    """
    JSON wrapper unit tests.
    """
    def setUp(self):
        """Init parameters"""
        self.record = {"test":1}

    def test_cjson(self):
        """
        Test default json implementation.
        """
        json_wrap._module = "cjson"
        result = json_wrap.dumps(self.record)
        expect = json.dumps(self.record)
        self.assertEqual(expect, result)

        data   = result

        result = json_wrap.loads(data)
        expect = json.loads(data)
        self.assertEqual(expect, result)

if __name__ == "__main__":
    if  __test:
        unittest.main()

