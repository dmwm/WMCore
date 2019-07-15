"""
Unit tests for Unified/RequestInfo.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import unittest

from WMCore.MicroService.Unified.RequestInfo import RequestInfo


class RequestInfoTest(unittest.TestCase):
    "Unit test for RequestInfo module"

    def setUp(self):
        self.mode = 'test'
        self.reqInfo = RequestInfo(microConfig={}, uniConfig={})


if __name__ == '__main__':
    unittest.main()
