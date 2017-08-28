"""
Unit tests for Unified/RequestInfo.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

import unittest

from WMCore.Services.MicroService.Unified.RequestInfo import findParent


class RequestInfoTest(unittest.TestCase):
    "Unit test for RequestInfo module"
    def setUp(self):
        self.datasets = ['/SingleElectron/Run2016B-18Apr2017_ver2-v1/AOD']

    def test_findParent(self):
        "Test function for findParent()"
        dataset = '/SingleElectron/Run2016B-18Apr2017_ver2-v1/AOD'
        parents = findParent(dataset)
        parent = '/SingleElectron/Run2016B-v2/RAW'
        self.assertEqual(parent, parents[0])


if __name__ == '__main__':
    unittest.main()
