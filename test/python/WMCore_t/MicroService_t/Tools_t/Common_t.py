"""
Unit tests for Unified/Common.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import json
import unittest

from nose.plugins.attrib import attr

from WMCore.MicroService.Tools.Common import (dbsInfo, getEventsLumis, findParent,
                                              hasHTTPFailed, isRelVal)


class CommonTest(unittest.TestCase):
    """Unit test for Common module"""

    def setUp(self):
        self.dbsUrl = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
        self.datasets = ['/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM',
                         '/ZMM_14TeV/Summer12-DESIGN42_V17_SLHCTk-v1/GEN-SIM']
        self.child = ['/SingleElectron/Run2016B-18Apr2017_ver2-v1/AOD']

    @attr("integration")
    def testDbsInfo(self):
        "Test function for dbsInfo()"
        datasetBlocks, datasetSizes, datasetTransfers = dbsInfo(self.datasets, self.dbsUrl)
        blocks = [b for d in self.datasets for b in datasetBlocks[d]]
        self.assertEqual(2, len(datasetBlocks))
        self.assertEqual(5, len(blocks))
        expect = 6271126523 + 7840499449
        sizes = sum([datasetSizes[d] for d in self.datasets])
        self.assertEqual(expect, sizes)
        self.assertEqual(len(self.datasets), len(datasetTransfers))

    @attr("integration")
    def testGetEventsLumis(self):
        "Test function for getEventsLumis()"
        totEvts = totLumis = 0
        for dataset in self.datasets:
            nevts, nlumis = getEventsLumis(dataset, self.dbsUrl)
            print("dataset: {} with evts: {} and lumis: {}".format(dataset, nevts, nlumis))
            totEvts += nevts
            totLumis += nlumis
        expect = 7398 + 0
        self.assertEqual(expect, totEvts)
        expect = 16 + 0
        self.assertEqual(expect, totLumis)

    @attr("integration")
    def test_findParent(self):
        "Test function for findParent()"
        parents = findParent(self.child, self.dbsUrl)
        self.assertEqual(parents[self.child[0]], '/SingleElectron/Run2016B-v2/RAW')

    def testHasHTTPFailed(self):
        """Test the hasHTTPFailed method"""
        self.assertTrue(hasHTTPFailed({}))
        self.assertTrue(hasHTTPFailed({'data': '', 'code': 301, 'error': ''}))
        self.assertTrue(hasHTTPFailed({'data': '', 'code': 301, 'error': 'blah'}))
        self.assertTrue(hasHTTPFailed({'data': 1, 'code': 400, 'error': 'blah'}))
        self.assertTrue(hasHTTPFailed({'data': 1, 'code': 500, 'error': []}))
        self.assertTrue(hasHTTPFailed({'data': 1, 'code': 500, 'error': None}))

        self.assertFalse(hasHTTPFailed({'data': []}))
        self.assertFalse(hasHTTPFailed({'data': None}))
        self.assertFalse(hasHTTPFailed({'data': json.dumps([])}))
        self.assertFalse(hasHTTPFailed({'data': json.dumps(None)}))
        self.assertFalse(hasHTTPFailed({'data': ['blah']}))
        self.assertFalse(hasHTTPFailed({'data': 'blah'}))
        self.assertFalse(hasHTTPFailed({'data': json.dumps('blah')}))
        self.assertFalse(hasHTTPFailed({'data': 'blah', 'code': 200}))
        # result below should never happen, but let's test the status code
        self.assertFalse(hasHTTPFailed({'data': 1, 'code': 200, 'error': 'blah'}))

    def testIsRelVal(self):
        """
        Test the `isRelVal` method functionality
        """
        badSubReqType = ("ReDigi", "TaskChain", "")
        goodSubReqType = ("RelVal", "HIRelVal")
        for subType in badSubReqType:
            testReqDict = {"RequestType": "StepChain", "DbsUrl": "a_dbs_url", "SubRequestType": subType}
            self.assertFalse(isRelVal(testReqDict))

        for subType in goodSubReqType:
            testReqDict = {"RequestType": "StepChain", "DbsUrl": "a_dbs_url", "SubRequestType": subType}
            self.assertTrue(isRelVal(testReqDict))

if __name__ == '__main__':
    unittest.main()
