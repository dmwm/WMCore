"""
Unit tests for Unified/Transferor.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import json
# system modules
import os
import unittest

# WMCore modules
from WMCore.MicroService.Unified.MSTransferor import MSTransferor
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


class TransferorTest(EmulatedUnitTestCase):
    "Unit test for Transferor module"

    def setUp(self):
        "init test class"
        self.msConfig = {'verbose': False,
                         'group': 'DataOps',
                         'interval': 1 * 60,
                         'readOnly': True,
                         'reqmgrUrl': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'}

        self.msTransferor = MSTransferor(self.msConfig)

        self.taskChainTempl = getTestFile('data/ReqMgr/requests/Integration/TaskChain_Prod.json')
        self.stepChainTempl = getTestFile('data/ReqMgr/requests/Integration/SC_LumiMask_PhEDEx.json')
        super(TransferorTest, self).setUp()

    def testRequestRecord(self):
        """
        Test the requestRecord method
        """
        default = {'name': '', 'reqStatus': None, 'SiteWhiteList': [],
                   'SiteBlackList': [], 'datasets': [], 'campaign': []}
        self.assertItemsEqual(self.msTransferor.requestRecord({}), default)

        with open(self.taskChainTempl) as jo:
            reqData = json.load(jo)['createRequest']
        expectedRes = [{'type': 'MCPileup',
                        'name': '/Neutrino_E-10_gun/RunIISummer17PrePremix-PUAutumn18_102X_upgrade2018_realistic_v15-v1/GEN-SIM-DIGI-RAW'},
                       {'type': 'MCPileup',
                        'name': '/Neutrino_E-10_gun/RunIISummer17PrePremix-PUAutumn18_102X_upgrade2018_realistic_v15-v1/GEN-SIM-DIGI-RAW'}]
        resp = self.msTransferor.requestRecord(reqData)['datasets']
        self.assertEqual(len(resp), 2)
        for idx in range(len(resp)):
            self.assertItemsEqual(resp[idx], expectedRes[idx])

        with open(self.stepChainTempl) as jo:
            reqData = json.load(jo)['createRequest']
        expectedRes = [{'type': 'InputDataset',
                        'name': '/RelValH125GGgluonfusion_14/CMSSW_10_6_1-106X_mcRun3_2021_realistic_v1_rsb-v1/GEN-SIM'},
                       {'type': 'MCPileup',
                        'name': '/RelValMinBias_14TeV/CMSSW_10_6_1-106X_mcRun3_2021_realistic_v1_rsb-v1/GEN-SIM'},
                       {'type': 'MCPileup',
                        'name': '/RelValMinBias_14TeV/CMSSW_10_6_1-106X_mcRun3_2021_realistic_v1_rsb-v1/GEN-SIM'}]
        resp = self.msTransferor.requestRecord(reqData)['datasets']
        self.assertEqual(len(resp), 3)
        for idx in range(len(resp)):
            self.assertItemsEqual(resp[idx], expectedRes[idx])


if __name__ == '__main__':
    unittest.main()
