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
from Utils.PythonVersion import PY3
from WMCore.MicroService.MSTransferor.MSTransferor import MSTransferor
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMCore.MicroService.MSTransferor.MSTransferorError import MSTransferorStorageError
from WMCore.MicroService.MSTransferor.DataStructs.Workflow import Workflow


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
        self.msConfig = {'services': ['transferor'],
                         'verbose': False,
                         'interval': 1 * 60,
                         'enableStatusTransition': True,
                         'enableDataTransfer': False,
                         'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'quotaUsage': 0.9,
                         'rucioAccount': 'wma_test',  # it should be wmcore_transferor
                         # 'rucioAuthUrl': 'https://cms-rucio-auth.cern.ch',
                         # 'rucioUrl': 'http://cms-rucio.cern.ch',
                         'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                         'rucioUrl': 'http://cms-rucio-int.cern.ch',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'}

        self.msTransferor = MSTransferor(self.msConfig)

        self.taskChainTempl = getTestFile('data/ReqMgr/requests/Integration/TaskChain_Prod.json')
        self.stepChainTempl = getTestFile('data/ReqMgr/requests/Integration/SC_LumiMask_PhEDEx.json')
        super(TransferorTest, self).setUp()
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testGetPNNsFromPSNs(self):
        """Test MSTransferor private method _getPNNsFromPSNs()"""
        self.assertItemsEqual(self.msTransferor.psn2pnnMap, {})

        # now fill up the cache
        self.msTransferor.psn2pnnMap = self.msTransferor.cric.PSNtoPNNMap()

        self.assertItemsEqual(self.msTransferor._getPNNsFromPSNs([]), set())
        pnns = self.msTransferor._getPNNsFromPSNs(["T1_IT_CNAF", "T1_IT_CNAF_Disk"])
        self.assertItemsEqual(pnns, set(["T1_IT_CNAF_Disk"]))

        # dropping T3s and CERNBOX
        pnns = self.msTransferor._getPNNsFromPSNs(["T1_US_FNAL", "T2_CH_CERN_HLT"])
        self.assertItemsEqual(pnns, set(["T1_US_FNAL_Disk", "T2_CH_CERN"]))

        # testing with non-existant PSNs
        psns = self.msTransferor._getPNNsFromPSNs(["T1_US_FNAL_Disk", "T2_CH_CERNBOX"])
        self.assertItemsEqual(psns, set())

    def testGetPSNsFromPNNs(self):
        """Test MSTransferor private method _getPSNsFromPNNs()"""
        self.assertItemsEqual(self.msTransferor.pnn2psnMap, {})

        # now fill up the cache
        self.msTransferor.pnn2psnMap = self.msTransferor.cric.PNNtoPSNMap()

        self.assertItemsEqual(self.msTransferor._getPSNsFromPNNs([]), set())
        psns = self.msTransferor._getPSNsFromPNNs(["T1_IT_CNAF", "T1_IT_CNAF_Disk"])
        self.assertItemsEqual(psns, set(["T1_IT_CNAF"]))

        # test dropping T3s
        psns = self.msTransferor._getPSNsFromPNNs(["T2_UK_SGrid_RALPP"])
        self.assertItemsEqual(psns, set(["T2_UK_SGrid_RALPP"]))

        # testing with non-existant PNNs
        psns = self.msTransferor._getPSNsFromPNNs(["T1_US_FNAL", "T2_CH_CERN_HLT"])
        self.assertItemsEqual(psns, set())

    def testDiskPNNs(self):
        """Test MSTransferor private method _diskPNNs()"""
        # empty list of pnns
        self.assertItemsEqual(self.msTransferor._diskPNNs([]), set())

        # only PNNs that will be dropped
        pnns = self.msTransferor._diskPNNs(["T1_US_FNAL_Tape", "T1_US_FNAL_MSS",
                                            "T2_CH_CERNBOX", "T0_CH_CERN_Export"])
        self.assertItemsEqual(pnns, set())

        # valid PNNs that can receive data
        pnns = self.msTransferor._diskPNNs(["T1_US_FNAL_Disk", "T2_CH_CERN", "T2_DE_DESY"])
        self.assertItemsEqual(pnns, set(["T1_US_FNAL_Disk", "T2_CH_CERN", "T2_DE_DESY"]))

        # finally, a mix of valid and invalid PNNs
        pnns = self.msTransferor._diskPNNs(["T1_US_FNAL_Disk", "T1_US_FNAL_MSS", "T1_US_FNAL_Tape",
                                            "T2_CH_CERN", "T2_DE_DESY"])
        self.assertItemsEqual(pnns, set(["T1_US_FNAL_Disk", "T2_CH_CERN", "T2_DE_DESY"]))

    def notestRequestRecord(self):
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

    def testUpdateStorage(self):
        """
        Test updateStorage method. We should be able to save and read
        JSON objects to persistent storage of MSTransferor.
        """
        # use default storage and check save/read operations
        wflow = 'testWorkflow'
        status = self.msTransferor.updateStorage(wflow)
        self.assertEqual(status, 'ok')
        wflowObject = Workflow(wflow, {'DbsUrl': 'https://cmsweb-testbed.cern.ch', 'RequestType': 'StoreResults'})
        self.msTransferor.checkDataReplacement(wflowObject)
        self.assertEqual(wflowObject.dataReplacement, True)

    def testUpdateSites(self):
        """
        Test the updateSites method.
        """
        rec = {'workflow': 'testWorkflow'}
        res = self.msTransferor.updateSites(rec)
        self.assertEqual(res, [])

        # now let's test transferor error
        self.msTransferor.storage = '/bla'
        res = self.msTransferor.updateSites(rec)
        err = MSTransferorStorageError("[Errno 2] No such file or directory: '/bla/testWorkflow'", **rec)
        self.assertEqual(res, [err.error()])
        self.assertEqual(err.code, 2)


if __name__ == '__main__':
    unittest.main()
