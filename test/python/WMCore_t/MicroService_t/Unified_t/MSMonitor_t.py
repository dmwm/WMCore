"""
Unit tests for Unified/Monitor.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

# system modules
import json
import unittest

# WMCore modules
from WMCore.MicroService.Unified.MSMonitor import MSMonitor
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.ReqMgrAux.MockReqMgrAux import MockReqMgrAux



class MonitorTest(EmulatedUnitTestCase):
    "Unit test for Monitor module"

    def setUp(self):
        "init test class"
        self.msConfig = {'verbose': False,
                         'group': 'DataOps',
                         'interval': 1 * 60,
                         'updateInterval': 0,
                         'readOnly': True,
                         'reqmgrUrl': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'}

        self.ms = MSMonitor(self.msConfig)
        self.ms.reqmgrAux = MockReqMgrAux()
        super(MonitorTest, self).setUp()

    def testGetTransferInfo(self):
        """
        Test the getTransferInfo method
        """
        transferRecords = [d for d in self.ms.getTransferInfo('ALL_DOCS')]
        self.assertNotEqual(transferRecords, [])
        for rec in transferRecords:
            self.assertEqual(isinstance(rec, dict), True)
            keys = sorted(['workflowName', 'lastupdate', 'transfers'])
            self.assertEqual(keys, sorted(rec.keys()))
            print('### transfer rec %s' % json.dumps(rec))

    def testCompletion(self):
        """
        Test the completion method
        """
        campaigns, transferRecords = self.ms.updateCaches()
        self.assertNotEqual(campaigns, [])
        self.assertNotEqual(transferRecords, [])
        transfers = []
        for rec in transferRecords:
            self.assertEqual('transfers' in rec, True)
            for item in rec['transfers']:
                transfers.append(item)
        print("### transfers: %s" % transfers)
        completion = self.ms.completion(transfers, campaigns)
        self.assertEqual(completion, True)

    def testGetCampaignConfig(self):
        """
        Test the getCampaignConfig method
        """
        campaigns, _ = self.ms.updateCaches()
        self.assertNotEqual(campaigns, [])
        for cname, cdict in campaigns.items():
            self.assertEqual(isinstance(cdict, dict), True)
            self.assertNotEqual(cdict.get('CampaignName', {}), {})
            print('### campaign %s config %s' % (cname, json.dumps(cdict)))

    def testUpdateTransferInfo(self):
        """
        Test the updateTransferInfo method
        """
        campaigns, transferRecords = self.ms.updateCaches()
        self.assertNotEqual(campaigns, [])
        self.assertNotEqual(transferRecords, [])
        self.ms.updateTransferInfo(transferRecords)


if __name__ == '__main__':
    unittest.main()
