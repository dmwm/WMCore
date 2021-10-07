"""
Unit tests for Unified/Monitor.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

from future.utils import viewitems

import time
# system modules
import unittest
from copy import deepcopy

# WMCore modules
from WMCore.MicroService.MSMonitor.MSMonitor import MSMonitor
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.ReqMgrAux.MockReqMgrAux import MockReqMgrAux


class MSMonitorTest(EmulatedUnitTestCase):
    "Unit test for Monitor module"

    def setUp(self):
        "init test class"
        self.msConfig = {'verbose': False,
                         'group': 'DataOps',
                         'interval': 1 * 60,
                         'updateInterval': 0,
                         'enableStatusTransition': True,
                         'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
                         'rucioAccount': "wma_test",
                         'rucioUrl': "http://cms-rucio-int.cern.ch",
                         'rucioAuthUrl': "https://cms-rucio-auth-int.cern.ch"}

        self.ms = MSMonitor(self.msConfig)
        self.ms.reqmgrAux = MockReqMgrAux()
        super(MSMonitorTest, self).setUp()

    def testUpdateCaches(self):
        """
        Test the getCampaignConfig method
        """
        campaigns, transfersDocs = self.ms.updateCaches()
        self.assertNotEqual(transfersDocs, [])
        self.assertEqual(len(transfersDocs[0]['transfers']), 1)
        self.assertTrue(time.time() > transfersDocs[0]['lastUpdate'], 1)

        self.assertNotEqual(campaigns, [])
        for cname, cdict in viewitems(campaigns):
            self.assertEqual(cname, cdict['CampaignName'])
            self.assertEqual(isinstance(cdict, dict), True)
            self.assertNotEqual(cdict.get('CampaignName', {}), {})

    def testGetTransferInfo(self):
        """
        Test the getTransferInfo method
        """
        _, transfersDocs = self.ms.updateCaches()
        transfersDocs[0]['transfers'] = []
        originalTransfers = deepcopy(transfersDocs)
        self.ms.getTransferInfo(transfersDocs)

        self.assertNotEqual(transfersDocs, [])
        self.assertEqual(len(transfersDocs), len(originalTransfers))
        for rec in transfersDocs:
            self.assertEqual(isinstance(rec, dict), True)
            keys = sorted(['workflowName', 'lastUpdate', 'transfers'])
            self.assertEqual(keys, sorted(rec.keys()))
            self.assertTrue(time.time() >= rec['lastUpdate'])

    def testCompletion(self):
        """
        Test the completion method
        """
        campaigns, transfersDocs = self.ms.updateCaches()
        transfersDocs.append(deepcopy(transfersDocs[0]))
        transfersDocs.append(deepcopy(transfersDocs[0]))
        transfersDocs[0]['transfers'] = []
        transfersDocs[0]['workflowName'] = 'workflow_0'
        transfersDocs[1]['transfers'][0]['completion'].append(100)
        transfersDocs[1]['workflowName'] = 'workflow_1'
        transfersDocs[2]['workflowName'] = 'workflow_2'
        self.assertEqual(len(transfersDocs), 3)

        completedWfs = self.ms.getCompletedWorkflows(transfersDocs, campaigns)
        self.assertEqual(len(completedWfs), 2)

    def testUpdateTransferInfo(self):
        """
        Test the updateTransferInfo method
        """
        _, transferRecords = self.ms.updateCaches()
        failed = self.ms.updateTransferDocs(transferRecords, workflowsToSkip=[])
        self.assertEqual(len(failed), len(transferRecords))


if __name__ == '__main__':
    unittest.main()
