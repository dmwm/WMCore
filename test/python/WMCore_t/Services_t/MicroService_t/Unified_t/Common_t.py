"""
Unit tests for Unified/Common.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import time
import unittest

from WMCore.Services.MicroService.Unified.Common import dbsInfo, getEventsLumis,\
        workflowsInfo, reqmgrUrl, getWorkflows, subscriptions, subsDetails4dataset,\
        subsDetails4block


class CommonTest(unittest.TestCase):
    "Unit test for Common module"
    def setUp(self):
        self.datasets = ['/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM',\
                '/ZMM_14TeV/Summer12-DESIGN42_V17_SLHCTk-v1/GEN-SIM']

    def testDbsInfo(self):
        "Test function for dbsInfo()"
        datasetBlocks, datasetSizes = dbsInfo(self.datasets)
        blocks = [b for d in self.datasets for b in datasetBlocks[d]]
        self.assertEqual(2, len(datasetBlocks))
        self.assertEqual(5, len(blocks))
        expect = 6271126523+7840499449
        sizes = sum([datasetSizes[d] for d in self.datasets])
        self.assertEqual(expect, sizes)

    def testWorkflowsInfo(self):
        "Test function for workflowsInfo()"
        state = 'assignment-approved'
        url = '%s/data/request' % reqmgrUrl()
        workflows = getWorkflows(url, state=state)
        winfo = workflowsInfo(workflows)
#         datasets = [d for row in winfo.values() for d in row['datasets']]
#         pileups = [d for row in winfo.values() for d in row['pileups']]
        keys = sorted(['datasets', 'pileups', 'priority', 'selist', 'campaign'])
        for wdict in winfo.values():
            self.assertEqual(keys, sorted(wdict.keys()))

    def testGetEventsLumis(self):
        "Test function for getEventsLumis()"
        totEvts = totLumis = 0
        for dataset in self.datasets:
            nevts, nlumis = getEventsLumis(dataset)
            totEvts += nevts
            totLumis += nlumis
        expect = 10250+10616
        self.assertEqual(expect, totEvts)
        expect = 22+10
        self.assertEqual(expect, totLumis)

    def testSubscriptions(self):
        "Test subscriptions functionality"
        params = {'group': 'DataOps', 'suspended': 'n'}
        data = subscriptions(**params)
        dataset = data[0]['name']
        datasetInfo = subsDetails4dataset(dataset)
        if datasetInfo and datasetInfo[0]:
            datasetBytes = datasetInfo[0]['bytes']
            print("\ndataset=%s" % dataset)
            print(datasetInfo)
            datasetBlocks, _ = dbsInfo([dataset])
            blocks = datasetBlocks[dataset]
            block = blocks[0]
            blockInfo = subsDetails4block(blocks)
            blockBytes = blockInfo[0]['bytes']
            nodes = list(set([s['node'] for b in blockInfo for s in b['subscription']]))
            print("\nblock=%s" % block)
            print("\nsubscribed to %s" % nodes)
            self.assertEqual(True, blockBytes < datasetBytes)
            self.assertEqual(1, len(nodes) > 1)

if __name__ == '__main__':
    unittest.main()
