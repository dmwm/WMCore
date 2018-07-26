"""
Unit tests for Unified/Common.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import unittest

from WMCore.MicroService.Unified.Common import dbsInfo, getEventsLumis, \
        workflowsInfo, getWorkflows, workqueueView


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
        workflows = getWorkflows(state)
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

    def testWorkqueueView(self):
        "Test workqueueView functionality"
        url = 'https://cmsweb.cern.ch:8443/couchdb/workqueue/_design/WorkQueue/_view/jobsByRequest?group=true&reduce=true'
        self.assertEqual(url, workqueueView('jobsByRequest'))
        url = 'https://cmsweb.cern.ch:8443/couchdb/workqueue/_design/WorkQueue/_view/jobsByRequest?a=1&b=2'
        self.assertEqual(url, workqueueView('jobsByRequest', {'a':1, 'b':2}))

if __name__ == '__main__':
    unittest.main()
