"""
Unit tests for Unified/Common.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import unittest

from WMCore.MicroService.Tools.Common import dbsInfo, getEventsLumis, findParent


class CommonTest(unittest.TestCase):
    "Unit test for Common module"

    def setUp(self):
        self.dbsUrl = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
        self.datasets = ['/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM',
                         '/ZMM_14TeV/Summer12-DESIGN42_V17_SLHCTk-v1/GEN-SIM']
        self.child = ['/SingleElectron/Run2016B-18Apr2017_ver2-v1/AOD']

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

    def testGetEventsLumis(self):
        "Test function for getEventsLumis()"
        totEvts = totLumis = 0
        for dataset in self.datasets:
            nevts, nlumis = getEventsLumis(dataset, self.dbsUrl)
            totEvts += nevts
            totLumis += nlumis
        expect = 10250 + 10616
        self.assertEqual(expect, totEvts)
        expect = 22 + 10
        self.assertEqual(expect, totLumis)

    def test_findParent(self):
        "Test function for findParent()"
        parents, _ = findParent(self.child, self.dbsUrl)
        self.assertEqual(parents[self.child[0]], '/SingleElectron/Run2016B-v2/RAW')


if __name__ == '__main__':
    unittest.main()
