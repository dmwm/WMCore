#!/usr/bin/env python

from __future__ import division, print_function

import json
import os
import unittest

from Utils.PythonVersion import PY3
from WMCore.WMStats.DataStructs.DataCache import DataCache


class DataCacheTests(unittest.TestCase):
    """
    Unit tests for WMStats DataCache
    """

    def setUp(self):
        self.fileCache = os.path.join(os.path.dirname(__file__), 'DataCache.json')
        with open(self.fileCache) as jo:
            data = json.load(jo)
        DataCache().setlatestJobData(data)
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        pass

    def testDuration(self):
        self.assertEqual(300, DataCache.getDuration())

        DataCache.setDuration(100)
        self.assertEqual(100, DataCache.getDuration())

    def testLatestJobData(self):
        self.assertEqual(20, len(DataCache.getlatestJobData()))
        self.assertItemsEqual(['time', 'data'], list(DataCache._lastedActiveDataFromAgent))

        DataCache.setlatestJobData("ALAN")
        self.assertEqual("ALAN", DataCache.getlatestJobData())
        self.assertItemsEqual(['time', 'data'], list(DataCache._lastedActiveDataFromAgent))

    def testLatestJobDataExpired(self):
        self.assertFalse(DataCache.islatestJobDataExpired())

        DataCache.setDuration(-1)
        self.assertTrue(DataCache.islatestJobDataExpired())

        DataCache.setDuration(300)
        self.assertFalse(DataCache.islatestJobDataExpired())
        DataCache._lastedActiveDataFromAgent = {}
        self.assertTrue(DataCache.islatestJobDataExpired())

        self.assertEqual({}, DataCache.getlatestJobData())

    def testFilterData(self):
        data = list(DataCache.filterData(filterDict={}, maskList=['RequestType']))
        self.assertEqual(20, len(data))
        self.assertItemsEqual(['ReReco', 'MonteCarlo', 'StepChain', 'MonteCarloFromGEN',
                               'ReDigi', 'TaskChain', 'DQMHarvest'], set(list(data)))

        data = list(DataCache.filterData(filterDict={}, maskList=['Campaign', 'RequestType']))
        self.assertEqual(40, len(data))

        data = list(DataCache.filterData(filterDict={'IncludeParents': 'True'}, maskList=['Campaign']))
        self.assertEqual(2, len(data))

        data = list(DataCache.filterData(filterDict={'Campaign': 'CMSSW_9_4_0__test2inwf-1510737328'},
                                         maskList=['RequestName']))
        self.assertItemsEqual(["amaltaro_TaskChain_InclParents_HG1812_Validation_181203_121005_1483"],
                              data)

    def testFilterDataByRequest(self):
        data = list(DataCache.filterDataByRequest(filterDict={}, maskList='RequestType'))
        self.assertEqual(20, len(data))
        self.assertItemsEqual(['RequestName', 'RequestType'], list(data[0]))
        reqTypes = [item['RequestType'] for item in data]
        self.assertItemsEqual(['ReReco', 'MonteCarlo', 'StepChain', 'MonteCarloFromGEN',
                               'ReDigi', 'TaskChain', 'DQMHarvest'], set(list(reqTypes)))

        data = list(DataCache.filterDataByRequest(filterDict={}, maskList=['Campaign', 'RequestType']))
        self.assertEqual(20, len(data))

        data = list(DataCache.filterDataByRequest(filterDict={'IncludeParents': 'True'}, maskList=['Campaign']))
        self.assertEqual(2, len(data))

        data = list(DataCache.filterDataByRequest(filterDict={'Campaign': 'CMSSW_9_4_0__test2inwf-1510737328'},
                                                  maskList=['RequestName']))
        self.assertEqual(1, len(data))
        self.assertEqual("amaltaro_TaskChain_InclParents_HG1812_Validation_181203_121005_1483",
                         data[0]['RequestName'])


if __name__ == '__main__':
    unittest.main()
