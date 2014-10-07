#!/usr/bin/env python
# encoding: utf-8
"""
AnalysisCollectionService_t.py
"""

import unittest
import nose

from WMCore.ACDC.AnalysisCollectionService import AnalysisCollectionService
from WMCore.DataStructs.Mask import Mask
from WMQuality.TestInitCouchApp import TestInitCouchApp

class AnalysisCollectionService_t(unittest.TestCase):
    """
    Unit tests for AnalysisCollectionService
    """

    def setUp(self):
        """Set up test couch instance"""
        self.dbsURL  = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.dataset = '/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO'

        """Set up couch test environment"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-acservice", "GroupUser", "ACDC")

        self.acService = AnalysisCollectionService(url=self.testInit.couchUrl, database = self.testInit.couchDbName)
        self.collection = self.acService.createCollection('ewv-testcoll', 'ewv2', 'Analysis')


    def tearDown(self):
        """Clean up couch instance"""
        self.testInit.tearDownCouch()


    def testCreateFilesetFromDBS(self):
        """Test creating an analysis fileset from DBS"""

        rangesMask = Mask()
        rangesMask.addRunWithLumiRanges(run=1, lumiList=[[1, 9], [12, 12], [31, 31], [38, 39], [49, 49], [51, 52], [64, 65], [82, 82], [92, 98]])

        fs, fl = self.acService.createFilesetFromDBS(self.collection, filesetName='test_fs',
                                                     dbsURL=self.dbsURL, dataset=self.dataset, mask=rangesMask)

        self.assertTrue(fl['_id'])
        self.assertEqual(len(fl['files']), 21)
        for file in fl['files']:
            self.assertTrue(fl['files'][file]['merged'])


if __name__ == '__main__':
    unittest.main()
