#!/usr/bin/env python
import os
import unittest
import shutil

from WMCore.Wrappers import JsonWrapper
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMStatsDocGenerator import *
class WMStatsTest(unittest.TestCase):
    """
    """
    def setUp(self):
        """
        _setUp_
        """
        self.schema = []
        self.couchApps = ["WMStats"]
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        self.testInit.setupCouch('wmstats_t', *self.couchApps)
        self.wmstatsWriter = WMStatsWriter(self.testInit.couchUrl, 'wmstats_t'); 
        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()

    def testWMStatsWriter(self):
        # test getWork
        schema = generate_reqmgr_schema()
        self.assertEquals(self.wmstatsWriter.insertRequest(schema[0]), 'OK', 'insert fail');
        self.assertEquals(self.wmstatsWriter.updateRequestStatus(schema[0]['RequestName'], "failed"), 'OK', 'update fail')
        self.assertEquals(self.wmstatsWriter.updateRequestStatus("not_exist_schema", "assigned"), 
                          'ERROR: request not found - not_exist_schema')
        self.assertEquals(self.wmstatsWriter.updateTeam(schema[0]['RequestName'], 'teamA'), 'OK', 'update fail')
        self.assertEquals(self.wmstatsWriter.updateTeam("not_exist_schema", 'teamA'),
                          'ERROR: request not found - not_exist_schema')
        totalStats = {'total_jobs': 100, 'input_events': 1000, 'input_lumis': 1234, 'input_num_file': 5}
        self.assertEquals(self.wmstatsWriter.insertTotalStats(schema[0]['RequestName'], totalStats), 'OK', 'update fail')
        self.assertEquals(self.wmstatsWriter.insertTotalStats("not_exist_schema", totalStats),
                          'ERROR: request not found - not_exist_schema')

if __name__ == '__main__':

    unittest.main()
    
