#!/usr/bin/env python
from __future__ import absolute_import
import os
import unittest
import shutil

from WMCore.Wrappers import JsonWrapper
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMQuality.TestInitCouchApp import TestInitCouchApp
from .WMStatsDocGenerator import *
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection
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
        dbName = 'wmstats_t'
        self.testInit.setupCouch(dbName, "WMStats")
        reqDBName = "reqmgrdb_t"
        self.testInit.setupCouch(reqDBName, "ReqMgr")
        wmstatsURL = "%s/%s" % (self.testInit.couchUrl, dbName)
        reqDBURL = "%s/%s" % (self.testInit.couchUrl, reqDBName)
        self.reqDBWriter = RequestDBWriter(reqDBURL)
        self.wmstatsReader = WMStatsReader(wmstatsURL, reqdbURL=reqDBURL)
        self.wmstatsReader.defaultStale = {}
        self.wmstatsReader.reqDB.defaultStale = {}
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
        
        result = self.reqDBWriter.insertGenericRequest(schema[0])
        self.assertEqual(result[0]['ok'], True, 'insert fail')
        
        result = self.reqDBWriter.updateRequestStatus(schema[0]['RequestName'], "failed")
        self.assertEqual(result, 'OK', 'update fail')
        
        result = self.reqDBWriter.updateRequestStatus("not_exist_schema", "assigned") 
        self.assertEqual(result,'Error: document not found')
        
        result = self.reqDBWriter.updateRequestProperty(schema[0]['RequestName'], {"Teams": ['teamA']})
        self.assertEqual(result, 'OK', 'update fail')
        
        result = self.reqDBWriter.updateRequestProperty("not_exist_schema", {"Teams": ['teamA']})                  
        self.assertEqual(result, 'Error: document not found')
        
        totalStats = {'TotalEstimatedJobs': 100, 'TotalInputEvents': 1000, 'TotalInputLumis': 1234, 'TotalInputFiles': 5}
        result = self.reqDBWriter.updateRequestProperty(schema[0]['RequestName'], totalStats)
        self.assertEqual(result, 'OK', 'update fail')
        
        result = self.reqDBWriter.updateRequestProperty(schema[0]['RequestName'], totalStats)
        self.assertEqual(result, 'OK', 'update fail')
        
        result = self.reqDBWriter.updateRequestProperty("not_exist_schema", totalStats)
        self.assertEqual(result, 'Error: document not found')
        
        spec1 = newWorkload(schema[0]['RequestName'])
        production = spec1.newTask("Production")
        production.setTaskType("Merge")
        production.setSiteWhitelist(['TEST_SITE'])
        properties = {"RequestPriority": spec1.priority(),
                      'SiteWhitelist': spec1.getTopLevelTask()[0].siteWhitelist(),
                      'OutputDatasets': spec1.listOutputDatasets()}
        result = self.reqDBWriter.updateRequestProperty(spec1.name(), properties)
        self.assertEqual(result, 'OK', 'update fail')
        
        spec2 = newWorkload("not_exist_schema")
        production = spec2.newTask("Production")
        production.setTaskType("Merge")
        properties = {"RequestPriority": spec2.priority(),
                      'SiteWhitelist': spec2.getTopLevelTask()[0].siteWhitelist(),
                      'OutputDatasets': spec2.listOutputDatasets()}
        result = self.reqDBWriter.updateRequestProperty(spec2.name(), properties)
        self.assertEqual(result, 'Error: document not found')

        requests = self.wmstatsReader.getRequestByStatus(["failed"], jobInfoFlag = False, legacyFormat = True)
        self.assertEqual(requests.keys(), [schema[0]['RequestName']])
        
        requestCollection = RequestInfoCollection(requests)
        result = requestCollection.getJSONData()
        self.assertEqual(result.keys(), [schema[0]['RequestName']])
        
        requests = self.wmstatsReader.getActiveData()
        self.assertEqual(requests.keys(), [schema[0]['RequestName']])
        requests = self.wmstatsReader.getRequestByStatus(["failed"])
        self.assertEqual(requests.keys(), [schema[0]['RequestName']])
        
        requests = self.wmstatsReader.getRequestSummaryWithJobInfo(schema[0]['RequestName'])
        self.assertEqual(requests.keys(), [schema[0]['RequestName']])
        

if __name__ == '__main__':

    unittest.main()
