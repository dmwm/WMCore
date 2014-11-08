#!/usr/bin/env python
import os
import unittest
import shutil

from WMCore.Wrappers import JsonWrapper
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore_t.Services_t.WMStats_t.WMStatsDocGenerator import generate_reqmgr_schema
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection
class RequestDBTest(unittest.TestCase):
    """
    """
    def setUp(self):
        """
        _setUp_
        """
        self.schema = []
        self.couchApps = ["ReqMgr"]
        self.testInit = TestInitCouchApp('RequestDBServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        dbName = 'requsetdb_t'
        self.testInit.setupCouch(dbName, *self.couchApps)
        self.requestWriter = RequestDBWriter(self.testInit.couchUrl, dbName)
        self.requestReader = RequestDBReader(self.testInit.couchUrl, dbName)
        self.requestWriter.defaultStale = {}
        self.requestReader.defaultStale = {}
        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()

    def testRequestDBWriter(self):
        # test getWork
        schema = generate_reqmgr_schema()
        result =  self.requestWriter.insertGenericRequest(schema[0])

        self.assertEquals(len(result), 1, 'insert fail');
        
        self.assertEquals(self.requestWriter.updateRequestStatus(schema[0]['RequestName'], "failed"), 'OK', 'update fail')
        self.assertEquals(self.requestWriter.updateRequestStatus("not_exist_schema", "assigned"),
                          'Error: document not found')
        result = self.requestWriter.updateRequestProperty(schema[0]['RequestName'], 
                                                                   {'Teams': ['teamA']})
        self.assertEquals(self.requestWriter.updateRequestProperty(schema[0]['RequestName'], 
                                                                   {'Teams': ['teamA']}), 'OK', 'update fail')
        self.assertEquals(self.requestWriter.updateRequestProperty("not_exist_schema", {'Teams': 'teamA'}),
                          'Error: document not found')
        
        result = self.requestWriter.getRequestByNames([schema[0]['RequestName']])
        self.assertEquals(len(result), 1, "should be 1")
        result = self.requestWriter.getRequestByStatus(["failed"], False, 1)
        self.assertEquals(len(result), 1, "should be 1")
      

if __name__ == '__main__':

    unittest.main()