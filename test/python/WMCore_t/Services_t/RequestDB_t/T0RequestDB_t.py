#!/usr/bin/env python
import os
import unittest
import shutil
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore_t.Services_t.WMStats_t.WMStatsDocGenerator import generate_reqmgr_schema

class T0RequestDBTest(unittest.TestCase):
    """
    """
    def setUp(self):
        """
        _setUp_
        """
        self.schema = []
        self.couchApps = ["T0Request"]
        self.testInit = TestInitCouchApp('RequestDBServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        dbName = 't0_requsetdb_t'
        self.testInit.setupCouch(dbName, *self.couchApps)
        reqDBURL = "%s/%s" % (self.testInit.couchUrl, dbName)
        self.requestWriter = RequestDBWriter(reqDBURL, self.couchApps[0])
        self.requestReader = RequestDBReader(reqDBURL, self.couchApps[0])
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
        
        result = self.requestWriter.updateRequestStatus(schema[0]['RequestName'], "assigned")

        self.assertEquals(result, 'not allowed state assigned', 'update fail')
        self.assertEquals(self.requestWriter.updateRequestStatus("not_exist_schema", "new"),
                          'Error: document not found')
        
        allowedStates = ["Closed", "Merge", "AlcaSkim", "Harvesting",  
                         "Processing Done", "completed"]
        for state in allowedStates:
            self.assertEquals(self.requestWriter.updateRequestStatus(schema[0]['RequestName'], state),
                          'OK')
        
        self.assertEquals(self.requestWriter.updateRequestStatus(schema[0]['RequestName'], "Processing Done"),
                          'not allowed transition completed to Processing Done')  
        
        self.assertEquals(self.requestWriter.updateRequestStatus(schema[0]['RequestName'], "normal-archived"),
                          'OK')  
        result = self.requestWriter.getRequestByStatus(["normal-archived"], False, 1)
        self.assertEquals(len(result), 1, "should be 1 but %s" % result)
      

if __name__ == '__main__':

    unittest.main()
