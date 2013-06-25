#!/usr/bin/env python

import os
import unittest
import shutil

from WMCore_t.ReqMgr_t.Config import Config
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend

class ReqMgrTest(RESTBaseUnitTestWithDBBackend):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB sets from environment variable.
    Client DB sets from environment variable.

    This checks whether DS call makes without error and return the results.
    Not the correctness of functions. That will be tested in different module.
    """

    def setUp(self):
        """
        setUP global values
        """
        appport = 19888
        config = Config(appport, os.getenv("COUCHURL"), False);
        self.setConfig(config)
        self.setCouchDBs([(config.views.restapihub.couch_reqmgr_db, "ReqMgr")])
        self.setSchemaModules([])
        RESTBaseUnitTestWithDBBackend.setUp(self)
        
    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)

    def testRequest(self):
        # add request related REST API test
        #
        #self.jsonSender.put('request/' + schema['RequestName'], schema) 
        #print self.jsonSender.get('request', incoming_headers=self.adminHeader)
        print self.jsonSender.get('request?statusList=[]')
    
    def atestHello(self):
        print self.jsonSender.get('hello')
        print self.jsonSender.get('hello?name=Tiger')
if __name__ == '__main__':

    unittest.main()
