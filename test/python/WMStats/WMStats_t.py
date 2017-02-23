#!/usr/bin/env python

from __future__ import print_function
import os
import unittest
import shutil

from WMCore.WMStats.Config import Config
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend
from WMCore.Lexicon import splitCouchServiceURL

class WMStatsTest(RESTBaseUnitTestWithDBBackend):
    """
    """

    def setUp(self):
        """
        setUP global values
        """
        appport = 19888
        config = TestConfig(appport, os.getenv("COUCHURL"), False);
        self.setConfig(config)
        reqmgrCouchDB = splitCouchServiceURL(config.views.wmstats.reqmgrCouchURL)[1]
        wmstatsCouchDB = splitCouchServiceURL(config.views.wmstats.wmstatsCouchURL)[1]
        self.setCouchDBs([(reqmgrCouchDB, "ReqMgr"), (wmstatsCouchDB, "WMStats")])
        self.setSchemaModules([])
        RESTBaseUnitTestWithDBBackend.setUp(self)

    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)

    def testRequest(self):
        # add request related REST API test
        #
        #self.jsonSender.put('request/' + schema['RequestName'], schema)
        #print self.jsonSender.get('request', incoming_headers=self.adminHeader)
        print(self.jsonSender.get('requests'))

if __name__ == '__main__':

    unittest.main()
