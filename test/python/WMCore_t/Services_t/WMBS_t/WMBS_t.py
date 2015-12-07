#!/usr/bin/env python

import os
import unittest
import shutil

from WMCore.Services.WMBS.WMBS import WMBS
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig


class WorkQueueTest(RESTBaseUnitTest):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB sets from environment variable.
    Client DB sets from environment variable.

    This checks whether DS call makes without error and return the results.
    Not the correctness of functions. That will be tested in different module.
    """
    def initialize(self):
        self.config = DefaultConfig(
                'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel')
        dbUrl = os.environ.get("DATABASE", None)
        self.config.setDBUrl(dbUrl)
        # mysql example
        #self.config.setDBUrl('mysql://username@host.fnal.gov:3306/TestDB')
        #self.config.setDBSocket('/var/lib/mysql/mysql.sock')
        self.schemaModules = ["WMCore.WMBS", "WMCore.ResourceControl", "BossAir"]

    def setUp(self):
        """
        setUP global values
        """
        RESTBaseUnitTest.setUp(self)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()

    def tearDown(self):
        RESTBaseUnitTest.tearDown(self)

    def testWorkQueueService(self):

        # test getWork

        wmbsApi = WMBS(self.params)
        self.assertEqual(wmbsApi.getResourceInfo(), [])
        self.assertEqual(wmbsApi.getResourceInfo(tableFormat = False), {})

if __name__ == '__main__':

    unittest.main()
