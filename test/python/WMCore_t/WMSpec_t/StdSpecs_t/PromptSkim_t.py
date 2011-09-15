#!/usr/bin/env python
"""
_PromptSkim_t_

Unit tests for the PromptSkim workflow.
"""

import unittest
import os

from WMCore.WMSpec.StdSpecs.PromptSkim import promptSkimWorkload
from WMCore.WMSpec.StdSpecs.PromptSkim import getTestArguments

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document

class PromptSkimTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_
        
        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("promptskim_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("promptskim_t")
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return

    def DISABLEDtestPromptSkim(self):
        """
        _testPromptSkim_

        Verify that PromptSkim workflows can be created.  Note that this
        requires a system that has all of the cms software installed on it.
        """
        dataProcArguments = getTestArguments()
        dataProcArguments["CouchUrl"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "promptskim_t"

        testWorkload = promptSkimWorkload("TestWorkload", dataProcArguments)
        return

if __name__ == '__main__':
    unittest.main()
