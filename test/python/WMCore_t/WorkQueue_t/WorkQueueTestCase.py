#!/usr/bin/env python
"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

import unittest
import os

from WMQuality.TestInitCouchApp import TestInitCouchApp

class WorkQueueTestCase(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("workqueue_t", "JobDump")        
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMComponent.DBSBuffer.Database",
                                                 "WMCore.WorkQueue.Database"],
                                useDefault = False)
        
        self.workDir = self.testInit.generateWorkDir()
        os.environ["COUCHDB"] = "workqueue_t"
        return

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()        
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        
    
