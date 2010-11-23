#!/usr/bin/env python
"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

import unittest
import os

from WMQuality.TestInit import TestInit

class WorkQueueTestCase(unittest.TestCase):

    def setSchema(self):
        "this can be override if the schema setting is different"
        self.schema = ["WMCore.WMBS","WMComponent.DBSBuffer.Database",
                      "WMCore.WorkQueue.Database"]

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """
        self.setSchema()
        self.testInit = TestInit()
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        
        self.workDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        
    
