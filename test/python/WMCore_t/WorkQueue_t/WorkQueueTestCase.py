#!/usr/bin/env python
"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

import unittest
import os

from WMQuality.TestInit import TestInit
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class WorkQueueTestCase(unittest.TestCase):

    def setSchema(self):
        "this can be override if the schema setting is different"
        self.schema = ["WMCore.WMBS","WMComponent.DBSBuffer.Database"]
        self.couchApps = ["WorkQueue"]

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """
        self.setSchema()
        self.testInit = TestInit('WorkQueueTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        self.testInit.setupCouch('workqueue_t', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_inbox', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_global', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_global_inbox', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_local_inbox', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_local', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_local2_inbox', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_local2', *self.couchApps)
        
        self.workDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        #self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        
    
