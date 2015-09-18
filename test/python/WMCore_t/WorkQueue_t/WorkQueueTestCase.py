#!/usr/bin/env python
"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

import unittest
import os

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Database.CMSCouch import CouchMonitor

from WMQuality.TestInit import TestInit
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class WorkQueueTestCase(unittest.TestCase):

    def setSchema(self):
        "this can be override if the schema setting is different"
        self.schema = ["WMCore.WMBS","WMComponent.DBS3Buffer","WMCore.BossAir"]
        self.couchApps = ["WorkQueue"]

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """
        self.queueDB = 'workqueue_t'
        self.queueInboxDB = 'workqueue_t_inbox'
        self.globalQDB = 'workqueue_t_global'
        self.globalQInboxDB = 'workqueue_t_global_inbox'
        self.localQDB = 'workqueue_t_local'
        self.localQInboxDB = 'workqueue_t_local_inbox'
        self.localQDB2 = 'workqueue_t_local2'
        self.localQInboxDB2 = 'workqueue_t_local2_inbox'
        self.configCacheDB = 'workqueue_t_config_cache'
        self.logDBName = 'logdb_t'
        
        self.setSchema()
        self.testInit = TestInit('WorkQueueTest')
        self.testInit.setLogging()
        self.testDB = 'unittest_%s' % self.__class__.__name__
        self.testInit.prepareDatabase(self.testDB)
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        self.testInit.setupCouch(self.queueDB, *self.couchApps)
        self.testInit.setupCouch(self.queueInboxDB, *self.couchApps)
        self.testInit.setupCouch(self.globalQDB, *self.couchApps)
        self.testInit.setupCouch(self.globalQInboxDB , *self.couchApps)
        self.testInit.setupCouch(self.localQDB, *self.couchApps)
        self.testInit.setupCouch(self.localQInboxDB, *self.couchApps)
        self.testInit.setupCouch(self.localQDB2, *self.couchApps)
        self.testInit.setupCouch(self.localQInboxDB2, *self.couchApps)
        self.testInit.setupCouch(self.configCacheDB, 'ConfigCache')
        self.testInit.setupCouch(self.logDBName, 'LogDB')
        

        couchServer = CouchServer(os.environ.get("COUCHURL"))
        self.configCacheDBInstance = couchServer.connectDatabase(self.configCacheDB)
        
        self.localCouchMonitor = CouchMonitor(os.environ.get("COUCHURL"))
    
        self.workDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        #self.localCouchMonitor.deleteReplicatorDocs()
        self.testInit.tearDownCouch()
        self.testInit.destroyDatabase(self.testDB)
        self.testInit.delWorkDir()
