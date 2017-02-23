#!/usr/bin/env python
"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

import logging
import os

from WMCore.Database.CMSCouch import CouchMonitor
from WMCore.Database.CMSCouch import CouchServer
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class WorkQueueTestCase(EmulatedUnitTestCase):

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
        super(WorkQueueTestCase, self).setUp()
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
        self.requestDBName = 'workqueue_t_reqmgr_workload_cache'

        self.setSchema()
        self.testInit = TestInit('WorkQueueTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.addCleanup(self.testInit.clearDatabase)
        self.addCleanup(logging.debug, 'Cleanup called clearDatabase()')
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
        self.testInit.setupCouch(self.requestDBName, 'ReqMgr')

        self.couchURL = os.environ.get("COUCHURL")
        couchServer = CouchServer(self.couchURL)
        self.configCacheDBInstance = couchServer.connectDatabase(self.configCacheDB)

        self.localCouchMonitor = CouchMonitor(self.couchURL)
        self.localCouchMonitor.deleteReplicatorDocs()
        self.addCleanup(self.localCouchMonitor.deleteReplicatorDocs)
        self.addCleanup(logging.debug, 'Cleanup called deleteReplicatorDocs()')
        self.addCleanup(self.testInit.tearDownCouch)
        self.addCleanup(logging.debug, 'Cleanup called tearDownCouch()')

        self.workDir = self.testInit.generateWorkDir()
        self.addCleanup(self.testInit.delWorkDir)
        self.addCleanup(logging.debug, 'Cleanup called delWorkDir()')

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """

        super(WorkQueueTestCase, self).tearDown()  # Left here in case it's needed by any of the sub-classes
        return
