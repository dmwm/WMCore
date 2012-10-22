#!/usr/bin/env python
#pylint: disable=C0103
"""
Component test TestComponent module and the harness
using CouchDB backend.
"""
import os
import threading
import unittest

from WMCore_t.Agent_t.TestComponent import TestComponent
from WMCore_t.Agent_t.TestComponentPoller import TestComponentPoller
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer

class HarnessCouchBackend_t(unittest.TestCase):
    """
    TestCase for TestComponent module
    """
    def setUp(self):
        """
        setup for test.
        """
        self.myThread = threading.currentThread()
        self.database_interface = None
        if hasattr(self.myThread, 'dbi'):
            self.database_interface = self.myThread.dbi
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.config = self.getConfig()
        self.testInit.setupCouch("agent_database", "Agent")

        # Connect to db
        self.agent_server = CouchServer( os.getenv("COUCHURL") )
        self.agent_db = self.agent_server.connectDatabase( "agent_database" )

    def tearDown(self):
        """
        Delete database
        """
        self.testInit.tearDownCouch(  )
        if  self.database_interface:
            self.myThread.dbi = self.database_interface

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration( connectUrl = os.getenv("COUCHURL") + "/agent_database" )
        return config

    def testAddComponent(self):
        """
        Test if the component is added to DB.
        """
        config = self.config
        self.tempDir = self.testInit.generateWorkDir(config)
        config.component_("TestComponent")
        config.TestComponent.logLevel = 'INFO'
        config.section_("General")
        config.TestComponent.componentDir = os.path.join( \
                                self.tempDir, "Components/TestComponent1")
        config.General.workDir = config.TestComponent.componentDir
        os.makedirs( config.TestComponent.componentDir )
        testComponent = TestComponent(config)
        testComponent.prepareToStart()
        query = {'key':"TestComponent"}
        workers = self.agent_db.loadView('Agent', 'existWorkers', query)['rows']
        assert len(workers) == 1

    def testAddWorker(self):
        """
        Test if the a component worked is added to DB.
        """
        config = self.config
        self.tempDir = self.testInit.generateWorkDir(config)
        config.component_("TestComponent")
        config.TestComponent.logLevel = 'INFO'
        config.section_("General")
        config.TestComponent.componentDir = os.path.join( \
                               self.tempDir, "Components/TestComponent1")
        config.General.workDir = config.TestComponent.componentDir
        os.makedirs( config.TestComponent.componentDir )
        testComponent = TestComponent(config)
        testComponent.prepareToStart()
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(TestComponentPoller(config),
                                               10)
        myThread.workerThreadManager.terminateWorkers()
        query = {'key':"TestComponent"}
        workers = self.agent_db.loadView('Agent', 'existWorkers', query)['rows']
        assert workers[0]['value'].has_key('TestComponentPoller') == True

if __name__ == '__main__':
    unittest.main()
