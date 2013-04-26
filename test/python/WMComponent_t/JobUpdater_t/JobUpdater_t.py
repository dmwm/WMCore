"""
__JobUpdater_t__

Test module for the JobUpdater component
Created on Apr 16, 2013

@author: dballest
"""

import os
import unittest
import logging
import threading

from WMComponent.JobUpdater.JobUpdaterPoller import JobUpdaterPoller
from WMCore.Configuration import Configuration
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.WMBase import getTestBase
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class JobUpdaterTest(unittest.TestCase):
    """
    _JobUpdaterTest_

    Test class for the JobUpdater
    """

    def setUp(self):
        """
        _setUp_

        Set up test environment
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMCore.BossAir"],
                                useDefault = False)
        self.testInit.setupCouch('workqueue_t', 'WorkQueue')
        self.testInit.setupCouch('workqueue_inbox_t', 'WorkQueue')
        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction = False)
        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                                    siteDB = True, requestMgr = True)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = logging,
                                     dbinterface = myThread.dbi)
        self.listWorkflows = self.daoFactory(classname = "Workflow.ListForSubmitter")

    def tearDown(self):
        """
        _tearDown_

        Tear down the databases
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        EmulatorHelper.resetEmulators()

    def getConfig(self):
        """
        _getConfig_

        Get a test configuration for
        the JobUpdater tests
        """
        config = Configuration()

        config.section_('Agent')
        config.Agent.agentName = 'testAgent'

        config.section_('CoreDatabase')
        config.CoreDatabase.connectUrl = os.environ['DATABASE']
        config.CoreDatabase.socket = os.getenv('DBSOCK')

        # JobTracker
        config.component_('JobUpdater')
        config.JobUpdater.reqMgrUrl = 'https://cmsweb-dev.cern.ch/reqmgr/reqMgr'

        # JobStateMachine
        config.section_('JobStateMachine')
        config.JobStateMachine.couchDBName = 'bogus'

        # BossAir
        config.section_('BossAir')
        config.BossAir.pluginNames = ['MockPlugin']
        config.BossAir.pluginDir = 'WMCore.BossAir.Plugins'
        config.BossAir.multicoreTaskTypes = ['MultiProcessing', 'MultiProduction']
        config.BossAir.nCondorProcesses = 1
        config.BossAir.section_('MockPlugin')
        config.BossAir.MockPlugin.fakeReport = os.path.join(getTestBase(),
                                                         'WMComponent_t/JobAccountant_t/fwjrs',
                                                         'MergedSkimSuccess.pkl')

        # WorkQueue
        config.component_('WorkQueueManager')
        config.WorkQueueManager.couchurl = os.environ['COUCHURL']
        config.WorkQueueManager.dbname = 'workqueue_t'
        config.WorkQueueManager.inboxDatabase = 'workqueue_inbox_t'

        return config

    def stuffWMBS(self):
        """
        _stuffWMBS_

        Stuff WMBS with workflows
        """
        workflow = Workflow(spec = 'spec.xml', name = 'ReRecoTest_v0Emulator',
                            task = '/ReRecoTest_v0Emulator/Test', priority = 10)
        workflow.create()
        inputFileset = Fileset(name = 'TestFileset')
        inputFileset.create()
        subscription = Subscription(inputFileset, workflow)
        subscription.create()

    def test_BasicTest(self):
        """
        _BasicTest_

        Basic sanity check
        """
        self.stuffWMBS()
        poller = JobUpdaterPoller(self.getConfig())
        poller.reqmgr.getAssignment(self)
        result = self.listWorkflows.execute()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['priority'], 10)
        poller.algorithm()
        result = self.listWorkflows.execute()
        self.assertEqual(result[0]['priority'], 100)
