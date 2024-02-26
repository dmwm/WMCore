#!/usr/bin/env python

import os
import threading
import unittest

from WMCore_t.WMSpec_t.TestSpec import createTestWorkload
from WMComponent.WorkflowUpdater.WorkflowUpdaterPoller import WorkflowUpdaterPoller
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class WorkflowUpdaterTest(EmulatedUnitTestCase):
    """
    Test case for the WorkflowUpdater
    """

    def setUp(self):
        """
        Setup the database and logging connection. Try to create all of the
        WMBS tables.
        """
        super(WorkflowUpdaterTest, self).setUp()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)

        self.testInit.setSchema(customModules=['WMCore.WMBS', 'WMCore.Agent.Database'],
                                useDefault=False)

        self.configFile = EmulatorSetup.setupWMAgentConfig()

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listActiveWflows = self.daoFactory(classname="Workflow.GetUnfinishedWorkflows")

        self.testDir = self.testInit.generateWorkDir()
        self.cwd = os.getcwd()

        # Set heartbeat
        self.componentName = 'WorkflowUpdater'
        self.heartbeatAPI = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()

        # don't execute tearDown because it hangs
        # self._teardown = False

        return

    def tearDown(self):
        """
        Drop all of the non-sql and sql databases
        """
        self.testInit.clearDatabase(modules=['WMCore.WMBS', 'WMCore.Agent.Database'])
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorSetup.deleteConfig(self.configFile)

    def getConfig(self):
        """
        Creates a common config.
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())
        config.section_("Agent")
        config.Agent.componentName = self.componentName

        # Now the CoreDatabase information
        # This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        config.component_("WorkflowUpdater")
        config.WorkflowUpdater.namespace = 'WMComponent.WorkflowUpdater.WorkflowUpdater'
        config.WorkflowUpdater.logLevel = 'INFO'
        config.WorkflowUpdater.componentDir = self.testDir
        config.WorkflowUpdater.rucioAccount = "wma_test"
        config.WorkflowUpdater.rucioUrl = "http://cms-rucio-int.cern.ch"
        config.WorkflowUpdater.rucioAuthUrl = "https://cms-rucio-auth-int.cern.ch"
        config.WorkflowUpdater.msPileupUrl = "https://cmsweb-testbed.cern.ch/ms-pileup/data/pileup"
        config.WorkflowUpdater.sandboxDir = "/tmp"

        return config

    def createWorkload(self, workloadName='Test'):
        """
        Creates a test workload for us to run on.
        """
        workload = createTestWorkload(workloadName)
        rereco = workload.getTask("ReReco")
        seederDict = {"generator.initialSeed": 1001, "evtgenproducer.initialSeed": 1001}
        rereco.addGenerator("PresetSeeder", **seederDict)

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        # taskMaker.skipSubscription = False
        taskMaker.processWorkload()

        return workload

    def stuffWMBS(self, workflowURL, name):
        """
        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test job creation.  Three completed job groups each
        containing several files are injected.  Another incomplete job group is
        also injected.  Also files are added to the "Mergeable" subscription as
        well as to the output fileset for their jobgroups.
        """
        testSite = "T2_CH_CERN"
        locationAction = self.daoFactory(classname="Locations.New")
        locationAction.execute(siteName=testSite, pnn=testSite)

        mergeFileset = Fileset(name="mergeFileset")
        mergeFileset.create()
        bogusFileset = Fileset(name="bogusFileset")
        bogusFileset.create()

        mergeWorkflow = Workflow(spec=workflowURL, name=name, task="/TestWorkload/ReReco")
        mergeWorkflow.create()

        mergeSubscription = Subscription(fileset=mergeFileset,
                                         workflow=mergeWorkflow,
                                         split_algo="ParentlessMergeBySize")
        mergeSubscription.create()
        dummySubscription = Subscription(fileset=bogusFileset,
                                         workflow=mergeWorkflow,
                                         split_algo="ParentlessMergeBySize")

        fileObjList = []
        for idx, fName in enumerate(["file1", "file2", "file3", "file4"]):
            testFile = File(lfn=fName, size=1024, events=1024, first_event=1024 * idx, locations={testSite})
            testFile.addRun(Run(1, *[45]))
            testFile.create()
            fileObjList.append(testFile)
        for idx, fName in enumerate(["fileA", "fileB", "fileC"]):
            testFile = File(lfn=fName, size=1024, events=1024, first_event=1024 * idx, locations={testSite})
            testFile.addRun(Run(1, *[46]))
            testFile.create()
            fileObjList.append(testFile)
        for idx, fName in enumerate(["fileI", "fileII", "fileIII", "fileIV"]):
            testFile = File(lfn=fName, size=1024, events=1024, first_event=1024 * idx, locations={testSite})
            testFile.addRun(Run(2, *[46]))
            testFile.create()
            fileObjList.append(testFile)

        for fileObj in fileObjList:
            mergeFileset.addFile(fileObj)
            bogusFileset.addFile(fileObj)

        mergeFileset.commit()
        bogusFileset.commit()

        return

    def testVerySimpleTest(self):
        """
        Inject a workflow into the database and run WorkflowUpdater algorithm
        """
        # populate the database
        workloadName = 'TestWorkload'
        _dummyWorkload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')
        self.stuffWMBS(workflowURL=workloadPath, name=makeUUID())

        myThread = threading.currentThread()

        config = self.getConfig()
        testWflowUpdater = WorkflowUpdaterPoller(config=config)
        testWflowUpdater.algorithm()
        # self.assertFalse(True)


if __name__ == "__main__":
    unittest.main()
