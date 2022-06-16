#!/usr/bin/env python
"""
_Couchapp_


This is a general unittest for me should I need to create and
test couchapps.  Its utility is probably limited for people who
know what they're doing, and for couchapps that will never need
to be altered
"""
import os
import threading
import unittest

from WMCore_t.WMSpec_t.TestSpec import testWorkload

import WMCore.WMBase
from WMCore.DataStructs.Run import Run
from WMCore.Database.CMSCouch import CouchServer
from WMCore.FwkJobReport.Report import Report
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class CouchappTest(unittest.TestCase):

    def setUp(self):
        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.databaseName = "couchapp_t_0"

        # Setup config for couch connections
        config = self.testInit.getConfiguration()

        self.testInit.setupCouch(self.databaseName, "WorkloadSummary")
        self.testInit.setupCouch("%s/jobs" % config.JobStateMachine.couchDBName, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % config.JobStateMachine.couchDBName, "FWJRDump")
        self.testInit.setupCouch(config.JobStateMachine.summaryStatsDBName, "SummaryStats")

        # Create couch server and connect to databases
        self.couchdb = CouchServer(config.JobStateMachine.couchurl)
        self.jobsdatabase = self.couchdb.connectDatabase("%s/jobs" % config.JobStateMachine.couchDBName)
        self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % config.JobStateMachine.couchDBName)
        self.statsumdatabase = self.couchdb.connectDatabase(config.JobStateMachine.summaryStatsDBName)

        # Create changeState
        self.changeState = ChangeState(config)
        self.config = config

        # Create testDir
        self.testDir = self.testInit.generateWorkDir()

        return

    def tearDown(self):

        self.testInit.clearDatabase(modules=["WMCore.WMBS"])
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        # self.testInit.tearDownCouch()
        return

    def createWorkload(self, workloadName='Test', emulator=True):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload("Tier1ReReco")

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def createTestJobGroup(self, name="TestWorkthrough",
                           specLocation="spec.xml", error=False,
                           task="/TestWorkload/ReReco", nJobs=10):
        """
        _createTestJobGroup_

        Generate a test WMBS JobGroup with real FWJRs
        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec=specLocation, owner="Simon",
                                name=name, task=task)
        testWorkflow.create()

        testWMBSFileset = Fileset(name=name)
        testWMBSFileset.create()

        testFileA = File(lfn=makeUUID(), size=1024, events=10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn=makeUUID(), size=1024, events=10)
        testFileB.addRun(Run(10, *[12312]))
        testFileB.setLocation('malpaquet')

        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()
        testWMBSFileset.markOpen(0)

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        for i in range(0, nJobs):
            testJob = Job(name=makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJob['mask'].addRunAndLumis(run=10, lumis=[12312, 12313])
            testJobGroup.add(testJob)

        testJobGroup.commit()

        report = Report()
        if error:
            path = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t/fwjrs", "badBackfillJobReport.pkl")
        else:
            path = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t/fwjrs", "PerformanceReport2.pkl")
        report.load(filename=path)

        self.changeState.propagate(testJobGroup.jobs, 'created', 'new')
        self.changeState.propagate(testJobGroup.jobs, 'executing', 'created')
        self.changeState.propagate(testJobGroup.jobs, 'complete', 'executing')
        for job in testJobGroup.jobs:
            job['fwjr'] = report
        self.changeState.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        self.changeState.propagate(testJobGroup.jobs, 'retrydone', 'jobfailed')
        self.changeState.propagate(testJobGroup.jobs, 'exhausted', 'retrydone')
        self.changeState.propagate(testJobGroup.jobs, 'cleanout', 'exhausted')

        testSubscription.completeFiles([testFileA, testFileB])

        return testJobGroup


if __name__ == '__main__':
    unittest.main()
