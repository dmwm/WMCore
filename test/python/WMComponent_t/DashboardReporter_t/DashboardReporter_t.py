#!/usr/bin/env python

import os
import time
import os.path
import unittest
import logging
import socket
import threading
import inspect

import WMCore.WMBase

from WMCore.WMBS.Subscription   import Subscription
from WMCore.WMBS.Workflow       import Workflow
from WMCore.WMBS.File           import File
from WMCore.WMBS.Fileset        import Fileset
from WMCore.WMBS.Job            import Job
from WMCore.WMBS.JobGroup       import JobGroup
from WMCore.DataStructs.Run     import Run

from WMQuality.TestInitCouchApp             import TestInitCouchApp as TestInit
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Agent.Configuration             import Configuration
from WMCore.WMSpec.Makers.TaskMaker         import TaskMaker
from WMCore.Services.UUID                   import makeUUID
from WMCore.JobStateMachine.ChangeState     import ChangeState
from WMCore.FwkJobReport.Report             import Report

from WMComponent.DashboardReporter.DashboardReporterPoller import DashboardReporterPoller

from WMCore_t.WMSpec_t.TestSpec         import testWorkload
from WMComponent_t.AlertGenerator_t.Pollers_t import utils



class DashboardReporterTest(unittest.TestCase):
    """
    _DashboardReporterTest_

    Test class for dashboardReporter
    """

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        
        myThread = threading.currentThread()

        self.sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ['WMCore.WMBS', 
                                                 'WMCore.ResourceControl',
                                                 'WMCore.Agent.Database'], useDefault = False)
        self.testInit.setupCouch("dashboardreporter_t/jobs", "JobDump")
        self.testInit.setupCouch("dashboardreporter_t/fwjrs", "FWJRDump")


        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName = site, seName = site, ceName = site)
            resourceControl.insertThreshold(siteName = site, taskType = 'Processing', \
                                            maxSlots = 10000)

        self.testDir = self.testInit.generateWorkDir()
        self.alertsReceiver = None
        return

    def tearDown(self):
        """
        _tearDown_
        
        Rip things down.
        
        """
        self.testInit.clearDatabase()        
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        if self.alertsReceiver:
            self.alertsReceiver.shutdown()        
        return


    def getConfig(self):
        """
        _getConfig_

        Creates a common config.
        """

        config = Configuration()

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        config.section_("Agent")
        config.Agent.componentName   = "DashboardReporter"
        config.Agent.useHeartbeat    = False

        config.section_("DashboardReporter")
        config.DashboardReporter.dashboardHost = "cmssrv52.fnal.gov"
        config.DashboardReporter.dashboardPort = 8884

        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'cmssrv52.fnal.gov:5984')
        config.JobStateMachine.couchDBName     = "dashboardreporter_t"
        
        # addition for Alerts messaging framework, work (alerts) and control
        # channel addresses to which the component will be sending alerts
        # these are destination addresses where AlertProcessor:Receiver listens
        config.section_("Alert")
        config.Alert.address = "tcp://127.0.0.1:5557"
        config.Alert.controlAddr = "tcp://127.0.0.1:5559"        

        return config


    def createWorkload(self, workloadName = 'Test', emulator = True):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload("Tier1ReReco")
        rereco = workload.getTask("ReReco")

        # Add RequestManager stuff
        workload.data.request.section_('schema')
        workload.data.request.schema.Requestor = 'nobody'
        workload.data.request.schema.Group     = 'testers'
        
        
        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def createTestJobGroup(self, nJobs = 10, retry_count = 0, workloadPath = 'test'):
        """
        Creates a group of several jobs
        """

        


        myThread = threading.currentThread()
        myThread.transaction.begin()
        testWorkflow = Workflow(spec = workloadPath, owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFile0 = File(lfn = "/this/is/a/parent", size = 1024, events = 10)
        testFile0.addRun(Run(10, *[12312]))
        testFile0.setLocation('malpaquet')

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileB.setLocation('malpaquet')

        testFile0.create()
        testFileA.create()
        testFileB.create()

        testFileA.addParent(lfn = "/this/is/a/parent")
        testFileB.addParent(lfn = "/this/is/a/parent")

        for i in range(0, nJobs):
            testJob = Job(name = makeUUID())
            testJob['retry_count'] = retry_count
            testJob['retry_max'] = 10
            testJob['group'] = 'BadGuys'
            testJob['user']  = 'amoron'
            testJob['taskType'] = 'Merge'
            #testJob['fwjr'] = myReport
            testJobGroup.add(testJob)
            testJob.create(group = testJobGroup)
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob.save()

        
        testJobGroup.commit()


        testSubscription.acquireFiles(files = [testFileA, testFileB])
        testSubscription.save()
        myThread.transaction.commit()
        
        return testJobGroup



    def testA_testSubmit(self):
        """
        _testSubmit_

        Test whether we pick up submitted jobs
        """

        #workload = self.createWorkload()
        jobGroup = self.createTestJobGroup()
        config   = self.getConfig()

        xmlPath = os.path.join(WMCore.WMBase.getTestBase(),
                               "WMCore_t/FwkJobReport_t/PerformanceReport.xml")
        myReport = Report("cmsRun1")
        myReport.parse(xmlPath)

        changer = ChangeState(config)
        for job in jobGroup.jobs:
            job['fwjr'] = myReport
        changer.propagate(jobGroup.jobs, "complete", "executing")
        changer.propagate(jobGroup.jobs, "success", "complete")

        dashboardReporter = DashboardReporterPoller(config = config)

        dashboardReporter.algorithm()

        # What the hell am I supposed to check?
        changer.propagate(jobGroup.jobs, 'jobfailed', 'executing')

        dashboardReporter.algorithm()

        return

    def testB_CheckExecutingJobsAndProfile(self):
        """
        _CheckExecutingJobsAndProfile_
        
        Pull up some executing jobs and profile them.
        """
        return
        jobGroup = self.createTestJobGroup()
        config   = self.getConfig()

        changer = ChangeState(config)
        changer.propagate(jobGroup.jobs, "executing", "created")

        dashboardReporter = DashboardReporterPoller(config = config)
        import cProfile, pstats
        cProfile.runctx("dashboardReporter.algorithm()", globals(), locals(), filename = "testStats.stat")

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(.2)
        #dashboardReporter.algorithm()

        return
    

    def testC_DashboardReporterPollerAlertSending_algorithm(self):
        """
        Cause exception (alert-worthy situation) in the algorithm()
        method.
        
        """
        myThread = threading.currentThread()
        config = self.getConfig()
        
        handler, self.alertsReceiver = \
            utils.setUpReceiver(config.Alert.address, config.Alert.controlAddr)

        # emulate exception behaviour
        def raiseException():
            raise Exception("My test exception.")
            
        dashboardReporter = DashboardReporterPoller(config = config)
        dashboardReporter.pollCouch = raiseException
        self.assertRaises(Exception, dashboardReporter.algorithm)
        # wait for the generated alert to arrive
        while len(handler.queue) == 0:
            time.sleep(0.3)
            print "%s waiting for alert to arrive ..." % inspect.stack()[0][3]
            
        self.alertsReceiver.shutdown()
        self.alertsReceiver = None
        # now check if the alert was properly sent
        self.assertEqual(len(handler.queue), 1)
        alert = handler.queue[0]
        self.assertEqual(alert["Source"], dashboardReporter.__class__.__name__)
        self.assertEqual(alert["Component"], "DashboardReporter")
        return


if __name__ == '__main__':
    unittest.main()
