#!/usr/bin/env python
"""
TaskArchiver test

Tests both the archiving of tasks and the creation of the
workloadSummary
"""

import os
import os.path
import logging
import threading
import unittest
import time
import shutil

import WMCore.WMInit

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
#from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.WMFactory     import WMFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job
from WMCore.DataStructs.Run   import Run
from WMCore.Lexicon           import sanitizeURL

from WMComponent.TaskArchiver.TaskArchiver       import TaskArchiver
from WMComponent.TaskArchiver.TaskArchiverPoller import TaskArchiverPoller

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.FwkJobReport.Report         import Report
from WMCore.Database.CMSCouch           import CouchServer

from WMCore_t.WMSpec_t.TestSpec     import testWorkload
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker

from WMComponent_t.AlertGenerator_t.Pollers_t import utils



class TaskArchiverTest(unittest.TestCase):
    """
    TestCase for TestTaskArchiver module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.databaseName = "taskarchiver_t_0"
        self.testInit.setupCouch(self.databaseName, "WorkloadSummary")
        self.testInit.setupCouch("%s/jobs" % self.databaseName, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % self.databaseName, "FWJRDump")
        

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        self.testDir = self.testInit.generateWorkDir()
        os.makedirs(os.path.join(self.testDir, 'specDir'))


        self.nJobs = 10
        self.campaignName = 'aCampaign'
        self.alertsReceiver = None
        return

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()

        self.testInit.clearDatabase(modules = ["WMCore.WMBS"])
        self.testInit.delWorkDir()
        #self.testInit.tearDownCouch()
        if self.alertsReceiver:
            self.alertsReceiver.shutdown()
            self.alertsReceiver = None        
        return

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()
        #self.testInit.generateWorkDir(config)

        config.section_("General")
        config.General.workDir = "."

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl     = os.getenv("COUCHURL", "cmssrv52.fnal.gov:5984")
        config.JobStateMachine.couchDBName  = self.databaseName

        config.component_("JobCreator")
        config.JobCreator.jobCacheDir       = os.path.join(self.testDir, 'testDir')

        config.component_("TaskArchiver")
        config.TaskArchiver.componentDir    = self.testDir
        config.TaskArchiver.WorkQueueParams = {}
        config.TaskArchiver.pollInterval    = 60
        config.TaskArchiver.logLevel        = 'SQLDEBUG'
        config.TaskArchiver.timeOut         = 0
        config.TaskArchiver.histogramKeys   = ['AvgEventTime', 'writeTotalMB']
        config.TaskArchiver.histogramBins   = 5
        config.TaskArchiver.histogramLimit  = 5
        config.TaskArchiver.summaryDBName   = self.databaseName

        config.section_("ACDC")
        config.ACDC.couchurl                = config.JobStateMachine.couchurl
        config.ACDC.database                = config.JobStateMachine.couchDBName

        # Make the jobCacheDir
        os.mkdir(config.JobCreator.jobCacheDir)
        
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
        
        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.setCampaign(self.campaignName)

        workload.save(workloadName)

        return workload
        
        

    def createTestJobGroup(self, config, name = "TestWorkthrough",
                           specLocation = "spec.xml", error = False,
                           task = "/TestWorkload/ReReco"):
        """
        Creates a group of several jobs

        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec = specLocation, owner = "Simon",
                                name = name, task = task)
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = name)
        testWMBSFileset.create()

        testFileA = File(lfn = "/this/is/a/lfnA" , size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileB.setLocation('malpaquet')
        
        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()
        testWMBSFileset.markOpen(0)
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        for i in range(0,self.nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJob['mask'].addRunAndLumis(run = 10, lumis = [12312, 12313])
            testJobGroup.add(testJob)
        
        testJobGroup.commit()

        changer = ChangeState(config)

        report1 = Report()
        report2 = Report()
        if error:
            path1 = os.path.join(WMCore.WMInit.getWMTESTBASE(),
                                 "test/python/WMComponent_t/JobAccountant_t/fwjrs", "badBackfillJobReport.pkl")
            path2 = path1
        else:
            path1 = os.path.join(WMCore.WMInit.getWMTESTBASE(),
                                 'test/python/WMComponent_t/TaskArchiver_t/fwjrs',
                                 'mergeReport1.pkl')
            path2 = os.path.join(WMCore.WMInit.getWMTESTBASE(),
                                 'test/python/WMComponent_t/TaskArchiver_t/fwjrs',
                                 'mergeReport2.pkl')
        report1.load(filename = path1)
        report2.load(filename = path2)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        for i in range(self.nJobs):
            if i < self.nJobs/2:
                testJobGroup.jobs[i]['fwjr'] = report1
            else:
                testJobGroup.jobs[i]['fwjr'] = report2
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'exhausted', 'jobfailed')
        changer.propagate(testJobGroup.jobs, 'cleanout', 'exhausted')

        testSubscription.completeFiles([testFileA, testFileB])

        return testJobGroup


    def createGiantJobSet(self, name, config, nSubs = 10, nJobs = 10,
                          nFiles = 1, spec = "spec.xml"):
        """
        Creates a massive set of jobs

        """


        jobList = []

        

        for i in range(0, nSubs):
            # Make a bunch of subscriptions
            localName = '%s-%i' % (name, i)
            testWorkflow = Workflow(spec = spec, owner = "Simon",
                                    name = localName, task="Test")
            testWorkflow.create()
            
            testWMBSFileset = Fileset(name = localName)
            testWMBSFileset.create()    


            testSubscription = Subscription(fileset = testWMBSFileset,
                                            workflow = testWorkflow)
            testSubscription.create()
            
            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()

            filesToComplete = []

            for j in range(0, nJobs):
                # Create jobs for each subscription
                testFileA = File(lfn = "%s-%i-lfnA" % (localName, j) , size = 1024, events = 10)
                testFileA.addRun(Run(10, *[11,12,13,14,15,16,17,18,19,20,
                                           21,22,23,24,25,26,27,28,29,30,
                                           31,32,33,34,35,36,37,38,39,40]))
                testFileA.setLocation('malpaquet')
                testFileA.create()

                testWMBSFileset.addFile(testFileA)
                testWMBSFileset.commit()

                filesToComplete.append(testFileA)
                
                testJob = Job(name = '%s-%i' % (localName, j))
                testJob.addFile(testFileA)
                testJob['retry_count'] = 1
                testJob['retry_max'] = 10
                testJobGroup.add(testJob)
                jobList.append(testJob)

                for k in range(0, nFiles):
                    # Create output files
                    testFile = File(lfn = "%s-%i-output" % (localName, k) , size = 1024, events = 10)
                    testFile.addRun(Run(10, *[12312]))
                    testFile.setLocation('malpaquet')
                    testFile.create()

                    testJobGroup.output.addFile(testFile)

                testJobGroup.output.commit()


            testJobGroup.commit()

            changer = ChangeState(config)

            changer.propagate(testJobGroup.jobs, 'created', 'new')
            changer.propagate(testJobGroup.jobs, 'executing', 'created')
            changer.propagate(testJobGroup.jobs, 'complete', 'executing')
            changer.propagate(testJobGroup.jobs, 'success', 'complete')
            changer.propagate(testJobGroup.jobs, 'cleanout', 'success')

            testWMBSFileset.markOpen(0)

            testSubscription.completeFiles(filesToComplete)


        return jobList
            

    def testA_BasicFunctionTest(self):
        """
        _BasicFunctionTest_
        
        Tests the components, by seeing if they can process a simple set of closeouts
        """

        myThread = threading.currentThread()

        config = self.getConfig()
        workloadPath = os.path.join(self.testDir, 'specDir', 'spec.pkl')
        workload     = self.createWorkload(workloadName = workloadPath)
        testJobGroup = self.createTestJobGroup(config = config,
                                               name = workload.name(),
                                               specLocation = workloadPath,
                                               error = False)

        # Create second workload
        testJobGroup2 = self.createTestJobGroup(config = config,
                                                name = "%s_2" % workload.name(),
                                                specLocation = workloadPath,
                                                task = "/TestWorkload/ReReco/LogCollect")

        cachePath = os.path.join(config.JobCreator.jobCacheDir,
                                 "TestWorkload", "ReReco")
        os.makedirs(cachePath)
        self.assertTrue(os.path.exists(cachePath))

        cachePath2 = os.path.join(config.JobCreator.jobCacheDir,
                                 "TestWorkload", "LogCollect")
        os.makedirs(cachePath2)
        self.assertTrue(os.path.exists(cachePath2))


        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 2)

        testTaskArchiver = TaskArchiverPoller(config = config)
        testTaskArchiver.algorithm()

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_fileset")[0].fetchall()
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)

        # Make sure we deleted the directory
        self.assertFalse(os.path.exists(cachePath))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'workloadTest/TestWorkload')))

        testWMBSFileset = Fileset(id = 1)
        self.assertEqual(testWMBSFileset.exists(), False)

        dbname       = getattr(config.JobStateMachine, "couchDBName")
        couchdb      = CouchServer(config.JobStateMachine.couchurl)
        workdatabase = couchdb.connectDatabase(dbname)

        workloadSummary = workdatabase.document(id = "TestWorkload")
        # Check ACDC
        self.assertEqual(workloadSummary['ACDCServer'], sanitizeURL(config.ACDC.couchurl)['url'])

        # Check the output
        self.assertEqual(workloadSummary['output'].keys(), ['/Electron/MorePenguins-v0/RECO',
                                                            '/Electron/MorePenguins-v0/ALCARECO'])

        # Check performance
        # Check histograms
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['histogram'][0]['average'],
                         0.062651899999999996)
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['histogram'][0]['nEvents'],
                         5)

        # Check standard performance
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['TotalJobCPU']['average'], 9.4950600000000005)
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['TotalJobCPU']['stdDev'], 8.2912400000000002)

        # Check worstOffenders
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['worstOffenders'],
                         [{'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 1},
                          {'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 2},
                          {'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 3}])
        
        # LogCollect task is made out of identical FWJRs
        # assert that it is identical
        for x in workloadSummary['performance']['/TestWorkload/ReReco/LogCollect']['cmsRun1'].keys():
            if x in config.TaskArchiver.histogramKeys:
                continue
            for y in ['average', 'stdDev']:
                self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco/LogCollect']['cmsRun1'][x][y],
                                 workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1'][x][y])

        return

    def atestB_testErrors(self):
        """
        _testErrors_

        Test with a failed FWJR
        """

        myThread = threading.currentThread()

        config = self.getConfig()
        workloadPath = os.path.join(self.testDir, 'specDir', 'spec.pkl')
        workload     = self.createWorkload(workloadName = workloadPath)
        testJobGroup = self.createTestJobGroup(config = config,
                                               name = workload.name(),
                                               specLocation = workloadPath,
                                               error = True)

        cachePath = os.path.join(config.JobCreator.jobCacheDir,
                                 "TestWorkload", "ReReco")
        os.makedirs(cachePath)
        self.assertTrue(os.path.exists(cachePath))

        testTaskArchiver = TaskArchiverPoller(config = config)
        testTaskArchiver.algorithm()

        dbname       = getattr(config.JobStateMachine, "couchDBName")
        couchdb      = CouchServer(config.JobStateMachine.couchurl)
        workdatabase = couchdb.connectDatabase(dbname)

        workloadSummary = workdatabase.document(id = "TestWorkload")

        self.assertEqual(workloadSummary['/TestWorkload/ReReco']['failureTime'], 500)
        self.assertTrue(workloadSummary['/TestWorkload/ReReco']['cmsRun1'].has_key('99999'))
        return


    def atestC_Profile(self):
        """
        _Profile_
        
        DON'T RUN THIS!
        """

        import cProfile, pstats

        return

        myThread = threading.currentThread()

        name    = makeUUID()

        config = self.getConfig()

        jobList = self.createGiantJobSet(name = name, config = config,
                                         nSubs = 10, nJobs = 1000, nFiles = 10)

        testTaskArchiver = TaskArchiverPoller(config = config)

        
        cProfile.runctx("testTaskArchiver.algorithm()", globals(), locals(), filename = "testStats.stat")

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()



        return



    def atestD_Timing(self):
        """
        _Timing_

        This is to see how fast things go.
        """

        return

        myThread = threading.currentThread()

        name    = makeUUID()

        config  = self.getConfig()
        jobList = self.createGiantJobSet(name = name, config = config, nSubs = 10,
                                         nJobs = 1000, nFiles = 10)


        testTaskArchiver = TaskArchiverPoller(config = config)

        startTime = time.time()
        testTaskArchiver.algorithm()
        stopTime  = time.time()


        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)
        testWMBSFileset = Fileset(id = 1)
        self.assertEqual(testWMBSFileset.exists(), False)


        logging.info("TaskArchiver took %f seconds" % (stopTime - startTime))
        
                
    def atestTaskArchiverPollerAlertsSending_notifyWorkQueue(self):
        """
        Cause exception (alert-worthy situation) in
        the TaskArchiverPoller notifyWorkQueue method.
        
        """
        myThread = threading.currentThread()
        config = self.getConfig()
        testTaskArchiver = TaskArchiverPoller(config = config)

        # shall later be called directly from utils module
        handler, self.alertsReceiver = \
            utils.setUpReceiver(config.Alert.address, config.Alert.controlAddr)
        
        # prepare input such input which will go until where it expectantly
        # fails and shall send an alert
        # this will currently fail in the TaskArchiverPoller killSubscriptions
        # on trying to access .load() method which items of below don't have.
        # should anything change in the TaskArchiverPoller without modifying this
        # test accordingly, it may be failing ...
        print "failures 'AttributeError: 'dict' object has no attribute 'load' expected ..."
        subList = [{'id': 1}, {'id': 2}, {'id': 3}]
        testTaskArchiver.notifyWorkQueue(subList)
        self.alertsReceiver.shutdown()
        self.alertsReceiver = None
        # now check if the alert was properly sent (expect this many failures)    
        self.assertEqual(len(handler.queue), len(subList))
        alert = handler.queue[0]
        self.assertEqual(alert["Source"], "TaskArchiverPoller")
        
    
    def atestTaskArchiverPollerAlertsSending_killSubscriptions(self):
        """
        Cause exception (alert-worthy situation) in
        the TaskArchiverPoller killSubscriptions method.
        (only 1 situation out of two tested).
        
        """
        myThread = threading.currentThread()
        config = self.getConfig()
        testTaskArchiver = TaskArchiverPoller(config = config)

        # shall later be called directly from utils module
        handler, self.alertsReceiver = \
            utils.setUpReceiver(config.Alert.address, config.Alert.controlAddr)

        # will fail on calling .load() - regardless, the same except block
        numAlerts = 3
        doneList = [{'id': x} for x in range(numAlerts)]
        # final re-raise is currently commented, so don't expect Exception here
        testTaskArchiver.killSubscriptions(doneList)
        
        self.alertsReceiver.shutdown()
        self.alertsReceiver = None
        # now check if the alert was properly sent
        self.assertEqual(len(handler.queue), numAlerts)
        alert = handler.queue[0]
        self.assertEqual(alert["Source"], "TaskArchiverPoller")
                
    

if __name__ == '__main__':
    unittest.main()
