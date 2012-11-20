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
import inspect

from nose.plugins.attrib import attr

import WMCore.WMBase

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

from WMComponent.DBS3Buffer.DBSBufferFile        import DBSBufferFile
from WMComponent.TaskArchiver.TaskArchiver       import TaskArchiver
from WMComponent.TaskArchiver.TaskArchiverPoller import TaskArchiverPoller

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.FwkJobReport.Report         import Report
from WMCore.Database.CMSCouch           import CouchServer, CouchNotFoundError

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
    OWNERDN = os.environ['OWNERDN'] if 'OWNERDN' in os.environ else "Generic/OWNERDN"

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMComponent.DBS3Buffer"],
                                useDefault = False)
        self.databaseName = "taskarchiver_t_0"
        self.testInit.setupCouch("%s/workloadsummary" % self.databaseName, "WorkloadSummary")
        self.testInit.setupCouch("%s/jobs" % self.databaseName, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % self.databaseName, "FWJRDump")
        self.testInit.setupCouch("wmagent_summary_t", "WMStats")
        self.testInit.setupCouch("wmagent_summary_central_t", "WMStats")

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")
        self.inject  = self.daofactory(classname = "Workflow.MarkInjectedWorkflows")

        self.testDir = self.testInit.generateWorkDir()
        os.makedirs(os.path.join(self.testDir, 'specDir'))


        self.nJobs = 10
        self.campaignName = 'aCampaign'
        self.alertsReceiver = None

        self.uploadPublishInfo = False
        self.uploadPublishDir  = None

        return

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()

        self.testInit.clearDatabase(modules = ["WMCore.WMBS"])
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
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
        config.JobStateMachine.jobSummaryDBName = 'wmagent_summary_t'

        config.component_("JobCreator")
        config.JobCreator.jobCacheDir       = os.path.join(self.testDir, 'testDir')

        config.component_("TaskArchiver")
        config.TaskArchiver.componentDir    = self.testDir
        config.TaskArchiver.WorkQueueParams = {}
        config.TaskArchiver.pollInterval    = 60
        config.TaskArchiver.logLevel        = 'INFO'
        config.TaskArchiver.timeOut         = 0
        config.TaskArchiver.histogramKeys   = ['AvgEventTime', 'writeTotalMB']
        config.TaskArchiver.histogramBins   = 5
        config.TaskArchiver.histogramLimit  = 5
        config.TaskArchiver.workloadSummaryCouchDBName = "%s/workloadsummary" % self.databaseName
        config.TaskArchiver.workloadSummaryCouchURL    = config.JobStateMachine.couchurl
        config.TaskArchiver.centralWMStatsURL          = '%s/wmagent_summary_central_t' % config.JobStateMachine.couchurl
        config.TaskArchiver.requireCouch               = True
        config.TaskArchiver.uploadPublishInfo = self.uploadPublishInfo
        config.TaskArchiver.uploadPublishDir  = self.uploadPublishDir
        config.TaskArchiver.userFileCacheURL = os.getenv('UFCURL', 'http://cms-xen38.fnal.gov:7725/userfilecache/')

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

        config.section_("BossAir")
        config.BossAir.UISetupScript = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'
        config.BossAir.gliteConf = '/afs/cern.ch/cms/LCG/LCG-2/UI/conf/glite_wms_CERN.conf'
        config.BossAir.credentialDir = '/home/crab/ALL_SETUP/credentials/'
        config.BossAir.gLiteProcesses = 2
        config.BossAir.gLitePrefixEnv = "/lib64/"
        config.BossAir.pluginNames = ["gLitePlugin"]
        config.BossAir.proxyDir = "/tmp/credentials"
        config.BossAir.manualProxyPath = os.environ['X509_USER_PROXY'] if 'X509_USER_PROXY' in os.environ else None

        config.section_("Agent")
        config.Agent.serverDN = "/we/bypass/myproxy/logon"

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
                           filesetName = "TestFileset",
                           specLocation = "spec.xml", error = False,
                           task = "/TestWorkload/ReReco", multicore = False):
        """
        Creates a group of several jobs

        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec = specLocation, owner = self.OWNERDN,
                                name = name, task = task, owner_vogroup="", owner_vorole="")
        testWorkflow.create()
        self.inject.execute(names = [name], injected = True)

        testWMBSFileset = Fileset(name = filesetName)
        testWMBSFileset.create()

        testFileA = File(lfn = "/this/is/a/lfnA" , size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12314]))
        testFileB.setLocation('malpaquet')

        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()
        testWMBSFileset.markOpen(0)

        outputWMBSFileset = Fileset(name = '%sOutput' % filesetName)
        outputWMBSFileset.create()
        testFileC = File(lfn = "/this/is/a/lfnC" , size = 1024, events = 10)
        testFileC.addRun(Run(10, *[12312]))
        testFileC.setLocation('malpaquet')
        testFileC.create()
        outputWMBSFileset.addFile(testFileC)
        outputWMBSFileset.commit()
        outputWMBSFileset.markOpen(0)

        testWorkflow.addOutput('output', outputWMBSFileset)


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
            path1 = os.path.join(WMCore.WMBase.getTestBase(),
                                 "WMComponent_t/JobAccountant_t/fwjrs", "badBackfillJobReport.pkl")
            path2 = path1
        elif multicore:
            path1 = os.path.join(WMCore.WMBase.getTestBase(),
                                 "WMCore_t/FwkJobReport_t/MulticoreReport.pkl")
            path2 = path1
        else:
            path1 = os.path.join(WMCore.WMBase.getTestBase(),
                                 'WMComponent_t/TaskArchiver_t/fwjrs',
                                 'mergeReport1.pkl')
            path2 = os.path.join(WMCore.WMBase.getTestBase(),
                                 'WMComponent_t/TaskArchiver_t/fwjrs',
                                 'logCollectReport2.pkl')
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
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'jobcooloff')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
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
            testWorkflow = Workflow(spec = spec, owner = self.OWNERDN,
                                    name = localName, task="Test", owner_vogroup="", owner_vorole="")
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
                                                name = workload.name(),
                                                filesetName = "TestFileset_2",
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

        workflowName = "TestWorkload"
        dbname       = config.TaskArchiver.workloadSummaryCouchDBName
        couchdb      = CouchServer(config.JobStateMachine.couchurl)
        workdatabase = couchdb.connectDatabase(dbname)
        jobdb        = couchdb.connectDatabase("%s/jobs" % self.databaseName)
        fwjrdb       = couchdb.connectDatabase("%s/fwjrs" % self.databaseName)
        jobs = jobdb.loadView("JobDump", "jobsByWorkflowName",
                              options = {"startkey": [workflowName],
                                         "endkey": [workflowName, {}]})['rows']
        self.assertEqual(len(jobs), 2*self.nJobs)

        from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase
        create = CreateWMBSBase()
        tables = []
        for x in create.requiredTables:
            tables.append(x[2:])

        testTaskArchiver = TaskArchiverPoller(config = config)
        testTaskArchiver.algorithm()

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_fileset")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)

        # Make sure we deleted the directory
        self.assertFalse(os.path.exists(cachePath))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'workloadTest/TestWorkload')))

        testWMBSFileset = Fileset(id = 1)
        self.assertEqual(testWMBSFileset.exists(), False)



        workloadSummary = workdatabase.document(id = "TestWorkload")
        # Check ACDC
        self.assertEqual(workloadSummary['ACDCServer'], sanitizeURL(config.ACDC.couchurl)['url'])

        # Check the output
        self.assertEqual(workloadSummary['output'].keys(), ['/Electron/MorePenguins-v0/RECO'])
        self.assertEqual(workloadSummary['output']['/Electron/MorePenguins-v0/RECO']['tasks'],
                        ['/TestWorkload/ReReco', '/TestWorkload/ReReco/LogCollect'])
        # Check performance
        # Check histograms
        self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['histogram'][0]['average'],
                                0.89405199999999996, places = 2)
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['histogram'][0]['nEvents'],
                         10)

        # Check standard performance
        self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['TotalJobCPU']['average'], 17.786300000000001,
                                places = 2)
        self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['TotalJobCPU']['stdDev'], 0.0,
                                places = 2)

        # Check worstOffenders
        self.assertEqual(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['worstOffenders'],
                         [{'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 1},
                          {'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 1},
                          {'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 2}])

        # Check retryData
        self.assertEqual(workloadSummary['retryData']['/TestWorkload/ReReco'], {'1': 10})

        # LogCollect task is made out of identical FWJRs
        # assert that it is identical
        for x in workloadSummary['performance']['/TestWorkload/ReReco/LogCollect']['cmsRun1'].keys():
            if x in config.TaskArchiver.histogramKeys:
                continue
            for y in ['average', 'stdDev']:
                self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco/LogCollect']['cmsRun1'][x][y],
                                        workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1'][x][y],
                                        places = 2)

        return

    def testB_testErrors(self):
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
        workdatabase = couchdb.connectDatabase("%s/workloadsummary" % dbname)

        workloadSummary = workdatabase.document(id = workload.name())

        self.assertEqual(workloadSummary['errors']['/TestWorkload/ReReco']['failureTime'], 1000)
        self.assertTrue(workloadSummary['errors']['/TestWorkload/ReReco']['cmsRun1'].has_key('99999'))
        self.assertEquals(workloadSummary['errors']['/TestWorkload/ReReco']['cmsRun1']['99999']['runs'], {'10' : [12312]},
                          "Wrong lumi information in the summary for failed jobs")

        # Check the failures by site histograms
        self.assertEqual(workloadSummary['histograms']['workflowLevel']['failuresBySite']['data']['T1_IT_CNAF']['Failed Jobs'], 20)
        self.assertEqual(workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['data']['T1_IT_CNAF']['99999'], 20)
        self.assertEqual(workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['data']['T1_IT_CNAF']['8020'], 20)
        self.assertEqual(workloadSummary['histograms']['workflowLevel']['failuresBySite']['average']['Failed Jobs'], 20)
        self.assertEqual(workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['average']['99999'], 20)
        self.assertEqual(workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['average']['8020'], 20)
        self.assertEqual(workloadSummary['histograms']['workflowLevel']['failuresBySite']['stdDev']['Failed Jobs'], 0)
        self.assertEqual(workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['stdDev']['99999'], 0)
        self.assertEqual(workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['stdDev']['8020'], 0)
        return

    def atestC_Profile(self):
        """
        _Profile_

        DON'T RUN THIS!
        """

        return

        import cProfile, pstats

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
        return
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
        # wait for the generated alert to arrive
        while len(handler.queue) < len(subList):
            time.sleep(0.3)
            print "%s waiting for alert to arrive ..." % inspect.stack()[0][3]

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
        return
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
        # wait for the generated alert to arrive
        while len(handler.queue) < numAlerts:
            time.sleep(0.3)
            print "%s waiting for alert to arrive ..." % inspect.stack()[0][3]

        self.alertsReceiver.shutdown()
        self.alertsReceiver = None
        # now check if the alert was properly sent
        self.assertEqual(len(handler.queue), numAlerts)
        alert = handler.queue[0]
        self.assertEqual(alert["Source"], "TaskArchiverPoller")
        return

    def testE_multicore(self):
        """
        _multicore_

        Create a workload summary based on the multicore job report
        """

        myThread = threading.currentThread()

        config = self.getConfig()
        workloadPath = os.path.join(self.testDir, 'specDir', 'spec.pkl')
        workload     = self.createWorkload(workloadName = workloadPath)
        testJobGroup = self.createTestJobGroup(config = config,
                                               name = workload.name(),
                                               specLocation = workloadPath,
                                               error = False,
                                               multicore = True)

        cachePath = os.path.join(config.JobCreator.jobCacheDir,
                                 "TestWorkload", "ReReco")
        os.makedirs(cachePath)
        self.assertTrue(os.path.exists(cachePath))

        dbname       = config.TaskArchiver.workloadSummaryCouchDBName
        couchdb      = CouchServer(config.JobStateMachine.couchurl)
        workdatabase = couchdb.connectDatabase(dbname)

        testTaskArchiver = TaskArchiverPoller(config = config)
        testTaskArchiver.algorithm()

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0, "No job should have survived")
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)

        workloadSummary = workdatabase.document(id = "TestWorkload")

        self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['minMergeTime']['average'],
                         5.7624950408900002, places = 2)
        self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['numberOfMerges']['average'],
                         3.0, places = 2)
        self.assertAlmostEquals(workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['averageProcessTime']['average'],
                         29.369966666700002, places = 2)
        return


    # Requires a running UserFileCache to succeed. https://cmsweb.cern.ch worked for me
    # The environment variable OWNERDN needs to be set. Used to retrieve an already delegated proxy and contact the ufc
    @attr('integration')
    def testPublishJSONCreate(self):
        """
        Re-run testA_BasicFunctionTest with data in DBSBuffer
        Make sure files are generated
        """

        # Set up uploading and write them elsewhere since the test deletes them.
        self.uploadPublishInfo = True
        self.uploadPublishDir  = self.testDir

        # Insert some DBSFiles
        testFileChildA = DBSBufferFile(lfn = "/this/is/a/child/lfnA", size = 1024, events = 20)
        testFileChildA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                    appFam = "RECO", psetHash = "GIBBERISH",
                                    configContent = "MOREGIBBERISH")
        testFileChildB = DBSBufferFile(lfn = "/this/is/a/child/lfnB", size = 1024, events = 20)
        testFileChildB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                    appFam = "RECO", psetHash = "GIBBERISH",
                                    configContent = "MOREGIBBERISH")
        testFileChildC = DBSBufferFile(lfn = "/this/is/a/child/lfnC", size = 1024, events = 20)
        testFileChildC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                    appFam = "RECO", psetHash = "GIBBERISH",
                                    configContent = "MOREGIBBERISH")

        testFileChildA.setDatasetPath("/Cosmics/USER-DATASET1-v1/USER")
        testFileChildB.setDatasetPath("/Cosmics/USER-DATASET1-v1/USER")
        testFileChildC.setDatasetPath("/Cosmics/USER-DATASET2-v1/USER")

        testFileChildA.create()
        testFileChildB.create()
        testFileChildC.create()

        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10)
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.create()

        testFileChildA.addParents([testFile["lfn"]])
        testFileChildB.addParents([testFile["lfn"]])
        testFileChildC.addParents([testFile["lfn"]])

        myThread = threading.currentThread()
        self.dbsDaoFactory = DAOFactory(package="WMComponent.DBS3Buffer", logger=myThread.logger, dbinterface=myThread.dbi)
        self.insertWorkflow = self.dbsDaoFactory(classname="InsertWorkflow")
        workflowID = self.insertWorkflow.execute(requestName='TestWorkload', taskPath='TestWorkload/Analysis')
        myThread.dbi.processData("update dbsbuffer_file set workflow=1 where id < 4")

        # Run the test again
        self.testA_BasicFunctionTest()

        # Reset default values
        self.uploadPublishInfo = False
        self.uploadPublishDir  = None

        # Make sure the files are there
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'TestWorkload_publish.json')))
        self.assertTrue(os.path.getsize(os.path.join(self.testDir, 'TestWorkload_publish.json')) > 100)
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'TestWorkload_publish.tgz' )))

        return



if __name__ == '__main__':
    unittest.main()
