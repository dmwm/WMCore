#!/usr/bin/env python
"""
TaskArchiver test

Tests both the archiving of tasks and the creation of the
workloadSummary
"""
from __future__ import division
from builtins import range

import json
import logging
import os.path
import re
import threading
import time
import unittest

from WMCore_t.WMSpec_t.TestSpec import createTestWorkload
from nose.plugins.attrib import attr

import WMCore.WMBase
from WMComponent.TaskArchiver.CleanCouchPoller import CleanCouchPoller
from WMComponent.TaskArchiver.TaskArchiverPoller import TaskArchiverPoller
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Database.CMSCouch import CouchServer
from WMCore.FwkJobReport.Report import Report
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Lexicon import sanitizeURL
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBase import getTestBase
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.Emulators.WMSpecGenerator.ReqMgrDocGenerator import generate_reqmgr_schema
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class TaskArchiverTest(EmulatedUnitTestCase):
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
        super(TaskArchiverTest, self).setUp()
        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.testInit.setSchema(customModules=["WMCore.WMBS", "WMComponent.DBS3Buffer"],
                                useDefault=False)
        self.databaseName = "taskarchiver_t_0"
        self.testInit.setupCouch("%s/workloadsummary" % self.databaseName, "WorkloadSummary")
        self.testInit.setupCouch("%s/jobs" % self.databaseName, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % self.databaseName, "FWJRDump")
        self.testInit.setupCouch("wmagent_summary_t", "WMStats")
        self.testInit.setupCouch("wmagent_summary_central_t", "WMStats")
        self.testInit.setupCouch("stat_summary_t", "SummaryStats")
        reqmgrdb = "reqmgrdb_t"
        self.testInit.setupCouch(reqmgrdb, "ReqMgr")

        reqDBURL = "%s/%s" % (self.testInit.couchUrl, reqmgrdb)
        self.requestWriter = RequestDBWriter(reqDBURL)
        self.requestWriter.defaultStale = {}

        self.daofactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.dbsDaoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                        logger=myThread.logger,
                                        dbinterface=myThread.dbi)

        self.getJobs = self.daofactory(classname="Jobs.GetAllJobs")
        self.inject = self.daofactory(classname="Workflow.MarkInjectedWorkflows")

        self.testDir = self.testInit.generateWorkDir()
        os.makedirs(os.path.join(self.testDir, 'specDir'))

        self.nJobs = 10
        self.campaignName = 'aCampaign'

        return

    def tearDown(self):
        """
        Database deletion
        """

        self.testInit.clearDatabase(modules=["WMCore.WMBS"])
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        return

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()
        # self.testInit.generateWorkDir(config)

        config.section_("General")
        config.General.workDir = "."
        config.General.ReqMgr2ServiceURL = "https://cmsweb-dev.cern.ch/reqmgr2"

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = os.getenv("COUCHURL", "cmssrv52.fnal.gov:5984")
        config.JobStateMachine.couchDBName = self.databaseName
        config.JobStateMachine.jobSummaryDBName = 'wmagent_summary_t'
        config.JobStateMachine.summaryStatsDBName = 'stat_summary_t'

        config.component_("JobCreator")
        config.JobCreator.jobCacheDir = os.path.join(self.testDir, 'testDir')

        config.component_("TaskArchiver")
        config.TaskArchiver.componentDir = self.testDir
        config.TaskArchiver.WorkQueueParams = {'CacheDir': config.JobCreator.jobCacheDir}
        config.TaskArchiver.pollInterval = 60
        config.TaskArchiver.logLevel = 'INFO'
        config.TaskArchiver.timeOut = 0
        config.TaskArchiver.histogramKeys = ['AvgEventTime', 'writeTotalMB', 'jobTime']
        config.TaskArchiver.histogramBins = 5
        config.TaskArchiver.histogramLimit = 5
        config.TaskArchiver.perfPrimaryDatasets = ['SingleMu', 'MuHad', 'MinimumBias']
        config.TaskArchiver.perfDashBoardMinLumi = 50
        config.TaskArchiver.perfDashBoardMaxLumi = 9000
        config.TaskArchiver.dqmUrl = 'https://cmsweb.cern.ch/dqm/dev/'
        config.TaskArchiver.dashBoardUrl = 'http://dashboard43.cern.ch/dashboard/request.py/putluminositydata'
        config.TaskArchiver.workloadSummaryCouchDBName = "%s/workloadsummary" % self.databaseName
        config.TaskArchiver.localWMStatsURL = "%s/%s" % (config.JobStateMachine.couchurl,
                                                         config.JobStateMachine.jobSummaryDBName)
        config.TaskArchiver.workloadSummaryCouchURL = config.JobStateMachine.couchurl
        config.TaskArchiver.requireCouch = True

        config.component_("AnalyticsDataCollector")
        config.AnalyticsDataCollector.centralRequestDBURL = '%s/reqmgrdb_t' % config.JobStateMachine.couchurl
        config.AnalyticsDataCollector.RequestCouchApp = "ReqMgr"

        config.section_("ACDC")
        config.ACDC.couchurl = config.JobStateMachine.couchurl
        config.ACDC.database = config.JobStateMachine.couchDBName

        # Make the jobCacheDir
        os.mkdir(config.JobCreator.jobCacheDir)

        # addition for Alerts messaging framework, work (alerts) and control
        # channel addresses to which the component will be sending alerts
        # these are destination addresses where AlertProcessor:Receiver listens
        config.section_("Alert")
        config.Alert.address = "tcp://127.0.0.1:5557"
        config.Alert.controlAddr = "tcp://127.0.0.1:5559"

        config.section_("Agent")
        config.Agent.serverDN = "/we/bypass/myproxy/logon"

        return config

    def createWorkload(self, workloadName):
        """
        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = createTestWorkload(workloadName)

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.setCampaign(self.campaignName)

        workload.save(workloadName)

        return workload

    def createTestJobGroup(self, config, name="TestWorkthrough",
                           filesetName="TestFileset",
                           specLocation="spec.xml", error=False,
                           task="/TestWorkload/ReReco",
                           jobType="Processing"):
        """
        Creates a group of several jobs

        """

        testWorkflow = Workflow(spec=specLocation, owner=self.OWNERDN,
                                name=name, task=task, owner_vogroup="", owner_vorole="")
        testWorkflow.create()
        self.inject.execute(names=[name], injected=True)

        testWMBSFileset = Fileset(name=filesetName)
        testWMBSFileset.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileB.addRun(Run(10, *[12314]))
        testFileB.setLocation('malpaquet')

        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()
        testWMBSFileset.markOpen(0)

        outputWMBSFileset = Fileset(name='%sOutput' % filesetName)
        outputWMBSFileset.create()
        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=10)
        testFileC.addRun(Run(10, *[12312]))
        testFileC.setLocation('malpaquet')
        testFileC.create()
        outputWMBSFileset.addFile(testFileC)
        outputWMBSFileset.commit()
        outputWMBSFileset.markOpen(0)

        testWorkflow.addOutput('output', outputWMBSFileset)

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow,
                                        type=jobType)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        for i in range(0, self.nJobs):
            testJob = Job(name=makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJob['mask'].addRunAndLumis(run=10, lumis=[12312, 12313])
            testJobGroup.add(testJob)

        testJobGroup.commit()

        changer = ChangeState(config)

        report1 = Report()
        report2 = Report()
        if error:
            path1 = os.path.join(WMCore.WMBase.getTestBase(),
                                 "WMComponent_t/JobAccountant_t/fwjrs", "badBackfillJobReport.pkl")
            path2 = os.path.join(WMCore.WMBase.getTestBase(),
                                 'WMComponent_t/TaskArchiver_t/fwjrs',
                                 'logCollectReport2.pkl')
        else:
            path1 = os.path.join(WMCore.WMBase.getTestBase(),
                                 'WMComponent_t/TaskArchiver_t/fwjrs',
                                 'mergeReport1.pkl')
            path2 = os.path.join(WMCore.WMBase.getTestBase(),
                                 'WMComponent_t/TaskArchiver_t/fwjrs',
                                 'logCollectReport2.pkl')
        report1.load(filename=path1)
        report2.load(filename=path2)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        for i in range(self.nJobs):
            if i < self.nJobs // 2:
                testJobGroup.jobs[i]['fwjr'] = report1
            else:
                testJobGroup.jobs[i]['fwjr'] = report2
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'jobcooloff')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'retrydone', 'jobfailed')
        changer.propagate(testJobGroup.jobs, 'exhausted', 'retrydone')
        changer.propagate(testJobGroup.jobs, 'cleanout', 'exhausted')

        testSubscription.completeFiles([testFileA, testFileB])

        return testJobGroup

    def createGiantJobSet(self, name, config, nSubs=10, nJobs=10,
                          nFiles=1, spec="spec.xml"):
        """
        Creates a massive set of jobs

        """

        jobList = []

        for i in range(0, nSubs):
            # Make a bunch of subscriptions
            localName = '%s-%i' % (name, i)
            testWorkflow = Workflow(spec=spec, owner=self.OWNERDN,
                                    name=localName, task="Test", owner_vogroup="", owner_vorole="")
            testWorkflow.create()

            testWMBSFileset = Fileset(name=localName)
            testWMBSFileset.create()

            testSubscription = Subscription(fileset=testWMBSFileset,
                                            workflow=testWorkflow)
            testSubscription.create()

            testJobGroup = JobGroup(subscription=testSubscription)
            testJobGroup.create()

            filesToComplete = []

            for j in range(0, nJobs):
                # Create jobs for each subscription
                testFileA = File(lfn="%s-%i-lfnA" % (localName, j), size=1024, events=10)
                testFileA.addRun(Run(10, *[11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                           21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                                           31, 32, 33, 34, 35, 36, 37, 38, 39, 40]))
                testFileA.setLocation('malpaquet')
                testFileA.create()

                testWMBSFileset.addFile(testFileA)
                testWMBSFileset.commit()

                filesToComplete.append(testFileA)

                testJob = Job(name='%s-%i' % (localName, j))
                testJob.addFile(testFileA)
                testJob['retry_count'] = 1
                testJob['retry_max'] = 10
                testJobGroup.add(testJob)
                jobList.append(testJob)

                for k in range(0, nFiles):
                    # Create output files
                    testFile = File(lfn="%s-%i-output" % (localName, k), size=1024, events=10)
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

    def getPerformanceFromDQM(self, dqmUrl, dataset, run):
        # Make function to fetch this from DQM. Returning Null or False if it fails
        getUrl = "%sjsonfairy/archive/%s%s/DQM/TimerService/event_byluminosity" % (dqmUrl, run, dataset)
        # Assert if the URL is assembled as expected
        if run == 207214:
            self.assertEqual(
                'https://cmsweb.cern.ch/dqm/dev/jsonfairy/archive/207214/MinimumBias/Commissioning10-v4/DQM/DQM/TimerService/event_byluminosity',
                getUrl)
        # let's suppose it works..
        testResponseFile = open(os.path.join(getTestBase(),
                                             'WMComponent_t/TaskArchiver_t/DQMGUIResponse.json'), 'r')
        response = testResponseFile.read()
        testResponseFile.close()
        responseJSON = json.loads(response)
        return responseJSON

    def filterInterestingPerfPoints(self, responseJSON, minLumi, maxLumi):
        worthPoints = {}
        points = responseJSON["hist"]["bins"]["content"]
        for i in range(responseJSON["hist"]["xaxis"]["first"]["id"], responseJSON["hist"]["xaxis"]["last"]["id"]):
            # is the point worth it? if yes add to interesting points dictionary.
            # 1 - non 0
            # 2 - between minimum and maximum expected luminosity
            # FIXME : 3 - population in dashboard for the bin interval < 100
            # Those should come from the config :
            if points[i] == 0:
                continue
            binSize = responseJSON["hist"]["xaxis"]["last"]["value"] // responseJSON["hist"]["xaxis"]["last"]["id"]
            # Fetching the important values
            instLuminosity = i * binSize
            timePerEvent = points[i]

            if instLuminosity > minLumi and instLuminosity < maxLumi:
                worthPoints[instLuminosity] = timePerEvent
        return worthPoints

    def publishPerformanceDashBoard(self, dashBoardUrl, PD, release, worthPoints):
        dashboardPayload = []
        for instLuminosity in worthPoints:
            timePerEvent = int(worthPoints[instLuminosity])
            dashboardPayload.append({"primaryDataset": PD,
                                     "release": release,
                                     "integratedLuminosity": instLuminosity,
                                     "timePerEvent": timePerEvent})

        data = "{\"data\":%s}" % str(dashboardPayload).replace("\'", "\"")

        # let's suppose it works..
        testDashBoardPayloadFile = open(os.path.join(getTestBase(),
                                                     'WMComponent_t/TaskArchiver_t/DashBoardPayload.json'), 'r')
        testDashBoardPayload = testDashBoardPayloadFile.read()
        testDashBoardPayloadFile.close()

        self.assertEqual(data, testDashBoardPayload)

        return True

    def populateWorkflowWithCompleteStatus(self, name="TestWorkload"):
        schema = generate_reqmgr_schema(1)
        schema[0]["RequestName"] = name

        self.requestWriter.insertGenericRequest(schema[0])
        result = self.requestWriter.updateRequestStatus(name, "completed")
        return result

    def testA_BasicFunctionTest(self):
        """
        _BasicFunctionTest_

        Tests the components, by seeing if they can process a simple set of closeouts
        """

        myThread = threading.currentThread()

        config = self.getConfig()
        workloadPath = os.path.join(self.testDir, 'specDir', 'spec.pkl')
        workload = self.createWorkload(workloadName=workloadPath)
        testJobGroup = self.createTestJobGroup(config=config,
                                               name=workload.name(),
                                               specLocation=workloadPath,
                                               error=False)

        # Create second workload
        testJobGroup2 = self.createTestJobGroup(config=config,
                                                name=workload.name(),
                                                filesetName="TestFileset_2",
                                                specLocation=workloadPath,
                                                task="/TestWorkload/ReReco/LogCollect",
                                                jobType="LogCollect")

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
        dbname = config.TaskArchiver.workloadSummaryCouchDBName
        couchdb = CouchServer(config.JobStateMachine.couchurl)
        workdatabase = couchdb.connectDatabase(dbname)
        jobdb = couchdb.connectDatabase("%s/jobs" % self.databaseName)
        fwjrdb = couchdb.connectDatabase("%s/fwjrs" % self.databaseName)
        jobs = jobdb.loadView("JobDump", "jobsByWorkflowName",
                              options={"startkey": [workflowName],
                                       "endkey": [workflowName, {}]})['rows']
        fwjrdb.loadView("FWJRDump", "fwjrsByWorkflowName", options={"startkey": [workflowName],
                                                                    "endkey": [workflowName, {}]})['rows']

        self.assertEqual(len(jobs), 2 * self.nJobs)

        from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase
        create = CreateWMBSBase()
        tables = []
        for x in create.requiredTables:
            tables.append(x[2:])

        self.populateWorkflowWithCompleteStatus()
        testTaskArchiver = TaskArchiverPoller(config=config)
        testTaskArchiver.algorithm()

        cleanCouch = CleanCouchPoller(config=config)
        cleanCouch.setup()
        cleanCouch.algorithm()

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

        testWMBSFileset = Fileset(id=1)
        self.assertEqual(testWMBSFileset.exists(), False)

        workloadSummary = workdatabase.document(id="TestWorkload")
        # Check ACDC
        self.assertEqual(workloadSummary['ACDCServer'], sanitizeURL(config.ACDC.couchurl)['url'])

        # Check the output
        self.assertEqual(list(workloadSummary['output']), ['/Electron/MorePenguins-v0/RECO'])
        self.assertEqual(sorted(workloadSummary['output']['/Electron/MorePenguins-v0/RECO']['tasks']),
                         ['/TestWorkload/ReReco', '/TestWorkload/ReReco/LogCollect'])
        # Check performance
        # Check histograms
        self.assertAlmostEqual(
            workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['histogram'][0][
                'average'],
            0.89405199999999996, places=2)
        self.assertEqual(
            workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['histogram'][0][
                'nEvents'],
            10)

        # Check standard performance
        self.assertAlmostEqual(
            workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['TotalJobCPU']['average'],
            17.786300000000001,
            places=2)
        self.assertAlmostEqual(
            workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['TotalJobCPU']['stdDev'], 0.0,
            places=2)

        # Check worstOffenders
        self.assertEqual(
            workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1']['AvgEventTime']['worstOffenders'],
            [{'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 1},
             {'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 1},
             {'logCollect': None, 'log': None, 'value': '0.894052', 'jobID': 2}])

        # Check retryData
        self.assertEqual(workloadSummary['retryData']['/TestWorkload/ReReco'], {'1': 10})
        logCollectPFN = 'srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms/store/logs/prod/2012/11/WMAgent/Run206446-MinimumBias-Run2012D-v1-Tier1PromptReco-4af7e658-23a4-11e2-96c7-842b2b4671d8/Run206446-MinimumBias-Run2012D-v1-Tier1PromptReco-4af7e658-23a4-11e2-96c7-842b2b4671d8-AlcaSkimLogCollect-1-logs.tar'
        self.assertEqual(workloadSummary['logArchives'],
                         {'/TestWorkload/ReReco/LogCollect': [logCollectPFN for _ in range(10)]})

        # LogCollect task is made out of identical FWJRs
        # assert that it is identical
        for x in workloadSummary['performance']['/TestWorkload/ReReco/LogCollect']['cmsRun1']:
            if x in config.TaskArchiver.histogramKeys:
                continue
            for y in ['average', 'stdDev']:
                self.assertAlmostEqual(
                    workloadSummary['performance']['/TestWorkload/ReReco/LogCollect']['cmsRun1'][x][y],
                    workloadSummary['performance']['/TestWorkload/ReReco']['cmsRun1'][x][y],
                    places=2)

        return

    def testB_testErrors(self):
        """
        _testErrors_

        Test with a failed FWJR
        """

        config = self.getConfig()
        workloadPath = os.path.join(self.testDir, 'specDir', 'spec.pkl')
        workload = self.createWorkload(workloadName=workloadPath)
        testJobGroup = self.createTestJobGroup(config=config,
                                               name=workload.name(),
                                               specLocation=workloadPath,
                                               error=True)
        # Create second workload
        testJobGroup2 = self.createTestJobGroup(config=config,
                                                name=workload.name(),
                                                filesetName="TestFileset_2",
                                                specLocation=workloadPath,
                                                task="/TestWorkload/ReReco/LogCollect",
                                                jobType="LogCollect")

        cachePath = os.path.join(config.JobCreator.jobCacheDir,
                                 "TestWorkload", "ReReco")
        os.makedirs(cachePath)
        self.assertTrue(os.path.exists(cachePath))

        couchdb = CouchServer(config.JobStateMachine.couchurl)
        jobdb = couchdb.connectDatabase("%s/jobs" % self.databaseName)
        fwjrdb = couchdb.connectDatabase("%s/fwjrs" % self.databaseName)
        jobdb.loadView("JobDump", "jobsByWorkflowName",
                       options={"startkey": [workload.name()],
                                "endkey": [workload.name(), {}]})['rows']
        fwjrdb.loadView("FWJRDump", "fwjrsByWorkflowName",
                        options={"startkey": [workload.name()],
                                 "endkey": [workload.name(), {}]})['rows']

        self.populateWorkflowWithCompleteStatus()
        testTaskArchiver = TaskArchiverPoller(config=config)
        testTaskArchiver.algorithm()

        cleanCouch = CleanCouchPoller(config=config)
        cleanCouch.setup()
        cleanCouch.algorithm()

        dbname = getattr(config.JobStateMachine, "couchDBName")
        workdatabase = couchdb.connectDatabase("%s/workloadsummary" % dbname)

        workloadSummary = workdatabase.document(id=workload.name())

        self.assertEqual(workloadSummary['errors']['/TestWorkload/ReReco']['failureTime'], 500)
        self.assertTrue('99999' in workloadSummary['errors']['/TestWorkload/ReReco']['cmsRun1'])

        failedRunInfo = workloadSummary['errors']['/TestWorkload/ReReco']['cmsRun1']['99999']['runs']
        self.assertEqual(failedRunInfo, {'10': [[12312, 12312]]},
                         "Wrong lumi information in the summary for failed jobs")

        # Check the failures by site histograms
        self.assertEqual(
            workloadSummary['histograms']['workflowLevel']['failuresBySite']['data']['T1_IT_CNAF']['Failed Jobs'], 10)
        self.assertEqual(
            workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['data'][
                'T1_IT_CNAF']['99999'], 10)
        self.assertEqual(
            workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['data'][
                'T1_IT_CNAF']['8020'], 10)
        self.assertEqual(workloadSummary['histograms']['workflowLevel']['failuresBySite']['average']['Failed Jobs'], 10)
        self.assertEqual(
            workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['average'][
                '99999'], 10)
        self.assertEqual(
            workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['average'][
                '8020'], 10)
        self.assertEqual(workloadSummary['histograms']['workflowLevel']['failuresBySite']['stdDev']['Failed Jobs'], 0)
        self.assertEqual(
            workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['stdDev'][
                '99999'], 0)
        self.assertEqual(
            workloadSummary['histograms']['stepLevel']['/TestWorkload/ReReco']['cmsRun1']['errorsBySite']['stdDev'][
                '8020'], 0)
        return

    @attr("integration")
    def testC_Profile(self):
        """
        _Profile_

        DON'T RUN THIS!
        """
        import cProfile
        import pstats

        name = makeUUID()

        config = self.getConfig()

        jobList = self.createGiantJobSet(name=name, config=config,
                                         nSubs=10, nJobs=1000, nFiles=10)

        cleanCouch = CleanCouchPoller(config=config)
        cleanCouch.setup()

        cProfile.runctx("cleanCouch.algorithm()", globals(), locals(), filename="testStats.stat")

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()
        return

    @attr("integration")
    def testD_Timing(self):
        """
        _Timing_

        This is to see how fast things go.
        """
        myThread = threading.currentThread()

        name = makeUUID()

        config = self.getConfig()
        jobList = self.createGiantJobSet(name=name, config=config, nSubs=10,
                                         nJobs=1000, nFiles=10)

        testTaskArchiver = TaskArchiverPoller(config=config)

        startTime = time.time()
        testTaskArchiver.algorithm()
        stopTime = time.time()

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)
        testWMBSFileset = Fileset(id=1)
        self.assertEqual(testWMBSFileset.exists(), False)

        logging.info("TaskArchiver took %f seconds", (stopTime - startTime))

    def testDQMRecoPerformanceToDashBoard(self):

        myThread = threading.currentThread()

        listRunsWorkflow = self.dbsDaoFactory(classname="ListRunsWorkflow")

        # Didn't like to have done that, but the test doesn't provide all info I need in the system, so faking it:
        myThread.dbi.processData("""insert into dbsbuffer_workflow(id, name) values (1, 'TestWorkload')""",
                                 transaction=False)
        myThread.dbi.processData(
            """insert into dbsbuffer_file (id, lfn, dataset_algo, workflow) values (1, '/store/t/e/s/t.test', 1, 1)""",
            transaction=False)
        myThread.dbi.processData(
            """insert into dbsbuffer_file (id, lfn, dataset_algo, workflow) values (2, '/store/t/e/s/t.test2', 1, 1)""",
            transaction=False)
        myThread.dbi.processData(
            """insert into dbsbuffer_file_runlumi_map (run, lumi, filename) values (207214, 100, 1)""",
            transaction=False)
        myThread.dbi.processData(
            """insert into dbsbuffer_file_runlumi_map (run, lumi, filename) values (207215, 200, 2)""",
            transaction=False)

        config = self.getConfig()

        dqmUrl = getattr(config.TaskArchiver, "dqmUrl")
        perfDashBoardMinLumi = getattr(config.TaskArchiver, "perfDashBoardMinLumi")
        perfDashBoardMaxLumi = getattr(config.TaskArchiver, "perfDashBoardMaxLumi")
        dashBoardUrl = getattr(config.TaskArchiver, "dashBoardUrl")

        workloadPath = os.path.join(self.testDir, 'specDir', 'spec.pkl')
        workload = self.createWorkload(workloadName=workloadPath)
        testJobGroup = self.createTestJobGroup(config=config,
                                               name=workload.name(),
                                               specLocation=workloadPath,
                                               error=True)
        testJobGroup2 = self.createTestJobGroup(config=config,
                                                name=workload.name(),
                                                filesetName="TestFileset_2",
                                                specLocation=workloadPath,
                                                task="/TestWorkload/ReReco/LogCollect",
                                                jobType="LogCollect")

        # Adding request type as ReReco, real ReqMgr requests have it
        workload.data.request.section_("schema")
        workload.data.request.schema.RequestType = "ReReco"
        workload.data.request.schema.CMSSWVersion = 'test_compops_CMSSW_5_3_6_patch1'
        workload.getTask('ReReco').addInputDataset(name='/a/b/c', primary='a', processed='b', tier='c')

        interestingPDs = getattr(config.TaskArchiver, "perfPrimaryDatasets")
        interestingDatasets = []
        # Are the datasets from this request interesting? Do they have DQM output? One might ask afterwards if they have harvest
        for dataset in workload.listOutputDatasets():
            (nothing, PD, procDataSet, dataTier) = dataset.split('/')
            if PD in interestingPDs and dataTier == "DQM":
                interestingDatasets.append(dataset)
        # We should have found 1 interesting dataset
        self.assertAlmostEqual(len(interestingDatasets), 1)
        if len(interestingDatasets) == 0:
            return
        # Request will be only interesting for performance if it's a ReReco or PromptReco
        (isReReco, isPromptReco) = (False, False)
        if getattr(workload.data.request.schema, "RequestType", None) == 'ReReco':
            isReReco = True
        # Yes, few people like magic strings, but have a look at :
        # https://github.com/dmwm/T0/blob/master/src/python/T0/RunConfig/RunConfigAPI.py#L718
        # Might be safe enough
        # FIXME: in TaskArchiver, add a test to make sure that the dataset makes sense (procDataset ~= /a/ERA-PromptReco-vVERSON/DQM)
        if re.search('PromptReco', workload.name()):
            isPromptReco = True
        if not (isReReco or isPromptReco):
            return

        self.assertTrue(isReReco)
        self.assertFalse(isPromptReco)

        # We are not interested if it's not a PromptReco or a ReReco
        if not (isReReco or isPromptReco):
            return
        if isReReco:
            release = getattr(workload.data.request.schema, "CMSSWVersion")
            if not release:
                logging.info("no release for %s, bailing out", workload.name())
        else:
            release = getattr(workload.tasks.Reco.steps.cmsRun1.application.setup, "cmsswVersion")
            if not release:
                logging.info("no release for %s, bailing out", workload.name())

        self.assertEqual(release, "test_compops_CMSSW_5_3_6_patch1")
        # If all is true, get the run numbers processed by this worklfow
        runList = listRunsWorkflow.execute(workflow=workload.name())
        self.assertEqual([207214, 207215], runList)
        # GO to DQM GUI, get what you want
        # https://cmsweb.cern.ch/dqm/offline/jsonfairy/archive/211313/PAMuon/HIRun2013-PromptReco-v1/DQM/DQM/TimerService/event
        for dataset in interestingDatasets:
            (nothing, PD, procDataSet, dataTier) = dataset.split('/')
            worthPoints = {}
            for run in runList:
                responseJSON = self.getPerformanceFromDQM(dqmUrl, dataset, run)
                worthPoints.update(
                    self.filterInterestingPerfPoints(responseJSON, perfDashBoardMinLumi, perfDashBoardMaxLumi))

            # Publish dataset performance to DashBoard.
            if not self.publishPerformanceDashBoard(dashBoardUrl, PD, release, worthPoints):
                logging.info("something went wrong when publishing dataset %s to DashBoard", dataset)

        return


if __name__ == '__main__':
    unittest.main()
