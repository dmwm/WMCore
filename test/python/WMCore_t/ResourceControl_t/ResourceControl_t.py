#!/usr/bin/env python
"""
_ResourceControl_t_

Unit tests for ResourceControl.
"""

from future.utils import viewitems

import os
import subprocess
import sys
import threading
import unittest

from Utils.PythonVersion import PY3

from WMCore.Agent.Configuration import Configuration
from WMCore.BossAir.RunJob import RunJob
from WMCore.DAOFactory import DAOFactory
from WMCore.FwkJobReport.Report import Report
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBase import getTestBase
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInit import TestInit


class ResourceControlTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Install schema and create a DAO factory for WMBS.
        """
        super(ResourceControlTest, self).setUp()
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS",
                                               "WMCore.ResourceControl",
                                               "WMCore.BossAir"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.baDaoFactory = DAOFactory(package="WMCore.BossAir",
                                       logger=myThread.logger,
                                       dbinterface=myThread.dbi)

        self.insertRunJob = self.baDaoFactory(classname="NewJobs")
        self.insertState = self.baDaoFactory(classname="NewState")
        states = ['PEND', 'RUN', 'Idle', 'Running']
        self.insertState.execute(states)

        self.tempDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear the schema.
        """
        super(ResourceControlTest, self).tearDown()
        self.testInit.clearDatabase()
        return

    def createJobs(self):
        """
        _createJobs_

        Create test jobs in WMBS and BossAir
        """
        testWorkflow = Workflow(spec=makeUUID(), owner="tapas",
                                name=makeUUID(), task="Test")
        testWorkflow.create()

        testFilesetA = Fileset(name="TestFilesetA")
        testFilesetA.create()
        testFilesetB = Fileset(name="TestFilesetB")
        testFilesetB.create()
        testFilesetC = Fileset(name="TestFilesetC")
        testFilesetC.create()

        testFileA = File(lfn="testFileA", locations=set(["testSE1", "testSE2"]))
        testFileA.create()
        testFilesetA.addFile(testFileA)
        testFilesetA.commit()
        testFilesetB.addFile(testFileA)
        testFilesetB.commit()
        testFilesetC.addFile(testFileA)
        testFilesetC.commit()

        testSubscriptionA = Subscription(fileset=testFilesetA,
                                         workflow=testWorkflow,
                                         type="Processing")
        testSubscriptionA.create()
        testSubscriptionA.addWhiteBlackList([{"site_name": "testSite1", "valid": True}])
        testSubscriptionB = Subscription(fileset=testFilesetB,
                                         workflow=testWorkflow,
                                         type="Processing")
        testSubscriptionB.create()
        testSubscriptionB.addWhiteBlackList([{"site_name": "testSite1", "valid": False}])
        testSubscriptionC = Subscription(fileset=testFilesetC,
                                         workflow=testWorkflow,
                                         type="Merge")
        testSubscriptionC.create()

        testJobGroupA = JobGroup(subscription=testSubscriptionA)
        testJobGroupA.create()
        testJobGroupB = JobGroup(subscription=testSubscriptionB)
        testJobGroupB.create()
        testJobGroupC = JobGroup(subscription=testSubscriptionC)
        testJobGroupC.create()

        # Site1, Has been assigned a location and is complete.
        testJobA = Job(name="testJobA", files=[testFileA])
        testJobA["couch_record"] = makeUUID()
        testJobA.create(group=testJobGroupA)
        testJobA["state"] = "success"

        # Site 1, Has been assigned a location and is incomplete.
        testJobB = Job(name="testJobB", files=[testFileA])
        testJobB["couch_record"] = makeUUID()
        testJobB["cache_dir"] = self.tempDir
        testJobB.create(group=testJobGroupA)
        testJobB["state"] = "executing"
        runJobB = RunJob()
        runJobB.buildFromJob(testJobB)
        runJobB["status"] = "PEND"

        # Does not have a location, white listed to site 1
        testJobC = Job(name="testJobC", files=[testFileA])
        testJobC["couch_record"] = makeUUID()
        testJobC.create(group=testJobGroupA)
        testJobC["state"] = "new"

        # Site 2, Has been assigned a location and is complete.
        testJobD = Job(name="testJobD", files=[testFileA])
        testJobD["couch_record"] = makeUUID()
        testJobD.create(group=testJobGroupB)
        testJobD["state"] = "success"

        # Site 2, Has been assigned a location and is incomplete.
        testJobE = Job(name="testJobE", files=[testFileA])
        testJobE["couch_record"] = makeUUID()
        testJobE.create(group=testJobGroupB)
        testJobE["state"] = "executing"
        runJobE = RunJob()
        runJobE.buildFromJob(testJobE)
        runJobE["status"] = "RUN"

        # Does not have a location, site 1 is blacklisted.
        testJobF = Job(name="testJobF", files=[testFileA])
        testJobF["couch_record"] = makeUUID()
        testJobF.create(group=testJobGroupB)
        testJobF["state"] = "new"

        # Site 3, Has been assigned a location and is complete.
        testJobG = Job(name="testJobG", files=[testFileA])
        testJobG["couch_record"] = makeUUID()
        testJobG.create(group=testJobGroupC)
        testJobG["state"] = "cleanout"

        # Site 3, Has been assigned a location and is incomplete.
        testJobH = Job(name="testJobH", files=[testFileA])
        testJobH["couch_record"] = makeUUID()
        testJobH.create(group=testJobGroupC)
        testJobH["state"] = "new"

        # Site 3, Does not have a location.
        testJobI = Job(name="testJobI", files=[testFileA])
        testJobI["couch_record"] = makeUUID()
        testJobI.create(group=testJobGroupC)
        testJobI["state"] = "new"

        # Site 3, Does not have a location and is in cleanout.
        testJobJ = Job(name="testJobJ", files=[testFileA])
        testJobJ["couch_record"] = makeUUID()
        testJobJ.create(group=testJobGroupC)
        testJobJ["state"] = "cleanout"

        changeStateAction = self.daoFactory(classname="Jobs.ChangeState")
        changeStateAction.execute(jobs=[testJobA, testJobB, testJobC, testJobD,
                                        testJobE, testJobF, testJobG, testJobH,
                                        testJobI, testJobJ])

        self.insertRunJob.execute([runJobB, runJobE])

        setLocationAction = self.daoFactory(classname="Jobs.SetLocation")
        setLocationAction.execute(testJobA["id"], "testSite1")
        setLocationAction.execute(testJobB["id"], "testSite1")
        setLocationAction.execute(testJobD["id"], "testSite1")
        setLocationAction.execute(testJobE["id"], "testSite2")
        setLocationAction.execute(testJobG["id"], "testSite1")
        setLocationAction.execute(testJobH["id"], "testSite1")

        return

    def testInsert(self):
        """
        _testInsert_

        Verify that inserting sites and thresholds works correctly, even if the
        site or threshold already exists.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, 20, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite1", 10, 20, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite2", 100, 200, "testSE2", "testCE2")

        myResourceControl.insertThreshold("testSite1", "Processing", 20, 10)
        myResourceControl.insertThreshold("testSite1", "Merge", 200, 100)
        myResourceControl.insertThreshold("testSite1", "Merge", 250, 150)
        myResourceControl.insertThreshold("testSite2", "Processing", 50, 30)
        myResourceControl.insertThreshold("testSite2", "Merge", 135, 100)

        createThresholds = myResourceControl.listThresholdsForCreate()

        self.assertEqual(len(createThresholds), 2,
                         "Error: Wrong number of site in Resource Control DB")

        self.assertTrue("testSite1" in createThresholds,
                        "Error: Test Site 1 missing from thresholds.")

        self.assertTrue("testSite2" in createThresholds,
                        "Error: Test Site 2 missing from thresholds.")

        self.assertEqual(createThresholds["testSite1"]["total_slots"], 10,
                         "Error: Wrong number of total slots.")

        self.assertEqual(createThresholds["testSite1"]["pending_jobs"], {0: 0},
                         "Error: Wrong number of running jobs: %s" %
                         createThresholds["testSite1"]["pending_jobs"])

        self.assertEqual(createThresholds["testSite2"]["total_slots"], 100,
                         "Error: Wrong number of total slots.")

        self.assertEqual(createThresholds["testSite2"]["pending_jobs"], {0: 0},
                         "Error: Wrong number of running jobs.")

        thresholds = myResourceControl.listThresholdsForSubmit()

        self.assertEqual(len(thresholds), 2,
                         "Error: Wrong number of sites in Resource Control DB")

        self.assertTrue("testSite1" in thresholds,
                        "Error: testSite1 missing from thresholds.")

        self.assertTrue("testSite2" in thresholds,
                        "Error: testSite2 missing from thresholds.")

        site1Info = thresholds["testSite1"]
        site2Info = thresholds["testSite2"]
        site1Thresholds = site1Info["thresholds"]
        site2Thresholds = site2Info["thresholds"]

        procThreshold1 = None
        procThreshold2 = None
        mergeThreshold1 = None
        mergeThreshold2 = None
        for taskType, threshold in viewitems(site1Thresholds):
            if taskType == "Merge":
                mergeThreshold1 = threshold
            elif taskType == "Processing":
                procThreshold1 = threshold
        for taskType, threshold in viewitems(site2Thresholds):
            if taskType == "Merge":
                mergeThreshold2 = threshold
            elif taskType == "Processing":
                procThreshold2 = threshold

        self.assertEqual(len(site1Thresholds), 2,
                         "Error: Wrong number of task types.")

        self.assertEqual(len(site2Thresholds), 2,
                         "Error: Wrong number of task types.")

        self.assertNotEqual(procThreshold1, None)
        self.assertNotEqual(procThreshold2, None)
        self.assertNotEqual(mergeThreshold1, None)
        self.assertNotEqual(mergeThreshold2, None)

        self.assertEqual(site1Info["total_pending_slots"], 10,
                         "Error: Site thresholds wrong")

        self.assertEqual(site1Info["total_running_slots"], 20,
                         "Error: Site thresholds wrong")

        self.assertEqual(site1Info["total_running_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(site1Info["total_pending_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold1["task_running_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold1["task_pending_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold1["max_slots"], 20,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold1["pending_slots"], 10,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold1["task_running_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold1["task_pending_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold1["max_slots"], 250,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold1["pending_slots"], 150,
                         "Error: Site thresholds wrong")

        self.assertEqual(site2Info["total_pending_slots"], 100,
                         "Error: Site thresholds wrong")

        self.assertEqual(site2Info["total_running_slots"], 200,
                         "Error: Site thresholds wrong")

        self.assertEqual(site2Info["total_running_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(site2Info["total_pending_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold2["task_running_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold2["task_pending_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold2["max_slots"], 50,
                         "Error: Site thresholds wrong")

        self.assertEqual(procThreshold2["pending_slots"], 30,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold2["task_running_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold2["task_pending_jobs"], 0,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold2["max_slots"], 135,
                         "Error: Site thresholds wrong")

        self.assertEqual(mergeThreshold2["pending_slots"], 100,
                         "Error: Site thresholds wrong")

    def testList(self):
        """
        _testList_

        Test the functions that list thresholds for creating jobs and submitting
        jobs.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, 20, "testSE1", "testCE1", "T1_US_FNAL", "LsfPlugin")
        myResourceControl.insertSite("testSite2", 20, 40, "testSE2", "testCE2", "T3_US_FNAL", "LsfPlugin")

        myResourceControl.insertThreshold("testSite1", "Processing", 20, 10)
        myResourceControl.insertThreshold("testSite1", "Merge", 200, 100)
        myResourceControl.insertThreshold("testSite2", "Processing", 50, 25)
        myResourceControl.insertThreshold("testSite2", "Merge", 135, 65)

        self.createJobs()

        createThresholds = myResourceControl.listThresholdsForCreate()
        submitThresholds = myResourceControl.listThresholdsForSubmit()

        self.assertEqual(len(createThresholds), 2,
                         "Error: Wrong number of sites in create thresholds")

        self.assertEqual(createThresholds["testSite1"]["total_slots"], 10,
                         "Error: Wrong number of slots for site 1")

        self.assertEqual(createThresholds["testSite2"]["total_slots"], 20,
                         "Error: Wrong number of slots for site 2")

        # We should have two running jobs with locations at site one,
        # two running jobs without locations at site two, and one running
        # job without a location at site one and two.
        self.assertEqual(createThresholds["testSite1"]["pending_jobs"], {0: 4},
                         "Error: Wrong number of pending jobs for site 1")

        # We should have one running job with a location at site 2 and
        # another running job without a location.
        self.assertEqual(createThresholds["testSite2"]["pending_jobs"], {0: 2},
                         "Error: Wrong number of pending jobs for site 2")

        # We should also have a phedex_name
        self.assertEqual(createThresholds["testSite1"]["cms_name"], "T1_US_FNAL")
        self.assertEqual(createThresholds["testSite2"]["cms_name"], "T3_US_FNAL")

        mergeThreshold1 = None
        mergeThreshold2 = None
        procThreshold1 = None
        procThreshold2 = None
        self.assertEqual(set(submitThresholds.keys()), set(["testSite1", "testSite2"]))
        for taskType, threshold in viewitems(submitThresholds["testSite1"]["thresholds"]):
            if taskType == "Merge":
                mergeThreshold1 = threshold
            elif taskType == "Processing":
                procThreshold1 = threshold
        for taskType, threshold in viewitems(submitThresholds["testSite2"]["thresholds"]):
            if taskType == "Merge":
                mergeThreshold2 = threshold
            elif taskType == "Processing":
                procThreshold2 = threshold

        self.assertEqual(submitThresholds["testSite1"]["total_running_jobs"], 0,
                         "Error: Wrong number of running jobs for submit thresholds.")
        self.assertEqual(submitThresholds["testSite2"]["total_running_jobs"], 1,
                         "Error: Wrong number of running jobs for submit thresholds.")
        self.assertEqual(submitThresholds["testSite1"]["total_pending_jobs"], 1,
                         "Error: Wrong number of pending jobs for submit thresholds.")
        self.assertEqual(submitThresholds["testSite2"]["total_pending_jobs"], 0,
                         "Error: Wrong number of pending jobs for submit thresholds.")

        self.assertEqual(mergeThreshold1["task_running_jobs"], 0,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(mergeThreshold1["task_pending_jobs"], 0,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(procThreshold1["task_running_jobs"], 0,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(procThreshold1["task_pending_jobs"], 1,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(mergeThreshold2["task_running_jobs"], 0,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(mergeThreshold2["task_pending_jobs"], 0,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(procThreshold2["task_running_jobs"], 1,
                         "Error: Wrong number of task running jobs for submit thresholds.")
        self.assertEqual(procThreshold2["task_pending_jobs"], 0,
                         "Error: Wrong number of task running jobs for submit thresholds.")

        return

    def testListSiteInfo(self):
        """
        _testListSiteInfo_

        Verify that the listSiteInfo() methods works properly.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, 20, "testSE1", "testCE1")
        myResourceControl.insertSite("testSite2", 100, 200, "testSE2", "testCE2")

        siteInfo = myResourceControl.listSiteInfo("testSite1")

        self.assertEqual(siteInfo["site_name"], "testSite1",
                         "Error: Site name is wrong.")

        self.assertEqual(siteInfo["pnn"], ["testSE1"],
                         "Error: SE name is wrong.")

        self.assertEqual(siteInfo["ce_name"], "testCE1",
                         "Error: CE name is wrong.")

        self.assertEqual(siteInfo["pending_slots"], 10,
                         "Error: Pending slots is wrong.")

        self.assertEqual(siteInfo["running_slots"], 20,
                         "Error: Pending slots is wrong.")

        return

    def testUpdateJobSlots(self):
        """
        _testUpdateJobSlots_

        Verify that it is possible to update the number of job slots at a site.
        """
        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 10, 20, "testSE1", "testCE1")

        siteInfo = myResourceControl.listSiteInfo("testSite1")

        self.assertEqual(siteInfo["pending_slots"], 10, "Error: Pending slots is wrong.")
        self.assertEqual(siteInfo["running_slots"], 20, "Error: Running slots is wrong.")

        myResourceControl.setJobSlotsForSite("testSite1", pendingJobSlots=20)

        siteInfo = myResourceControl.listSiteInfo("testSite1")

        self.assertEqual(siteInfo["pending_slots"], 20, "Error: Pending slots is wrong.")

        myResourceControl.setJobSlotsForSite("testSite1", runningJobSlots=40)

        siteInfo = myResourceControl.listSiteInfo("testSite1")

        self.assertEqual(siteInfo["running_slots"], 40, "Error: Running slots is wrong.")

        myResourceControl.setJobSlotsForSite("testSite1", 5, 10)

        siteInfo = myResourceControl.listSiteInfo("testSite1")

        self.assertEqual(siteInfo["pending_slots"], 5, "Error: Pending slots is wrong.")
        self.assertEqual(siteInfo["running_slots"], 10, "Error: Running slots is wrong.")

        return

    def testThresholdsForSite(self):
        """
        _testThresholdsForSite_

        Check that we can get the thresholds in intelligible form
        for each site
        """

        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 20, 40, "testSE1", "testCE1")
        myResourceControl.insertThreshold("testSite1", "Processing", 10, 8)
        myResourceControl.insertThreshold("testSite1", "Merge", 5, 3)

        result = myResourceControl.thresholdBySite(siteName="testSite1")
        procInfo = {}
        mergInfo = {}
        for res in result:
            if res['task_type'] == 'Processing':
                procInfo = res
            elif res['task_type'] == 'Merge':
                mergInfo = res
        self.assertEqual(procInfo.get('pending_slots', None), 20)
        self.assertEqual(procInfo.get('running_slots', None), 40)
        self.assertEqual(procInfo.get('max_slots', None), 10)
        self.assertEqual(procInfo.get('task_pending_slots', None), 8)
        self.assertEqual(mergInfo.get('pending_slots', None), 20)
        self.assertEqual(mergInfo.get('running_slots', None), 40)
        self.assertEqual(mergInfo.get('max_slots', None), 5)
        self.assertEqual(mergInfo.get('task_pending_slots', None), 3)

        return

    def testThresholdPriority(self):
        """
        _testThresholdPriority_

        Test that we get things back in priority order
        """

        myResourceControl = ResourceControl()
        myResourceControl.insertSite("testSite1", 20, 40, "testSE1", "testCE1")
        myResourceControl.insertThreshold("testSite1", "Processing", 10, 8)
        myResourceControl.insertThreshold("testSite1", "Merge", 5, 3)

        # test default task priorities
        result = myResourceControl.listThresholdsForSubmit()
        self.assertEqual(result['testSite1']['thresholds']['Merge']['priority'], 4)
        self.assertEqual(result['testSite1']['thresholds']['Processing']['priority'], 0)

        myResourceControl.changeTaskPriority("Merge", 3)
        myResourceControl.changeTaskPriority("Processing", 1)

        result = myResourceControl.listThresholdsForSubmit()
        self.assertEqual(result['testSite1']['thresholds']['Merge']['priority'], 3)
        self.assertEqual(result['testSite1']['thresholds']['Processing']['priority'], 1)

        myResourceControl.changeTaskPriority("Merge", 1)
        myResourceControl.changeTaskPriority("Processing", 3)

        result = myResourceControl.listThresholdsForSubmit()
        self.assertEqual(result['testSite1']['thresholds']['Merge']['priority'], 1)
        self.assertEqual(result['testSite1']['thresholds']['Processing']['priority'], 3)

        return

    def testChangeSiteState(self):
        """
        _testNewState_

        Check that we can change the state between different values and
        retrieve it through the threshold methods
        """
        self.tempDir = self.testInit.generateWorkDir()
        config = self.createConfig()
        myResourceControl = ResourceControl(config)
        myResourceControl.insertSite("testSite1", 20, 40, "testSE1", "testCE1")
        myResourceControl.insertThreshold("testSite1", "Processing", 10, 5)

        result = myResourceControl.listThresholdsForCreate()
        self.assertEqual(result['testSite1']['state'], 'Normal', 'Error: Wrong site state')

        myResourceControl.changeSiteState("testSite1", "Down")
        result = myResourceControl.listThresholdsForCreate()
        self.assertEqual(result['testSite1']['state'], 'Down', 'Error: Wrong site state')

        # If you set the value to 'Normal' instead of 'Down' this test should FAIL
        # self.assertEqual(result['testSite1']['state'], 'Normal', 'Error: Wrong site state')

    def testAbortedState(self):
        """
        _testAbortedState_

        Check that we can kill jobs when a site is set to aborted
        ### We no longer need this test as we are not killing jobs that are running
        """
        self.tempDir = self.testInit.generateWorkDir()
        config = self.createConfig()
        myResourceControl = ResourceControl(config)
        myResourceControl.insertSite("testSite1", 10, 20, "testSE1", "testCE1", "T1_US_FNAL", "MockPlugin")
        myResourceControl.insertSite("testSite2", 20, 40, "testSE2", "testCE2", "T1_IT_CNAF", "MockPlugin")

        myResourceControl.insertThreshold("testSite1", "Processing", 20, 10)
        myResourceControl.insertThreshold("testSite1", "Merge", 200, 100)
        myResourceControl.insertThreshold("testSite2", "Processing", 50, 25)
        myResourceControl.insertThreshold("testSite2", "Merge", 135, 65)

        self.createJobs()

        myResourceControl.changeSiteState("testSite1", "Aborted")

        ## Now check the tempDir for a FWJR for the killed job
        reportPath = os.path.join(self.tempDir, "Report.0.pkl")
        report = Report()
        report.load(reportPath)
        self.assertEqual(report.getExitCode(), 71301)
        return

    def createConfig(self):
        """
        _createConfig_

        Create a config and save it to the temp dir.  Set the WMAGENT_CONFIG
        environment variable so the config gets picked up.
        """
        config = Configuration()
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())
        config.section_("Agent")
        config.Agent.componentName = "resource_control_t"
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = os.getenv('COUCHURL')
        config.JobStateMachine.couchDBName = "bossair_t"
        config.JobStateMachine.jobSummaryDBName = 'wmagent_summary_t'
        config.JobStateMachine.summaryStatsDBName = 'stat_summary_t'
        config.section_("BossAir")
        config.BossAir.pluginDir = "WMCore.BossAir.Plugins"
        config.BossAir.pluginNames = ["MockPlugin"]
        config.BossAir.section_("MockPlugin")
        config.BossAir.MockPlugin.fakeReport = os.path.join(getTestBase(),
                                                            'WMComponent_t/JobAccountant_t/fwjrs',
                                                            "MergeSuccess.pkl")

        configHandle = open(os.path.join(self.tempDir, "config.py"), "w")
        configHandle.write(str(config))
        configHandle.close()

        os.environ["WMAGENT_CONFIG"] = os.path.join(self.tempDir, "config.py")
        return config

    def testInsertSite(self):
        """
        _testInsertSite_

        Test to see if we can insert a fake test site alone
        with a single option
        """
        self.createConfig()

        resControlPath = os.path.join(getTestBase(), "../../bin/wmagent-resource-control")
        env = os.environ
        env['PYTHONPATH'] = ":".join(sys.path)
        if PY3:
            cmdline = ["python3", resControlPath, "--add-Test"]
        else:
            cmdline = [resControlPath, "--add-Test"]
        retval = subprocess.Popen(cmdline,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  env=env)
        (_, _) = retval.communicate()
        myResourceControl = ResourceControl()
        result = myResourceControl.listThresholdsForSubmit()
        self.assertEqual(len(result), 1)
        self.assertTrue('CERN' in result)
        for x in result:
            self.assertEqual(len(result[x]['thresholds']), 7)
            self.assertEqual(result[x]['total_pending_slots'], 500)
            self.assertEqual(result[x]['total_running_slots'], 1)
            for taskType, thresh in viewitems(result[x]['thresholds']):
                if taskType == 'Processing':
                    self.assertEqual(thresh['priority'], 0)
                    self.assertEqual(thresh['max_slots'], 1)

        # Verify that sites with more than one SE were added correctly.
        cernInfo = myResourceControl.listSiteInfo("CERN")
        self.assertTrue(len(cernInfo["pnn"]) == 2)
        return


if __name__ == '__main__':
    unittest.main()
