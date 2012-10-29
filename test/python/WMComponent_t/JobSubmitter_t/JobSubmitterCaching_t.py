#!/usr/bin/env python
"""
_JobSubmitterCaching_t_

Verify that the caching of jobs and white/black lists works correctly.
"""

import unittest
import os
import pickle

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.WorkQueue.WMBSHelper import killWorkflow
from WMQuality.Emulators import EmulatorSetup

class JobSubmitterCachingTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Set everything up.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.BossAir",
                                                 "WMCore.ResourceControl"],
                                useDefault = False)
        self.testInit.setupCouch("jobsubmittercaching_t/jobs", "JobDump")
        self.testInit.setupCouch("jobsubmittercaching_t/fwjrs", "FWJRDump")

        resourceControl = ResourceControl()
        for siteName in ["T1_US_FNAL", "T1_UK_RAL"]:
            resourceControl.insertSite(siteName = siteName, seName = "se.%s" % (siteName),
                                       ceName = siteName, plugin = "CondorPlugin", cmsName = siteName)
            resourceControl.insertThreshold(siteName = siteName, taskType = "Processing",
                                            maxSlots = 10000)

        self.testDir = self.testInit.generateWorkDir()
        #os.environ["COUCHDB"] = "jobsubmittercaching_t"
        self.configFile = EmulatorSetup.setupWMAgentConfig()
        return

    def tearDown(self):
        """
        _tearDown_

        Tear everything down.
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorSetup.deleteConfig(self.configFile)
        return

    def createConfig(self):
        """
        _createConfig_

        Create a config for the JobSubmitter.  These parameters are still pulled
        from the environment.
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = os.getenv("COUCHURL")
        config.JobStateMachine.couchDBName = "jobsubmittercaching_t"

        config.section_("BossAir")
        config.BossAir.pluginDir = "WMCore.BossAir.Plugins"
        config.BossAir.pluginNames = ["CondorPlugin"]
        config.BossAir.nCondorProcesses = 1

        config.component_("JobSubmitter")
        config.JobSubmitter.submitDir = self.testDir
        return config

    def injectJobs(self):
        """
        _injectJobs_

        Inject two workflows into WMBS and save the job objects to disk.
        """
        testWorkflowA = Workflow(spec = "specA.pkl", owner = "Steve",
                                 name = "wf001", task = "TestTaskA")
        testWorkflowA.create()
        testWorkflowB = Workflow(spec = "specB.pkl", owner = "Steve",
                                 name = "wf002", task = "TestTaskB")
        testWorkflowB.create()

        testFileset = Fileset("testFileset")
        testFileset.create()

        testSubA = Subscription(fileset = testFileset, workflow = testWorkflowA)
        testSubA.create()
        testSubB = Subscription(fileset = testFileset, workflow = testWorkflowB)
        testSubB.create()

        testGroupA = JobGroup(subscription = testSubA)
        testGroupA.create()
        testGroupB = JobGroup(subscription = testSubB)
        testGroupB.create()

        stateChanger = ChangeState(self.createConfig(), "jobsubmittercaching_t")

        for i in range(10):
            newFile = File(lfn = "testFile%s" % i,
                           locations = set(["se.T1_US_FNAL", "se.T1_UK_RAL"]))
            newFile.create()

            newJobA = Job(name = "testJobA-%s" % i, files = [newFile])
            newJobA["workflow"] = "wf001"
            newJobA["siteWhitelist"] = ["T1_US_FNAL"]
            newJobA["siteBlacklist"] = []
            newJobA["sandbox"] = "%s/somesandbox" % self.testDir
            newJobA["owner"] = "Steve"

            jobCacheDir = os.path.join(self.testDir, "jobA-%s" % i)
            os.mkdir(jobCacheDir)
            newJobA["cache_dir"] = jobCacheDir
            newJobA["type"] = "Processing"
            newJobA.create(testGroupA)

            jobHandle = open(os.path.join(jobCacheDir, "job.pkl"), "w")
            pickle.dump(newJobA, jobHandle)
            jobHandle.close()

            stateChanger.propagate([newJobA], "created", "new")

            newJobB = Job(name = "testJobB-%s" % i, files = [newFile])
            newJobB["workflow"] = "wf001"
            newJobB["siteWhitelist"] = ["T1_UK_RAL"]
            newJobB["siteBlacklist"] = []
            newJobB["sandbox"] = "%s/somesandbox" % self.testDir
            newJobB["owner"] = "Steve"

            jobCacheDir = os.path.join(self.testDir, "jobB-%s" % i)
            os.mkdir(jobCacheDir)
            newJobB["cache_dir"] = jobCacheDir
            newJobB["type"] = "Processing"
            newJobB.create(testGroupB)

            jobHandle = open(os.path.join(jobCacheDir, "job.pkl"), "w")
            pickle.dump(newJobB, jobHandle)
            jobHandle.close()

            stateChanger.propagate([newJobB], "created", "new")

        return

    def testCaching(self):
        """
        _testCaching_

        Verify that JobSubmitter caching works.
        """
        config            = self.createConfig()
        mySubmitterPoller = JobSubmitterPoller(config)
        mySubmitterPoller.refreshCache()

        self.assertEqual(len(mySubmitterPoller.cachedJobIDs), 0,
                         "Error: The job cache should be empty.")

        self.injectJobs()
        mySubmitterPoller.refreshCache()

        # Verify the cache is full
        self.assertEqual(len(mySubmitterPoller.cachedJobIDs), 20,
                         "Error: The job cache should contain 20 jobs.  Contains: %i" % len(mySubmitterPoller.cachedJobIDs))

        killWorkflow("wf001", jobCouchConfig = config)
        mySubmitterPoller.refreshCache()

        # Verify that the workflow is gone from the cache
        self.assertEqual(len(mySubmitterPoller.cachedJobIDs), 10,
                         "Error: The job cache should contain 10 jobs. Contains: %i" % len(mySubmitterPoller.cachedJobIDs))

        killWorkflow("wf002", jobCouchConfig = config)
        mySubmitterPoller.refreshCache()

        # Verify that the workflow is gone from the cache
        self.assertEqual(len(mySubmitterPoller.cachedJobIDs), 0,
                         "Error: The job cache should be empty.  Contains: %i" % len(mySubmitterPoller.cachedJobIDs))
        return

if __name__ == "__main__":
    unittest.main()
