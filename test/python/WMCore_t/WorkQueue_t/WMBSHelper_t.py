#!/usr/bin/env python
"""
_WMBSHelper_t_

Unit tests for the WMBSHelper class.
"""

import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job

from WMCore.DataStructs.Mask import Mask

from WMCore.DAOFactory import DAOFactory
from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WMBSHelper import killWorkflow

from WMQuality.Emulators.DataBlockGenerator.Globals import GlobalParams
from WMQuality.Emulators.DBSClient.DBSReader import DBSReader as MockDBSReader
from WMQuality.Emulators.SiteDBClient.SiteDB import SiteDBJSON as fakeSiteDB
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload, \
                                          getTestArguments as getRerecoArgs

from WMQuality.Emulators.WMSpecGenerator.Samples.TestMonteCarloWorkload \
    import monteCarloWorkload, getMCArgs

from WMQuality.Emulators import EmulatorSetup
from WMQuality.TestInitCouchApp import TestInitCouchApp

from WMCore.BossAir.BossAirAPI              import BossAirAPI
from WMCore.Configuration                   import loadConfigurationFile
from WMCore.ResourceControl.ResourceControl import ResourceControl

rerecoArgs = getRerecoArgs()
mcArgs = getMCArgs()

def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable-msg=E1101,E1103
    return wmspec.taskIterator().next()

class WMBSHelperTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("wmbshelper_t", "JobDump")
        os.environ["COUCHDB"] = "wmbshelper_t"
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMComponent.DBSBuffer.Database",
                                                 "WMCore.WorkQueue.Database",
                                                 "WMCore.BossAir",
                                                 "WMCore.ResourceControl"],
                                useDefault = False)
        
        self.workDir = self.testInit.generateWorkDir()
        
        self.wmspec = self.createWMSpec()
        self.topLevelTask = getFirstTask(self.wmspec)
        self.inputDataset = self.topLevelTask.inputDataset()
        self.dataset = self.topLevelTask.getInputDatasetPath()
        self.dbs = MockDBSReader(self.inputDataset.dbsurl)
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = threading.currentThread().logger,
                                     dbinterface = threading.currentThread().dbi)
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()        
        return

    def setupForKillTest(self, baAPI = None):
        """
        _setupForKillTest_

        Inject a workflow into WMBS that has a processing task, a merge task and
        a cleanup task.  Inject files into the various tasks at various
        processing states (acquired, complete, available...).  Also create jobs
        for each subscription in various states.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daoFactory(classname = "Locations.New")
        changeStateAction = daoFactory(classname = "Jobs.ChangeState")
        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName = 'site1', seName = 'goodse.cern.ch',
                                   ceName = 'site1', plugin = "TestPlugin")
        resourceControl.insertThreshold(siteName = 'site1', taskType = 'Processing', \
                                        maxSlots = 10000)

        inputFileset = Fileset("input")
        inputFileset.create()

        inputFileA = File("lfnA", locations = "goodse.cern.ch")
        inputFileB = File("lfnB", locations = "goodse.cern.ch")
        inputFileC = File("lfnC", locations = "goodse.cern.ch")
        inputFileA.create()
        inputFileB.create()
        inputFileC.create()

        inputFileset.addFile(inputFileA)
        inputFileset.addFile(inputFileB)
        inputFileset.addFile(inputFileC)
        inputFileset.commit()
        
        unmergedOutputFileset = Fileset("unmerged")        
        unmergedOutputFileset.create()

        unmergedFileA = File("lfnA", locations = "goodse.cern.ch")
        unmergedFileB = File("lfnB", locations = "goodse.cern.ch")
        unmergedFileC = File("lfnC", locations = "goodse.cern.ch")
        unmergedFileA.create()
        unmergedFileB.create()
        unmergedFileC.create()        

        unmergedOutputFileset.addFile(unmergedFileA)
        unmergedOutputFileset.addFile(unmergedFileB)
        unmergedOutputFileset.addFile(unmergedFileC)
        unmergedOutputFileset.commit()

        mainProcWorkflow = Workflow(spec = "spec1", owner = "Steve",
                                    name = "Main", task = "Proc")
        mainProcWorkflow.create()
        mainProcMergeWorkflow = Workflow(spec = "spec1", owner = "Steve",
                                         name = "Main", task = "ProcMerge")
        mainProcMergeWorkflow.create()
        mainCleanupWorkflow = Workflow(spec = "spec1", owner = "Steve",
                                       name = "Main", task = "Cleanup")
        mainCleanupWorkflow.create()

        self.mainProcSub = Subscription(fileset = inputFileset,
                                        workflow = mainProcWorkflow,
                                        type = "Processing")
        self.mainProcSub.create()
        self.mainProcSub.acquireFiles(inputFileA)
        self.mainProcSub.completeFiles(inputFileB)

        procJobGroup = JobGroup(subscription = self.mainProcSub)
        procJobGroup.create()
        self.procJobA = Job(name = "ProcJobA")
        self.procJobA["state"] = "new"
        self.procJobA["location"] = "site1"
        self.procJobB = Job(name = "ProcJobB")
        self.procJobB["state"] = "executing"
        self.procJobB["location"] = "site1"
        self.procJobC = Job(name = "ProcJobC")
        self.procJobC["state"] = "complete"
        self.procJobC["location"] = "site1"
        self.procJobA.create(procJobGroup)
        self.procJobB.create(procJobGroup)
        self.procJobC.create(procJobGroup)

        self.mainMergeSub = Subscription(fileset = unmergedOutputFileset,
                                         workflow = mainProcMergeWorkflow,
                                         type = "Merge")
        self.mainMergeSub.create()
        self.mainMergeSub.acquireFiles(unmergedFileA)
        self.mainMergeSub.failFiles(unmergedFileB)

        mergeJobGroup = JobGroup(subscription = self.mainMergeSub)
        mergeJobGroup.create()
        self.mergeJobA = Job(name = "MergeJobA")
        self.mergeJobA["state"] = "exhausted"
        self.mergeJobA["location"] = "site1"
        self.mergeJobB = Job(name = "MergeJobB")
        self.mergeJobB["state"] = "cleanout"
        self.mergeJobB["location"] = "site1"
        self.mergeJobC = Job(name = "MergeJobC")
        self.mergeJobC["state"] = "new"
        self.mergeJobC["location"] = "site1"
        self.mergeJobA.create(mergeJobGroup)
        self.mergeJobB.create(mergeJobGroup)
        self.mergeJobC.create(mergeJobGroup)
        
        self.mainCleanupSub = Subscription(fileset = unmergedOutputFileset,
                                           workflow = mainCleanupWorkflow,
                                           type = "Cleanup")
        self.mainCleanupSub.create()
        self.mainCleanupSub.acquireFiles(unmergedFileA)
        self.mainCleanupSub.completeFiles(unmergedFileB)

        cleanupJobGroup = JobGroup(subscription = self.mainCleanupSub)
        cleanupJobGroup.create()
        self.cleanupJobA = Job(name = "CleanupJobA")
        self.cleanupJobA["state"] = "new"
        self.cleanupJobA["location"] = "site1"
        self.cleanupJobB = Job(name = "CleanupJobB")
        self.cleanupJobB["state"] = "executing"
        self.cleanupJobB["location"] = "site1"
        self.cleanupJobC = Job(name = "CleanupJobC")
        self.cleanupJobC["state"] = "complete"
        self.cleanupJobC["location"] = "site1"
        self.cleanupJobA.create(cleanupJobGroup)
        self.cleanupJobB.create(cleanupJobGroup)
        self.cleanupJobC.create(cleanupJobGroup)

        jobList = [self.procJobA, self.procJobB, self.procJobC,
                   self.mergeJobA, self.mergeJobB, self.mergeJobC,
                   self.cleanupJobA, self.cleanupJobB, self.cleanupJobC]

        changeStateAction.execute(jobList)

        if baAPI:
            for job in jobList:
                job['plugin'] = 'TestPlugin'
                job['userdn'] = 'Steve'
            baAPI.createNewJobs(wmbsJobs = jobList)

        # We'll create an unrelated workflow to verify that it isn't affected
        # by the killing code.
        bogusFileset = Fileset("dontkillme")
        bogusFileset.create()

        bogusFileA = File("bogus/lfnA", locations = "goodse.cern.ch")
        bogusFileA.create()
        bogusFileset.addFile(bogusFileA)
        bogusFileset.commit()
        
        bogusWorkflow = Workflow(spec = "spec2", owner = "Steve",
                                 name = "Bogus", task = "Proc")
        bogusWorkflow.create()
        self.bogusSub = Subscription(fileset = bogusFileset,
                                     workflow = bogusWorkflow,
                                     type = "Processing")
        self.bogusSub.create()
        self.bogusSub.acquireFiles(bogusFileA)
        return
        
    def verifyFileKillStatus(self):
        """
        _verifyFileKillStatus_

        Verify that all files were killed correctly.  The status of files in
        Cleanup and LogCollect subscriptions isn't modified.  Status of
        already completed and failed files is not modified.  Also verify that
        the bogus subscription is untouched.
        """
        failedFiles = self.mainProcSub.filesOfStatus("Failed")
        acquiredFiles = self.mainProcSub.filesOfStatus("Acquired")
        completedFiles = self.mainProcSub.filesOfStatus("Completed")
        availableFiles = self.mainProcSub.filesOfStatus("Available")
        bogusAcquiredFiles = self.bogusSub.filesOfStatus("Acquired")

        self.assertEqual(len(availableFiles), 0, \
                         "Error: There should be no available files.")
        self.assertEqual(len(acquiredFiles), 0, \
                         "Error: There should be no acquired files.")
        self.assertEqual(len(bogusAcquiredFiles), 1, \
                         "Error: There should be one acquired file.")
        
        self.assertEqual(len(completedFiles), 1, \
                         "Error: There should be only one completed file.")
        self.assertEqual(list(completedFiles)[0]["lfn"], "lfnB", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(failedFiles), 2, \
                         "Error: There should be only two failed files.")
        goldenLFNs = ["lfnA", "lfnC"]
        for failedFile in failedFiles:
            self.assertTrue(failedFile["lfn"] in goldenLFNs, \
                          "Error: Extra failed file.")
            goldenLFNs.remove(failedFile["lfn"])

        self.assertEqual(len(goldenLFNs), 0, \
                         "Error: Missing LFN")

        failedFiles = self.mainMergeSub.filesOfStatus("Failed")
        acquiredFiles = self.mainMergeSub.filesOfStatus("Acquired")
        completedFiles = self.mainMergeSub.filesOfStatus("Completed")
        availableFiles = self.mainMergeSub.filesOfStatus("Available")

        self.assertEqual(len(acquiredFiles), 0, \
                         "Error: Merge subscription should have 0 acq files.")
        self.assertEqual(len(availableFiles), 0, \
                         "Error: Merge subscription should have 0 avail files.") 
        self.assertEqual(len(completedFiles), 0, \
                         "Error: Merge subscription should have 0 compl files.")

        self.assertEqual(len(failedFiles), 3, \
                         "Error: There should be only three failed files.")
        goldenLFNs = ["lfnA", "lfnB", "lfnC"]
        for failedFile in failedFiles:
            self.assertTrue(failedFile["lfn"] in goldenLFNs, \
                          "Error: Extra failed file.")
            goldenLFNs.remove(failedFile["lfn"])

        self.assertEqual(len(goldenLFNs), 0, \
                         "Error: Missing LFN")

        failedFiles = self.mainCleanupSub.filesOfStatus("Failed")
        acquiredFiles = self.mainCleanupSub.filesOfStatus("Acquired")
        completedFiles = self.mainCleanupSub.filesOfStatus("Completed")
        availableFiles = self.mainCleanupSub.filesOfStatus("Available")

        self.assertEqual(len(failedFiles), 0, \
                         "Error: Cleanup subscription should have 0 fai files.")

        self.assertEqual(len(acquiredFiles), 1, \
                         "Error: There should be only one acquired file.")
        self.assertEqual(list(acquiredFiles)[0]["lfn"], "lfnA", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(completedFiles), 1, \
                         "Error: There should be only one completed file.")
        self.assertEqual(list(completedFiles)[0]["lfn"], "lfnB", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(availableFiles), 1, \
                         "Error: There should be only one available file.")
        self.assertEqual(list(availableFiles)[0]["lfn"], "lfnC", \
                         "Error: Wrong completed LFN.")

        return

    def verifyJobKillStatus(self):
        """
        _verifyJobKillStatus_

        Verify that jobs are killed correctly.  Jobs belonging to Cleanup and
        LogCollect subscriptions are not killed.  The status of jobs that have
        already finished running is not changed.
        """
        self.procJobA.load()
        self.procJobB.load()
        self.procJobC.load()

        self.assertEqual(self.procJobA["state"], "killed", \
                         "Error: Proc job A should be killed.")
        self.assertEqual(self.procJobB["state"], "killed", \
                         "Error: Proc job B should be killed.")
        self.assertEqual(self.procJobC["state"], "complete", \
                         "Error: Proc job C should be complete.")

        self.mergeJobA.load()
        self.mergeJobB.load()
        self.mergeJobC.load()

        self.assertEqual(self.mergeJobA["state"], "exhausted", \
                         "Error: Merge job A should be exhausted.")
        self.assertEqual(self.mergeJobB["state"], "cleanout", \
                         "Error: Merge job B should be cleanout.")
        self.assertEqual(self.mergeJobC["state"], "killed", \
                         "Error: Merge job C should be killed.")

        self.cleanupJobA.load()
        self.cleanupJobB.load()
        self.cleanupJobC.load()

        self.assertEqual(self.cleanupJobA["state"], "new", \
                         "Error: Cleanup job A should be new.")
        self.assertEqual(self.cleanupJobB["state"], "executing", \
                         "Error: Cleanup job B should be executing.")
        self.assertEqual(self.cleanupJobC["state"], "complete", \
                         "Error: Cleanup job C should be complete.")
        return

    def testKillWorkflow(self):
        """
        _testKillWorkflow_

        Verify that workflow killing works correctly.
        """
        configFile = EmulatorSetup.setupWMAgentConfig()

        config = loadConfigurationFile(configFile)

        baAPI = BossAirAPI(config = config)

        # Create nine jobs
        self.setupForKillTest(baAPI = baAPI)
        self.assertEqual(len(baAPI._listRunJobs()), 9)

        killWorkflow("Main")

        self.verifyFileKillStatus()
        self.verifyJobKillStatus()
        self.assertEqual(len(baAPI._listRunJobs()), 8)
        EmulatorSetup.deleteConfig(configFile)
        return

    def createTestWMSpec(self):
        """
        _createTestWMSpec_

        Create a WMSpec that has a processing, merge and skims tasks that can
        be used by the subscription creation test.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))
        testWorkload.setOwner("sfoulkes@fnal.gov")
        testWorkload.setSpecUrl("/path/to/workload")

        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setTaskType("Processing")
        procTask.setSplittingAlgorithm("FileBased", files_per_job = 1)        
        procTaskCMSSW = procTask.makeStep("cmsRun1")
        procTaskCMSSW.setStepType("CMSSW")
        procTaskCMSSWHelper = procTaskCMSSW.getTypeHelper()
        procTask.setTaskType("Processing")
        procTask.setSiteWhitelist(["site1"])
        procTask.setSiteBlacklist(["site2"])
        procTask.applyTemplates()

        procTaskCMSSWHelper.addOutputModule("OutputA",
                                            primaryDataset = "bogusPrimary",
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DataTierA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = None)

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setInputReference(procTaskCMSSW, outputModule = "OutputA")
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", min_merge_size = 1,
                                        max_merge_size = 2, max_merge_events = 3)
        mergeTaskCMSSW = mergeTask.makeStep("cmsRun1")
        mergeTaskCMSSW.setStepType("CMSSW")
        mergeTaskCMSSWHelper = mergeTaskCMSSW.getTypeHelper()
        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()

        mergeTaskCMSSWHelper.addOutputModule("Merged",
                                             primaryDataset = "bogusPrimary",
                                             processedDataset = "bogusProcessed",
                                             dataTier = "DataTierA",
                                             lfnBase = "bogusUnmerged",
                                             mergedLFNBase = "bogusMerged",
                                             filterName = None)        

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTask.setInputReference(mergeTaskCMSSW, outputModule = "Merged")
        skimTask.setSplittingAlgorithm("TwoFileBased", files_per_job = 1)
        skimTaskCMSSW = skimTask.makeStep("cmsRun1")
        skimTaskCMSSW.setStepType("CMSSW")
        skimTaskCMSSWHelper = skimTaskCMSSW.getTypeHelper()
        skimTask.setTaskType("Skim")
        skimTask.applyTemplates()

        skimTaskCMSSWHelper.addOutputModule("SkimOutputA",
                                            primaryDataset = "bogusPrimary",
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DataTierA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = None)

        skimTaskCMSSWHelper.addOutputModule("SkimOutputB",
                                            primaryDataset = "bogusPrimary",
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DataTierA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = None)
        return testWorkload

    def testCreateSubscription(self):
        """
        _testCreateSubscription_

        Verify that the subscription creation code works correctly.
        """
        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName = 'site1', seName = 'goodse.cern.ch',
                                   ceName = 'site1', plugin = "TestPlugin")
        resourceControl.insertSite(siteName = 'site2', seName = 'goodse2.cern.ch',
                                   ceName = 'site2', plugin = "TestPlugin")        

        testWorkload = self.createTestWMSpec()
        testWMBSHelper = WMBSHelper(testWorkload, "SomeBlock")
        testWMBSHelper.createSubscription()

        procWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/ProcessingTask")
        procWorkflow.load()

        self.assertEqual(procWorkflow.owner, "sfoulkes@fnal.gov",
                         "Error: Wrong owner.")
        self.assertEqual(procWorkflow.spec, "/path/to/workload",
                         "Error: Wrong spec URL")
        self.assertEqual(len(procWorkflow.outputMap.keys()), 1,
                         "Error: Wrong number of WF outputs.")

        mergedProcOutput = procWorkflow.outputMap["OutputA"]["merged_output_fileset"]
        unmergedProcOutput = procWorkflow.outputMap["OutputA"]["output_fileset"]

        mergedProcOutput.loadData()
        unmergedProcOutput.loadData()

        self.assertEqual(mergedProcOutput.name, "/TestWorkload/ProcessingTask/MergeTask/merged-Merged",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedProcOutput.name, "/TestWorkload/ProcessingTask/unmerged-OutputA",
                         "Error: Unmerged output fileset is wrong.")

        mergeWorkflow = Workflow(name = "TestWorkload",
                                 task = "/TestWorkload/ProcessingTask/MergeTask")
        mergeWorkflow.load()

        self.assertEqual(mergeWorkflow.owner, "sfoulkes@fnal.gov",
                         "Error: Wrong owner.")
        self.assertEqual(mergeWorkflow.spec, "/path/to/workload",
                         "Error: Wrong spec URL")
        self.assertEqual(len(mergeWorkflow.outputMap.keys()), 1,
                         "Error: Wrong number of WF outputs.")

        mergedMergeOutput = mergeWorkflow.outputMap["Merged"]["merged_output_fileset"]
        unmergedMergeOutput = mergeWorkflow.outputMap["Merged"]["output_fileset"]

        mergedMergeOutput.loadData()
        unmergedMergeOutput.loadData()

        self.assertEqual(mergedMergeOutput.name, "/TestWorkload/ProcessingTask/MergeTask/merged-Merged",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/ProcessingTask/MergeTask/merged-Merged",
                         "Error: Unmerged output fileset is wrong.")

        skimWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/ProcessingTask/MergeTask/SkimTask")
        skimWorkflow.load()

        self.assertEqual(skimWorkflow.owner, "sfoulkes@fnal.gov",
                         "Error: Wrong owner.")
        self.assertEqual(skimWorkflow.spec, "/path/to/workload",
                         "Error: Wrong spec URL")
        self.assertEqual(len(skimWorkflow.outputMap.keys()), 2,
                         "Error: Wrong number of WF outputs.")

        mergedSkimOutputA = skimWorkflow.outputMap["SkimOutputA"]["merged_output_fileset"]
        unmergedSkimOutputA = skimWorkflow.outputMap["SkimOutputA"]["output_fileset"]
        mergedSkimOutputB = skimWorkflow.outputMap["SkimOutputB"]["merged_output_fileset"]
        unmergedSkimOutputB = skimWorkflow.outputMap["SkimOutputB"]["output_fileset"]

        mergedSkimOutputA.loadData()
        unmergedSkimOutputA.loadData()
        mergedSkimOutputB.loadData()
        unmergedSkimOutputB.loadData()

        self.assertEqual(mergedSkimOutputA.name, "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputA",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedSkimOutputA.name, "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputA",
                         "Error: Unmerged output fileset is wrong.")
        self.assertEqual(mergedSkimOutputB.name, "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputB",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedSkimOutputB.name, "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputB",
                         "Error: Unmerged output fileset is wrong.")

        topLevelFileset = Fileset(name = "TestWorkload-ProcessingTask-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset = topLevelFileset, workflow = procWorkflow)
        procSubscription.loadData()

        self.assertEqual(len(procSubscription.getWhiteBlackList()), 2,
                         "Error: Wrong site white/black list for proc sub.")
        for site in procSubscription.getWhiteBlackList():
            if site["site_name"] == "site1":
                self.assertEqual(site["valid"], 1,
                                 "Error: Site should be white listed.")
            else:
                self.assertEqual(site["valid"], 0,
                                 "Error: Site should be black listed.")                

        self.assertEqual(procSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        mergeSubscription = Subscription(fileset = unmergedProcOutput, workflow = mergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(len(mergeSubscription.getWhiteBlackList()), 0,
                         "Error: Wrong white/black list for merge sub.")

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algo.")        

        skimSubscription = Subscription(fileset = mergedMergeOutput, workflow = skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "TwoFileBased",
                         "Error: Wrong split algo.")
        return

    def setupMCWMSpec(self):
        """Setup MC workflow"""
        self.wmspec = self.createMCWMSpec()
        self.topLevelTask = getFirstTask(self.wmspec)
        self.inputDataset = self.topLevelTask.inputDataset()
        self.dataset = self.topLevelTask.getInputDatasetPath()
        self.dbs = None
        self.siteDB = fakeSiteDB()

    def createWMSpec(self, name = 'ReRecoWorkload'):
        wmspec = rerecoWorkload(name, rerecoArgs)
        wmspec.setSpecUrl("/path/to/workload")
        return wmspec 

    def createMCWMSpec(self, name = 'MonteCarloWorkload'):
        wmspec = monteCarloWorkload(name, mcArgs)
        wmspec.setSpecUrl("/path/to/workload")        
        getFirstTask(wmspec).addProduction(totalevents = 10000)
        getFirstTask(wmspec).setSiteWhitelist(['SiteA', 'SiteB', 'SiteC'])
        return wmspec

    def getDBS(self, wmspec):
        topLevelTask = getFirstTask(wmspec)
        inputDataset = topLevelTask.inputDataset()
        dbs = MockDBSReader(inputDataset.dbsurl)
        #dbsDict = {self.inputDataset.dbsurl : self.dbs}
        return dbs
        
    def createWMBSHelperWithTopTask(self, wmspec, block, mask = None):
        
        topLevelTask = getFirstTask(wmspec)
         
        wmbs = WMBSHelper(wmspec, block, mask)
        if block:
            block = self.dbs.getFileBlock(block)[block]
        wmbs.createSubscriptionAndAddFiles(dbsBlock = block)
        return wmbs

#    def testProduction(self):
#        """Production workflow"""
#        pass

    def testReReco(self):
        """ReReco workflow"""
        # create workflow
        block = self.dataset + "#1"
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block))
        self.assertEqual(len(files), 1)

    def testReRecoBlackRunRestriction(self):
        """ReReco workflow with Run restrictions"""
        block = self.dataset + "#2"
        #add run blacklist
        self.topLevelTask.setInputRunBlacklist([1, 2, 3, 4])
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        
        files = wmbs.validFiles(self.dbs.getFileBlock(block)[block]['Files'])
        self.assertEqual(len(files), 0)


    def testReRecoWhiteRunRestriction(self):
        block = self.dataset + "#2"
        # Run Whitelist
        self.topLevelTask.setInputRunWhitelist([2])
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block)[block]['Files'])
        self.assertEqual(len(files), GlobalParams.numOfFilesPerBlock())
        
    def testDuplicateFileInsert(self):
        # using default wmspec
        block = self.dataset + "#1"
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        wmbs.topLevelFileset.loadData()
        numOfFiles = len(wmbs.topLevelFileset.files)
        # check initially inserted files.
        dbsFiles = self.dbs.getFileBlock(block)[block]['Files']
        self.assertEqual(numOfFiles, len(dbsFiles))
        firstFileset = wmbs.topLevelFileset
        wmbsDao = wmbs.daofactory(classname = "Files.InFileset")
        
        numOfFiles = len(wmbsDao.execute(firstFileset.id))
        self.assertEqual(numOfFiles, len(dbsFiles))
        
        # use the new spec with same inputdataset
        block = self.dataset + "#1"
        wmspec = self.createWMSpec("TestSpec1")
        dbs = self.getDBS(wmspec)
        wmbs = self.createWMBSHelperWithTopTask(wmspec, block)
        # check duplicate insert
        dbsFiles = dbs.getFileBlock(block)[block]['Files']
        numOfFiles = wmbs.addFiles(dbs.getFileBlock(block)[block])
        self.assertEqual(numOfFiles, 0)
        secondFileset = wmbs.topLevelFileset
        
        wmbsDao = wmbs.daofactory(classname = "Files.InFileset")
        numOfFiles = len(wmbsDao.execute(secondFileset.id))
        self.assertEqual(numOfFiles, len(dbsFiles))
        
        self.assertNotEqual(firstFileset.id, secondFileset.id)
    
    def testParentage(self):
        """
        TODO: add the parentage test. 
        1. check whether parent files are created in wmbs.
        2. check parent files are associated to child.
        3. When 2 specs with the same input data (one with parent processing, one without it)
           is inserted, if one without parent processing inserted first then the other with 
           parent processing insert, it still needs to create parent files although child files
           are duplicate 
        """
        pass



    def testMCFakeFileInjection(self):
        """Inject fake Monte Carlo files into WMBS"""
        self.setupMCWMSpec()

        mask = Mask(FirstRun = 12, FirstLumi = 1234, FirstEvent = 12345,
                    LastEvent = 999995, LastLumi = 12345, LastRun = 12)

        # add sites that would normally be added by operator via resource_control
        locationDAO = self.daoFactory(classname = "Locations.New")
        ses = []
        for site in ['SiteA', 'SiteB']:
            locationDAO.execute(siteName = site, seName = self.siteDB.cmsNametoSE(site))
            ses.append(self.siteDB.cmsNametoSE(site))

        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, None, mask)
        subscription = wmbs.topLevelSubscription
        self.assertEqual(1, subscription.exists())
        fileset = subscription['fileset']
        self.assertEqual(1, fileset.exists())
        fileset.loadData() # need to refresh from database

        self.assertEqual(len(fileset.files), 1)
        self.assertEqual(len(fileset.parents), 0)
        self.assertFalse(fileset.open)

        file = list(fileset.files)[0]
        self.assertEqual(file['first_event'], mask['FirstEvent'])
        self.assertEqual(file['last_event'], mask['LastEvent'])
        self.assertEqual(file['events'], mask['LastEvent'] - mask['FirstEvent'] + 1) # inclusive range
        self.assertEqual(file['merged'], False) # merged files get added to dbs
        self.assertEqual(len(file['parents']), 0)
        #file.loadData()
        self.assertEqual(sorted(file['locations']), sorted(ses))
        self.assertEqual(len(file.getParentLFNs()), 0)

        self.assertEqual(len(file.getRuns()), 1)
        run = file.getRuns()[0]
        self.assertEqual(run.run, mask['FirstRun'])
        self.assertEqual(run.lumis[0], mask['FirstLumi'])
        self.assertEqual(run.lumis[-1], mask['LastLumi'])
        self.assertEqual(len(run.lumis), mask['LastLumi'] - mask['FirstLumi'] + 1)

if __name__ == '__main__':
    unittest.main()
