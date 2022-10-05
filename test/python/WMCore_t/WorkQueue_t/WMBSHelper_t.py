#!/usr/bin/env python
"""
_WMBSHelper_t_

Unit tests for the WMBSHelper class.
"""

from builtins import next
import os
import threading
import unittest

from WMCore.BossAir.BossAirAPI import BossAirAPI
from WMCore.Configuration import loadConfigurationFile
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Mask import Mask
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBase import getTestBase
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WMBSHelper import killWorkflow
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.WMSpecGenerator.Samples.BasicProductionWorkload import getProdArgs, taskChainWorkload
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig
from WMQuality.TestInitCouchApp import TestInitCouchApp

rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
mcArgs = getProdArgs()

BLOCK1 = '03fe83c2-0c23-11e1-b764-003048caaace'
BLOCK2 = '04be2fcc-0b8f-11e1-b764-003048caaace'


def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable=E1101,E1103
    return next(wmspec.taskIterator())


class WMBSHelperTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        """
        super(WMBSHelperTest, self).setUp()

        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.testInit.setupCouch("wmbshelper_t/jobs", "JobDump")
        self.testInit.setupCouch("wmbshelper_t/fwjrs", "FWJRDump")
        self.testInit.setupCouch("config_test", "GroupUser", "ConfigCache")
        os.environ["COUCHDB"] = "wmbshelper_t"
        self.testInit.setSchema(customModules=["WMCore.WMBS",
                                               "WMComponent.DBS3Buffer",
                                               "WMCore.BossAir",
                                               "WMCore.ResourceControl"],
                                useDefault=False)

        self.workDir = self.testInit.generateWorkDir()

        self.wmspec = self.createWMSpec()
        self.topLevelTask = getFirstTask(self.wmspec)
        self.inputDataset = self.topLevelTask.inputDataset()
        self.dataset = self.topLevelTask.getInputDatasetPath()
        self.dbs = DBSReader(self.inputDataset.dbsurl)
        self.rucioAcct = "wmcore_transferor"
        self.rucio = Rucio(self.rucioAcct)
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=threading.currentThread().logger,
                                     dbinterface=threading.currentThread().dbi)

        self.configFile = EmulatorSetup.setupWMAgentConfig()
        self.config = loadConfigurationFile(self.configFile)

        self.config.component_("JobSubmitter")
        self.config.JobSubmitter.submitDir = self.workDir
        self.config.JobSubmitter.submitScript = os.path.join(getTestBase(),
                                                             'WMComponent_t/JobSubmitter_t',
                                                             'submit.sh')

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        EmulatorSetup.deleteConfig(self.configFile)
        super(WMBSHelperTest, self).tearDown()

        return

    def setupForKillTest(self, baAPI=None):
        """
        _setupForKillTest_

        Inject a workflow into WMBS that has a processing task, a merge task and
        a cleanup task.  Inject files into the various tasks at various
        processing states (acquired, complete, available...).  Also create jobs
        for each subscription in various states.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        dummyLocationAction = daoFactory(classname="Locations.New")
        changeStateAction = daoFactory(classname="Jobs.ChangeState")
        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName='site1', pnn='goodse.cern.ch',
                                   ceName='site1', plugin="TestPlugin")
        resourceControl.insertThreshold(siteName='site1', taskType='Processing', \
                                        maxSlots=10000, pendingSlots=10000)

        userDN = 'someDN'
        userAction = daoFactory(classname="Users.New")
        userAction.execute(dn=userDN, group_name='DEFAULT', role_name='DEFAULT')

        inputFileset = Fileset("input")
        inputFileset.create()

        inputFileA = File("lfnA", locations="goodse.cern.ch")
        inputFileB = File("lfnB", locations="goodse.cern.ch")
        inputFileC = File("lfnC", locations="goodse.cern.ch")
        inputFileA.create()
        inputFileB.create()
        inputFileC.create()

        inputFileset.addFile(inputFileA)
        inputFileset.addFile(inputFileB)
        inputFileset.addFile(inputFileC)
        inputFileset.commit()

        unmergedOutputFileset = Fileset("unmerged")
        unmergedOutputFileset.create()

        unmergedFileA = File("ulfnA", locations="goodse.cern.ch")
        unmergedFileB = File("ulfnB", locations="goodse.cern.ch")
        unmergedFileC = File("ulfnC", locations="goodse.cern.ch")
        unmergedFileA.create()
        unmergedFileB.create()
        unmergedFileC.create()

        unmergedOutputFileset.addFile(unmergedFileA)
        unmergedOutputFileset.addFile(unmergedFileB)
        unmergedOutputFileset.addFile(unmergedFileC)
        unmergedOutputFileset.commit()

        mainProcWorkflow = Workflow(spec="spec1", owner="Steve",
                                    name="Main", task="Proc")
        mainProcWorkflow.create()
        mainProcMergeWorkflow = Workflow(spec="spec1", owner="Steve",
                                         name="Main", task="ProcMerge")
        mainProcMergeWorkflow.create()
        mainCleanupWorkflow = Workflow(spec="spec1", owner="Steve",
                                       name="Main", task="Cleanup")
        mainCleanupWorkflow.create()

        self.mainProcSub = Subscription(fileset=inputFileset,
                                        workflow=mainProcWorkflow,
                                        type="Processing")
        self.mainProcSub.create()
        self.mainProcSub.acquireFiles(inputFileA)
        self.mainProcSub.completeFiles(inputFileB)

        procJobGroup = JobGroup(subscription=self.mainProcSub)
        procJobGroup.create()
        self.procJobA = Job(name="ProcJobA")
        self.procJobA["state"] = "new"
        self.procJobA["location"] = "site1"
        self.procJobB = Job(name="ProcJobB")
        self.procJobB["state"] = "executing"
        self.procJobB["location"] = "site1"
        self.procJobC = Job(name="ProcJobC")
        self.procJobC["state"] = "complete"
        self.procJobC["location"] = "site1"
        self.procJobA.create(procJobGroup)
        self.procJobB.create(procJobGroup)
        self.procJobC.create(procJobGroup)

        self.mainMergeSub = Subscription(fileset=unmergedOutputFileset,
                                         workflow=mainProcMergeWorkflow,
                                         type="Merge")
        self.mainMergeSub.create()
        self.mainMergeSub.acquireFiles(unmergedFileA)
        self.mainMergeSub.failFiles(unmergedFileB)

        mergeJobGroup = JobGroup(subscription=self.mainMergeSub)
        mergeJobGroup.create()
        self.mergeJobA = Job(name="MergeJobA")
        self.mergeJobA["state"] = "exhausted"
        self.mergeJobA["location"] = "site1"
        self.mergeJobB = Job(name="MergeJobB")
        self.mergeJobB["state"] = "cleanout"
        self.mergeJobB["location"] = "site1"
        self.mergeJobC = Job(name="MergeJobC")
        self.mergeJobC["state"] = "new"
        self.mergeJobC["location"] = "site1"
        self.mergeJobA.create(mergeJobGroup)
        self.mergeJobB.create(mergeJobGroup)
        self.mergeJobC.create(mergeJobGroup)

        self.mainCleanupSub = Subscription(fileset=unmergedOutputFileset,
                                           workflow=mainCleanupWorkflow,
                                           type="Cleanup")
        self.mainCleanupSub.create()
        self.mainCleanupSub.acquireFiles(unmergedFileA)
        self.mainCleanupSub.completeFiles(unmergedFileB)

        cleanupJobGroup = JobGroup(subscription=self.mainCleanupSub)
        cleanupJobGroup.create()
        self.cleanupJobA = Job(name="CleanupJobA")
        self.cleanupJobA["state"] = "new"
        self.cleanupJobA["location"] = "site1"
        self.cleanupJobB = Job(name="CleanupJobB")
        self.cleanupJobB["state"] = "executing"
        self.cleanupJobB["location"] = "site1"
        self.cleanupJobC = Job(name="CleanupJobC")
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
                job['userdn'] = userDN
                job['usergroup'] = 'DEFAULT'
                job['userrole'] = 'DEFAULT'
                job['custom']['location'] = 'site1'
            baAPI.createNewJobs(wmbsJobs=jobList)

        # We'll create an unrelated workflow to verify that it isn't affected
        # by the killing code.
        bogusFileset = Fileset("dontkillme")
        bogusFileset.create()

        bogusFileA = File("bogus/lfnA", locations="goodse.cern.ch")
        bogusFileA.create()
        bogusFileset.addFile(bogusFileA)
        bogusFileset.commit()

        bogusWorkflow = Workflow(spec="spec2", owner="Steve",
                                 name="Bogus", task="Proc")
        bogusWorkflow.create()
        self.bogusSub = Subscription(fileset=bogusFileset,
                                     workflow=bogusWorkflow,
                                     type="Processing")
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

        self.assertEqual(len(completedFiles), 3, \
                         "Error: There should be only one completed file.")
        goldenLFNs = ["lfnA", "lfnB", "lfnC"]
        for completedFile in completedFiles:
            self.assertTrue(completedFile["lfn"] in goldenLFNs, \
                            "Error: Extra completed file.")
            goldenLFNs.remove(completedFile["lfn"])

        self.assertEqual(len(failedFiles), 0, \
                         "Error: There should be no failed files.")

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

        self.assertEqual(len(failedFiles), 1, \
                         "Error: Merge subscription should have 1 failed files.")
        self.assertEqual(list(failedFiles)[0]["lfn"], "ulfnB",
                         "Error: Wrong failed file.")

        self.assertEqual(len(completedFiles), 2, \
                         "Error: Merge subscription should have 2 compl files.")
        goldenLFNs = ["ulfnA", "ulfnC"]
        for completedFile in completedFiles:
            self.assertTrue(completedFile["lfn"] in goldenLFNs, \
                            "Error: Extra complete file.")
            goldenLFNs.remove(completedFile["lfn"])

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
        self.assertEqual(list(acquiredFiles)[0]["lfn"], "ulfnA", \
                         "Error: Wrong acquired LFN.")

        self.assertEqual(len(completedFiles), 1, \
                         "Error: There should be only one completed file.")
        self.assertEqual(list(completedFiles)[0]["lfn"], "ulfnB", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(availableFiles), 1, \
                         "Error: There should be only one available file.")
        self.assertEqual(list(availableFiles)[0]["lfn"], "ulfnC", \
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

    def createTestWMSpec(self):
        """
        _createTestWMSpec_

        Create a WMSpec that has a processing, merge, cleanup and skims tasks that
        can be used by the subscription creation test.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))
        testWorkload.setDashboardActivity("TestReReco")
        testWorkload.setSpecUrl("/path/to/workload")
        testWorkload.setOwnerDetails("sfoulkes", "DMWM", {'dn': 'MyDN'})

        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setTaskType("Processing")
        procTask.setSplittingAlgorithm("FileBased", files_per_job=1)
        procTaskCMSSW = procTask.makeStep("cmsRun1")
        procTaskCMSSW.setStepType("CMSSW")
        procTaskCMSSWHelper = procTaskCMSSW.getTypeHelper()
        procTask.setTaskType("Processing")
        procTask.setSiteWhitelist(["site1"])
        procTask.setSiteBlacklist(["site2"])
        procTask.applyTemplates()

        procTaskCMSSWHelper.addOutputModule("OutputA",
                                            primaryDataset="bogusPrimary",
                                            processedDataset="bogusProcessed",
                                            dataTier="DataTierA",
                                            lfnBase="bogusUnmerged",
                                            mergedLFNBase="bogusMerged",
                                            filterName=None)

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setInputReference(procTaskCMSSW, outputModule="OutputA", dataTier='DataTierA')
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", min_merge_size=1,
                                        max_merge_size=2, max_merge_events=3)
        mergeTaskCMSSW = mergeTask.makeStep("cmsRun1")
        mergeTaskCMSSW.setStepType("CMSSW")
        mergeTaskCMSSWHelper = mergeTaskCMSSW.getTypeHelper()
        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()

        mergeTaskCMSSWHelper.addOutputModule("Merged",
                                             primaryDataset="bogusPrimary",
                                             processedDataset="bogusProcessed",
                                             dataTier="DataTierA",
                                             lfnBase="bogusUnmerged",
                                             mergedLFNBase="bogusMerged",
                                             filterName=None)

        cleanupTask = procTask.addTask("CleanupTask")
        cleanupTask.setInputReference(procTaskCMSSW, outputModule="OutputA", dataTier="DataTierA")
        cleanupTask.setTaskType("Merge")
        cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job=50)
        cleanupTaskCMSSW = cleanupTask.makeStep("cmsRun1")
        cleanupTaskCMSSW.setStepType("CMSSW")
        cleanupTask.setTaskType("Cleanup")
        cleanupTask.applyTemplates()

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTask.setInputReference(mergeTaskCMSSW, outputModule="Merged", dataTier="DataTierA")
        skimTask.setSplittingAlgorithm("FileBased", files_per_job=1, include_parents=True)
        skimTaskCMSSW = skimTask.makeStep("cmsRun1")
        skimTaskCMSSW.setStepType("CMSSW")
        skimTaskCMSSWHelper = skimTaskCMSSW.getTypeHelper()
        skimTask.setTaskType("Skim")
        skimTask.applyTemplates()

        skimTaskCMSSWHelper.addOutputModule("SkimOutputA",
                                            primaryDataset="bogusPrimary",
                                            processedDataset="bogusProcessed",
                                            dataTier="DataTierA",
                                            lfnBase="bogusUnmerged",
                                            mergedLFNBase="bogusMerged",
                                            filterName=None)

        skimTaskCMSSWHelper.addOutputModule("SkimOutputB",
                                            primaryDataset="bogusPrimary",
                                            processedDataset="bogusProcessed",
                                            dataTier="DataTierB",
                                            lfnBase="bogusUnmerged",
                                            mergedLFNBase="bogusMerged",
                                            filterName=None)

        return testWorkload

    def setupMCWMSpec(self):
        """Setup MC workflow"""
        self.wmspec = self.createMCWMSpec()
        self.topLevelTask = getFirstTask(self.wmspec)
        self.inputDataset = self.topLevelTask.inputDataset()
        self.dataset = self.topLevelTask.getInputDatasetPath()
        self.dbs = None

        # add sites that would normally be added by operator via resource_control
        locationDAO = self.daoFactory(classname="Locations.New")
        self.pnns = []
        for site in ['T2_XX_SiteA', 'T2_XX_SiteB']:
            locationDAO.execute(siteName=site, pnn=site)
            self.pnns.append(site)

    def createWMSpec(self, name='ReRecoWorkload'):
        factory = ReRecoWorkloadFactory()
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        wmspec = factory.factoryWorkloadConstruction(name, rerecoArgs)
        wmspec.setSpecUrl("/path/to/workload")
        wmspec.setSubscriptionInformation(custodialSites=[],
                                          nonCustodialSites=[],
                                          priority="Low")
        return wmspec

    def createMCWMSpec(self, name='MonteCarloWorkload'):
        mcArgs = TaskChainWorkloadFactory.getTestArguments()
        mcArgs["CouchDBName"] = rerecoArgs["CouchDBName"]
        mcArgs["Task1"]["ConfigCacheID"] = createConfig(mcArgs["CouchDBName"])

        wmspec = taskChainWorkload(name, mcArgs)
        wmspec.setSpecUrl("/path/to/workload")
        getFirstTask(wmspec).addProduction(totalevents=10000)
        return wmspec

    def getDBS(self, wmspec):
        topLevelTask = getFirstTask(wmspec)
        inputDataset = topLevelTask.inputDataset()
        dbs = DBSReader(inputDataset.dbsurl)
        # dbsDict = {self.inputDataset.dbsurl : self.dbs}
        return dbs

    def createWMBSHelperWithTopTask(self, wmspec, block, mask=None, parentFlag=False,
                                    detail=False, commonLocation=None):

        topLevelTask = getFirstTask(wmspec)

        wmbs = WMBSHelper(wmspec, topLevelTask.name(), block, mask, cachepath=self.workDir,
                          commonLocation=commonLocation)
        if block:
            blockName = block
            if parentFlag:
                block = self.dbs.getFileBlockWithParents(blockName)
                data = self.rucio.getReplicaInfoForBlocks(block=[blockName])
                block['PhEDExNodeNames'] = data[0]["replica"]
            else:
                block = self.dbs.getFileBlock(blockName)
                data = self.rucio.getReplicaInfoForBlocks(block=[blockName])
                block['PhEDExNodeNames'] = data[0]["replica"]
        sub, files = wmbs.createSubscriptionAndAddFiles(block=block)
        if detail:
            return wmbs, sub, files
        else:
            return wmbs

    def testKillWorkflow(self):
        """
        _testKillWorkflow_

        Verify that workflow killing works correctly.
        """
        baAPI = BossAirAPI(config=self.config, insertStates=True)

        # Create nine jobs
        self.setupForKillTest(baAPI=baAPI)
        self.assertEqual(len(baAPI._listRunJobs()), 9)
        killWorkflow("Main", self.config, self.config)

        self.verifyFileKillStatus()
        self.verifyJobKillStatus()
        self.assertEqual(len(baAPI._listRunJobs()), 8)

        return

    def testCreateSubscription(self):
        """
        _testCreateSubscription_

        Verify that the subscription creation code works correctly.
        """
        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName='site1', pnn='goodse.cern.ch',
                                   ceName='site1', plugin="TestPlugin")
        resourceControl.insertSite(siteName='site2', pnn='goodse2.cern.ch',
                                   ceName='site2', plugin="TestPlugin")

        testWorkload = self.createTestWMSpec()
        testTopLevelTask = getFirstTask(testWorkload)
        testWMBSHelper = WMBSHelper(testWorkload, testTopLevelTask.name(), "SomeBlock", cachepath=self.workDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testTopLevelTask, testWMBSHelper.topLevelFileset)

        procWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/ProcessingTask")
        procWorkflow.load()

        self.assertEqual(procWorkflow.owner, "sfoulkes",
                         "Error: Wrong owner: %s" % procWorkflow.owner)
        self.assertEqual(procWorkflow.group, "DMWM",
                         "Error: Wrong group: %s" % procWorkflow.group)
        self.assertEqual(procWorkflow.wfType, "TestReReco",
                         "Error: Wrong type.")
        self.assertEqual(procWorkflow.spec, os.path.join(self.workDir, procWorkflow.name,
                                                         "WMSandbox", "WMWorkload.pkl"),
                         "Error: Wrong spec URL")
        self.assertEqual(len(procWorkflow.outputMap), 1,
                         "Error: Wrong number of WF outputs.")
        mergedProcOutput = procWorkflow.outputMap["OutputADataTierA"][0]["merged_output_fileset"]
        unmergedProcOutput = procWorkflow.outputMap["OutputADataTierA"][0]["output_fileset"]

        mergedProcOutput.loadData()
        unmergedProcOutput.loadData()
        self.assertEqual(mergedProcOutput.name, "/TestWorkload/ProcessingTask/MergeTask/merged-MergedDataTierA",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedProcOutput.name, "/TestWorkload/ProcessingTask/unmerged-OutputADataTierA",
                         "Error: Unmerged output fileset is wrong.")

        mergeWorkflow = Workflow(name="TestWorkload",
                                 task="/TestWorkload/ProcessingTask/MergeTask")
        mergeWorkflow.load()

        self.assertEqual(mergeWorkflow.owner, "sfoulkes",
                         "Error: Wrong owner.")
        self.assertEqual(mergeWorkflow.spec, os.path.join(self.workDir, mergeWorkflow.name,
                                                          "WMSandbox", "WMWorkload.pkl"),
                         "Error: Wrong spec URL")
        self.assertEqual(len(mergeWorkflow.outputMap), 1,
                         "Error: Wrong number of WF outputs.")

        cleanupWorkflow = Workflow(name="TestWorkload",
                                   task="/TestWorkload/ProcessingTask/CleanupTask")
        cleanupWorkflow.load()

        self.assertEqual(cleanupWorkflow.owner, "sfoulkes",
                         "Error: Wrong owner.")
        self.assertEqual(cleanupWorkflow.spec, os.path.join(self.workDir, cleanupWorkflow.name,
                                                            "WMSandbox", "WMWorkload.pkl"),
                         "Error: Wrong spec URL")
        self.assertEqual(len(cleanupWorkflow.outputMap), 0,
                         "Error: Wrong number of WF outputs.")

        unmergedMergeOutput = mergeWorkflow.outputMap["MergedDataTierA"][0]["output_fileset"]
        unmergedMergeOutput.loadData()

        self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/ProcessingTask/MergeTask/merged-MergedDataTierA",
                         "Error: Unmerged output fileset is wrong.")

        skimWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/ProcessingTask/MergeTask/SkimTask")
        skimWorkflow.load()

        self.assertEqual(skimWorkflow.owner, "sfoulkes",
                         "Error: Wrong owner.")
        self.assertEqual(skimWorkflow.spec, os.path.join(self.workDir, skimWorkflow.name,
                                                         "WMSandbox", "WMWorkload.pkl"),
                         "Error: Wrong spec URL")
        self.assertEqual(len(skimWorkflow.outputMap), 2,
                         "Error: Wrong number of WF outputs.")

        mergedSkimOutputA = skimWorkflow.outputMap["SkimOutputADataTierA"][0]["merged_output_fileset"]
        unmergedSkimOutputA = skimWorkflow.outputMap["SkimOutputADataTierA"][0]["output_fileset"]
        mergedSkimOutputB = skimWorkflow.outputMap["SkimOutputBDataTierB"][0]["merged_output_fileset"]
        unmergedSkimOutputB = skimWorkflow.outputMap["SkimOutputBDataTierB"][0]["output_fileset"]

        mergedSkimOutputA.loadData()
        mergedSkimOutputB.loadData()
        unmergedSkimOutputA.loadData()
        unmergedSkimOutputB.loadData()

        self.assertEqual(mergedSkimOutputA.name,
                         "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputADataTierA",
                         "Error: Merged output fileset is wrong: %s" % mergedSkimOutputA.name)
        self.assertEqual(unmergedSkimOutputA.name,
                         "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputADataTierA",
                         "Error: Unmerged output fileset is wrong.")
        self.assertEqual(mergedSkimOutputB.name,
                         "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputBDataTierB",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedSkimOutputB.name,
                         "/TestWorkload/ProcessingTask/MergeTask/SkimTask/unmerged-SkimOutputBDataTierB",
                         "Error: Unmerged output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-ProcessingTask-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset=topLevelFileset, workflow=procWorkflow)
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

        mergeSubscription = Subscription(fileset=unmergedProcOutput, workflow=mergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(len(mergeSubscription.getWhiteBlackList()), 0,
                         "Error: Wrong white/black list for merge sub.")

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algo.")

        skimSubscription = Subscription(fileset=unmergedMergeOutput, workflow=skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")
        return

    def testTruncatedWFInsertion(self):
        """
        _testTruncatedWFInsertion_

        """
        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName='site1', pnn='goodse.cern.ch',
                                   ceName='site1', plugin="TestPlugin")
        resourceControl.insertSite(siteName='site2', pnn='goodse2.cern.ch',
                                   ceName='site2', plugin="TestPlugin")

        testWorkload = self.createTestWMSpec()
        testTopLevelTask = getFirstTask(testWorkload)
        testWMBSHelper = WMBSHelper(testWorkload, testTopLevelTask.name(), "SomeBlock", cachepath=self.workDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testTopLevelTask, testWMBSHelper.topLevelFileset)

        testWorkload.truncate("ResubmitTestWorkload", "/TestWorkload/ProcessingTask/MergeTask",
                              "someserver", "somedatabase")

        # create  the subscription for multiple top task (MergeTask and CleanupTask for the same block)
        for task in testWorkload.getTopLevelTask():
            testResubmitWMBSHelper = WMBSHelper(testWorkload, task.name(), "SomeBlock2", cachepath=self.workDir)
            testResubmitWMBSHelper.createTopLevelFileset()
            testResubmitWMBSHelper._createSubscriptionsInWMBS(task, testResubmitWMBSHelper.topLevelFileset)

        mergeWorkflow = Workflow(name="ResubmitTestWorkload",
                                 task="/ResubmitTestWorkload/MergeTask")
        mergeWorkflow.load()

        self.assertEqual(mergeWorkflow.owner, "sfoulkes",
                         "Error: Wrong owner.")
        self.assertEqual(mergeWorkflow.spec, os.path.join(self.workDir, mergeWorkflow.name,
                                                          "WMSandbox", "WMWorkload.pkl"),
                         "Error: Wrong spec URL")
        self.assertEqual(len(mergeWorkflow.outputMap), 1, "Error: Wrong number of WF outputs.")

        unmergedMergeOutput = mergeWorkflow.outputMap["MergedDataTierA"][0]["output_fileset"]
        unmergedMergeOutput.loadData()

        self.assertEqual(unmergedMergeOutput.name, "/ResubmitTestWorkload/MergeTask/merged-MergedDataTierA",
                         "Error: Unmerged output fileset is wrong.")

        skimWorkflow = Workflow(name="ResubmitTestWorkload",
                                task="/ResubmitTestWorkload/MergeTask/SkimTask")
        skimWorkflow.load()

        self.assertEqual(skimWorkflow.owner, "sfoulkes",
                         "Error: Wrong owner.")
        self.assertEqual(skimWorkflow.spec, os.path.join(self.workDir, skimWorkflow.name,
                                                         "WMSandbox", "WMWorkload.pkl"),
                         "Error: Wrong spec URL")
        self.assertEqual(len(skimWorkflow.outputMap), 2,
                         "Error: Wrong number of WF outputs.")

        mergedSkimOutputA = skimWorkflow.outputMap["SkimOutputADataTierA"][0]["merged_output_fileset"]
        unmergedSkimOutputA = skimWorkflow.outputMap["SkimOutputADataTierA"][0]["output_fileset"]
        mergedSkimOutputB = skimWorkflow.outputMap["SkimOutputBDataTierB"][0]["merged_output_fileset"]
        unmergedSkimOutputB = skimWorkflow.outputMap["SkimOutputBDataTierB"][0]["output_fileset"]

        mergedSkimOutputA.loadData()
        mergedSkimOutputB.loadData()
        unmergedSkimOutputA.loadData()
        unmergedSkimOutputB.loadData()

        self.assertEqual(mergedSkimOutputA.name,
                         "/ResubmitTestWorkload/MergeTask/SkimTask/unmerged-SkimOutputADataTierA",
                         "Error: Merged output fileset is wrong: %s" % mergedSkimOutputA.name)
        self.assertEqual(unmergedSkimOutputA.name,
                         "/ResubmitTestWorkload/MergeTask/SkimTask/unmerged-SkimOutputADataTierA",
                         "Error: Unmerged output fileset is wrong.")
        self.assertEqual(mergedSkimOutputB.name,
                         "/ResubmitTestWorkload/MergeTask/SkimTask/unmerged-SkimOutputBDataTierB",
                         "Error: Merged output fileset is wrong.")
        self.assertEqual(unmergedSkimOutputB.name,
                         "/ResubmitTestWorkload/MergeTask/SkimTask/unmerged-SkimOutputBDataTierB",
                         "Error: Unmerged output fileset is wrong.")

        topLevelFileset = Fileset(name="ResubmitTestWorkload-MergeTask-SomeBlock2")
        topLevelFileset.loadData()

        mergeSubscription = Subscription(fileset=topLevelFileset, workflow=mergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(len(mergeSubscription.getWhiteBlackList()), 0,
                         "Error: Wrong white/black list for merge sub.")

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algo.")

        skimSubscription = Subscription(fileset=unmergedMergeOutput, workflow=skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        return

    def testReReco(self):
        """ReReco workflow"""
        # create workflow
        block = self.dataset + "#" + BLOCK1
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block)['Files'])
        self.assertEqual(len(files), 5)

    def testReRecoBlackRunRestriction(self):
        """ReReco workflow with Run restrictions"""
        block = self.dataset + "#" + BLOCK2
        self.topLevelTask.setInputRunBlacklist([181183])  # Set run blacklist to only run in the block
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)

        files = wmbs.validFiles(self.dbs.getFileBlock(block)['Files'])
        self.assertEqual(len(files), 0)

    def testReRecoWhiteRunRestriction(self):
        block = self.dataset + "#" + BLOCK2
        self.topLevelTask.setInputRunWhitelist([181183])  # Set run whitelist to only run in the block
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block)['Files'])
        self.assertEqual(len(files), 1)

    def testLumiMaskRestrictionsOK(self):
        block = self.dataset + "#" + BLOCK1
        self.wmspec.getTopLevelTask()[0].data.input.splitting.runs = ['181367']
        self.wmspec.getTopLevelTask()[0].data.input.splitting.lumis = ['57,80']
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block)['Files'])
        self.assertEqual(len(files), 1)

    def testLumiMaskRestrictionsKO(self):
        block = self.dataset + "#" + BLOCK1
        self.wmspec.getTopLevelTask()[0].data.input.splitting.runs = ['123454321']
        self.wmspec.getTopLevelTask()[0].data.input.splitting.lumis = ['123,123']
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block)['Files'])
        self.assertEqual(len(files), 0)

    def testDuplicateFileInsert(self):
        # using default wmspec
        block = self.dataset + "#" + BLOCK1
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        wmbs.topLevelFileset.loadData()
        numOfFiles = len(wmbs.topLevelFileset.files)
        # check initially inserted files.
        dbsFiles = self.dbs.getFileBlock(block)['Files']
        self.assertEqual(numOfFiles, len(dbsFiles))
        firstFileset = wmbs.topLevelFileset
        wmbsDao = wmbs.daofactory(classname="Files.InFileset")

        numOfFiles = len(wmbsDao.execute(firstFileset.id))
        self.assertEqual(numOfFiles, len(dbsFiles))

        # use the new spec with same inputdataset
        block = self.dataset + "#" + BLOCK1
        wmspec = self.createWMSpec("TestSpec1")
        dbs = self.getDBS(wmspec)
        wmbs = self.createWMBSHelperWithTopTask(wmspec, block)
        # check duplicate insert
        dbsFiles = dbs.getFileBlock(block)
        data = self.rucio.getReplicaInfoForBlocks(block=[block])
        dbsFiles['PhEDExNodeNames'] = data[0]["replica"]
        numOfFiles = wmbs.addFiles(dbsFiles)
        self.assertEqual(numOfFiles, 0)
        secondFileset = wmbs.topLevelFileset

        wmbsDao = wmbs.daofactory(classname="Files.InFileset")
        numOfFiles = len(wmbsDao.execute(secondFileset.id))
        self.assertEqual(numOfFiles, len(dbsFiles['Files']))

        self.assertNotEqual(firstFileset.id, secondFileset.id)

    def testDuplicateSubscription(self):
        """Can't duplicate subscriptions"""
        siteWhitelist = ["T2_XX_SiteA", "T2_XX_SiteB"]
        # using default wmspec
        block = self.dataset + "#" + BLOCK1
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        wmbs.topLevelFileset.loadData()
        numOfFiles = len(wmbs.topLevelFileset.files)
        filesetId = wmbs.topLevelFileset.id
        subId = wmbs.topLevelSubscription['id']

        # check initially inserted files.
        dbsFiles = self.dbs.getFileBlock(block)['Files']
        self.assertEqual(numOfFiles, len(dbsFiles))

        # Not clear what's supposed to happen here, 2nd test is completely redundant
        dummyFirstFileset = wmbs.topLevelFileset
        self.assertEqual(numOfFiles, len(dbsFiles))

        # reinsert subscription - shouldn't create anything new
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        wmbs.topLevelFileset.loadData()
        self.assertEqual(numOfFiles, len(wmbs.topLevelFileset.files))
        self.assertEqual(filesetId, wmbs.topLevelFileset.id)
        self.assertEqual(subId, wmbs.topLevelSubscription['id'])

        # now do a montecarlo workflow
        self.setupMCWMSpec()
        mask = Mask(FirstRun=12, FirstLumi=1234, FirstEvent=12345,
                    LastEvent=999995, LastLumi=12345, LastRun=12)
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, None, mask,
                                                commonLocation=siteWhitelist)
        wmbs.topLevelFileset.loadData()
        numOfFiles = len(wmbs.topLevelFileset.files)
        filesetId = wmbs.topLevelFileset.id
        subId = wmbs.topLevelSubscription['id']

        # check initially inserted files.
        # Not clear what's supposed to happen here, 2nd test is completely redundant
        numDbsFiles = 1
        self.assertEqual(numOfFiles, numDbsFiles)
        dummyFirstFileset = wmbs.topLevelFileset
        self.assertEqual(numOfFiles, numDbsFiles)

        # reinsert subscription - shouldn't create anything new
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, None, mask,
                                                commonLocation=siteWhitelist)
        wmbs.topLevelFileset.loadData()
        self.assertEqual(numOfFiles, len(wmbs.topLevelFileset.files))
        self.assertEqual(filesetId, wmbs.topLevelFileset.id)
        self.assertEqual(subId, wmbs.topLevelSubscription['id'])

    def testParentage(self):
        """
        1. check whether parent files are created in wmbs.
        2. check parent files are associated to child.
        3. When 2 specs with the same input data (one with parent processing, one without it)
           is inserted, if one without parent processing inserted first then the other with
           parent processing insert, it still needs to create parent files although child files
           are duplicate
        """

        # Swap out the dataset for one that has parents
        task = next(self.wmspec.taskIterator())
        oldDS = task.inputDataset()  # Copy the old dataset, only will use DBS URL from it
        task.addInputDataset(name="/Cosmics/ComissioningHI-PromptReco-v1/RECO",
                             primary='Cosmics', processed='ComissioningHI-PromptReco-v1',
                             tier='RECO', dbsurl=oldDS.dbsurl)
        block = '/Cosmics/ComissioningHI-PromptReco-v1/RECO' + '#5b89ba9c-0dbf-11e1-9b6c-003048caaace'

        # File creation without parents
        wmbs, _, numFiles = self.createWMBSHelperWithTopTask(self.wmspec, block, parentFlag=False, detail=True)
        self.assertEqual(8, numFiles)
        wmbs.topLevelFileset.loadData()
        for child in wmbs.topLevelFileset.files:
            self.assertEqual(len(child["parents"]), 0)  # no parents per child

        # File creation with parents
        wmbs, _, numFiles = self.createWMBSHelperWithTopTask(self.wmspec, block, parentFlag=True, detail=True)
        self.assertEqual(8, numFiles)
        wmbs.topLevelFileset.loadData()
        for child in wmbs.topLevelFileset.files:
            self.assertEqual(len(child["parents"]), 1)  # one parent per child

    def testMCFakeFileInjection(self):
        """Inject fake Monte Carlo files into WMBS"""

        # This test is failing because the name of the couch DB is set to None
        # in BasicProductionWorkload.getProdArgs() but changing it to
        # "reqmgr_config_cache_t" from StdBase test arguments does not fix the
        # situation. testDuplicateSubscription probably has the same issue
        siteWhitelist = ["T2_XX_SiteA", "T2_XX_SiteB"]

        self.setupMCWMSpec()

        mask = Mask(FirstRun=12, FirstLumi=1234, FirstEvent=12345,
                    LastEvent=999995, LastLumi=12345, LastRun=12)

        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, None, mask,
                                                commonLocation=siteWhitelist)
        subscription = wmbs.topLevelSubscription
        self.assertEqual(1, subscription.exists())
        fileset = subscription['fileset']
        self.assertEqual(1, fileset.exists())
        fileset.loadData()  # need to refresh from database

        self.assertEqual(len(fileset.files), 1)
        self.assertEqual(len(fileset.parents), 0)
        self.assertFalse(fileset.open)

        firstFile = list(fileset.files)[0]
        self.assertEqual(firstFile['events'], mask['LastEvent'] - mask['FirstEvent'] + 1)  # inclusive range
        self.assertEqual(firstFile['merged'], False)  # merged files get added to dbs
        self.assertEqual(len(firstFile['parents']), 0)
        # firstFile.loadData()
        self.assertEqual(sorted(firstFile['locations']), sorted(self.pnns))
        self.assertEqual(len(firstFile.getParentLFNs()), 0)

        self.assertEqual(len(firstFile.getRuns()), 1)
        run = firstFile.getRuns()[0]
        self.assertEqual(run.run, mask['FirstRun'])
        self.assertEqual(run.lumis[0], mask['FirstLumi'])
        self.assertEqual(run.lumis[-1], mask['LastLumi'])
        self.assertEqual(len(run.lumis), mask['LastLumi'] - mask['FirstLumi'] + 1)


if __name__ == '__main__':
    unittest.main()
