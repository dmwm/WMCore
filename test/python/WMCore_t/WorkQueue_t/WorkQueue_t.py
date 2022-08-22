#!/usr/bin/env python
"""
_WorkQueue_t_

WorkQueue tests
"""
from __future__ import print_function

from builtins import next, range
from future.utils import viewitems

import os
import threading
import time
import unittest
import logging

from retry import retry

from Utils.PythonVersion import PY3

from WMCore.WMBase import getTestBase
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.Configuration import Configuration
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.File import File as WMFile
from WMCore.DataStructs.Run import Run
from WMCore.Lexicon import sanitizeURL
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.UUIDLib import makeUUID
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMCore.WMBS.Job import Job
from WMCore.WMSpec.StdSpecs.DQMHarvest import DQMHarvestWorkloadFactory
from WMCore.WMSpec.StdSpecs.StepChain import StepChainWorkloadFactory
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper
from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore.WorkQueue.WorkQueueExceptions import (WorkQueueWMSpecError, WorkQueueNoMatchingElements,
                                                  WorkQueueNoWorkError)
from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.DataBlockGenerator import Globals
from WMQuality.Emulators.RucioClient.MockRucioApi import PILEUP_DATASET
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig

from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload \
    import workload as MultiTaskProductionWorkload
from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase


NBLOCKS_HICOMM = 47
NFILES_HICOMM = 72
NBLOCKS_COSMIC = 58
NFILES_COSMIC = 108
NFILES_COSMICRAW = 141
TOTAL_EVENTS=10000


def rerecoWorkload(workloadName, arguments, assignArgs=None):
    factory = ReRecoWorkloadFactory()
    wmspec = factory.factoryWorkloadConstruction(workloadName, arguments)
    if assignArgs:
        args = factory.getAssignTestArguments()
        args.update(assignArgs)
        wmspec.updateArguments(args)
    return wmspec


def stepchainWorkload(workloadName, arguments):
    factory = StepChainWorkloadFactory()
    wmspec = factory.factoryWorkloadConstruction(workloadName, arguments)
    return wmspec


def getFirstTask(wmspec):
    """Return the 1st top level task"""
    return next(wmspec.taskIterator())


def syncQueues(queue, skipWMBS=False):
    """Sync parent & local queues and split work
        Workaround having to wait for couchdb replication and splitting polling
    """
    queue.backend.forceQueueSync()
    time.sleep(1)
    work = queue.processInboundWork()
    queue.performQueueCleanupActions(skipWMBS=skipWMBS)
    # after replication need to wait a while to update result
    time.sleep(3)
    return work


class WorkQueueTest(WorkQueueTestCase):
    """
    _WorkQueueTest_

    For /MinimumBias/ComissioningHI-v1/RAW the dataset has 47 blocks with 72 files.
    The Rucio emulator sets the block locations like:
        17 at 'T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC'
        19 at 'T2_XX_SiteA', 'T2_XX_SiteB'
        11 at 'T2_XX_SiteA' only
    """

    def __init__(self, methodName='runTest'):
        super(WorkQueueTest, self).__init__(methodName=methodName, mockDBS=True, mockRucio=True)
        self.queueParams = {}
        self.queueParams['log_reporter'] = "WorkQueue_Unittest"
        self.queueParams['rucioAccount'] = "wma_test"
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"


    def setupConfigCacheAndAgrs(self):
        self.rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
        self.rerecoArgs["CouchDBName"] = self.configCacheDB
        self.rerecoArgs["ConfigCacheID"] = createConfig(self.rerecoArgs["CouchDBName"])

        self.mcArgs = StepChainWorkloadFactory.getTestArguments()
        self.mcArgs["CouchDBName"] = self.configCacheDB
        self.mcArgs['Step1']["ConfigCacheID"] = createConfig(self.mcArgs["CouchDBName"])

        self.parentProcArgs = ReRecoWorkloadFactory.getTestArguments()
        self.parentProcArgs.update(IncludeParents="True")
        self.parentProcArgs.update(InputDataset="/Cosmics/ComissioningHI-PromptReco-v1/RECO")
        self.parentProcArgs["CouchDBName"] = self.configCacheDB
        self.parentProcArgs["ConfigCacheID"] = createConfig(self.parentProcArgs["CouchDBName"])

        self.openRunningProcArgs = ReRecoWorkloadFactory.getTestArguments()
        self.openRunningProcArgs.update(OpenRunningTimeout=10)
        self.openRunningProcArgs["CouchDBName"] = self.configCacheDB
        self.openRunningProcArgs["ConfigCacheID"] = createConfig(self.openRunningProcArgs["CouchDBName"])

        self.pileupArgs = StepChainWorkloadFactory.getTestArguments()
        self.pileupArgs['Step1'].update(MCPileup=PILEUP_DATASET)
        self.pileupArgs['Step1'].update(InputDataset="/MinimumBias/ComissioningHI-v1/RAW",
                                        RequestNumEvents=TOTAL_EVENTS,
                                        SplittingAlgo="EventAwareLumiBased")
        self.pileupArgs["CouchDBName"] = self.configCacheDB
        self.pileupArgs['Step1']["ConfigCacheID"] = createConfig(self.pileupArgs["CouchDBName"])

        self.pileupMcArgs = StepChainWorkloadFactory.getTestArguments()
        self.pileupMcArgs['Step1'].update(MCPileup=PILEUP_DATASET)
        self.pileupArgs['Step1'].update(RequestNumEvents=TOTAL_EVENTS)
        self.pileupMcArgs["CouchDBName"] = self.configCacheDB
        self.pileupMcArgs['Step1']["ConfigCacheID"] = createConfig(self.pileupMcArgs["CouchDBName"])

    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        # undo any customizations
        Globals.GlobalParams.resetParams()

        # set up WMAgent config file for couchdb
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        WorkQueueTestCase.setUp(self)
        self.setupConfigCacheAndAgrs()

        # Basic production Spec
        self.spec = stepchainWorkload('testProduction', self.mcArgs)
        self.spec.setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(self.spec).addProduction(totalEvents=TOTAL_EVENTS)
        self.spec.setSpecUrl(os.path.join(self.workDir, 'testworkflow.spec'))
        self.spec.save(self.spec.specUrl())

        # Production spec plus pileup
        self.productionPileupSpec = stepchainWorkload('testProduction', self.pileupMcArgs)
        self.productionPileupSpec.setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(self.productionPileupSpec).addProduction(totalEvents=TOTAL_EVENTS)
        self.productionPileupSpec.setSpecUrl(os.path.join(self.workDir, 'testworkflowPileupMc.spec'))
        self.productionPileupSpec.save(self.productionPileupSpec.specUrl())

        # Processing spec plus pileup
        self.processingPileupSpec = stepchainWorkload('testProcessing', self.pileupArgs)
        self.processingPileupSpec.setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC'])
        getFirstTask(self.processingPileupSpec).addProduction(totalEvents=TOTAL_EVENTS)
        self.processingPileupSpec.setSpecUrl(os.path.join(self.workDir, 'testworkflowPileup.spec'))
        self.processingPileupSpec.save(self.processingPileupSpec.specUrl())

        # ReReco spec with whitelist
        self.whitelistSpec = rerecoWorkload('whitelistlistSpec', self.rerecoArgs)
        self.whitelistSpec.setSpecUrl(os.path.join(self.workDir,
                                                   'testWhitelist.spec'))
        getFirstTask(self.whitelistSpec).data.constraints.sites.whitelist = ['T2_XX_SiteB']
        self.whitelistSpec.save(self.whitelistSpec.specUrl())

        # ReReco spec with delay for running open
        self.openRunningSpec = rerecoWorkload('openRunningSpec', self.openRunningProcArgs)
        self.openRunningSpec.setSpecUrl(os.path.join(self.workDir,
                                                     'testOpenRunningSpec.spec'))
        self.openRunningSpec.save(self.openRunningSpec.specUrl())

        # Create queues
        globalCouchUrl = "%s/%s" % (self.testInit.couchUrl, self.globalQDB)
        logdbCouchUrl = "%s/%s" % (self.testInit.couchUrl, self.logDBName)
        reqdbUrl = "%s/%s" % (self.testInit.couchUrl, self.requestDBName)
        self.globalQueue = globalQueue(DbName=self.globalQDB,
                                       InboxDbName=self.globalQInboxDB,
                                       QueueURL=globalCouchUrl,
                                       central_logdb_url=logdbCouchUrl,
                                       UnittestFlag=True,
                                       RequestDBURL=reqdbUrl,
                                       **self.queueParams)
        #        self.midQueue = WorkQueue(SplitByBlock = False, # mid-level queue
        #                            PopulateFilesets = False,
        #                            ParentQueue = self.globalQueue,
        #                            CacheDir = None)
        # ignore mid queue as it causes database duplication's
        # copy jobStateMachine couchDB configuration here since we don't want/need to pass whole configuration
        jobCouchConfig = Configuration()
        jobCouchConfig.section_("JobStateMachine")
        jobCouchConfig.JobStateMachine.couchurl = os.environ["COUCHURL"]
        jobCouchConfig.JobStateMachine.couchDBName = "testcouchdb"
        jobCouchConfig.JobStateMachine.jobSummaryDBName = "wmagent_summary_test"
        jobCouchConfig.JobStateMachine.summaryStatsDBName = "stat_summary_test"
        # copy bossAir configuration here since we don't want/need to pass whole configuration
        bossAirConfig = Configuration()
        bossAirConfig.section_("BossAir")
        bossAirConfig.BossAir.pluginDir = "WMCore.BossAir.Plugins"
        bossAirConfig.BossAir.pluginNames = ["MockPlugin"]
        bossAirConfig.BossAir.section_("MockPlugin")
        bossAirConfig.BossAir.MockPlugin.fakeReport = os.path.join(getTestBase(),
                                                            'WMComponent_t/JobAccountant_t/fwjrs',
                                                            "MergeSuccess.pkl")
        bossAirConfig.section_("Agent")
        bossAirConfig.Agent.agentName = "TestAgent"
        bossAirConfig.section_("JobStateMachine")
        bossAirConfig.JobStateMachine.couchurl = os.environ["COUCHURL"]
        bossAirConfig.JobStateMachine.couchDBName = "testcouchdb"
        bossAirConfig.JobStateMachine.jobSummaryDBName = "wmagent_summary_test"
        bossAirConfig.JobStateMachine.summaryStatsDBName = "stat_summary_test"

        self.localQueue = localQueue(DbName=self.localQDB,
                                     InboxDbName=self.localQInboxDB,
                                     ParentQueueCouchUrl=globalCouchUrl,
                                     ParentQueueInboxCouchDBName=self.globalQInboxDB,
                                     JobDumpConfig=jobCouchConfig,
                                     BossAirConfig=bossAirConfig,
                                     CacheDir=self.workDir,
                                     central_logdb_url=logdbCouchUrl,
                                     RequestDBURL=reqdbUrl,
                                     **self.queueParams)

        self.localQueue2 = localQueue(DbName=self.localQDB2,
                                      InboxDbName=self.localQInboxDB2,
                                      ParentQueueCouchUrl=globalCouchUrl,
                                      ParentQueueInboxCouchDBName=self.globalQInboxDB,
                                      JobDumpConfig=jobCouchConfig,
                                      BossAirConfig=bossAirConfig,
                                      CacheDir=self.workDir,
                                      central_logdb_url=logdbCouchUrl,
                                      RequestDBURL=reqdbUrl,
                                      **self.queueParams)

        # configuration for the Alerts messaging framework, work (alerts) and
        # control  channel addresses to which alerts
        # these are destination addresses where AlertProcessor:Receiver listens
        config = Configuration()
        config.section_("Alert")
        config.Alert.address = "tcp://127.0.0.1:5557"
        config.Alert.controlAddr = "tcp://127.0.0.1:5559"

        # standalone queue for unit tests
        self.queue = WorkQueue(JobDumpConfig=jobCouchConfig,
                               BossAirConfig=bossAirConfig,
                               DbName=self.queueDB,
                               InboxDbName=self.queueInboxDB,
                               CacheDir=self.workDir,
                               config=config,
                               central_logdb_url=logdbCouchUrl,
                               RequestDBURL=reqdbUrl,
                               **self.queueParams)

        # create relevant sites in wmbs
        rc = ResourceControl()
        site_se_mapping = {'T2_XX_SiteA': 'T2_XX_SiteA', 'T2_XX_SiteB': 'T2_XX_SiteB'}
        for site, se in viewitems(site_se_mapping):
            rc.insertSite(site, 100, 200, se, cmsName=site, plugin="MockPlugin")
            daofactory = DAOFactory(package="WMCore.WMBS",
                                    logger=threading.currentThread().logger,
                                    dbinterface=threading.currentThread().dbi)
            addLocation = daofactory(classname="Locations.New")
            addLocation.execute(siteName=site, pnn=se)

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def setupReReco(self, assignArgs=None, **kwargs):
        # Sample Tier1 ReReco spec
        self.rerecoArgs.update(kwargs)
        processingSpec = rerecoWorkload('testProcessing', self.rerecoArgs, assignArgs=assignArgs)
        processingSpec.setSpecUrl(os.path.join(self.workDir, 'testProcessing.spec'))
        processingSpec.save(processingSpec.specUrl())
        return processingSpec

    def setupParentProcSpec(self, assignArgs=None, **kwargs):
        # Sample Tier1 ReReco spec with parent
        self.parentProcArgs.update(kwargs)
        parentProcSpec = rerecoWorkload('testParentProcessing', self.parentProcArgs, assignArgs=assignArgs)
        parentProcSpec.setSpecUrl(os.path.join(self.workDir, 'testParentProcessing.spec'))
        parentProcSpec.save(parentProcSpec.specUrl())
        return parentProcSpec

    def setupHighPrioReReco(self, assignArgs=None, **kwargs):
        # High priority ReReco spec
        self.rerecoArgs.update(kwargs)
        highPrioReReco = rerecoWorkload('highPrioSpec', self.rerecoArgs, assignArgs=assignArgs)
        highPrioReReco.data.request.priority = 999998
        highPrioReReco.setSpecUrl(os.path.join(self.workDir, 'highPrioSpec.spec'))
        highPrioReReco.save(highPrioReReco.specUrl())
        return highPrioReReco

    def tearDown(self):
        """tearDown"""
        super(WorkQueueTest, self).tearDown()
        # Delete WMBSAgent config file
        EmulatorSetup.deleteConfig(self.configFile)

    def createResubmitSpec(self, serverUrl, couchDB, parentage=False):
        """
        _createResubmitSpec_
        Create a bogus resubmit workload.
        """
        site = ["T1_US_FNAL"]
        workload = WMWorkloadHelper(WMWorkload("TestWorkload"))
        reco = workload.newTask("reco")
        workload.setOwnerDetails(name="evansde77", group="DMWM")
        workload.setSiteWhitelist(site)
        # first task uses the input dataset
        reco.addInputDataset(name="/PRIMARY/processed-v1/TIERONE",
                             primary="PRIMARY", processed="processed-v1", tier="TIERONE")
        reco.data.input.splitting.algorithm = "File"
        reco.data.input.splitting.include_parents = parentage
        reco.setTaskType("Processing")
        cmsRunReco = reco.makeStep("cmsRun1")
        cmsRunReco.setStepType("CMSSW")
        reco.applyTemplates()
        cmsRunRecoHelper = cmsRunReco.getTypeHelper()
        cmsRunRecoHelper.addOutputModule("outputRECO",
                                         primaryDataset="PRIMARY",
                                         processedDataset="processed-v2",
                                         dataTier="TIERTWO",
                                         lfnBase="/store/dunkindonuts",
                                         mergedLFNBase="/store/kfc")

        workload.setTrustLocationFlag(inputFlag=True, pileupFlag=False)
        dcs = DataCollectionService(url=serverUrl, database=couchDB)

        def getJob(workload):
            job = Job()
            job["task"] = workload.getTask("reco").getPathName()
            job["workflow"] = workload.name()
            job["location"] = site
            job["owner"] = workload.getOwner().get("name")
            job["group"] = workload.getOwner().get("group")
            return job

        testFileA = WMFile(lfn=makeUUID(), size=1024, events=1024, parents=['parent1'])
        testFileA.setLocation(site)
        testFileA.addRun(Run(1, 1, 2))
        testFileB = WMFile(lfn=makeUUID(), size=1024, events=1024, parents=['parent2'])
        testFileB.setLocation(site)
        testFileB.addRun(Run(1, 3, 4))
        testJobA = getJob(workload)
        testJobA.addFile(testFileA)
        testJobA.addFile(testFileB)

        dcs.failedJobs([testJobA])
        topLevelTask = workload.getTopLevelTask()[0]
        workload.truncate("Resubmit_TestWorkload", topLevelTask.getPathName(),
                          serverUrl, couchDB)

        return workload

    def testProduction(self):
        """
        Enqueue and get work for a production WMSpec.
        """
        specfile = self.spec.specUrl()
        numUnit = 1
        jobSlot = [10] * numUnit  # array of jobs per block
        total = sum(jobSlot)
        for _ in range(numUnit):
            self.queue.queueWork(specfile)
        self.assertEqual(numUnit, len(self.queue))

        # try to get work
        work = self.queue.getWork({'SiteDoesNotExist': jobSlot[0]}, {})
        self.assertEqual([], work)  # not in whitelist

        work = self.queue.getWork({'T2_XX_SiteA': 0}, {})
        self.assertEqual([], work)
        work = self.queue.getWork({'T2_XX_SiteA': jobSlot[0]}, {})
        self.assertEqual(len(work), 1)

        # no more work available
        self.assertEqual(0, len(self.queue.getWork({'T2_XX_SiteA': total}, {})))

    def testProductionMultiQueue(self):
        """Test production with multiple queueus"""
        specfile = self.spec.specUrl()
        numUnit = 1
        jobSlot = [10] * numUnit  # array of jobs per block
        total = sum(jobSlot)

        self.globalQueue.queueWork(specfile)
        self.assertEqual(numUnit, len(self.globalQueue))

        # pull work to localQueue2 - check local doesn't get any
        numWork = self.localQueue2.pullWork({'T2_XX_SiteA': total})
        self.assertEqual(numUnit, numWork)
        self.assertEqual(0, self.localQueue.pullWork({'T2_XX_SiteA': total}))
        syncQueues(self.localQueue)
        syncQueues(self.localQueue2)
        self.assertEqual(0, len(self.localQueue.status(status='Available')))
        self.assertEqual(numUnit, len(self.localQueue2.status(status='Available')))
        self.assertEqual(numUnit, len(self.globalQueue.status(status='Acquired')))
        self.assertEqual(sanitizeURL(self.localQueue2.params['QueueURL'])['url'],
                         self.globalQueue.status()[0]['ChildQueueUrl'])

    def testPriority(self):
        """
        Test priority change functionality
        """
        jobSlot = 10
        totalSlices = 1

        self.queue.queueWork(self.spec.specUrl())
        self.queue.processInboundWork()

        # priority change
        self.queue.setPriority(50, self.spec.name())
        # test elements are now cancelled
        self.assertEqual([x['Priority'] for x in self.queue.status(RequestName=self.spec.name())],
                         [50] * totalSlices)
        self.assertRaises(RuntimeError, self.queue.setPriority, 50, 'blahhhhh')

        # claim all work
        work = self.queue.getWork({'T2_XX_SiteA': jobSlot}, {})
        self.assertEqual(len(work), totalSlices)

        # no more work available
        self.assertEqual(0, len(self.queue.getWork({'T2_XX_SiteA': jobSlot}, {})))

    def testProcessing(self):
        """
        Enqueue and get work for a processing WMSpec.
        """
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"]})
        specfile = processingSpec.specUrl()

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.queue.processInboundWork()
        self.assertEqual(NBLOCKS_HICOMM, len(self.queue))

        self.queue.updateLocationInfo()
        # No resources
        work = self.queue.getWork({}, {})
        self.assertEqual(len(work), 0)
        work = self.queue.getWork({'T2_XX_SiteA': 0, 'T2_XX_SiteB': 0}, {})
        self.assertEqual(len(work), 0)

        # Get the first bit of work available at site C
        work = self.queue.getWork({'T2_XX_SiteC': 1}, {})
        self.assertEqual(len(work), 1)  # Double check A
        processedBlocks = len(work)
        processedFiles = work[0]["NumOfFilesAdded"]

        # Get the rest the of work available at site C
        work = self.queue.getWork({'T2_XX_SiteC': 1000}, {})
        processedBlocks += len(work)
        for element in work:
            processedFiles += element["NumOfFilesAdded"]
        self.assertEqual(processedBlocks, 9)
        self.assertEqual(processedFiles, 14)

        # Get the rest the of work available at site B
        work = self.queue.getWork({'T2_XX_SiteB': 1000}, {})
        processedBlocks += len(work)
        for element in work:
            processedFiles += element["NumOfFilesAdded"]
        self.assertEqual(processedBlocks, 31)
        self.assertEqual(processedFiles, 52)

        # Make sure no work left for B or C
        work = self.queue.getWork({'T2_XX_SiteB': 1000, 'T2_XX_SiteC': 1000}, {})
        self.assertEqual(len(work), 0)

        # Make sure we get all the work when we include A
        work = self.queue.getWork({'T2_XX_SiteA': 1000}, {})
        processedBlocks += len(work)
        for element in work:
            processedFiles += element["NumOfFilesAdded"]
        self.assertEqual(processedBlocks, NBLOCKS_HICOMM)
        self.assertEqual(processedFiles, NFILES_HICOMM)

        # Make sure no remaining work
        work = self.queue.getWork({'T2_XX_SiteA': 1000, 'T2_XX_SiteB': 1000}, {})
        self.assertEqual(len(work), 0)  # no more work available

    def testBlackList(self):
        """
        Black list functionality
        """
        blacklistSpec = self.setupReReco(assignArgs={'SiteWhitelist':["T2_XX_SiteB"], 'SiteBlacklist': ["T2_XX_SiteA"]})
        specfile = blacklistSpec.specUrl()

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.queue.processInboundWork()
        self.assertEqual(NBLOCKS_HICOMM, len(self.queue))
        self.queue.updateLocationInfo()

        # T2_XX_SiteA is in blacklist, no work pulled
        work = self.queue.getWork({'T2_XX_SiteA': 1000}, {})
        self.assertEqual(len(work), 0)

        # T2_XX_SiteB can run most blocks
        work = self.queue.getWork({'T2_XX_SiteB': 1000}, {})
        self.assertEqual(len(work), 31)

    def testWhiteList(self):
        """
        White list functionality
        """
        specfile = self.whitelistSpec.specUrl()

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.queue.processInboundWork()
        self.assertEqual(NBLOCKS_HICOMM, len(self.queue))
        self.queue.updateLocationInfo()

        # Only SiteB in whitelist
        work = self.queue.getWork({'T2_XX_SiteA': 1000}, {})
        self.assertEqual(len(work), 0)

        # Site B can run
        work = self.queue.getWork({'T2_XX_SiteB': 1000, 'T2_XX_SiteAA': 1000}, {})
        self.assertEqual(len(work), 31)

    def testQueueChaining(self):
        """
        Chain WorkQueues, pull work down and verify splitting
        """
        self.assertEqual(0, len(self.globalQueue))
        # check no work in local queue
        self.assertEqual(0, len(self.localQueue.getWork({'T2_XX_SiteA': 1000}, {})))
        # Add work to top most queue
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteD"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))

        # check work isn't passed down to site without subscription
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteD': 1000}), 0)

        # put at correct site
        self.globalQueue.updateLocationInfo()

        # check work isn't passed down to the wrong agent
        work = self.localQueue.getWork({'T2_XX_SiteD': 1000}, {})  # Not in subscription
        self.assertEqual(0, len(work))
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))

        # pull work down to the lowest queue
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1000}), NBLOCKS_HICOMM)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue), NBLOCKS_HICOMM)

        self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'T2_XX_SiteA': 1000},
                                       {})
        self.assertEqual(0, len(self.localQueue))
        self.assertEqual(NBLOCKS_HICOMM, len(work))

        # check work in local and subscription made
        for x in work:
            self.assertTrue(x['SubscriptionId'] > 0)
        for x in self.localQueue.status():
            self.assertTrue(x['SubscriptionId'] > 0)
        # mark work done & check this passes upto the top level
        self.localQueue.setStatus('Done', [x.id for x in work])

    def testQueueChainingStatusUpdates(self):
        """Chain workQueues, pass work down and verify lifecycle"""

        self.assertEqual(0, len(self.globalQueue))
        self.assertEqual(0, len(self.localQueue.getWork({'T2_XX_SiteA': 1000}, {})))

        # Add work to top most queue
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteD"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.globalQueue.processInboundWork()
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))

        # pull to local queue
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1000}), NBLOCKS_HICOMM)
        syncQueues(self.localQueue)  # Tell parent local has acquired
        self.assertEqual(len(self.globalQueue.status('Acquired')), NBLOCKS_HICOMM)
        self.assertEqual(len(self.localQueue.status('Available')), NBLOCKS_HICOMM)

        # run work
        self.globalQueue.updateLocationInfo()
        work = self.localQueue.getWork({'T2_XX_SiteA': 1000},
                                       {})
        self.assertEqual(len(work), NBLOCKS_HICOMM)

        # resend info
        syncQueues(self.localQueue)
        self.assertEqual(len(self.globalQueue.status('Running')), NBLOCKS_HICOMM)
        self.assertEqual(len(self.localQueue.status('Running')), NBLOCKS_HICOMM)

        # finish work locally and propagate to global
        self.localQueue.doneWork([x.id for x in work])
        # just update the elements but not using the result
        for x in work:
            self.localQueue.backend.updateElements(x.id, PercentComplete=100, PercentSuccess=99)
        elements = self.localQueue.status('Done')
        self.assertEqual(len(elements), len(work))
        self.assertEqual([x['PercentComplete'] for x in elements],
                         [100] * len(work))
        self.assertEqual([x['PercentSuccess'] for x in elements],
                         [99] * len(work))
        syncQueues(self.localQueue, skipWMBS=True)

        elements = self.globalQueue.status('Done')
        self.assertEqual(len(elements), NBLOCKS_HICOMM)
        self.assertEqual([x['PercentComplete'] for x in elements], [100] * NBLOCKS_HICOMM)
        self.assertEqual([x['PercentSuccess'] for x in elements], [99] * NBLOCKS_HICOMM)

        self.globalQueue.performQueueCleanupActions()
        # gq still has 2 elements since elements won't be cleaned up untill requset status updated
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue.status()))
        elements = self.globalQueue.backend.getInboxElements('Done')
        self.assertEqual(len(elements), 1)
        self.assertEqual([x['PercentComplete'] for x in elements], [100])
        self.assertEqual([x['PercentSuccess'] for x in elements], [99])

    def testMultiTaskProduction(self):
        """
        Test Multi top level task production spec.
        multiTaskProduction spec consist 2 top level tasks each task has event size 1000 and 2000
        respectfully
        """
        # TODO: needs more rigorous test on each element per task
        # Basic production Spec
        spec = MultiTaskProductionWorkload
        for task in spec.taskIterator():
            delattr(task.steps().data.application.configuration, 'configCacheUrl')
        spec.setSpecUrl(os.path.join(self.workDir, 'multiTaskProduction.spec'))
        spec.setOwnerDetails("evansde77", "DMWM", {'dn': 'MyDN'})
        spec.save(spec.specUrl())

        specfile = spec.specUrl()
        numElements = 3
        njobs = [10] * numElements  # array of jobs per block
        total = sum(njobs)

        # Queue Work &njobs check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(2, len(self.queue))

        # try to get work
        work = self.queue.getWork({'T2_XX_SiteA': 0},
                                  {})
        self.assertEqual([], work)
        # check individual task whitelists obeyed when getting work
        work = self.queue.getWork({'T2_XX_SiteA': total},
                                  {})
        self.assertEqual(len(work), 1)
        work2 = self.queue.getWork({'T2_XX_SiteB': total},
                                   {})
        self.assertEqual(len(work2), 1)
        work.extend(work2)
        self.assertEqual(len(work), 2)
        self.assertEqual(sum([x['Jobs'] for x in self.queue.status(status='Running')]),
                         total)
        # check we have all tasks and no extra/missing ones
        for task in spec.taskIterator():
            # note: order of elements in work is undefined (both inserted simultaneously)
            element = [x for x in work if x['Subscription']['workflow'].task == task.getPathName()]
            if not element:
                self.fail("Top level task %s not in wmbs" % task.getPathName())
            element = element[0]

            # check restrictions - only whitelist for now
            whitelist = element['Subscription'].getWhiteBlackList()
            whitelist = [x['site_name'] for x in whitelist if x['valid'] == 1]
            self.assertEqual(sorted(task.siteWhitelist()), sorted(whitelist))

        # no more work available
        self.assertEqual(0, len(self.queue.getWork({'T2_XX_SiteA': total, 'T2_XX_SiteB': total},
                                                   {})))
        try:
            os.unlink(specfile)
        except OSError:
            pass

    def testTeams(self):
        """
        Team behaviour
        """
        specfile = self.spec.specUrl()
        self.globalQueue.queueWork(specfile, team='The A-Team')
        self.globalQueue.processInboundWork()
        self.assertEqual(1, len(self.globalQueue))
        slots = {'T2_XX_SiteA': 1000, 'T2_XX_SiteB': 1000}

        # Can't get work for wrong team
        self.localQueue.params['Team'] = 'other'
        self.assertEqual(self.localQueue.pullWork(slots), 0)
        # and with correct team name
        self.localQueue.params['Team'] = 'The A-Team'
        self.assertEqual(self.localQueue.pullWork(slots), 1)
        syncQueues(self.localQueue)
        # when work leaves the queue in the agent it doesn't care about teams
        self.localQueue.params['Team'] = 'other'
        self.assertEqual(len(self.localQueue.getWork(slots, {})), 1)
        self.assertEqual(0, len(self.globalQueue))

    def testSplittingLargeInputs(self):
        """
        _testSplittingLargeInputs_

        Check that we can split large inputs and store the processed inputs
        in the inbox element correctly.
        """
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        inboxElement = self.globalQueue.backend.getInboxElements(elementIDs=[processingSpec.name()])
        self.assertEqual(len(inboxElement[0]['ProcessedInputs']), NBLOCKS_HICOMM)
        return

    def testGlobalBlockSplitting(self):
        """Block splitting at global level"""
        # force global queue to split work on block
        self.globalQueue.params['SplittingMapping']['DatasetBlock']['name'] = 'Block'
        self.globalQueue.params['SplittingMapping']['Block']['name'] = 'Block'
        self.globalQueue.params['SplittingMapping']['Dataset']['name'] = 'Block'

        # queue work, globally for block, pass down, report back -> complete
        totalSpec = 1
        totalBlocks = totalSpec * NBLOCKS_HICOMM
        self.assertEqual(0, len(self.globalQueue))
        for _ in range(totalSpec):
            processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
            self.globalQueue.queueWork(processingSpec.specUrl())
        self.globalQueue.processInboundWork()
        self.assertEqual(totalBlocks, len(self.globalQueue))
        # both blocks in global belong to same parent, but have different inputs
        status = self.globalQueue.status()
        self.assertEqual(status[0]['ParentQueueId'], status[1]['ParentQueueId'])
        self.assertNotEqual(status[0]['Inputs'], status[1]['Inputs'])

        # pull to local
        # location info should already be added
        # self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1000}),
                         totalBlocks)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.status(status='Available')),
                         totalBlocks)  # 2 in local
        # self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'T2_XX_SiteA': 1000, 'T2_XX_SiteB': 1000},
                                       {})
        self.assertEqual(len(work), totalBlocks)
        # both refer to same wmspec
        self.assertEqual(work[0]['RequestName'], work[1]['RequestName'])
        self.localQueue.doneWork([str(x.id) for x in work])
        # elements in local deleted at end of update, only global ones left
        self.assertEqual(len(self.localQueue.status(status='Done')),
                         totalBlocks)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.status(status='Done')),
                         totalBlocks)
        self.assertEqual(len(self.globalQueue.status(status='Done')),
                         totalBlocks)

    def testGlobalDatasetSplitting(self):
        """Dataset splitting at global level"""

        dqmHarvArgs = DQMHarvestWorkloadFactory.getTestArguments()
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        factory = DQMHarvestWorkloadFactory()
        dqmWorkload = factory.factoryWorkloadConstruction('dqmTest', dqmHarvArgs)
        dqmWorkload.setSpecUrl((os.path.join(self.workDir, 'dqmTest.spec')))
        dqmWorkload.setSiteWhitelist('T2_XX_SiteA')
        dqmWorkload.setTrustLocationFlag()
        dqmWorkload.save(dqmWorkload.specUrl())

        # force global queue to split work on Dataset
        self.globalQueue.params['SplittingMapping']['DatasetBlock']['name'] = 'Dataset'
        self.globalQueue.params['SplittingMapping']['Block']['name'] = 'Dataset'
        self.globalQueue.params['SplittingMapping']['Dataset']['name'] = 'Dataset'

        # queue work, globally for block, pass down, report back -> complete
        totalSpec = 1
        self.assertEqual(0, len(self.globalQueue))
        for _ in range(totalSpec):
            self.globalQueue.queueWork(dqmWorkload.specUrl())
        # now we have elements inserted in workqueue
        self.assertEqual(len(self.globalQueue.status(status='Available')), totalSpec)
        # this should be a continuous processing of the spec, so no changes
        self.globalQueue.processInboundWork()
        self.assertEqual(totalSpec, len(self.globalQueue))
        self.assertEqual(len(self.globalQueue.status(status='Available')), totalSpec)

        # pull to local
        # self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1000}), totalSpec)
        syncQueues(self.localQueue)
        # give a few extra seconds for work to move between local/global queues
        time.sleep(2)
        self.assertEqual(len(self.localQueue.status(status='Available')), totalSpec)
        self.assertEqual(len(self.globalQueue.status(status='Acquired')), totalSpec)
        self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'T2_XX_SiteA': 1000}, {})
        self.assertEqual(len(work), 1)
        self.assertEqual(len(self.localQueue.status(status='Running')), totalSpec)
        self.assertEqual(len(self.globalQueue.status(status='Acquired')), totalSpec)
        self.localQueue.doneWork([str(x.id) for x in work])
        self.assertEqual(len(self.localQueue.status(status='Done')), totalSpec)
        syncQueues(self.localQueue)
        # elements are not deleted until request status is changed
        self.assertEqual(len(self.localQueue.status(status='Done')), totalSpec)
        self.assertEqual(len(self.globalQueue.status(status='Done')), totalSpec)

    def testResetWork(self):
        """Reset work in global to different child queue"""
        # TODO: This test sometimes fails - i suspect a race condition (maybe conflict in couch)
        # Cancel code needs reworking so this will hopefully be fixed then
        totalBlocks = NBLOCKS_HICOMM
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1000}),
                         totalBlocks)
        syncQueues(self.localQueue)
        work = self.localQueue.getWork({'T2_XX_SiteA': 1000, 'T2_XX_SiteB': 1000},
                                       {})
        self.assertEqual(len(work), totalBlocks)
        self.assertEqual(len(self.localQueue.status(status='Running')), totalBlocks)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.globalQueue.status(status='Running')), totalBlocks)

        # Re-assign work in global
        self.globalQueue.resetWork([x.id for x in self.globalQueue.status(status='Running')])

        # work should be canceled in local
        # TODO: Note the work in local will be orphaned but not canceled
        syncQueues(self.localQueue)
        work_at_local = [x for x in self.globalQueue.status(status='Running') \
                         if x['ChildQueueUrl'] == sanitizeURL(self.localQueue.params['QueueURL'])['url']]
        self.assertEqual(len(work_at_local), 0)

        # now 2nd queue calls and acquires work
        self.assertEqual(self.localQueue2.pullWork({'T2_XX_SiteA': 1000}),
                         totalBlocks)
        syncQueues(self.localQueue2)

        # check work in global assigned to local2
        self.assertEqual(len(self.localQueue2.status(status='Available')), totalBlocks)  # work in local2
        work_at_local2 = [x for x in self.globalQueue.status(status='Acquired')
                          if x['ChildQueueUrl'] == sanitizeURL(self.localQueue2.params['QueueURL'])['url']]
        self.assertEqual(len(work_at_local2), totalBlocks)

    def testCancelWork(self):
        """Cancel work"""
        processingSpec = self.setupReReco(assignArgs={"SiteWhitelist":["T2_XX_SiteA", "T2_XX_SiteB"]})
        self.queue.queueWork(processingSpec.specUrl())
        elements = len(self.queue)
        self.queue.updateLocationInfo()
        self.assertEqual(len(self.queue.status()), NBLOCKS_HICOMM)
        work = self.queue.getWork({'T2_XX_SiteA': 1000, 'T2_XX_SiteB': 1000}, {})
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(len(self.queue.status(status='Running')), elements)
        ids = [x.id for x in work]
        self.assertEqual(len(ids), NBLOCKS_HICOMM)
        canceled = self.queue.cancelWork(ids)
        self.assertEqual(sorted(canceled), sorted(ids))
        self.assertEqual(len(self.queue.status()), NBLOCKS_HICOMM)
        self.assertEqual(len(self.queue.status(status='Running')), NBLOCKS_HICOMM)
        self.assertEqual(len(self.queue.statusInbox(status='Canceled')), 1)

        # create a new request with one fake file
        self.queue.queueWork(self.spec.specUrl())
        self.assertEqual(len(self.queue), 1)
        work = self.queue.getWork({'T2_XX_SiteA': 1000, 'T2_XX_SiteB': 1000}, {})
        self.assertEqual(len(self.queue.status(status='Running')), len(self.queue.status()))
        ids = [x.id for x in work]
        canceled = self.queue.cancelWork(WorkflowName='testProduction')
        self.assertEqual(canceled, ids)
        self.assertEqual(len(self.queue), 0)

    def testCancelWorkGlobal(self):
        """Cancel work in global queue"""
        # queue to global & pull an element to local
        processingSpec = self.setupReReco(assignArgs= {'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)
        syncQueues(self.localQueue)

        # cancel in global and propagate to local
        service = WorkQueueService(self.localQueue.backend.parentCouchUrlWithAuth)
        service.cancelWorkflow(processingSpec.name())
        # marked for cancel
        self.assertEqual(len(self.globalQueue.status(status='CancelRequested')), NBLOCKS_HICOMM)
        self.assertEqual(len(self.globalQueue.statusInbox(status='Acquired')), 1)

        # will cancel element left in global, one sent to local queue stays CancelRequested
        syncQueues(self.globalQueue)
        self.assertEqual(len(self.globalQueue.status(status='CancelRequested')), 1)
        self.assertEqual(len(self.globalQueue.status(status='Canceled')), NBLOCKS_HICOMM - 1)
        self.assertEqual(len(self.globalQueue.statusInbox(status='CancelRequested')), 1)
        # global parent stays CancelRequested till child queue cancels
        syncQueues(self.globalQueue)
        self.assertEqual(len(self.globalQueue.status(status='CancelRequested')), 1)
        self.assertEqual(len(self.globalQueue.status(status='Canceled')), NBLOCKS_HICOMM - 1)
        self.assertEqual(len(self.globalQueue.statusInbox(status='CancelRequested')), 1)

        # during sync local queue will synced with globalQueue but not gets deleted until workflow finished
        syncQueues(self.localQueue)

        self.assertEqual(len(self.localQueue.statusInbox(status='Canceled')), 1)  # inbox is synced
        self.assertEqual(len(self.globalQueue.status(status='Canceled')), NBLOCKS_HICOMM)
        self.assertEqual(len(self.globalQueue.statusInbox(status='CancelRequested')), 1)
        syncQueues(self.globalQueue)
        self.assertEqual(len(self.globalQueue.status(status='Canceled')), NBLOCKS_HICOMM)
        self.assertEqual(len(self.globalQueue.statusInbox(status='Canceled')), 1)
        syncQueues(self.localQueue)
        # local cancelded
        # self.assertEqual(len(self.localQueue.status(status='Canceled')), 1)
        # clear global
        self.globalQueue.deleteWorkflows(processingSpec.name())
        self.assertEqual(len(self.globalQueue.statusInbox()), 0)

        ### check cancel of work negotiating in agent works
        self.globalQueue.queueWork(self.whitelistSpec.specUrl())
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteB': 1}), 1)
        self.localQueue.backend.forceQueueSync()
        time.sleep(2)
        self.assertEqual(len(self.localQueue.statusInbox(status='Negotiating')), 1)

        # now cancel
        service.cancelWorkflow(self.whitelistSpec.name())
        syncQueues(self.globalQueue)
        self.localQueue.backend.forceQueueSync()  # pull in cancelation
        time.sleep(2)
        self.assertEqual(len(self.globalQueue.status(status='Canceled')), 2 * NBLOCKS_HICOMM - 1)
        self.assertEqual(len(self.localQueue.statusInbox(status='CancelRequested')), 1)
        syncQueues(self.localQueue, skipWMBS=True)
        self.assertEqual(len(self.localQueue.statusInbox(status='Canceled')), 2)
        syncQueues(self.localQueue)
        syncQueues(self.globalQueue)

        self.assertEqual(len(self.localQueue.statusInbox(WorkflowName=self.whitelistSpec.name())), 1)
        self.assertEqual(len(self.globalQueue.status(WorkflowName=self.whitelistSpec.name())), NBLOCKS_HICOMM)
        self.assertEqual(len(self.globalQueue.statusInbox(status='Canceled')), 1)
        # clear global
        self.globalQueue.deleteWorkflows(self.whitelistSpec.name())
        self.assertEqual(len(self.globalQueue.statusInbox()), 0)

    def testInvalidSpecs(self):
        """Complain on invalid WMSpecs"""
        # request != workflow name
        self.assertRaises(WorkQueueWMSpecError, self.queue.queueWork,
                          self.setupReReco().specUrl(),
                          request='fail_this')

        # invalid white list
        mcspec = stepchainWorkload('testProductionInvalid', self.mcArgs)
        getFirstTask(mcspec).setSiteWhitelist('ThisIsInvalid')
        mcspec.setSpecUrl(os.path.join(self.workDir, 'testProductionInvalid.spec'))
        mcspec.save(mcspec.specUrl())
        self.assertRaises(WorkQueueWMSpecError, self.queue.queueWork, mcspec.specUrl())
        getFirstTask(mcspec).setSiteWhitelist(['T2_XX_SiteB'])
        self.queue.deleteWorkflows(mcspec.name())

        # 0 events
        getFirstTask(mcspec).addProduction(totalEvents=0)
        getFirstTask(mcspec).setSiteWhitelist(['T2_XX_SiteB'])
        mcspec.save(mcspec.specUrl())
        self.assertRaises(WorkQueueNoWorkError, self.queue.queueWork, mcspec.specUrl())

        # no dataset
        processingSpec = rerecoWorkload('testProcessingInvalid', self.rerecoArgs)
        getFirstTask(processingSpec).setSiteWhitelist(['T2_XX_SiteB'])
        processingSpec.setSpecUrl(os.path.join(self.workDir,
                                               'testProcessingInvalid.spec'))
        processingSpec.save(processingSpec.specUrl())
        getFirstTask(processingSpec).data.input.dataset = None
        processingSpec.save(processingSpec.specUrl())
        self.assertRaises(WorkQueueWMSpecError, self.queue.queueWork, processingSpec.specUrl())

        # invalid dbs url
        processingSpec = rerecoWorkload('testProcessingInvalid', self.rerecoArgs)
        getFirstTask(processingSpec).setSiteWhitelist(['T2_XX_SiteB'])
        processingSpec.setSpecUrl(os.path.join(self.workDir,
                                               'testProcessingInvalid.spec'))
        getFirstTask(processingSpec).data.input.dataset.dbsurl = 'wrongprot://dbs.example.com'
        processingSpec.save(processingSpec.specUrl())
        self.assertRaises(WorkQueueWMSpecError, self.queue.queueWork, processingSpec.specUrl())
        self.queue.deleteWorkflows(processingSpec.name())

        # invalid dataset name
        processingSpec = rerecoWorkload('testProcessingInvalid', self.rerecoArgs)
        getFirstTask(processingSpec).setSiteWhitelist(['T2_XX_SiteB'])
        processingSpec.setSpecUrl(os.path.join(self.workDir,
                                               'testProcessingInvalid.spec'))
        getFirstTask(processingSpec).data.input.dataset.name = '/MinimumBias/FAKE-Filter-v1/RECO'
        processingSpec.save(processingSpec.specUrl())
        self.assertRaises(DBSReaderError, self.queue.queueWork, processingSpec.specUrl())
        self.queue.deleteWorkflows(processingSpec.name())

        # Cant have a slash in primary ds name - validation should fail
        getFirstTask(processingSpec).data.input.dataset.primary = 'a/b'
        processingSpec.save(processingSpec.specUrl())
        self.assertRaises(DBSReaderError, self.queue.queueWork, processingSpec.specUrl())
        self.queue.deleteWorkflows(processingSpec.name())

        # dataset splitting with invalid run whitelist
        processingSpec = rerecoWorkload('testProcessingInvalid', self.rerecoArgs)
        getFirstTask(processingSpec).setSiteWhitelist(['T2_XX_SiteB'])
        processingSpec.setSpecUrl(os.path.join(self.workDir,
                                               'testProcessingInvalid.spec'))
        processingSpec.setStartPolicy('Dataset')
        processingSpec.setRunWhitelist([666])  # not in this dataset
        processingSpec.save(processingSpec.specUrl())
        self.assertRaises(DBSReaderError, self.queue.queueWork, processingSpec.specUrl())
        self.queue.deleteWorkflows(processingSpec.name())

        # block splitting with invalid run whitelist
        processingSpec = rerecoWorkload('testProcessingInvalid', self.rerecoArgs)
        getFirstTask(processingSpec).setSiteWhitelist(['T2_XX_SiteB'])
        processingSpec.setSpecUrl(os.path.join(self.workDir,
                                               'testProcessingInvalid.spec'))
        processingSpec.setStartPolicy('Block')
        processingSpec.setRunWhitelist([666])  # not in this dataset
        processingSpec.save(processingSpec.specUrl())
        self.assertRaises(DBSReaderError, self.queue.queueWork, processingSpec.specUrl())
        self.queue.deleteWorkflows(processingSpec.name())

    def testIgnoreDuplicates(self):
        """Ignore duplicate work"""
        specfile = self.spec.specUrl()
        self.globalQueue.queueWork(specfile)
        self.assertEqual(1, len(self.globalQueue))

        # queue work again
        self.globalQueue.queueWork(specfile)
        self.assertEqual(1, len(self.globalQueue))

    def testConflicts(self):
        """Resolve conflicts between global & local queue"""
        self.globalQueue.queueWork(self.spec.specUrl())
        self.localQueue.pullWork({'T2_XX_SiteA': TOTAL_EVENTS})
        self.localQueue.getWork({'T2_XX_SiteA': TOTAL_EVENTS},
                                {})
        syncQueues(self.localQueue)
        global_ids = [x.id for x in self.globalQueue.status()]
        self.localQueue.backend.updateInboxElements(*global_ids, Status='Done', PercentComplete=69)
        self.globalQueue.backend.updateElements(*global_ids, Status='Canceled')
        self.localQueue.backend.forceQueueSync()
        time.sleep(2)
        self.assertForConflicts()

    @retry(AssertionError, tries=3, delay=10)
    def assertForConflicts(self):
        """
        Make the assertions in a separate function so we can use retry
        """

        global_ids = [x.id for x in self.globalQueue.status()]

        self.localQueue.backend.fixConflicts()
        self.localQueue.backend.forceQueueSync()
        time.sleep(2)
        self.assertEqual([x['Status'] for x in self.globalQueue.status(elementIDs=global_ids)],
                         ['Canceled'])
        self.assertEqual([x['PercentComplete'] for x in self.globalQueue.status(elementIDs=global_ids)],
                         [69])
        self.assertEqual([x for x in self.localQueue.statusInbox()],
                         [x for x in self.globalQueue.status()])

    def testDeleteWork(self):
        """Delete finished work
        TODO: do emulate the reqmgr2 and change the status of request
        so actually request gets deleted when performCleanupAction is run.
        """
        self.globalQueue.queueWork(self.spec.specUrl())
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': TOTAL_EVENTS}), 1)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.getWork({'T2_XX_SiteA': TOTAL_EVENTS},
                                                     {})), 1)
        syncQueues(self.localQueue)
        self.localQueue.doneWork(WorkflowName=self.spec.name())
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.status(WorkflowName=self.spec.name())),
                         1)  # not deleted until request status is updated
        self.assertEqual('Done',
                         self.globalQueue.status(WorkflowName=self.spec.name())[0]['Status'])
        self.globalQueue.performQueueCleanupActions()
        self.assertEqual('Done',
                         self.globalQueue.statusInbox(WorkflowName=self.spec.name())[0]['Status'])
        self.assertEqual(len(self.globalQueue.status(WorkflowName=self.spec.name())),
                         1)  # not deleted until request status is update
        self.globalQueue.deleteWorkflows(self.spec.name())
        self.assertEqual(len(self.globalQueue.statusInbox(WorkflowName=self.spec.name())),
                         0)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.statusInbox(WorkflowName=self.spec.name())),
                         1)  # not deleted until request status is update

    def testResubmissionWorkflow(self):
        """Test workflow resubmission via ACDC"""
        acdcCouchDB = "workqueue_t_acdc"
        self.testInit.setupCouch(acdcCouchDB, "GroupUser", "ACDC")

        spec = self.createResubmitSpec(self.testInit.couchUrl,
                                       acdcCouchDB)
        spec.setSpecUrl(os.path.join(self.workDir, 'resubmissionWorkflow.spec'))
        spec.setSiteWhitelist('T1_US_FNAL')
        spec.save(spec.specUrl())
        self.localQueue.params['Team'] = 'cmsdataops'
        self.globalQueue.queueWork(spec.specUrl(), "Resubmit_TestWorkload", team="cmsdataops")
        self.assertEqual(self.localQueue.pullWork({"T1_US_FNAL": 100}), 1)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.getWork({"T1_US_FNAL": 100}, {})), 1)

    def testResubmissionWithParentsWorkflow(self):
        """Test workflow resubmission with parentage via ACDC"""
        acdcCouchDB = "workqueue_t_acdc"
        self.testInit.setupCouch(acdcCouchDB, "GroupUser", "ACDC")

        spec = self.createResubmitSpec(self.testInit.couchUrl,
                                       acdcCouchDB, parentage=True)
        spec.setSpecUrl(os.path.join(self.workDir, 'resubmissionWorkflow.spec'))
        spec.setSiteWhitelist('T1_US_FNAL')
        spec.save(spec.specUrl())
        self.localQueue.params['Team'] = 'cmsdataops'
        self.globalQueue.queueWork(spec.specUrl(), "Resubmit_TestWorkload", team="cmsdataops")
        self.localQueue.pullWork({"T1_US_FNAL": 100})
        syncQueues(self.localQueue)
        self.localQueue.getWork({"T1_US_FNAL": 100}, {})

    def testResubmissionWorkflowSiteWhitelistLocations(self):
        """ Test an ACDC workflow where we use the site whitelist as locations"""
        acdcCouchDB = "workqueue_t_acdc"
        self.testInit.setupCouch(acdcCouchDB, "GroupUser", "ACDC")

        spec = self.createResubmitSpec(self.testInit.couchUrl,
                                       acdcCouchDB)
        spec.setSpecUrl(os.path.join(self.workDir, 'resubmissionWorkflow.spec'))
        spec.setSiteWhitelist('T1_US_FNAL')
        spec.setTrustLocationFlag(inputFlag=True, pileupFlag=False)
        spec.save(spec.specUrl())
        self.localQueue.params['Team'] = 'cmsdataops'
        self.globalQueue.queueWork(spec.specUrl(), "Resubmit_TestWorkload", team="cmsdataops")
        self.assertEqual(self.localQueue.pullWork({"T1_UK_RAL": 100}), 0)
        self.assertEqual(self.localQueue.pullWork({"T1_US_FNAL": 100}), 1)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.getWork({"T1_US_FNAL": 100}, {})), 1)

    def testThrottling(self):
        """Pull work only if all previous work processed in child"""
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})

        self.globalQueue.queueWork(processingSpec.specUrl())
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)
        # further pull will fail till we replicate to child
        # hopefully couch replication wont happen till we manually sync
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 0)
        self.assertEqual(NBLOCKS_HICOMM - 1, len(self.globalQueue))
        self.assertEqual(0, len(self.localQueue))
        syncQueues(self.localQueue)
        self.assertEqual(1, len(self.localQueue))
        # pull works again
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)

    def testSitesFromResourceControl(self):
        """Test sites from resource control"""
        # Most tests pull work for specific sites (to give us control)
        # In reality site list will come from resource control so test
        # that here (just a simple check that we can get sites from rc)
        self.globalQueue.queueWork(self.spec.specUrl())
        self.assertEqual(self.localQueue.pullWork(), 1)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue.status()), 1)

    def testParentProcessing(self):
        """
        Enqueue and get work for a processing WMSpec.
        """
        parentProcSpec = self.setupParentProcSpec(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
        specfile = parentProcSpec.specUrl()

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.queue.processInboundWork()
        self.assertEqual(NBLOCKS_COSMIC, len(self.queue))

        self.queue.updateLocationInfo()
        # No resources
        work = self.queue.getWork({}, {})
        self.assertEqual(len(work), 0)
        work = self.queue.getWork({'T2_XX_SiteA': 0,
                                   'T2_XX_SiteB': 0}, {})
        self.assertEqual(len(work), 0)

        # Get 1 work element when any resources free
        work = self.queue.getWork({'T2_XX_SiteB': 1}, {})
        self.assertEqual(len(work), 1)
        processedFiles = work[0]["NumOfFilesAdded"]

        # claim remaining work
        work = self.queue.getWork({'T2_XX_SiteA': TOTAL_EVENTS, 'T2_XX_SiteB': TOTAL_EVENTS}, {})
        self.assertEqual(len(work), NBLOCKS_COSMIC - 1)
        for element in work:
            processedFiles += element["NumOfFilesAdded"]
        self.assertEqual(processedFiles, NFILES_COSMIC + NFILES_COSMICRAW)

        # no more work available
        self.assertEqual(0, len(self.queue.getWork({'T2_XX_SiteA': 1000}, {})))

    def testWMBSInjectionStatus(self):
        syncQueues(self.localQueue)
        self.globalQueue.queueWork(self.spec.specUrl())
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        # test globalqueue status (no parent queue case)
        self.assertEqual(self.globalQueue.getWMBSInjectionStatus(),
                         [{'testProcessing': False}, {'testProduction': False}])
        self.assertEqual(self.globalQueue.getWMBSInjectionStatus(self.spec.name()),
                         False)
        # Amount of work varies, just make sure it's positive
        self.assertGreater(self.localQueue.pullWork(), 0)
        # test local queue status with parents (globalQueue is not synced yet
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(),
                         [{'testProcessing': False}, {'testProduction': False}])
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(self.spec.name()),
                         False)
        self.localQueue.processInboundWork()
        self.localQueue.updateLocationInfo()
        self.localQueue.getWork({'T2_XX_SiteA': 1000},
                                {})
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(),
                         [{'testProcessing': False}, {'testProduction': False}])
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(self.spec.name()),
                         False)

        # update parents status but is still running open since it is the default
        self.localQueue.performQueueCleanupActions()
        # NOTE: these 2 assertions below are different when running from local docker container
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(),
                         [{'testProcessing': False}, {'testProduction': False}])
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(self.spec.name()), False)

        # close the global inbox elements, they won't be split anymore
        #self.globalQueue.closeWork(['testProcessing', 'testProduction'])
        self.globalQueue.closeWork()
        self.localQueue.getWMBSInjectionStatus()
        time.sleep(1)
        # There are too many jobs to pull down for testProcessing still has element not in WMBS
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(),
                         [{'testProcessing': False}, {'testProduction': True}])
        self.assertEqual(self.localQueue.getWMBSInjectionStatus(self.spec.name()),
                         True)

        # test not existing workflow
        self.assertRaises(WorkQueueNoMatchingElements,
                          self.localQueue.getWMBSInjectionStatus,
                          "NotExistWorkflow")

    def testEndPolicyNegotiating(self):
        """Test end policy processing of request before splitting"""
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})
        work = self.globalQueue.queueWork(processingSpec.specUrl())
        self.assertEqual(work, NBLOCKS_HICOMM)
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)
        self.localQueue.backend.pullFromParent()  # pull work into inbox (Negotiating state)
        self.localQueue.processInboundWork()
        syncQueues(self.localQueue)
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)
        # should print message but not raise an error
        self.localQueue.performQueueCleanupActions(skipWMBS=True)
        self.localQueue.backend.pullFromParent(continuous=False)
        self.assertEqual(len(self.localQueue.statusInbox(Status='Negotiating')), 1)
        self.assertEqual(len(self.localQueue), 1)

    def testSiteStatus(self):
        """Check that we only pull work on sites in Normal status"""
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.globalQueue.queueWork(self.spec.specUrl())
        # acquire 1 element of a wf and then mark site as draining.
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)
        syncQueues(self.localQueue)
        existing_wf = [x['RequestName'] for x in self.localQueue.statusInbox()]
        self.assertEqual(1, len(existing_wf))
        existing_wf = existing_wf[0]
        bossAirConfig = Configuration()
        bossAirConfig.section_("BossAir")
        bossAirConfig.BossAir.pluginDir = "WMCore.BossAir.Plugins"
        bossAirConfig.BossAir.pluginNames = []
        bossAirConfig.section_("Agent")
        bossAirConfig.Agent.agentName = "TestAgent"
        bossAirConfig.section_("JobStateMachine")
        bossAirConfig.JobStateMachine.couchurl = os.environ["COUCHURL"]
        bossAirConfig.JobStateMachine.couchDBName = "testcouchdb"
        rc = ResourceControl(bossAirConfig)
        rc.changeSiteState('T2_XX_SiteA', 'Draining')
        rc.changeSiteState('T2_XX_SiteB', 'Draining')
        # pull more work, no work should be acquired
        self.localQueue.pullWork()
        syncQueues(self.localQueue)
        for x in self.localQueue.statusInbox():
            if x['RequestName'] != existing_wf:
                self.fail('Got new wf %s for draining site' % x['RequestName'])
        # wmbs injection for draining sites continues to work
        self.assertTrue(self.localQueue.getWork({'T2_XX_SiteA': 10},
                                                {}))
        # re-enable site and get remainder of work
        rc.changeSiteState('T2_XX_SiteA', 'Normal')
        self.assertTrue(self.localQueue.pullWork())
        syncQueues(self.localQueue)
        self.assertTrue(self.localQueue.getWork({'T2_XX_SiteA': 100},
                                                {}))

    def test0eventBlock(self):
        """0 event blocks should be processed as usual"""
        # use event splitting and 0 events so we get 0 jobs - verify this doesn't cause any problems
        # FIXME: This does not work currently because we don't actually have 0 event blocks.

        Globals.GlobalParams.setNumOfEventsPerFile(0)
        syncQueues(self.localQueue)

        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})
        processingSpec.setStartPolicy('Block', SliceType='NumberOfEvents')
        processingSpec.save(processingSpec.specUrl())
        self.globalQueue.queueWork(processingSpec.specUrl())
        # pulls one block from global queue to local queue inbox
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 1)
        time.sleep(1)
        # now we should have 1 element in the local inbox and 0 in local queue
        self.assertEqual(len(self.localQueue.statusInbox()), 1)
        self.assertEqual(len(self.localQueue.status()), 0)

        self.localQueue.processInboundWork()
        time.sleep(1)
        # now acquire the local inbox element (create it in the local queue)
        self.assertEqual(len(self.localQueue.getWork({'T2_XX_SiteA': 1}, {})), 1)
        for element in self.localQueue.status():
            # check files added and subscription made
            self.assertEqual(element['NumOfFilesAdded'], 1)
            self.assertTrue(element['SubscriptionId'] >= 0)
            self.assertEqual(element['Jobs'], 1)

        # complete workflow
        self.localQueue.performQueueCleanupActions(skipWMBS=True)
        self.localQueue.doneWork([str(x.id) for x in self.localQueue.status()])
        self.assertEqual(len(self.localQueue.status(status='Done')), 1)
        syncQueues(self.localQueue)
        self.assertEqual(len(self.globalQueue.status(status='Done')), 1)

    def testProcessingWithContinuousSplitting(self):
        """Test the open request handling in the WorkQueue"""
        # Put normal work in
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"]})
        specfile = processingSpec.specUrl()

        # Queue work with initial block count
        logging.info("Queuing work for spec name: %s", processingSpec.name())
        self.assertEqual(NBLOCKS_HICOMM, self.globalQueue.queueWork(specfile))
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))

        # Try adding work, no change in blocks available. No work should be added
        logging.info("Adding work - already added - for spec name: %s", processingSpec.name())
        workInbox = self.globalQueue.backend.getInboxElements(WorkflowName=processingSpec.name(),
                                                              loadSpec=True)
        self.assertEqual(0, self.globalQueue.addWork(workInbox[0]))
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))

        # Now pull work from the global to the local queue
        logging.info("Pulling 1 workqueue element from the parent queue")
        self.localQueue.pullWork({'T2_XX_SiteA': 1})
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue), 1)
        self.assertEqual(len(self.globalQueue), NBLOCKS_HICOMM - 1)

        # This time pull work from the local queue into WMBS
        logging.info("Getting 1 workqueue element from the local queue")
        dummyWork = self.localQueue.getWork({'T2_XX_SiteA': 1000},
                                            {})
        syncQueues(self.localQueue)
        syncQueues(self.globalQueue)

        workInbox = self.globalQueue.backend.getInboxElements(status="Running", loadSpec=True)
        self.assertEqual(0, self.globalQueue.addWork(workInbox[0]))

        self.assertEqual(NBLOCKS_HICOMM - 1, len(self.globalQueue))
        self.assertEqual(len(self.globalQueue.backend.getInboxElements(status="Running")), 1)

        # Now pull the new work to the local queue
        self.localQueue.pullWork({'T2_XX_SiteB': 1000, 'T2_XX_SiteC': 1000})
        syncQueues(self.localQueue)
        self.assertEqual(len(self.localQueue), 30)
        self.assertEqual(len(self.globalQueue), NBLOCKS_HICOMM - 30 - 1)

        workInbox = self.globalQueue.backend.getInboxElements(WorkflowName=processingSpec.name(), loadSpec=True)
        self.assertEqual(0, self.globalQueue.addWork(workInbox[0]))

        return

    def testProcessingWithPileup(self):
        """Test a full WorkQueue cycle in a request with pileup datasets"""
        specfile = self.processingPileupSpec.specUrl()
        # Queue work with initial block count
        self.assertEqual(NBLOCKS_HICOMM, self.globalQueue.queueWork(specfile))
        self.assertEqual(NBLOCKS_HICOMM, len(self.globalQueue))

        # All blocks are in Site A, B, and C, but the pileup is only at C.
        # We should not be able to pull all the work.
        self.assertGreaterEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1,
                                                          'T2_XX_SiteB': 1,
                                                          'T2_XX_SiteC': 1}), 3)
        # The Rucio emulator will move the pileup blocks to site A
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteB': 1,
                                                   'T2_XX_SiteC': 1}), 0)

        # Now try with just site A (no work)
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': 1}), 0)
        syncQueues(self.localQueue)
        self.assertGreaterEqual(len(self.localQueue), 3)
        self.assertEqual(len(self.globalQueue), NBLOCKS_HICOMM - 3)

        # Pull it to WMBS, first try with an impossible site
        # The pileup was split again in the local queue so site A is not there
        self.assertGreaterEqual(len(self.localQueue.getWork({'T2_XX_SiteA': 1,
                                                             'T2_XX_SiteB': 1,
                                                             'T2_XX_SiteC': 1}, {})), 2)
        Globals.moveBlock({'%s#1' % PILEUP_DATASET: ['T2_XX_SiteA', 'T2_XX_SiteC'],
                           '%s#2' % PILEUP_DATASET: ['T2_XX_SiteA', 'T2_XX_SiteC']})
        self.localQueue.updateLocationInfo()

        #FIXME: this test gives different results in jenkins and in private tests
        self.assertGreaterEqual(len(self.localQueue.getWork({'T2_XX_SiteA': 1}, {})), 0)

        self.assertGreaterEqual(len(self.localQueue.status()), 3)

    def testPileupOnProduction(self):
        """Test that we can split properly a Production workflow with pileup"""
        specfile = self.productionPileupSpec.specUrl()

        # Sanity check on queueWork only
        self.assertEqual(1, self.globalQueue.queueWork(specfile))
        self.assertEqual(1, len(self.globalQueue))
        self.assertEqual(len(self.globalQueue.backend.getActivePileupData()), 1)
        self.assertNotEqual(self.globalQueue.backend.getActivePileupData()[0]['dbs_url'], None)

    def testPrioritiesWorkPolling(self):
        """Test how the priorities and current jobs in the queue affect the workqueue behavior
           for acquiring and injecting work"""
        # Queue a low prio workflow and a high prio workflow
        highPrioReReco = self.setupHighPrioReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.globalQueue.queueWork(highPrioReReco.specUrl())

        # Pull all into local queue
        self.assertEqual(self.localQueue.pullWork({'T2_XX_SiteA': TOTAL_EVENTS}), 2 * NBLOCKS_HICOMM)
        syncQueues(self.localQueue)

        # Try pulling work into WMBS when "there is" a job of higher priority than the high prio workflow
        self.assertEqual(len(self.localQueue.getWork({'T2_XX_SiteA': 1},
                                                     {'T2_XX_SiteA': {highPrioReReco.priority() + 1: 1}})),
                         0)

        # Allow one more job slot
        self.assertEqual(len(self.localQueue.getWork({'T2_XX_SiteA': 2},
                                                     {'T2_XX_SiteA': {highPrioReReco.priority() + 1: 1}})),
                         1)

        # Allow 1 slot more and many slots occupied by low prio jobs
        self.assertEqual(len(self.localQueue.getWork({'T2_XX_SiteA': 2},
                                                     {'T2_XX_SiteA': {1: 50}})),
                         1)
        self.assertEqual(len(self.localQueue.backend.getElements(WorkflowName=highPrioReReco.name())),
                         NBLOCKS_HICOMM)

    def testMonitorWorkQueue(self):
        """
        Test several WorkQueue couch queries to monitor amount of work in the system
        """
        # Run some bootstrap, same code as in the test above...
        highPrioReReco = self.setupHighPrioReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
        processingSpec = self.setupReReco(assignArgs={'SiteWhitelist': ["T2_XX_SiteA", "T2_XX_SiteB"]})
        self.globalQueue.queueWork(processingSpec.specUrl())
        self.globalQueue.queueWork(highPrioReReco.specUrl())

        initialStatus = ['Available', 'Negotiating', 'Acquired']
        metrics = self.globalQueue.monitorWorkQueue(status=initialStatus)
        time.sleep(1)  # HACKY: query again to get the up-to-date views
        metrics = self.globalQueue.monitorWorkQueue(status=initialStatus)

        expectedMetrics = ('workByStatus', 'workByStatusAndPriority', 'workByAgentAndStatus',
                           'workByAgentAndPriority', 'uniqueJobsPerSiteAAA', 'possibleJobsPerSiteAAA',
                           'uniqueJobsPerSite', 'possibleJobsPerSite', 'total_query_time')
        self.assertItemsEqual(list(metrics), expectedMetrics)

        self.assertItemsEqual(list(metrics['workByStatus']), STATES)
        self.assertEqual(metrics['workByStatus']['Available']['sum_jobs'], 678)
        self.assertEqual(metrics['workByStatus']['Acquired'], {})

        self.assertItemsEqual(list(metrics['workByStatusAndPriority']), STATES)
        prios = [item['priority'] for item in metrics['workByStatusAndPriority']['Available']]
        self.assertItemsEqual(prios, [8000, 999998])
        self.assertEqual(metrics['workByStatusAndPriority']['Acquired'], [])

        self.assertEqual(len(metrics['workByAgentAndStatus']), 1)
        self.assertEqual(metrics['workByAgentAndStatus'][0]['agent_name'], 'AgentNotDefined')
        self.assertEqual(metrics['workByAgentAndStatus'][0]['status'], 'Available')

        self.assertEqual(len(metrics['workByAgentAndPriority']), 2)
        self.assertEqual(metrics['workByAgentAndPriority'][0]['agent_name'], 'AgentNotDefined')
        self.assertEqual([item['priority'] for item in metrics['workByAgentAndPriority']], [8000, 999998])

        for met in ('uniqueJobsPerSiteAAA', 'possibleJobsPerSiteAAA', 'uniqueJobsPerSite', 'possibleJobsPerSite'):
            self.assertItemsEqual(list(metrics[met]), initialStatus)
            self.assertEqual(len(metrics[met]['Available']), 2)
            self.assertEqual(len(metrics[met]['Acquired']), 0)
            self.assertItemsEqual(list(metrics[met]['Available']), ['T2_XX_SiteA', 'T2_XX_SiteB'])

        self.assertTrue(metrics['total_query_time'] >= 0)

        # Pull all into local queue (get them into Acquired status)
        self.localQueue.pullWork({'T2_XX_SiteA': 500})
        syncQueues(self.localQueue)
        metrics = self.globalQueue.monitorWorkQueue(status=initialStatus)
        time.sleep(1)  # HACKY: query again to get the up-to-date views
        metrics = self.globalQueue.monitorWorkQueue(status=initialStatus)

        self.assertTrue(metrics['workByStatus']['Available']['sum_jobs'] < 200)
        self.assertTrue(metrics['workByStatus']['Acquired']['sum_jobs'] >= 500)

        self.assertEqual(len(metrics['workByStatusAndPriority']['Available']), 1)
        self.assertEqual(len(metrics['workByStatusAndPriority']['Acquired']), 2)
        self.assertEqual(metrics['workByStatusAndPriority']['Available'][0]['priority'], 8000)
        prios = [item['priority'] for item in metrics['workByStatusAndPriority']['Acquired']]
        self.assertItemsEqual(prios, [8000, 999998])

        self.assertEqual(len(metrics['workByAgentAndStatus']), 2)
        for elem in metrics['workByAgentAndStatus']:
            if elem['status'] == 'Available':
                self.assertEqual(elem['agent_name'], 'AgentNotDefined')
            else:  # in Acquired
                self.assertTrue(elem['agent_name'] != 'AgentNotDefined')

        self.assertEqual(len(metrics['workByAgentAndPriority']), 3)
        prios = []
        for item in metrics['workByAgentAndPriority']:
            if item['agent_name'] != 'AgentNotDefined':
                prios.append(item['priority'])
        self.assertItemsEqual(prios, [8000, 999998])

        for met in ('uniqueJobsPerSiteAAA', 'possibleJobsPerSiteAAA', 'uniqueJobsPerSite', 'possibleJobsPerSite'):
            self.assertItemsEqual(list(metrics[met]), initialStatus)
            self.assertEqual(len(metrics[met]['Available']), 2)
            self.assertEqual(len(metrics[met]['Acquired']), 2)
            self.assertItemsEqual(list(metrics[met]['Available']), ['T2_XX_SiteA', 'T2_XX_SiteB'])
            self.assertItemsEqual(list(metrics[met]['Acquired']), ['T2_XX_SiteA', 'T2_XX_SiteB'])


if __name__ == "__main__":
    unittest.main()
