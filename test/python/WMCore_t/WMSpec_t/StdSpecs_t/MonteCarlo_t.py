#!/usr/bin/env python
"""
_MonteCarlo_t_

Unit tests for the Monte Carlo workflow.
"""
from __future__ import print_function

import os
import threading
import unittest
from hashlib import md5

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Mask import Mask
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp

TEST_DB_NAME = 'db_configcache_test'
DATA_PU = '/HighPileUp/Run2011A-v1/RAW'
COSMICS_PU = '/Cosmics/ComissioningHI-v1/RAW'


class MonteCarloTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        super(MonteCarloTest, self).setUp()

        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch(TEST_DB_NAME, "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)
        self.testInit.generateWorkDir()

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase(TEST_DB_NAME)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()

        super(MonteCarloTest, self).tearDown()

        return

    def injectMonteCarloConfig(self):
        """
        _injectMonteCarlo_

        Create a bogus config cache document for the montecarlo generation and
        inject it into couch.  Return the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": ["OutputA", "OutputB"],
                                                       "OutputA": {"dataset": {"filterName": "OutputAFilter",
                                                                               "dataTier": "RECO"}},
                                                       "OutputB": {"dataset": {"filterName": "OutputBFilter",
                                                                               "dataTier": "USER"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def _commonMonteCarloTest(self):
        """
        Retrieve the workload from WMBS and test all its properties.
        """
        goldenOutputMods = {"OutputA": "RECO", "OutputB": "USER"}

        prodWorkflow = Workflow(name="TestWorkload", task="/TestWorkload/Production")
        prodWorkflow.load()

        self.assertEqual(len(list(prodWorkflow.outputMap.keys())), 3,
                         "Error: Wrong number of WF outputs.")

        for goldenOutputMod, tier in list(goldenOutputMods.items()):
            fset = goldenOutputMod + tier
            mergedOutput = prodWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = prodWorkflow.outputMap[fset][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Production/ProductionMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Production/unmerged-%s" % (goldenOutputMod + tier),
                             "Error: Unmerged output fileset is wrong.")

        logArchOutput = prodWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = prodWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Production/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Production/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod, tier in list(goldenOutputMods.items()):
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Production/ProductionMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(list(mergeWorkflow.outputMap.keys())), 2,
                             "Error: Wrong number of WF outputs.")
            from pprint import pformat
            print(pformat(mergeWorkflow.outputMap))
            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Production/ProductionMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Production/ProductionMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Production/ProductionMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Production/ProductionMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Production-SomeBlock")
        topLevelFileset.loadData()

        prodSubscription = Subscription(fileset=topLevelFileset, workflow=prodWorkflow)
        prodSubscription.loadData()

        self.assertEqual(prodSubscription["type"], "Production",
                         "Error: Wrong subscription type.")
        self.assertEqual(prodSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algo.")

        for goldenOutputMod, tier in list(goldenOutputMods.items()):
            fset = goldenOutputMod + tier
            unmergedOutput = Fileset(name="/TestWorkload/Production/unmerged-%s" % fset)
            unmergedOutput.loadData()
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Production/ProductionMerge%s" % goldenOutputMod)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedOutput, workflow=mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])

        for goldenOutputMod, tier in list(goldenOutputMods.items()):
            fset = goldenOutputMod + tier
            unmerged = Fileset(name="/TestWorkload/Production/unmerged-%s" % fset)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Production/ProductionCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmerged, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        procLogCollect = Fileset(name="/TestWorkload/Production/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/Production/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        for goldenOutputMod in goldenOutputMods:
            mergeLogCollect = Fileset(
                name="/TestWorkload/Production/ProductionMerge%s/merged-logArchive" % goldenOutputMod)
            mergeLogCollect.loadData()
            mergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                               task="/TestWorkload/Production/ProductionMerge%s/Production%sMergeLogCollect" % (
                                                   goldenOutputMod, goldenOutputMod))
            mergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset=mergeLogCollect, workflow=mergeLogCollectWorkflow)
            logCollectSub.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")

    def testMonteCarlo(self):
        """
        _testMonteCarlo_

        Create a Monte Carlo workflow and verify that it is injected correctly
        into WMBS and invoke its detailed test.
        """
        defaultArguments = MonteCarloWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = TEST_DB_NAME
        defaultArguments["ConfigCacheID"] = self.injectMonteCarloConfig()

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._commonMonteCarloTest()

        return

    def testMonteCarloExtension(self):
        """
        _testMonteCarloExtension_

        Create a Monte Carlo workflow and verify that it is injected correctly
        into WMBS and invoke its detailed test. This uses a non-zero first
        lumi. Check that the splitting arguments are correctly
        set for the lfn counter.
        """
        defaultArguments = MonteCarloWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = TEST_DB_NAME
        defaultArguments["ConfigCacheID"] = self.injectMonteCarloConfig()
        defaultArguments["FirstLumi"] = 10001
        defaultArguments["EventsPerJob"] = 100
        defaultArguments["FirstEvent"] = 10001
        # defaultArguments["FirstEvent"] = 10001

        initial_lfn_counter = 100  # EventsPerJob == EventsPerLumi, then the number of previous jobs is equal to the number of the initial lumi

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._commonMonteCarloTest()

        productionTask = testWorkload.getTaskByPath('/TestWorkload/Production')
        productionSplitting = productionTask.jobSplittingParameters()
        self.assertTrue("initial_lfn_counter" in productionSplitting, "No initial lfn counter was stored")
        self.assertEqual(productionSplitting["initial_lfn_counter"], initial_lfn_counter, "Wrong initial LFN counter")

        for outputMod in ["OutputA", "OutputB"]:
            mergeTask = testWorkload.getTaskByPath('/TestWorkload/Production/ProductionMerge%s' % outputMod)
            mergeSplitting = mergeTask.jobSplittingParameters()
            self.assertTrue("initial_lfn_counter" in mergeSplitting, "No initial lfn counter was stored")
            self.assertEqual(mergeSplitting["initial_lfn_counter"], initial_lfn_counter, "Wrong initial LFN counter")

        return

    def testMCWithPileup(self):
        """
        _testMCWithPileup_

        Create a Monte Carlo workflow and verify that it is injected correctly
        into WMBS and invoke its detailed test.
        The input configuration includes pileup input files.
        """
        defaultArguments = MonteCarloWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = TEST_DB_NAME
        defaultArguments["ConfigCacheID"] = self.injectMonteCarloConfig()

        # Add pileup inputs
        defaultArguments["MCPileup"] = COSMICS_PU
        defaultArguments["DataPileup"] = DATA_PU
        defaultArguments["DeterministicPileup"] = True

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._commonMonteCarloTest()
        productionTask = testWorkload.getTaskByPath('/TestWorkload/Production')
        cmsRunStep = productionTask.getStep("cmsRun1").getTypeHelper()
        pileupData = cmsRunStep.getPileup()
        self.assertEqual(pileupData.data.dataset, [DATA_PU])
        self.assertEqual(pileupData.mc.dataset, [COSMICS_PU])

        splitting = productionTask.jobSplittingParameters()
        self.assertTrue(splitting["deterministicPileup"])
        return

    def testMCWithLHE(self):
        """
        _testMCWithLHE_

        Create a MonteCarlo workflow with a variation on the type of work
        done, this refers to the previous LHEStepZero where the input
        can be .lhe files and there is more than one lumi per job.
        """
        defaultArguments = MonteCarloWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = TEST_DB_NAME
        defaultArguments["ConfigCacheID"] = self.injectMonteCarloConfig()
        defaultArguments["LheInputFiles"] = "True"
        defaultArguments["EventsPerJob"] = 200
        defaultArguments["EventsPerLumi"] = 50

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._commonMonteCarloTest()

        productionTask = testWorkload.getTaskByPath('/TestWorkload/Production')
        splitting = productionTask.jobSplittingParameters()
        self.assertEqual(splitting["events_per_job"], 200)
        self.assertEqual(splitting["events_per_lumi"], 50)
        self.assertEqual(splitting["lheInputFiles"], True)
        self.assertFalse(splitting["deterministicPileup"])

        return

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        defaultArguments = MonteCarloWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = TEST_DB_NAME
        defaultArguments["ConfigCacheID"] = self.injectMonteCarloConfig()

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        # test default values
        taskObj = testWorkload.getTask('Production')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = taskObj.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = taskObj.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # now test case where args are provided
        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        taskObj = testWorkload.getTask('Production')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = taskObj.getStepHelper(step)
            if step == 'cmsRun1':
                self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = taskObj.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/Production',
                       '/TestWorkload/Production/ProductionMergeOutputB',
                       '/TestWorkload/Production/ProductionMergeOutputA']
        expWfTasks = ['/TestWorkload/Production',
                      '/TestWorkload/Production/LogCollect',
                      '/TestWorkload/Production/ProductionCleanupUnmergedOutputA',
                      '/TestWorkload/Production/ProductionCleanupUnmergedOutputB',
                      '/TestWorkload/Production/ProductionMergeOutputA',
                      '/TestWorkload/Production/ProductionMergeOutputA/ProductionOutputAMergeLogCollect',
                      '/TestWorkload/Production/ProductionMergeOutputB',
                      '/TestWorkload/Production/ProductionMergeOutputB/ProductionOutputBMergeLogCollect']
        expFsets = ['FILESET_DEFINED_DURING_RUNTIME',
                    '/TestWorkload/Production/unmerged-OutputBUSER',
                    '/TestWorkload/Production/ProductionMergeOutputA/merged-logArchive',
                    '/TestWorkload/Production/ProductionMergeOutputA/merged-MergedRECO',
                    '/TestWorkload/Production/ProductionMergeOutputB/merged-logArchive',
                    '/TestWorkload/Production/ProductionMergeOutputB/merged-MergedUSER',
                    '/TestWorkload/Production/unmerged-logArchive',
                    '/TestWorkload/Production/unmerged-OutputARECO']
        subMaps = ['FILESET_DEFINED_DURING_RUNTIME',
                   (6,
                    '/TestWorkload/Production/ProductionMergeOutputA/merged-logArchive',
                    '/TestWorkload/Production/ProductionMergeOutputA/ProductionOutputAMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (3,
                    '/TestWorkload/Production/ProductionMergeOutputB/merged-logArchive',
                    '/TestWorkload/Production/ProductionMergeOutputB/ProductionOutputBMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (8,
                    '/TestWorkload/Production/unmerged-logArchive',
                    '/TestWorkload/Production/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/Production/unmerged-OutputARECO',
                    '/TestWorkload/Production/ProductionCleanupUnmergedOutputA',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (5,
                    '/TestWorkload/Production/unmerged-OutputARECO',
                    '/TestWorkload/Production/ProductionMergeOutputA',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (4,
                    '/TestWorkload/Production/unmerged-OutputBUSER',
                    '/TestWorkload/Production/ProductionCleanupUnmergedOutputB',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (2,
                    '/TestWorkload/Production/unmerged-OutputBUSER',
                    '/TestWorkload/Production/ProductionMergeOutputB',
                    'ParentlessMergeBySize',
                    'Merge')]

        testArguments = MonteCarloWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = TEST_DB_NAME
        testArguments["ConfigCacheID"] = self.injectMonteCarloConfig()

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        myMask = Mask(FirstRun=1, FirstLumi=1, FirstEvent=1, LastRun=1, LastLumi=10, LastEvent=1000)
        testWMBSHelper = WMBSHelper(testWorkload, "Production", mask=myMask,
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # same function as in WMBSHelper, otherwise we cannot know which fileset name is
        maskString = ",".join(["%s=%s" % (x, myMask[x]) for x in sorted(myMask)])
        topFilesetName = 'TestWorkload-Production-%s' % md5(maskString).hexdigest()
        expFsets[0] = topFilesetName
        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps[0] = (1, topFilesetName, '/TestWorkload/Production', 'EventBased', 'Production')
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

        ### create another top level subscription
        myMask = Mask(FirstRun=1, FirstLumi=11, FirstEvent=1001, LastRun=1, LastLumi=20, LastEvent=2000)
        testWMBSHelper = WMBSHelper(testWorkload, "Production", mask=myMask,
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # same function as in WMBSHelper, otherwise we cannot know which fileset name is
        maskString = ",".join(["%s=%s" % (x, myMask[x]) for x in sorted(myMask)])
        topFilesetName = 'TestWorkload-Production-%s' % md5(maskString).hexdigest()
        expFsets.append(topFilesetName)
        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps.append((9, topFilesetName, '/TestWorkload/Production', 'EventBased', 'Production'))
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)


if __name__ == '__main__':
    unittest.main()
