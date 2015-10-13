#!/usr/bin/env python
"""
_MonteCarlo_t_

Unit tests for the Monte Carlo workflow.
"""

import unittest
import os

from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WMQuality.TestInitCouchApp import TestInitCouchApp

TEST_DB_NAME = 'db_configcache_test'
class MonteCarloTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch(TEST_DB_NAME, "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.generateWorkDir()

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase(TEST_DB_NAME)
        EmulatorHelper.setEmulators(dbs = True)
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        EmulatorHelper.resetEmulators()
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
        prodWorkflow = Workflow(name = "TestWorkload", task = "/TestWorkload/Production")
        prodWorkflow.load()

        self.assertEqual(len(prodWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["OutputA", "OutputB"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = prodWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = prodWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/Production/ProductionMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Production/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

        logArchOutput = prodWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = prodWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Production/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Production/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Production/ProductionMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Production/ProductionMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Production/ProductionMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Production/ProductionMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Production/ProductionMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name = "TestWorkload-Production-SomeBlock")
        topLevelFileset.loadData()

        prodSubscription = Subscription(fileset = topLevelFileset, workflow = prodWorkflow)
        prodSubscription.loadData()

        self.assertEqual(prodSubscription["type"], "Production",
                         "Error: Wrong subscription type.")
        self.assertEqual(prodSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algo.")

        for outputName in ["OutputA", "OutputB"]:
            unmergedOutput = Fileset(name = "/TestWorkload/Production/unmerged-%s" % outputName)
            unmergedOutput.loadData()
            mergeWorkflow = Workflow(name = "TestWorkload",
                                            task = "/TestWorkload/Production/ProductionMerge%s" % outputName)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedOutput, workflow = mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])

        for outputName in ["OutputA", "OutputB"]:
            unmerged = Fileset(name = "/TestWorkload/Production/unmerged-%s" % outputName)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/Production/ProductionCleanupUnmerged%s" % outputName)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/Production/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/Production/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        for outputName in ["OutputA", "OutputB"]:
            mergeLogCollect = Fileset(name = "/TestWorkload/Production/ProductionMerge%s/merged-logArchive" % outputName)
            mergeLogCollect.loadData()
            mergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                              task = "/TestWorkload/Production/ProductionMerge%s/Production%sMergeLogCollect" % (outputName, outputName))
            mergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = mergeLogCollect, workflow = mergeLogCollectWorkflow)
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

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath = self.testInit.testDir)
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
        #defaultArguments["FirstEvent"] = 10001

        initial_lfn_counter = 100 # EventsPerJob == EventsPerLumi, then the number of previous jobs is equal to the number of the initial lumi

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath = self.testInit.testDir)
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
        defaultArguments["MCPileup"] = "/some/cosmics/dataset1"
        defaultArguments["DataPileup"] = "/some/minbias/dataset1"
        defaultArguments["DeterministicPileup"] = True

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath = self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._commonMonteCarloTest()
        productionTask = testWorkload.getTaskByPath('/TestWorkload/Production')
        cmsRunStep = productionTask.getStep("cmsRun1").getTypeHelper()
        pileupData = cmsRunStep.getPileup()
        self.assertEqual(pileupData.data.dataset, ["/some/minbias/dataset1"])
        self.assertEqual(pileupData.mc.dataset, ["/some/cosmics/dataset1"])

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

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath = self.testInit.testDir)
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

if __name__ == '__main__':
    unittest.main()
