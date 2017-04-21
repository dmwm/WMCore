#!/usr/bin/env python
"""
_MonteCarloFromGEN_t_

Unit tests for the MonteCarloFromGEN workflow.
"""

import unittest
import os

from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.MonteCarloFromGEN import MonteCarloFromGENWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase 
from WMQuality.TestInitCouchApp import TestInitCouchApp


class MonteCarloFromGENTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        super(MonteCarloFromGENTest, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("mclhe_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("mclhe_t")
        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        super(MonteCarloFromGENTest, self).tearDown()
        return

    def injectConfig(self):
        """
        _injectConfig_

        Create a bogus config cache document and inject it into couch.  Return
        the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": ["outputRECORECO", "outputALCARECOALCARECO"],
                                                       "outputRECORECO": {"dataset": {"filterName": "FilterRECO",
                                                                                      "dataTier": "RECO"}},
                                                       "outputALCARECOALCARECO": {
                                                           "dataset": {"filterName": "FilterALCARECO",
                                                                       "dataTier": "ALCARECO"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def testMonteCarloFromGEN(self):
        """
        _testMonteCarloFromGEN_

        Create a MonteCarloFromGEN workflow and verify it installs into WMBS
        correctly.
        """
        arguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        arguments["ConfigCacheID"] = self.injectConfig()
        arguments["CouchDBName"] = "mclhe_t"
        arguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", arguments)

        outputDatasets = testWorkload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 2)
        self.assertTrue("/WaitThisIsNotMinimumBias/FAKE-FilterRECO-FAKE-v1/RECO" in outputDatasets)
        self.assertTrue("/WaitThisIsNotMinimumBias/FAKE-FilterALCARECO-FAKE-v1/ALCARECO" in outputDatasets)

        productionTask = testWorkload.getTaskByPath('/TestWorkload/MonteCarloFromGEN')
        splitting = productionTask.jobSplittingParameters()
        self.assertFalse(splitting["deterministicPileup"])

        testWMBSHelper = WMBSHelper(testWorkload, "MonteCarloFromGEN", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        procWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/MonteCarloFromGEN")
        procWorkflow.load()

        self.assertEqual(len(procWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")
        self.assertEqual(procWorkflow.wfType, 'production')

        goldenOutputMods = ["outputRECORECO", "outputALCARECOALCARECO"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/MonteCarloFromGEN/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

        logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/MonteCarloFromGEN/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/MonteCarloFromGEN/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-MonteCarloFromGEN-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset=topLevelFileset, workflow=procWorkflow)
        procSubscription.loadData()

        self.assertEqual(procSubscription["type"], "Production",
                         "Error: Wrong subscription type: %s" % procSubscription["type"])
        self.assertEqual(procSubscription["split_algo"], "EventAwareLumiBased",
                         "Error: Wrong split algo.")

        unmergedReco = Fileset(name="/TestWorkload/MonteCarloFromGEN/unmerged-outputRECORECO")
        unmergedReco.loadData()
        recoMergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputRECORECO")
        recoMergeWorkflow.load()
        mergeSubscription = Subscription(fileset=unmergedReco, workflow=recoMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])

        unmergedAlca = Fileset(name="/TestWorkload/MonteCarloFromGEN/unmerged-outputALCARECOALCARECO")
        unmergedAlca.loadData()
        alcaMergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO")
        alcaMergeWorkflow.load()
        mergeSubscription = Subscription(fileset=unmergedAlca, workflow=alcaMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])

        for procOutput in ["outputRECORECO", "outputALCARECOALCARECO"]:
            unmerged = Fileset(name="/TestWorkload/MonteCarloFromGEN/unmerged-%s" % procOutput)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmerged%s" % procOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmerged, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        procLogCollect = Fileset(name="/TestWorkload/MonteCarloFromGEN/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/MonteCarloFromGEN/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(
            name="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputRECORECO/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputRECORECO/MonteCarloFromGENoutputRECORECOMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(
            name="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO/MonteCarloFromGENoutputALCARECOALCARECOMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

    def testMCFromGENWithPileup(self):
        """
        _testMonteCarloFromGEN_

        Create a MonteCarloFromGEN workflow and verify it installs into WMBS
        correctly.
        """
        arguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        arguments["ConfigCacheID"] = self.injectConfig()
        arguments["CouchDBName"] = "mclhe_t"
        arguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        # Add pileup inputs
        arguments["MCPileup"] = "/HighPileUp/Run2011A-v1/RAW"
        arguments["DataPileup"] = "/Cosmics/ComissioningHI-v1/RAW"
        arguments["DeterministicPileup"] = True

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", arguments)

        productionTask = testWorkload.getTaskByPath('/TestWorkload/MonteCarloFromGEN')
        cmsRunStep = productionTask.getStep("cmsRun1").getTypeHelper()
        pileupData = cmsRunStep.getPileup()
        self.assertEqual(pileupData.mc.dataset, [arguments["MCPileup"]])
        self.assertEqual(pileupData.data.dataset, [arguments["DataPileup"]])

        splitting = productionTask.jobSplittingParameters()
        self.assertTrue(splitting["deterministicPileup"])

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        defaultArguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        defaultArguments["ConfigCacheID"] = self.injectConfig()
        defaultArguments["CouchDBName"] = "mclhe_t"
        defaultArguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        # test default values
        taskObj = testWorkload.getTask('MonteCarloFromGEN')
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
        taskObj = testWorkload.getTask('MonteCarloFromGEN')
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


if __name__ == '__main__':
    unittest.main()
