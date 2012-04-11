#!/usr/bin/env python
"""
_T1PromptReco_t_

Unit tests for the new T1 PromptReconstruction workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.Tier1PromptReco import getTestArguments, tier1promptrecoWorkload

from WMQuality.TestInit import TestInit

class DataProcessingTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        return

    def testT1PromptReco(self):
        """
        _testT1PromptReco_

        Create a T1 Prompt Reconstruction workflow and verify it installs into WMBS
        correctly.
        """

        alcaProducers = ['TkAlCosmics0T','MuAlGlobalCosmics','HcalCalHOCosmics','DtCalibCosmics']
        dataTiers = ['AOD', 'RECO', 'ALCARECO']

        testArguments = getTestArguments()
        testArguments['CMSSWVersion'] = 'CMSSW 5_2_0'
        testArguments['ProcessingVersion'] = 't1PromptReco-v1'
        testArguments['ProcScenario'] = 'cosmics'
        testArguments['GlobalTag'] = 'GR_P_V28::All'
        testArguments['InputDataset'] = '/Cosmics/Commissioning12-v1/RAW'
        testArguments['WriteTiers'] = dataTiers
        testArguments['AlcaSkims'] = alcaProducers


        testWorkload = tier1promptrecoWorkload("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        self.assertEqual(len(recoWorkflow.outputMap.keys()), len(dataTiers) + 1,
                         "Error: Wrong number of WF outputs in the Reco WF.")

        goldenOutputMods = ["write_RECO", "write_ALCARECO", "write_AOD"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_ALCARECO":
                self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = recoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = recoWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        alcaSkimWorkflow = Workflow(name = "TestWorkload",
                                    task = "/TestWorkload/Reco/AlcaSkim")
        alcaSkimWorkflow.load()
        self.assertEqual(len(alcaSkimWorkflow.outputMap.keys()), len(alcaProducers) + 1,
                        "Error: Wrong number of WF outputs in the AlcaSkim WF.")

        goldenOutputMods = []
        for alcaProd in alcaProducers:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergedOutput = alcaSkimWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = alcaSkimWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)

        logArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["write_RECO", "write_AOD"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/RecoMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = []
        for alcaProd in alcaProducers:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")


        topLevelFileset = Fileset(name = "TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset = topLevelFileset, workflow = recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        alcaRecoFileset = Fileset(name = "/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset = alcaRecoFileset, workflow = alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")


        unmergedOutputs = ["write_RECO", "write_AOD"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name = "/TestWorkload/Reco/unmerged-%s" % unmergedOutput)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name = "TestWorkload",
                                             task = "/TestWorkload/Reco/RecoMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedDataTier, workflow = dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm.")
        unmergedOutputs = []
        for alcaProd in alcaProducers:
            unmergedOutputs.append("ALCARECOStream%s" % alcaProd)
        for unmergedOutput in unmergedOutputs:
            unmergedAlcaSkim = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % unmergedOutput)
            unmergedAlcaSkim.loadData()
            alcaSkimMergeWorkflow = Workflow(name = "TestWorkload",
                                             task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % unmergedOutput)
            alcaSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedAlcaSkim, workflow = alcaSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_RECO", "write_AOD"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = []
        for alcaProd in alcaProducers:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmerged%s" %goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        recoLogCollect = Fileset(name = "/TestWorkload/Reco/unmerged-logArchive")
        recoLogCollect.loadData()
        recoLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/Reco/LogCollect")
        recoLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = recoLogCollect, workflow = recoLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        goldenOutputMods = ["write_RECO", "write_AOD"]
        for goldenOutputMod in goldenOutputMods:
            recoMergeLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoMerge%s/Reco%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            recoMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = recoMergeLogCollect, workflow = recoMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        goldenOutputMods = []
        for alcaProd in alcaProducers:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            alcaSkimLogCollect = Fileset(name = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod)
            alcaSkimLogCollect.loadData()
            alcaSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/AlcaSkim%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            alcaSkimLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = alcaSkimLogCollect, workflow = alcaSkimLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

if __name__ == '__main__':
    unittest.main()
