#!/usr/bin/env python
"""
_ReReco_t_

Unit tests for the ReReco workflow.
"""
from __future__ import print_function
from future.utils import viewitems

import os
import threading
import unittest

from Utils.PythonVersion import PY3

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp



class ReRecoTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("rereco_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testDir = self.testInit.generateWorkDir()
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("rereco_t")

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")
        if PY3:
            self.assertItemsEqual = self.assertCountEqual
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def injectReRecoConfig(self):
        """
        _injectReRecoConfig_

        Inject a ReReco config document that we can use to set the outputModules
        """

        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": ['RECOoutput', 'DQMoutput'],
                                                       "RECOoutput": {'dataset': {'filterName': 'RECOoutputFilter',
                                                                                  'dataTier': 'RECO'}},
                                                       "DQMoutput": {'dataset': {'filterName': 'DQMoutputFilter',
                                                                                 'dataTier': 'DQM'}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def injectDQMHarvestConfig(self):
        """
        _injectDQMHarvest_

        Create a bogus config cache document for DQMHarvest and
        inject it into couch.  Return the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e234f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10876a7"
        newConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": []}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def injectSkimConfig(self):
        """
        _injectSkimConfig_

        Create a bogus config cache document for the skims and inject it into
        couch.  Return the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": ["SkimA", "SkimB"],
                                                       "SkimA": {"dataset": {"filterName": "SkimAFilter",
                                                                             "dataTier": "RAW-RECO"}},
                                                       "SkimB": {"dataset": {"filterName": "SkimBFilter",
                                                                             "dataTier": "USER"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def testReReco(self):
        """
        _testReReco_

        Verify that ReReco workflows can be created and inserted into WMBS
        correctly.
        """
        skimConfig = self.injectSkimConfig()
        recoConfig = self.injectReRecoConfig()
        dataProcArguments = ReRecoWorkloadFactory.getTestArguments()
        dataProcArguments["ProcessingString"] = "ProcString"
        dataProcArguments["ConfigCacheID"] = recoConfig
        dataProcArguments.update({"SkimName1": "SomeSkim",
                                  "SkimInput1": "RECOoutput",
                                  "Skim1ConfigCacheID": skimConfig})
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "rereco_t"
        dataProcArguments["EnableHarvesting"] = True
        dataProcArguments["DQMConfigCacheID"] = self.injectDQMHarvestConfig()

        factory = ReRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", dataProcArguments)

        self.assertEqual(testWorkload.data.tasks.DataProcessing.tree.children.DataProcessingMergeRECOoutput. \
                         tree.children.SomeSkim.tree.children.SomeSkimMergeSkimB.steps.cmsRun1.output.modules. \
                         Merged.mergedLFNBase,
                         '/store/data/FAKE/MinimumBias/USER/SkimBFilter-ProcString-v1')

        testWMBSHelper = WMBSHelper(testWorkload, "DataProcessing", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        procWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/DataProcessing")
        procWorkflow.load()

        self.assertEqual(len(procWorkflow.outputMap), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = {"RECOoutput": "RECO", "DQMoutput": "DQM"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = procWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = procWorkflow.outputMap[fset][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/DataProcessing/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong.")

        logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/DataProcessing/DataProcessingMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-DataProcessing-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset=topLevelFileset, workflow=procWorkflow)
        procSubscription.loadData()

        self.assertEqual(procSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "EventAwareLumiBased",
                         "Error: Wrong split algo.")

        unmergedReco = Fileset(name="/TestWorkload/DataProcessing/unmerged-RECOoutputRECO")
        unmergedReco.loadData()
        recoMergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput")
        recoMergeWorkflow.load()
        mergeSubscription = Subscription(fileset=unmergedReco, workflow=recoMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo.")

        unmergedDqm = Fileset(name="/TestWorkload/DataProcessing/unmerged-DQMoutputDQM")
        unmergedDqm.loadData()
        dqmMergeWorkflow = Workflow(name="TestWorkload",
                                    task="/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput")
        dqmMergeWorkflow.load()
        mergeSubscription = Subscription(fileset=unmergedDqm, workflow=dqmMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            unmerged = Fileset(name="/TestWorkload/DataProcessing/unmerged-%s" % fset)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/DataProcessing/DataProcessingCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmerged, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        procLogCollect = Fileset(name="/TestWorkload/DataProcessing/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/DataProcessing/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/DataProcessingRECOoutputMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/DataProcessingRECOoutputMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=procLogCollect, workflow=procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        skimWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim")
        skimWorkflow.load()

        self.assertEqual(len(skimWorkflow.outputMap), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = {"SkimA": "RAW-RECO", "SkimB": "USER"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = skimWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = skimWorkflow.outputMap[fset][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = skimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = skimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-MergedRECO")
        topLevelFileset.loadData()

        skimSubscription = Subscription(fileset=topLevelFileset, workflow=skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            unmerged = Fileset(
                name="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-%s" % fset)
            unmerged.loadData()
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmerged, workflow=mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            unmerged = Fileset(
                name="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-%s" % fset)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmerged, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        for skimOutput in goldenOutputMods:
            skimMergeLogCollect = Fileset(
                name="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-logArchive" % skimOutput)
            skimMergeLogCollect.loadData()
            skimMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                   task="/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/SomeSkim%sMergeLogCollect" % (
                                                       skimOutput, skimOutput))
            skimMergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset=skimMergeLogCollect, workflow=skimMergeLogCollectWorkflow)
            logCollectSub.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")

        dqmWorkflow = Workflow(name="TestWorkload",
                               task="/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged")
        dqmWorkflow.load()

        topLevelFileset = Fileset(name="/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/merged-MergedDQM")
        topLevelFileset.loadData()

        dqmSubscription = Subscription(fileset=topLevelFileset, workflow=dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        logArchOutput = dqmWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = dqmWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        dqmHarvestLogCollect = Fileset(
            name="/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive")
        dqmHarvestLogCollect.loadData()
        dqmHarvestLogCollectWorkflow = Workflow(name="TestWorkload",
                                                task="/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/DataProcessingMergeDQMoutputMergedEndOfRunDQMHarvestLogCollect")
        dqmHarvestLogCollectWorkflow.load()

        logCollectSub = Subscription(fileset=dqmHarvestLogCollect, workflow=dqmHarvestLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

    def testReRecoDroppingRECO(self):
        """
        _testReRecoDroppingRECO_

        Verify that ReReco workflows can be created and inserted into WMBS
        correctly.  The ReReco workflow is just a DataProcessing workflow with
        skims tacked on. This tests run on unmerged RECO output
        """
        skimConfig = self.injectSkimConfig()
        recoConfig = self.injectReRecoConfig()
        dataProcArguments = ReRecoWorkloadFactory.getTestArguments()
        dataProcArguments["ProcessingString"] = "ProcString"
        dataProcArguments["ConfigCacheID"] = recoConfig
        dataProcArguments.update({"SkimName1": "SomeSkim",
                                  "SkimInput1": "RECOoutput",
                                  "Skim1ConfigCacheID": skimConfig})
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "rereco_t"
        dataProcArguments["TransientOutputModules"] = ["RECOoutput"]
        dataProcArguments["EnableHarvesting"] = True
        dataProcArguments["DQMConfigCacheID"] = self.injectDQMHarvestConfig()

        factory = ReRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", dataProcArguments)

        self.assertEqual(testWorkload.data.tasks.DataProcessing.tree.children. \
                         SomeSkim.tree.children.SomeSkimMergeSkimB.steps.cmsRun1.output.modules. \
                         Merged.mergedLFNBase,
                         '/store/data/FAKE/MinimumBias/USER/SkimBFilter-ProcString-v1')

        testWMBSHelper = WMBSHelper(testWorkload, "DataProcessing", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        skimWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/DataProcessing/SomeSkim")
        skimWorkflow.load()

        self.assertEqual(len(skimWorkflow.outputMap), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = {"SkimA": "RAW-RECO", "SkimB": "USER"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = skimWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = skimWorkflow.outputMap[fset][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/DataProcessing/SomeSkim/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = skimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = skimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-Merged%s" % (
                             goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="/TestWorkload/DataProcessing/unmerged-RECOoutputRECO")
        topLevelFileset.loadData()

        skimSubscription = Subscription(fileset=topLevelFileset, workflow=skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        for skimOutput, tier in viewitems(goldenOutputMods):
            fset = skimOutput + tier
            unmerged = Fileset(name="/TestWorkload/DataProcessing/SomeSkim/unmerged-%s" % fset)
            unmerged.loadData()
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s" % skimOutput)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmerged, workflow=mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")

        for skimOutput, tier in viewitems(goldenOutputMods):
            fset = skimOutput + tier
            unmerged = Fileset(name="/TestWorkload/DataProcessing/SomeSkim/unmerged-%s" % fset)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/DataProcessing/SomeSkim/SomeSkimCleanupUnmerged%s" % skimOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmerged, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        for skimOutput in goldenOutputMods:
            skimMergeLogCollect = Fileset(
                name="/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-logArchive" % skimOutput)
            skimMergeLogCollect.loadData()
            skimMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                   task="/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/SomeSkim%sMergeLogCollect" % (
                                                       skimOutput, skimOutput))
            skimMergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset=skimMergeLogCollect, workflow=skimMergeLogCollectWorkflow)
            logCollectSub.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")

        return

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        skimConfig = self.injectSkimConfig()
        recoConfig = self.injectReRecoConfig()
        dataProcArguments = ReRecoWorkloadFactory.getTestArguments()
        dataProcArguments["ConfigCacheID"] = recoConfig
        dataProcArguments.update({"SkimName1": "SomeSkim",
                                  "SkimInput1": "RECOoutput",
                                  "Skim1ConfigCacheID": skimConfig})
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "rereco_t"
        dataProcArguments["EnableHarvesting"] = True
        dataProcArguments["DQMConfigCacheID"] = self.injectDQMHarvestConfig()

        factory = ReRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", dataProcArguments)

        # test default values
        taskPaths = {'/TestWorkload/DataProcessing': ['cmsRun1', 'stageOut1', 'logArch1'],
                     '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim': ['cmsRun1', 'stageOut1',
                                                                                             'logArch1'],
                     '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged': [
                         'cmsRun1', 'upload1', 'logArch1']}
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in taskPaths[task]:
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # FIXME: not sure whether we should set performance parameters to Harvest jobs?!?
            if task == '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged':
                continue
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # now test case where args are provided
        dataProcArguments["Multicore"] = 6
        dataProcArguments["Memory"] = 4600.0
        dataProcArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", dataProcArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in taskPaths[task]:
                stepHelper = taskObj.getStepHelper(step)
                if not task.endswith('DQMHarvestMerged') and step == 'cmsRun1':
                    self.assertEqual(stepHelper.getNumberOfCores(), dataProcArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), dataProcArguments["EventStreams"])
                elif step in ('stageOut1', 'upload1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should be single-core" % task)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # FIXME: not sure whether we should set performance parameters to Harvest jobs?!?
            if task == '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged':
                continue
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], dataProcArguments["Memory"])

        return

    def testFilesets(self):
        """
        _testFilesets_

        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/DataProcessing',
                       '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput',
                       '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim',
                       '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA',
                       '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB',
                       '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput']
        expWfTasks = ['/TestWorkload/DataProcessing',
                      '/TestWorkload/DataProcessing/DataProcessingCleanupUnmergedDQMoutput',
                      '/TestWorkload/DataProcessing/DataProcessingCleanupUnmergedRECOoutput',
                      '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput',
                      '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingDQMoutputMergeLogCollect',
                      '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged',
                      '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/DataProcessingMergeDQMoutputMergedEndOfRunDQMHarvestLogCollect',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/DataProcessingRECOoutputMergeLogCollect',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimCleanupUnmergedSkimA',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimCleanupUnmergedSkimB',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimLogCollect',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA/SomeSkimSkimAMergeLogCollect',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB',
                      '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB/SomeSkimSkimBMergeLogCollect',
                      '/TestWorkload/DataProcessing/LogCollect']
        expFsets = ['TestWorkload-DataProcessing-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/DataProcessing/unmerged-RECOoutputRECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-MergedRECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA/merged-MergedRAW-RECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB/merged-MergedUSER',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-SkimARAW-RECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-SkimBUSER',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/merged-MergedDQM',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-logArchive',
                    '/TestWorkload/DataProcessing/unmerged-DQMoutputDQM',
                    '/TestWorkload/DataProcessing/unmerged-logArchive']
        subMaps = [(5,
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/DataProcessingMergeDQMoutputMergedEndOfRunDQMHarvestLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (6,
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingDQMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (4,
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/merged-MergedDQM',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged',
                    'Harvest',
                    'Harvesting'),
                   (17,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/DataProcessingRECOoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (9,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-MergedRECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim',
                    'FileBased',
                    'Skim'),
                   (12,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA/SomeSkimSkimAMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (15,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB/merged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB/SomeSkimSkimBMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (16,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-logArchive',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (10,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-SkimARAW-RECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimCleanupUnmergedSkimA',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (11,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-SkimARAW-RECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimA',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (13,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-SkimBUSER',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimCleanupUnmergedSkimB',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (14,
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-SkimBUSER',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkimB',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (2,
                    '/TestWorkload/DataProcessing/unmerged-DQMoutputDQM',
                    '/TestWorkload/DataProcessing/DataProcessingCleanupUnmergedDQMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (3,
                    '/TestWorkload/DataProcessing/unmerged-DQMoutputDQM',
                    '/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (18,
                    '/TestWorkload/DataProcessing/unmerged-logArchive',
                    '/TestWorkload/DataProcessing/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/DataProcessing/unmerged-RECOoutputRECO',
                    '/TestWorkload/DataProcessing/DataProcessingCleanupUnmergedRECOoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (8,
                    '/TestWorkload/DataProcessing/unmerged-RECOoutputRECO',
                    '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (1,
                    'TestWorkload-DataProcessing-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/DataProcessing',
                    'EventAwareLumiBased',
                    'Processing')]

        testArguments = ReRecoWorkloadFactory.getTestArguments()
        testArguments["ConfigCacheID"] = self.injectReRecoConfig()
        testArguments.update({"SkimName1": "SomeSkim",
                              "SkimInput1": "RECOoutput",
                              "Skim1ConfigCacheID": self.injectSkimConfig()})
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "rereco_t"
        testArguments["EnableHarvesting"] = True
        testArguments["DQMConfigCacheID"] = self.injectDQMHarvestConfig()

        factory = ReRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "DataProcessing", blockName=testArguments['InputDataset'],
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)


    def testCampaignName(self):
        """
        Make sure the campaign name has been set in the processing task
        """
        skimConfig = self.injectSkimConfig()
        recoConfig = self.injectReRecoConfig()
        dataProcArguments = ReRecoWorkloadFactory.getTestArguments()
        dataProcArguments["ConfigCacheID"] = recoConfig
        dataProcArguments.update({"SkimName1": "SomeSkim",
                                  "SkimInput1": "RECOoutput",
                                  "Skim1ConfigCacheID": skimConfig})
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "rereco_t"
        dataProcArguments["EnableHarvesting"] = True
        dataProcArguments["DQMConfigCacheID"] = self.injectDQMHarvestConfig()

        factory = ReRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", dataProcArguments)

        # test default values
        taskPaths = {'/TestWorkload/DataProcessing': ['cmsRun1', 'stageOut1', 'logArch1'],
                     '/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim': ['cmsRun1', 'stageOut1',
                                                                                             'logArch1']}

        # Check task object has campaign name method
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            self.assertEqual(taskObj.getCampaignName(), '')

        return

if __name__ == '__main__':
    unittest.main()
