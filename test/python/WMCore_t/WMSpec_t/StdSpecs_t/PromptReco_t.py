#!/usr/bin/env python
# pylint: disable=W0212
# Access to a protected member (_createSubscriptionsInWMBS)  of a client class
"""
_PromptReco_t_

Unit tests for the new Tier1 PromptReconstruction workflow.
"""
from __future__ import print_function
from future.utils import viewitems

import os
import threading
import unittest

from Utils.PythonVersion import PY3

from WMCore.Configuration import ConfigSection
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.PromptReco import PromptRecoWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp


class PromptRecoTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("promptreco_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("promptreco_t")
        self.testDir = self.testInit.generateWorkDir()

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")
        self.promptSkim = None
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

    def setupPromptSkimConfigObject(self):
        """
        _setupPromptSkimConfigObject_
        Creates a custom config object for testing
        of the skim functionality
        """
        self.promptSkim = ConfigSection(name="Tier1Skim")
        self.promptSkim.SkimName = "TestSkim1"
        self.promptSkim.DataTier = "RECO"
        self.promptSkim.TwoFileRead = False
        self.promptSkim.ProcessingVersion = "PromptSkim-v1"
        self.promptSkim.ConfigURL = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"

        # def testPromptReco(self):
        #     """
        #     _testPromptReco_

    #
    #        Create a Prompt Reconstruction workflow
    #        and verify it installs into WMBS correctly.
    #        """
    def testPromptRecoWithSkims(self):
        """
        _testT1PromptRecoWithSkim_

        Create a T1 Prompt Reconstruction workflow with PromptSkims
        and verify it installs into WMBS correctly.
        """
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["EnableHarvesting"] = True
        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/Reco")
        recoWorkflow.load()
        self.assertEqual(len(recoWorkflow.outputMap), len(testArguments["WriteTiers"]) + 1,
                         "Error: Wrong number of WF outputs in the Reco WF.")

        goldenOutputMods = {"write_RECO": "RECO", "write_ALCARECO": "ALCARECO", "write_AOD": "AOD", "write_DQM": "DQM"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = recoWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = recoWorkflow.outputMap[fset][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_ALCARECO":
                self.assertEqual(mergedOutput.name,
                                 "/TestWorkload/Reco/RecoMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = recoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = recoWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        alcaSkimWorkflow = Workflow(name="TestWorkload",
                                    task="/TestWorkload/Reco/AlcaSkim")
        alcaSkimWorkflow.load()
        self.assertEqual(len(alcaSkimWorkflow.outputMap), len(testArguments["AlcaSkims"]) + 1,
                         "Error: Wrong number of WF outputs in the AlcaSkim WF.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            fset = goldenOutputMod + "ALCARECO"
            mergedOutput = alcaSkimWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = alcaSkimWorkflow.outputMap[fset][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-MergedALCARECO" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-%sALCARECO" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        dqmWorkflow = Workflow(name="TestWorkload",
                               task="/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged")
        dqmWorkflow.load()

        logArchOutput = dqmWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = dqmWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = {"write_RECO": "RECO", "write_AOD": "AOD", "write_DQM": "DQM"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Reco/RecoMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Reco/RecoMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Reco/RecoMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap))

            mergedMergeOutput = mergeWorkflow.outputMap["MergedALCARECO"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["MergedALCARECO"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-MergedALCARECO" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-MergedALCARECO" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset=topLevelFileset, workflow=recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventAwareLumiBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaRecoFileset = Fileset(name="/TestWorkload/Reco/unmerged-write_ALCARECOALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset=alcaRecoFileset, workflow=alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedDQMFileset = Fileset(name="/TestWorkload/Reco/RecoMergewrite_DQM/merged-MergedDQM")
        mergedDQMFileset.loadData()

        dqmSubscription = Subscription(fileset=mergedDQMFileset, workflow=dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        unmergedOutputs = {"write_RECO": "RECO", "write_AOD": "AOD", "write_DQM": "DQM"}
        for unmergedOutput, tier in viewitems(unmergedOutputs):
            fset = unmergedOutput + tier
            unmergedDataTier = Fileset(name="/TestWorkload/Reco/unmerged-%s" % fset)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Reco/RecoMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedDataTier, workflow=dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])
        unmergedOutputs = []
        for alcaProd in testArguments["AlcaSkims"]:
            unmergedOutputs.append("ALCARECOStream%s" % alcaProd)
        for unmergedOutput in unmergedOutputs:
            unmergedAlcaSkim = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-%sALCARECO" % unmergedOutput)
            unmergedAlcaSkim.loadData()
            alcaSkimMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % unmergedOutput)
            alcaSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedAlcaSkim, workflow=alcaSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = {"write_RECO": "RECO", "write_AOD": "AOD", "write_DQM": "DQM"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            unmergedFileset = Fileset(name="/TestWorkload/Reco/unmerged-%s" % fset)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Reco/RecoCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-%sALCARECO" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        recoLogCollect = Fileset(name="/TestWorkload/Reco/unmerged-logArchive")
        recoLogCollect.loadData()
        recoLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/Reco/LogCollect")
        recoLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=recoLogCollect, workflow=recoLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        alcaSkimLogCollect = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-logArchive")
        alcaSkimLogCollect.loadData()
        alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                              task="/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect")
        alcaSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            recoMergeLogCollect = Fileset(name="/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                   task="/TestWorkload/Reco/RecoMerge%s/Reco%sMergeLogCollect" % (
                                                       goldenOutputMod, goldenOutputMod))
            recoMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=recoMergeLogCollect, workflow=recoMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            alcaSkimLogCollect = Fileset(
                name="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod)
            alcaSkimLogCollect.loadData()
            alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                                  task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/AlcaSkim%sMergeLogCollect" % (
                                                      goldenOutputMod, goldenOutputMod))
            alcaSkimLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        dqmHarvestLogCollect = Fileset(
            name="/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive")
        dqmHarvestLogCollect.loadData()
        dqmHarvestLogCollectWorkflow = Workflow(name="TestWorkload",
                                                task="/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect")
        dqmHarvestLogCollectWorkflow.load()

        logCollectSub = Subscription(fileset=dqmHarvestLogCollect, workflow=dqmHarvestLogCollectWorkflow)
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
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "promptreco_t"
        testArguments["EnableHarvesting"] = True

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test default values
        taskPaths = ['/TestWorkload/Reco', '/TestWorkload/Reco/AlcaSkim']
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # now test case where args are provided
        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                if task == '/TestWorkload/Reco' and step == 'cmsRun1':
                    self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
                elif step in ('stageOut1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should be single-core" % task)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        return

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/Reco',
                       '/TestWorkload/Reco/AlcaSkim',
                       '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                       '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                       '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                       '/TestWorkload/Reco/RecoMergewrite_AOD',
                       '/TestWorkload/Reco/RecoMergewrite_DQM',
                       '/TestWorkload/Reco/RecoMergewrite_RECO']
        expWfTasks = ['/TestWorkload/Reco',
                      '/TestWorkload/Reco/AlcaSkim',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamHcalCalHOCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamMuAlGlobalCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamTkAlCosmics0T',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/AlcaSkimALCARECOStreamHcalCalHOCosmicsMergeLogCollect',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/AlcaSkimALCARECOStreamMuAlGlobalCosmicsMergeLogCollect',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/AlcaSkimALCARECOStreamTkAlCosmics0TMergeLogCollect',
                      '/TestWorkload/Reco/LogCollect',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_ALCARECO',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_AOD',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_DQM',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_RECO',
                      '/TestWorkload/Reco/RecoMergewrite_AOD',
                      '/TestWorkload/Reco/RecoMergewrite_AOD/Recowrite_AODMergeLogCollect',
                      '/TestWorkload/Reco/RecoMergewrite_DQM',
                      '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged',
                      '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect',
                      '/TestWorkload/Reco/RecoMergewrite_DQM/Recowrite_DQMMergeLogCollect',
                      '/TestWorkload/Reco/RecoMergewrite_RECO',
                      '/TestWorkload/Reco/RecoMergewrite_RECO/Recowrite_RECOMergeLogCollect']
        expFsets = ['TestWorkload-Reco-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/merged-MergedALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamHcalCalHOCosmicsALCARECO',
                    '/TestWorkload/Reco/unmerged-write_ALCARECOALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/merged-MergedALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/merged-MergedALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamMuAlGlobalCosmicsALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamTkAlCosmics0TALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_AOD/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_AOD/merged-MergedAOD',
                    '/TestWorkload/Reco/unmerged-write_AODAOD',
                    '/TestWorkload/Reco/unmerged-write_DQMDQM',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-MergedDQM',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_RECO/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_RECO/merged-MergedRECO',
                    '/TestWorkload/Reco/unmerged-logArchive',
                    '/TestWorkload/Reco/unmerged-write_RECORECO']
        subMaps = [(5,
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/AlcaSkimALCARECOStreamHcalCalHOCosmicsMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (8,
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/AlcaSkimALCARECOStreamMuAlGlobalCosmicsMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (11,
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/AlcaSkimALCARECOStreamTkAlCosmics0TMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (3,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamHcalCalHOCosmicsALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamHcalCalHOCosmics',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (4,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamHcalCalHOCosmicsALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (6,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamMuAlGlobalCosmicsALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamMuAlGlobalCosmics',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (7,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamMuAlGlobalCosmicsALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (9,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamTkAlCosmics0TALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamTkAlCosmics0T',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (10,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamTkAlCosmics0TALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (12,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (16,
                    '/TestWorkload/Reco/RecoMergewrite_AOD/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_AOD/Recowrite_AODMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (21,
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/Recowrite_DQMMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (19,
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-MergedDQM',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged',
                    'Harvest',
                    'Harvesting'),
                   (20,
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (24,
                    '/TestWorkload/Reco/RecoMergewrite_RECO/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_RECO/Recowrite_RECOMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (25,
                    '/TestWorkload/Reco/unmerged-logArchive',
                    '/TestWorkload/Reco/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (2,
                    '/TestWorkload/Reco/unmerged-write_ALCARECOALCARECO',
                    '/TestWorkload/Reco/AlcaSkim',
                    'ParentlessMergeBySize',
                    'Processing'),
                   (13,
                    '/TestWorkload/Reco/unmerged-write_ALCARECOALCARECO',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_ALCARECO',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (14,
                    '/TestWorkload/Reco/unmerged-write_AODAOD',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_AOD',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (15,
                    '/TestWorkload/Reco/unmerged-write_AODAOD',
                    '/TestWorkload/Reco/RecoMergewrite_AOD',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (17,
                    '/TestWorkload/Reco/unmerged-write_DQMDQM',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_DQM',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (18,
                    '/TestWorkload/Reco/unmerged-write_DQMDQM',
                    '/TestWorkload/Reco/RecoMergewrite_DQM',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (22,
                    '/TestWorkload/Reco/unmerged-write_RECORECO',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_RECO',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (23,
                    '/TestWorkload/Reco/unmerged-write_RECORECO',
                    '/TestWorkload/Reco/RecoMergewrite_RECO',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (1,
                    'TestWorkload-Reco-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/Reco',
                    'EventAwareLumiBased',
                    'Processing')]

        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "promptreco_t"
        testArguments["EnableHarvesting"] = True

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", blockName=testArguments['InputDataset'],
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


    def testallowCreationFailureArgExists(self):
        """
        Test allowCreationFailure arguments exists.
        """
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["EnableHarvesting"] = True
        factory = PromptRecoWorkloadFactory()
        factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        factory.procJobSplitArgs["allowCreationFailure"] = False
        self.assertItemsEqual(factory.procJobSplitArgs, {'events_per_job': 500, 'allowCreationFailure': False, 'job_time_limit': 345600})

if __name__ == '__main__':
    unittest.main()
