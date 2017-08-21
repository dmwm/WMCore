#!/usr/bin/env python
"""
_ReDigi_t_

Unit tests for the ReDigi workflow.
"""
from __future__ import print_function

import os
import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.ReDigi import ReDigiWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.PhEDExClient.MockPhEDExApi import PILEUP_DATASET
from WMQuality.TestInitCouchApp import TestInitCouchApp


def injectReDigiConfigs(configDatabase, combinedStepOne=False):
    """
    _injectReDigiConfigs_

    Create bogus config cache documents for the various steps of the
    ReDigi workflow.  Return the IDs of the documents.
    """
    stepOneConfig = Document()
    stepOneConfig["info"] = None
    stepOneConfig["config"] = None
    stepOneConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
    stepOneConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
    stepOneConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    if combinedStepOne:
        stepOneConfig["pset_tweak_details"] = {"process": {"outputModules_": ["RECODEBUGoutput", "DQMoutput"],
                                                           "RECODEBUGoutput": {"dataset": {"filterName": "",
                                                                                           "dataTier": "RECO-DEBUG-OUTPUT"}},
                                                           "DQMoutput": {"dataset": {"filterName": "",
                                                                                     "dataTier": "DQM"}}}}
    else:
        stepOneConfig["pset_tweak_details"] = {"process": {"outputModules_": ["RAWDEBUGoutput"],
                                                           "RAWDEBUGoutput": {"dataset": {"filterName": "",
                                                                                          "dataTier": "RAW-DEBUG-OUTPUT"}}}}

    stepTwoConfig = Document()
    stepTwoConfig["info"] = None
    stepTwoConfig["config"] = None
    stepTwoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
    stepTwoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
    stepTwoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    stepTwoConfig["pset_tweak_details"] = {"process": {"outputModules_": ["RECODEBUGoutput", "DQMoutput"],
                                                       "RECODEBUGoutput": {"dataset": {"filterName": "",
                                                                                       "dataTier": "RECO-DEBUG-OUTPUT"}},
                                                       "DQMoutput": {"dataset": {"filterName": "",
                                                                                 "dataTier": "DQM"}}}}

    stepThreeConfig = Document()
    stepThreeConfig["info"] = None
    stepThreeConfig["config"] = None
    stepThreeConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
    stepThreeConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
    stepThreeConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    stepThreeConfig["pset_tweak_details"] = {"process": {"outputModules_": ["aodOutputModule"],
                                                         "aodOutputModule": {"dataset": {"filterName": "",
                                                                                         "dataTier": "AODSIM"}}}}
    stepOne = configDatabase.commitOne(stepOneConfig)[0]["id"]
    stepTwo = configDatabase.commitOne(stepTwoConfig)[0]["id"]
    stepThree = configDatabase.commitOne(stepThreeConfig)[0]["id"]
    return (stepOne, stepTwo, stepThree)


class ReDigiTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        super(ReDigiTest, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("redigi_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testInit.generateWorkDir()

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("redigi_t")

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
        super(ReDigiTest, self).tearDown()
        return

    def testDependentReDigi(self):
        """
        _testDependentReDigi_

        Verfiy that a dependent ReDigi workflow that keeps stages out
        RAW data is created and installed into WMBS correctly.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        topLevelFileset = Fileset(name="TestWorkload-StepOneProc-SomeBlock")
        topLevelFileset.loadData()

        stepOneUnmergedRAWFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-RAWDEBUGoutputRAW-DEBUG-OUTPUT")
        stepOneUnmergedRAWFileset.loadData()
        stepOneMergedRAWFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/merged-MergedRAW-DEBUG-OUTPUT")
        stepOneMergedRAWFileset.loadData()
        stepOneLogArchiveFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-logArchive")
        stepOneLogArchiveFileset.loadData()
        stepOneMergeLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/merged-logArchive")
        stepOneMergeLogArchiveFileset.loadData()

        stepTwoUnmergedDQMFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/unmerged-DQMoutputDQM")
        stepTwoUnmergedDQMFileset.loadData()
        stepTwoUnmergedRECOFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/unmerged-RECODEBUGoutputRECO-DEBUG-OUTPUT")
        stepTwoUnmergedRECOFileset.loadData()
        stepTwoMergedDQMFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeDQMoutput/merged-MergedDQM")
        stepTwoMergedDQMFileset.loadData()
        stepTwoMergedRECOFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/merged-MergedRECO-DEBUG-OUTPUT")
        stepTwoMergedRECOFileset.loadData()
        stepTwoLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/unmerged-logArchive")
        stepTwoLogArchiveFileset.loadData()
        stepTwoMergeDQMLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeDQMoutput/merged-logArchive")
        stepTwoMergeDQMLogArchiveFileset.loadData()
        stepTwoMergeRECOLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/merged-logArchive")
        stepTwoMergeRECOLogArchiveFileset.loadData()

        stepThreeUnmergedAODFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/unmerged-aodOutputModuleAODSIM")
        stepThreeUnmergedAODFileset.loadData()
        stepThreeMergedAODFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcMergeaodOutputModule/merged-MergedAODSIM")
        stepThreeMergedAODFileset.loadData()
        stepThreeLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/unmerged-logArchive")
        stepThreeLogArchiveFileset.loadData()

        stepThreeMergeLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcMergeaodOutputModule/merged-logArchive")
        stepThreeMergeLogArchiveFileset.loadData()

        stepOneWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                   task="/TestWorkload/StepOneProc")
        stepOneWorkflow.load()
        self.assertEqual(stepOneWorkflow.wfType, 'reprocessing')
        self.assertTrue("logArchive" in list(stepOneWorkflow.outputMap.keys()),
                        "Error: Step one missing output module.")
        self.assertTrue("RAWDEBUGoutputRAW-DEBUG-OUTPUT" in list(stepOneWorkflow.outputMap.keys()),
                        "Error: Step one missing output module.")
        self.assertEqual(stepOneWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepOneLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepOneLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneWorkflow.outputMap["RAWDEBUGoutputRAW-DEBUG-OUTPUT"][0]["merged_output_fileset"].id,
                         stepOneMergedRAWFileset.id,
                         "Error: RAWDEBUG output fileset is wrong.")
        self.assertEqual(stepOneWorkflow.outputMap["RAWDEBUGoutputRAW-DEBUG-OUTPUT"][0]["output_fileset"].id,
                         stepOneUnmergedRAWFileset.id,
                         "Error: RAWDEBUG output fileset is wrong.")

        for outputMod in list(stepOneWorkflow.outputMap.keys()):
            self.assertTrue(len(stepOneWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepOneSub = Subscription(workflow=stepOneWorkflow, fileset=topLevelFileset)
        stepOneSub.loadData()
        self.assertEqual(stepOneSub["type"], "Processing",
                         "Error: Step one sub has wrong type.")

        stepOneCleanupWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                          task="/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedRAWDEBUGoutput")
        stepOneCleanupWorkflow.load()
        self.assertEqual(len(list(stepOneCleanupWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup should have no output.")
        stepOneCleanupSub = Subscription(workflow=stepOneCleanupWorkflow, fileset=stepOneUnmergedRAWFileset)
        stepOneCleanupSub.loadData()
        self.assertEqual(stepOneCleanupSub["type"], "Cleanup",
                         "Error: Step one sub has wrong type.")

        stepOneLogCollectWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/LogCollect")
        stepOneLogCollectWorkflow.load()
        self.assertEqual(len(list(stepOneLogCollectWorkflow.outputMap.keys())), 0,
                         "Error: LogCollect should have no output.")
        stepOneLogCollectSub = Subscription(workflow=stepOneLogCollectWorkflow, fileset=stepOneLogArchiveFileset)
        stepOneLogCollectSub.loadData()
        self.assertEqual(stepOneLogCollectSub["type"], "LogCollect",
                         "Error: Step one sub has wrong type.")

        stepOneMergeWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                        task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput")
        stepOneMergeWorkflow.load()
        self.assertTrue("MergedRAW-DEBUG-OUTPUT" in list(stepOneMergeWorkflow.outputMap.keys()),
                        "Error: Step one merge missing output module.")
        self.assertTrue("logArchive" in list(stepOneMergeWorkflow.outputMap.keys()),
                        "Error: Step one merge missing output module.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepOneMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepOneMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["MergedRAW-DEBUG-OUTPUT"][0]["merged_output_fileset"].id,
                         stepOneMergedRAWFileset.id,
                         "Error: RAWDEBUG merge output fileset is wrong.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["MergedRAW-DEBUG-OUTPUT"][0]["output_fileset"].id,
                         stepOneMergedRAWFileset.id,
                         "Error: RAWDEBUG merge output fileset is wrong.")
        for outputMod in list(stepOneMergeWorkflow.outputMap.keys()):
            self.assertTrue(len(stepOneMergeWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")
        stepOneMergeSub = Subscription(workflow=stepOneMergeWorkflow, fileset=stepOneUnmergedRAWFileset)
        stepOneMergeSub.loadData()
        self.assertEqual(stepOneMergeSub["type"], "Merge",
                         "Error: Step one sub has wrong type.")

        stepTwoWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                   task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc")
        stepTwoWorkflow.load()
        self.assertTrue("RECODEBUGoutputRECO-DEBUG-OUTPUT" in list(stepTwoWorkflow.outputMap.keys()),
                        "Error: Step two missing output module.")
        self.assertTrue("DQMoutputDQM" in list(stepTwoWorkflow.outputMap.keys()),
                        "Error: Step two missing output module.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutputRECO-DEBUG-OUTPUT"][0]["merged_output_fileset"].id,
                         stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutputRECO-DEBUG-OUTPUT"][0]["output_fileset"].id,
                         stepTwoUnmergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutputDQM"][0]["merged_output_fileset"].id,
                         stepTwoMergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutputDQM"][0]["output_fileset"].id,
                         stepTwoUnmergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        stepTwoSub = Subscription(workflow=stepTwoWorkflow, fileset=stepOneMergedRAWFileset)
        stepTwoSub.loadData()
        self.assertEqual(stepTwoSub["type"], "Processing",
                         "Error: Step two sub has wrong type.")

        for outputMod in list(stepTwoWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoCleanupDQMWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcCleanupUnmergedDQMoutput")
        stepTwoCleanupDQMWorkflow.load()
        self.assertEqual(len(list(stepTwoCleanupDQMWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupDQMSub = Subscription(workflow=stepTwoCleanupDQMWorkflow, fileset=stepTwoUnmergedDQMFileset)
        stepTwoCleanupDQMSub.loadData()
        self.assertEqual(stepTwoCleanupDQMSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoCleanupRECOWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                              task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcCleanupUnmergedRECODEBUGoutput")
        stepTwoCleanupRECOWorkflow.load()
        self.assertEqual(len(list(stepTwoCleanupRECOWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupRECOSub = Subscription(workflow=stepTwoCleanupRECOWorkflow, fileset=stepTwoUnmergedRECOFileset)
        stepTwoCleanupRECOSub.loadData()
        self.assertEqual(stepTwoCleanupRECOSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoLogCollectWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcLogCollect")
        stepTwoLogCollectWorkflow.load()
        self.assertEqual(len(list(stepTwoLogCollectWorkflow.outputMap.keys())), 0,
                         "Error: LogCollect shouldn't have any output.")
        stepTwoLogCollectSub = Subscription(workflow=stepTwoLogCollectWorkflow, fileset=stepTwoLogArchiveFileset)
        stepTwoLogCollectSub.loadData()
        self.assertEqual(stepTwoLogCollectSub["type"], "LogCollect",
                         "Error: Step two sub has wrong type.")

        stepTwoMergeRECOWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                            task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput")
        stepTwoMergeRECOWorkflow.load()
        self.assertTrue("MergedRECO-DEBUG-OUTPUT" in list(stepTwoMergeRECOWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in list(stepTwoMergeRECOWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["MergedRECO-DEBUG-OUTPUT"][0]["merged_output_fileset"].id,
                         stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["MergedRECO-DEBUG-OUTPUT"][0]["output_fileset"].id,
                         stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        stepTwoMergeRECOSub = Subscription(workflow=stepTwoMergeRECOWorkflow, fileset=stepTwoUnmergedRECOFileset)
        stepTwoMergeRECOSub.loadData()
        self.assertEqual(stepTwoMergeRECOSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in list(stepTwoMergeRECOWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoMergeRECOWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoMergeDQMWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                           task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeDQMoutput")
        stepTwoMergeDQMWorkflow.load()
        self.assertTrue("MergedDQM" in list(stepTwoMergeDQMWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in list(stepTwoMergeDQMWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["MergedDQM"][0]["merged_output_fileset"].id,
                         stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["MergedDQM"][0]["output_fileset"].id,
                         stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        stepTwoMergeDQMSub = Subscription(workflow=stepTwoMergeDQMWorkflow, fileset=stepTwoUnmergedDQMFileset)
        stepTwoMergeDQMSub.loadData()
        self.assertEqual(stepTwoMergeDQMSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in list(stepTwoMergeDQMWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoMergeDQMWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepThreeWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                     task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc")
        stepThreeWorkflow.load()
        self.assertTrue("aodOutputModuleAODSIM" in list(stepThreeWorkflow.outputMap.keys()),
                        "Error: Step three missing output module.")
        self.assertTrue("logArchive" in list(stepThreeWorkflow.outputMap.keys()),
                        "Error: Step three missing output module.")
        self.assertEqual(stepThreeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepThreeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepThreeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeWorkflow.outputMap["aodOutputModuleAODSIM"][0]["merged_output_fileset"].id,
                         stepThreeMergedAODFileset.id,
                         "Error: aodOutputModuleAODSIM output fileset is wrong.")
        self.assertEqual(stepThreeWorkflow.outputMap["aodOutputModuleAODSIM"][0]["output_fileset"].id,
                         stepThreeUnmergedAODFileset.id,
                         "Error: aodOutputModuleAODSIM output fileset is wrong.")
        stepThreeSub = Subscription(workflow=stepThreeWorkflow, fileset=stepTwoMergedRECOFileset)
        stepThreeSub.loadData()
        self.assertEqual(stepThreeSub["type"], "Processing",
                         "Error: Step three sub has wrong type.")
        for outputMod in list(stepThreeWorkflow.outputMap.keys()):
            self.assertTrue(len(stepThreeWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepThreeCleanupWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                            task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcCleanupUnmergedaodOutputModule")
        stepThreeCleanupWorkflow.load()
        self.assertEqual(len(list(stepThreeCleanupWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup should have no output.")
        stepThreeCleanupSub = Subscription(workflow=stepThreeCleanupWorkflow, fileset=stepThreeUnmergedAODFileset)
        stepThreeCleanupSub.loadData()
        self.assertEqual(stepThreeCleanupSub["type"], "Cleanup",
                         "Error: Step three sub has wrong type.")

        stepThreeLogCollectWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                               task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcLogCollect")
        stepThreeLogCollectWorkflow.load()
        self.assertEqual(len(list(stepThreeLogCollectWorkflow.outputMap.keys())), 0,
                         "Error: LogCollect should have no output.")
        stepThreeLogCollectSub = Subscription(workflow=stepThreeLogCollectWorkflow, fileset=stepThreeLogArchiveFileset)
        stepThreeLogCollectSub.loadData()
        self.assertEqual(stepThreeLogCollectSub["type"], "LogCollect",
                         "Error: Step three sub has wrong type.")

        stepThreeMergeWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                          task="/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcMergeaodOutputModule")
        stepThreeMergeWorkflow.load()
        self.assertTrue("MergedAODSIM" in list(stepThreeMergeWorkflow.outputMap.keys()),
                        "Error: Step three merge missing output module.")
        self.assertTrue("logArchive" in list(stepThreeMergeWorkflow.outputMap.keys()),
                        "Error: Step three merge missing output module.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepThreeMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepThreeMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["MergedAODSIM"][0]["merged_output_fileset"].id,
                         stepThreeMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["MergedAODSIM"][0]["output_fileset"].id,
                         stepThreeMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        stepThreeMergeSub = Subscription(workflow=stepThreeMergeWorkflow, fileset=stepThreeUnmergedAODFileset)
        stepThreeMergeSub.loadData()
        self.assertEqual(stepThreeMergeSub["type"], "Merge",
                         "Error: Step three sub has wrong type.")
        for outputMod in list(stepThreeMergeWorkflow.outputMap.keys()):
            self.assertTrue(len(stepThreeMergeWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        return

    def verifyDiscardRAW(self):
        """
        _verifyDiscardRAW_

        Verify that a workflow that discards the RAW was installed into WMBS
        correctly.
        """
        topLevelFileset = Fileset(name="TestWorkload-StepOneProc-SomeBlock")
        topLevelFileset.loadData()

        stepTwoUnmergedDQMFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-DQMoutputDQM")
        stepTwoUnmergedDQMFileset.loadData()
        stepTwoUnmergedRECOFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-RECODEBUGoutputRECO-DEBUG-OUTPUT")
        stepTwoUnmergedRECOFileset.loadData()
        stepTwoMergedDQMFileset = Fileset(name="/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-MergedDQM")
        stepTwoMergedDQMFileset.loadData()
        stepTwoMergedRECOFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-MergedRECO-DEBUG-OUTPUT")
        stepTwoMergedRECOFileset.loadData()
        stepTwoLogArchiveFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-logArchive")
        stepTwoLogArchiveFileset.loadData()
        stepTwoMergeDQMLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-logArchive")
        stepTwoMergeDQMLogArchiveFileset.loadData()
        stepTwoMergeRECOLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-logArchive")
        stepTwoMergeRECOLogArchiveFileset.loadData()

        stepTwoWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                   task="/TestWorkload/StepOneProc")
        stepTwoWorkflow.load()
        self.assertTrue("RECODEBUGoutputRECO-DEBUG-OUTPUT" in list(stepTwoWorkflow.outputMap.keys()),
                        "Error: Step two missing output module.")
        self.assertTrue("DQMoutputDQM" in list(stepTwoWorkflow.outputMap.keys()),
                        "Error: Step two missing output module.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutputRECO-DEBUG-OUTPUT"][0]["merged_output_fileset"].id,
                         stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutputRECO-DEBUG-OUTPUT"][0]["output_fileset"].id,
                         stepTwoUnmergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutputDQM"][0]["merged_output_fileset"].id,
                         stepTwoMergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutputDQM"][0]["output_fileset"].id,
                         stepTwoUnmergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        stepTwoSub = Subscription(workflow=stepTwoWorkflow, fileset=topLevelFileset)
        stepTwoSub.loadData()
        self.assertEqual(stepTwoSub["type"], "Processing",
                         "Error: Step two sub has wrong type.")

        for outputMod in list(stepTwoWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoCleanupDQMWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedDQMoutput")
        stepTwoCleanupDQMWorkflow.load()
        self.assertEqual(len(list(stepTwoCleanupDQMWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupDQMSub = Subscription(workflow=stepTwoCleanupDQMWorkflow, fileset=stepTwoUnmergedDQMFileset)
        stepTwoCleanupDQMSub.loadData()
        self.assertEqual(stepTwoCleanupDQMSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoCleanupRECOWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                              task="/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedRECODEBUGoutput")
        stepTwoCleanupRECOWorkflow.load()
        self.assertEqual(len(list(stepTwoCleanupRECOWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupRECOSub = Subscription(workflow=stepTwoCleanupRECOWorkflow, fileset=stepTwoUnmergedRECOFileset)
        stepTwoCleanupRECOSub.loadData()
        self.assertEqual(stepTwoCleanupRECOSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoLogCollectWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/LogCollect")
        stepTwoLogCollectWorkflow.load()
        self.assertEqual(len(list(stepTwoLogCollectWorkflow.outputMap.keys())), 0,
                         "Error: LogCollect shouldn't have any output.")
        stepTwoLogCollectSub = Subscription(workflow=stepTwoLogCollectWorkflow, fileset=stepTwoLogArchiveFileset)
        stepTwoLogCollectSub.loadData()
        self.assertEqual(stepTwoLogCollectSub["type"], "LogCollect",
                         "Error: Step two sub has wrong type.")

        stepTwoMergeRECOWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                            task="/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput")
        stepTwoMergeRECOWorkflow.load()
        self.assertTrue("MergedRECO-DEBUG-OUTPUT" in list(stepTwoMergeRECOWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in list(stepTwoMergeRECOWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["MergedRECO-DEBUG-OUTPUT"][0]["merged_output_fileset"].id,
                         stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["MergedRECO-DEBUG-OUTPUT"][0]["output_fileset"].id,
                         stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        stepTwoMergeRECOSub = Subscription(workflow=stepTwoMergeRECOWorkflow, fileset=stepTwoUnmergedRECOFileset)
        stepTwoMergeRECOSub.loadData()
        self.assertEqual(stepTwoMergeRECOSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in list(stepTwoMergeRECOWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoMergeRECOWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoMergeDQMWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                           task="/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput")
        stepTwoMergeDQMWorkflow.load()
        self.assertTrue("MergedDQM" in list(stepTwoMergeDQMWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in list(stepTwoMergeDQMWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["MergedDQM"][0]["merged_output_fileset"].id,
                         stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["MergedDQM"][0]["output_fileset"].id,
                         stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        stepTwoMergeDQMSub = Subscription(workflow=stepTwoMergeDQMWorkflow, fileset=stepTwoUnmergedDQMFileset)
        stepTwoMergeDQMSub.loadData()
        self.assertEqual(stepTwoMergeDQMSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in list(stepTwoMergeDQMWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoMergeDQMWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")
        return

    def verifyKeepAOD(self):
        """
        _verifyKeepAOD_

        Verify that a workflow that only produces AOD in a single step was
        installed correctly into WMBS.
        """
        topLevelFileset = Fileset(name="TestWorkload-StepOneProc-SomeBlock")
        topLevelFileset.loadData()

        stepTwoUnmergedAODFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-aodOutputModuleAODSIM")
        stepTwoUnmergedAODFileset.loadData()
        stepTwoMergedAODFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-MergedAODSIM")
        stepTwoMergedAODFileset.loadData()
        stepTwoLogArchiveFileset = Fileset(name="/TestWorkload/StepOneProc/unmerged-logArchive")
        stepTwoLogArchiveFileset.loadData()
        stepTwoMergeAODLogArchiveFileset = Fileset(
            name="/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-logArchive")
        stepTwoMergeAODLogArchiveFileset.loadData()

        stepTwoWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                   task="/TestWorkload/StepOneProc")
        stepTwoWorkflow.load()
        self.assertTrue("aodOutputModuleAODSIM" in list(stepTwoWorkflow.outputMap.keys()),
                        "Error: Step two missing output module.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["aodOutputModuleAODSIM"][0]["merged_output_fileset"].id,
                         stepTwoMergedAODFileset.id,
                         "Error: AOD output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["aodOutputModuleAODSIM"][0]["output_fileset"].id,
                         stepTwoUnmergedAODFileset.id,
                         "Error: AOD output fileset is wrong.")
        stepTwoSub = Subscription(workflow=stepTwoWorkflow, fileset=topLevelFileset)
        stepTwoSub.loadData()
        self.assertEqual(stepTwoSub["type"], "Processing",
                         "Error: Step two sub has wrong type.")

        for outputMod in list(stepTwoWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoCleanupAODWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedaodOutputModule")
        stepTwoCleanupAODWorkflow.load()
        self.assertEqual(len(list(stepTwoCleanupAODWorkflow.outputMap.keys())), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupAODSub = Subscription(workflow=stepTwoCleanupAODWorkflow, fileset=stepTwoUnmergedAODFileset)
        stepTwoCleanupAODSub.loadData()
        self.assertEqual(stepTwoCleanupAODSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoLogCollectWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                             task="/TestWorkload/StepOneProc/LogCollect")
        stepTwoLogCollectWorkflow.load()
        self.assertEqual(len(list(stepTwoLogCollectWorkflow.outputMap.keys())), 0,
                         "Error: LogCollect shouldn't have any output.")
        stepTwoLogCollectSub = Subscription(workflow=stepTwoLogCollectWorkflow, fileset=stepTwoLogArchiveFileset)
        stepTwoLogCollectSub.loadData()
        self.assertEqual(stepTwoLogCollectSub["type"], "LogCollect",
                         "Error: Step two sub has wrong type.")

        stepTwoMergeAODWorkflow = Workflow(spec="somespec", name="TestWorkload",
                                           task="/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule")
        stepTwoMergeAODWorkflow.load()
        self.assertTrue("MergedAODSIM" in list(stepTwoMergeAODWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in list(stepTwoMergeAODWorkflow.outputMap.keys()),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id,
                         stepTwoMergeAODLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["logArchive"][0]["output_fileset"].id,
                         stepTwoMergeAODLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["MergedAODSIM"][0]["merged_output_fileset"].id,
                         stepTwoMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["MergedAODSIM"][0]["output_fileset"].id,
                         stepTwoMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        stepTwoMergeAODSub = Subscription(workflow=stepTwoMergeAODWorkflow, fileset=stepTwoUnmergedAODFileset)
        stepTwoMergeAODSub.loadData()
        self.assertEqual(stepTwoMergeAODSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in list(stepTwoMergeAODWorkflow.outputMap.keys()):
            self.assertTrue(len(stepTwoMergeAODWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")
        return

    def testChainedReDigi(self):
        """
        _testChaninedReDigi_

        Verify that a chained ReDigi workflow that discards RAW data can be
        created and installed into WMBS correctly.  This will only verify the
        step one/step two information in WMBS as the step three information is
        the same as the dependent workflow.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"
        defaultArguments["MCPileup"] = PILEUP_DATASET
        defaultArguments["KeepStepOneOutput"] = False

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        # Verify that pileup is configured for both of the cmsRun steps in the
        # top level task.
        topLevelTask = testWorkload.getTopLevelTask()[0]
        cmsRun1Helper = topLevelTask.getStepHelper("cmsRun1")
        cmsRun2Helper = topLevelTask.getStepHelper("cmsRun2")
        cmsRun1PileupConfig = cmsRun1Helper.getPileup()
        cmsRun2PileupConfig = cmsRun2Helper.getPileup()

        self.assertTrue(cmsRun1PileupConfig.mc.dataset, "/some/cosmics/dataset")
        self.assertTrue(cmsRun2PileupConfig.mc.dataset, "/some/cosmics/dataset")

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.verifyDiscardRAW()
        return

    def testThreeStepChainedReDigi(self):
        """
        _testThreeStepChaninedReDigi_

        Verify that a chained ReDigi workflow that discards RAW and RECO data
        can be created and installed into WMBS correctly.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["KeepStepOneOutput"] = False
        defaultArguments["KeepStepTwoOutput"] = False
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        self.assertTrue(len(testWorkload.getTopLevelTask()) == 1,
                        "Error: Wrong number of top level tasks.")
        topLevelTask = testWorkload.getTopLevelTask()[0]
        topLevelStep = topLevelTask.steps()
        cmsRun2Step = topLevelStep.getStep("cmsRun2").getTypeHelper()
        self.assertTrue(len(cmsRun2Step.listOutputModules()) == 2,
                        "Error: Wrong number of output modules in cmsRun2.")

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.verifyKeepAOD()
        return

    def testCombinedReDigiRecoConfig(self):
        """
        _testCombinedReDigiRecoConfig_

        Verify that a ReDigi workflow that uses a single step one config
        installs into WMBS correctly.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase, combinedStepOne=True)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[2]
        defaultArguments["StepOneOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath=self.testInit.testDir)

        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.verifyDiscardRAW()
        return

    def testSingleStepReDigi(self):
        """
        _testSingleStepReDigi_

        Verify that a single step ReDigi workflow can be created and installed
        correctly into WMBS.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[2]

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath=self.testInit.testDir)

        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.verifyKeepAOD()
        return

    def test1StepMemCoresSettings(self):
        """
        _test1StepMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps. Single step in a task.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[2]

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        stepOne = testWorkload.getTask('StepOneProc')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = stepOne.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = stepOne.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", defaultArguments)
        stepOne = testWorkload.getTask('StepOneProc')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = stepOne.getStepHelper(step)
            if step in ('stageOut1', 'logArch1'):
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
        perfParams = stepOne.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def test2StepDependCoresSettings(self):
        """
        _test2StepSepCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps. One step in each task/job
        """
        taskPaths = ('/TestWorkload/StepOneProc',
                     '/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc')

        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["KeepStepOneOutput"] = True
        defaultArguments["KeepStepTwoOutput"] = True
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                if step in ('stageOut1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def test2StepChainedCoresSettings(self):
        """
        _test2StepSepCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps. Two steps in the same task/job.
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["KeepStepOneOutput"] = False
        defaultArguments["KeepStepTwoOutput"] = True
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        taskObj = testWorkload.getTask('StepOneProc')
        for step in ('cmsRun1', 'cmsRun2', 'stageOut1', 'logArch1'):
            stepHelper = taskObj.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = taskObj.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        taskObj = testWorkload.getTask('StepOneProc')
        for step in ('cmsRun1', 'cmsRun2', 'stageOut1', 'logArch1'):
            stepHelper = taskObj.getStepHelper(step)
            if step in ('stageOut1', 'logArch1'):
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
        perfParams = taskObj.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def test3StepsDependMemCoresSettings(self):
        """
        _test3StepsDependMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps. Each step in a different task/job.
        """
        taskPaths = ('/TestWorkload/StepOneProc',
                     '/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc',
                     '/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc')

        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["KeepStepOneOutput"] = True
        defaultArguments["KeepStepTwoOutput"] = True
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                if step in ('stageOut1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def test3StepsSemiChainedMemCoresSettings(self):
        """
        _test3StepsSemiChainedMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps. First and second steps in the same task/job,
        third step in a separate task.
        """
        taskPaths = ('/TestWorkload/StepOneProc',
                     '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/StepThreeProc')

        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["KeepStepOneOutput"] = False
        defaultArguments["KeepStepTwoOutput"] = True
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'cmsRun2', 'stageOut1', 'logArch1'):
                if step == 'cmsRun2' and task == taskPaths[1]:
                    continue
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'cmsRun2', 'stageOut1', 'logArch1'):
                if step == 'cmsRun2' and task == taskPaths[1]:
                    continue
                stepHelper = taskObj.getStepHelper(step)
                if step in ('stageOut1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def test3StepsChainedMemCoresSettings(self):
        """
        _test3StepsChainedMemCoresSettings_

        Make sure the multicore and memory seetings are properly propagated to
        all tasks and steps
        """
        defaultArguments = ReDigiWorkloadFactory.getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["KeepStepOneOutput"] = False
        defaultArguments["KeepStepTwoOutput"] = False
        defaultArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        defaultArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload1", defaultArguments)
        stepOne = testWorkload.getTask('StepOneProc')
        for step in ['cmsRun1', 'cmsRun2', 'cmsRun3', 'stageOut1', 'logArch1']:
            stepHelper = stepOne.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = stepOne.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", defaultArguments)
        stepOne = testWorkload.getTask('StepOneProc')
        for step in ['cmsRun1', 'cmsRun2', 'cmsRun3', 'stageOut1', 'logArch1']:
            stepHelper = stepOne.getStepHelper(step)
            if step in ['stageOut1', 'logArch1']:
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
        perfParams = stepOne.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def test1StepFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation for a single step ReDigi
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/StepOneProc',
                       '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule']
        expWfTasks = ['/TestWorkload/StepOneProc',
                      '/TestWorkload/StepOneProc/LogCollect',
                      '/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedaodOutputModule',
                      '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule',
                      '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/StepOneProcaodOutputModuleMergeLogCollect']
        expFsets = ['TestWorkload-StepOneProc-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-logArchive',
                    '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-MergedAODSIM',
                    '/TestWorkload/StepOneProc/unmerged-aodOutputModuleAODSIM',
                    '/TestWorkload/StepOneProc/unmerged-logArchive']
        subMaps = [(3,
                    '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-logArchive',
                    '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/StepOneProcaodOutputModuleMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (4,
                    '/TestWorkload/StepOneProc/unmerged-aodOutputModuleAODSIM',
                    '/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedaodOutputModule',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (2,
                    '/TestWorkload/StepOneProc/unmerged-aodOutputModuleAODSIM',
                    '/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (5,
                    '/TestWorkload/StepOneProc/unmerged-logArchive',
                    '/TestWorkload/StepOneProc/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (1,
                    'TestWorkload-StepOneProc-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/StepOneProc',
                    'EventAwareLumiBased',
                    'Processing')]

        testArguments = ReDigiWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        testArguments["StepOneConfigCacheID"] = configs[2]

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", blockName=testArguments['InputDataset'],
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

    def test2StepFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation for a double steps ReDigi
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/StepOneProc',
                       '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput',
                       '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput']
        expWfTasks = ['/TestWorkload/StepOneProc',
                      '/TestWorkload/StepOneProc/LogCollect',
                      '/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedDQMoutput',
                      '/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedRECODEBUGoutput',
                      '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput',
                      '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/StepOneProcDQMoutputMergeLogCollect',
                      '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput',
                      '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/StepOneProcRECODEBUGoutputMergeLogCollect']
        expFsets = ['TestWorkload-StepOneProc-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/StepOneProc/unmerged-DQMoutputDQM',
                    '/TestWorkload/StepOneProc/unmerged-RAWDEBUGoutputRAW-DEBUG-OUTPUT',
                    '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-MergedDQM',
                    '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-logArchive',
                    '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-MergedRECO-DEBUG-OUTPUT',
                    '/TestWorkload/StepOneProc/unmerged-logArchive',
                    '/TestWorkload/StepOneProc/unmerged-RECODEBUGoutputRECO-DEBUG-OUTPUT']
        subMaps = [(3,
                    '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/StepOneProcDQMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (6,
                    '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-logArchive',
                    '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/StepOneProcRECODEBUGoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (4,
                    '/TestWorkload/StepOneProc/unmerged-DQMoutputDQM',
                    '/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedDQMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (2,
                    '/TestWorkload/StepOneProc/unmerged-DQMoutputDQM',
                    '/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (8,
                    '/TestWorkload/StepOneProc/unmerged-logArchive',
                    '/TestWorkload/StepOneProc/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/StepOneProc/unmerged-RECODEBUGoutputRECO-DEBUG-OUTPUT',
                    '/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedRECODEBUGoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (5,
                    '/TestWorkload/StepOneProc/unmerged-RECODEBUGoutputRECO-DEBUG-OUTPUT',
                    '/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (1,
                    'TestWorkload-StepOneProc-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/StepOneProc',
                    'EventAwareLumiBased',
                    'Processing')]

        testArguments = ReDigiWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        testArguments["StepOneConfigCacheID"] = configs[0]
        testArguments["StepTwoConfigCacheID"] = configs[1]
        testArguments["KeepStepOneOutput"] = False
        testArguments["KeepStepTwoOutput"] = True
        testArguments["StepOneOutputModuleName"] = "RAWDEBUGoutput"
        testArguments["StepTwoOutputModuleName"] = "RECODEBUGoutput"

        factory = ReDigiWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", blockName=testArguments['InputDataset'],
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


if __name__ == '__main__':
    unittest.main()
