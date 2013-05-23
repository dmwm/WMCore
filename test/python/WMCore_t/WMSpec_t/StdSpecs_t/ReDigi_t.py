#!/usr/bin/env python
"""
_ReDigi_t_

Unit tests for the ReDigi workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.ReDigi import getTestArguments, reDigiWorkload

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.Services.EmulatorSwitch import EmulatorHelper

def injectReDigiConfigs(configDatabase, combinedStepOne = False):
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
        stepOneConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RECODEBUGoutput", "DQMoutput"],
                                                          "RECODEBUGoutput": {"dataset": {"filterName": "",
                                                                                          "dataTier": "RECO-DEBUG-OUTPUT"}},
                                                          "DQMoutput": {"dataset": {"filterName": "",
                                                                                    "dataTier": "DQM"}}}}
    else:
        stepOneConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RAWDEBUGoutput"],
                                                          "RAWDEBUGoutput": {"dataset": {"filterName": "",
                                                                                         "dataTier": "RAW-DEBUG-OUTPUT"}}}}

    stepTwoConfig = Document()
    stepTwoConfig["info"] = None
    stepTwoConfig["config"] = None
    stepTwoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
    stepTwoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
    stepTwoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    stepTwoConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RECODEBUGoutput", "DQMoutput"],
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
    stepThreeConfig["pset_tweak_details"] ={"process": {"outputModules_": ["aodOutputModule"],
                                                        "aodOutputModule": {"dataset": {"filterName": "",
                                                                                        "dataTier": "AODSIM"}}}}
    stepOne = configDatabase.commitOne(stepOneConfig)[0]["id"]
    stepTwo = configDatabase.commitOne(stepTwoConfig)[0]["id"]
    stepThree = configDatabase.commitOne(stepThreeConfig)[0]["id"]
    return (stepOne, stepTwo, stepThree)

class ReDigiTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("redigi_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.generateWorkDir()

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("redigi_t")

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

    def testDependentReDigi(self):
        """
        _testDependentReDigi_

        Verfiy that a dependent ReDigi workflow that keeps stages out
        RAW data is created and installed into WMBS correctly.
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath = self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        topLevelFileset = Fileset(name = "TestWorkload-StepOneProc-SomeBlock")
        topLevelFileset.loadData()

        stepOneUnmergedRAWFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-RAWDEBUGoutput")
        stepOneUnmergedRAWFileset.loadData()
        stepOneMergedRAWFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/merged-Merged")
        stepOneMergedRAWFileset.loadData()
        stepOneLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-logArchive")
        stepOneLogArchiveFileset.loadData()
        stepOneMergeLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/merged-logArchive")
        stepOneMergeLogArchiveFileset.loadData()

        stepTwoUnmergedDQMFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/unmerged-DQMoutput")
        stepTwoUnmergedDQMFileset.loadData()
        stepTwoUnmergedRECOFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/unmerged-RECODEBUGoutput")
        stepTwoUnmergedRECOFileset.loadData()
        stepTwoMergedDQMFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeDQMoutput/merged-Merged")
        stepTwoMergedDQMFileset.loadData()
        stepTwoMergedRECOFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/merged-Merged")
        stepTwoMergedRECOFileset.loadData()
        stepTwoLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/unmerged-logArchive")
        stepTwoLogArchiveFileset.loadData()
        stepTwoMergeDQMLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeDQMoutput/merged-logArchive")
        stepTwoMergeDQMLogArchiveFileset.loadData()
        stepTwoMergeRECOLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/merged-logArchive")
        stepTwoMergeRECOLogArchiveFileset.loadData()

        stepThreeUnmergedAODFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/unmerged-aodOutputModule")
        stepThreeUnmergedAODFileset.loadData()
        stepThreeMergedAODFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcMergeaodOutputModule/merged-Merged")
        stepThreeMergedAODFileset.loadData()
        stepThreeLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/unmerged-logArchive")
        stepThreeLogArchiveFileset.loadData()

        stepThreeMergeLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcMergeaodOutputModule/merged-logArchive")
        stepThreeMergeLogArchiveFileset.loadData()

        stepOneWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                   task = "/TestWorkload/StepOneProc")
        stepOneWorkflow.load()
        self.assertEqual(stepOneWorkflow.wfType, 'redigi')
        self.assertTrue("logArchive" in stepOneWorkflow.outputMap.keys(),
                        "Error: Step one missing output module.")
        self.assertTrue("RAWDEBUGoutput" in stepOneWorkflow.outputMap.keys(),
                        "Error: Step one missing output module.")
        self.assertEqual(stepOneWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepOneLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepOneLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneWorkflow.outputMap["RAWDEBUGoutput"][0]["merged_output_fileset"].id, stepOneMergedRAWFileset.id,
                         "Error: RAWDEBUG output fileset is wrong.")
        self.assertEqual(stepOneWorkflow.outputMap["RAWDEBUGoutput"][0]["output_fileset"].id, stepOneUnmergedRAWFileset.id,
                         "Error: RAWDEBUG output fileset is wrong.")

        for outputMod in stepOneWorkflow.outputMap.keys():
            self.assertTrue(len(stepOneWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepOneSub = Subscription(workflow = stepOneWorkflow, fileset = topLevelFileset)
        stepOneSub.loadData()
        self.assertEqual(stepOneSub["type"], "Processing",
                         "Error: Step one sub has wrong type.")

        stepOneCleanupWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                          task = "/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedRAWDEBUGoutput")
        stepOneCleanupWorkflow.load()
        self.assertEqual(len(stepOneCleanupWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup should have no output.")
        stepOneCleanupSub = Subscription(workflow = stepOneCleanupWorkflow, fileset = stepOneUnmergedRAWFileset)
        stepOneCleanupSub.loadData()
        self.assertEqual(stepOneCleanupSub["type"], "Cleanup",
                         "Error: Step one sub has wrong type.")

        stepOneLogCollectWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/LogCollect")
        stepOneLogCollectWorkflow.load()
        self.assertEqual(len(stepOneLogCollectWorkflow.outputMap.keys()), 0,
                         "Error: LogCollect should have no output.")
        stepOneLogCollectSub = Subscription(workflow = stepOneLogCollectWorkflow, fileset = stepOneLogArchiveFileset)
        stepOneLogCollectSub.loadData()
        self.assertEqual(stepOneLogCollectSub["type"], "LogCollect",
                         "Error: Step one sub has wrong type.")

        stepOneMergeWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                        task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput")
        stepOneMergeWorkflow.load()
        self.assertTrue("Merged" in stepOneMergeWorkflow.outputMap.keys(),
                        "Error: Step one merge missing output module.")
        self.assertTrue("logArchive" in stepOneMergeWorkflow.outputMap.keys(),
                        "Error: Step one merge missing output module.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepOneMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepOneMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepOneMergedRAWFileset.id,
                         "Error: RAWDEBUG merge output fileset is wrong.")
        self.assertEqual(stepOneMergeWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepOneMergedRAWFileset.id,
                         "Error: RAWDEBUG merge output fileset is wrong.")
        for outputMod in stepOneMergeWorkflow.outputMap.keys():
            self.assertTrue(len(stepOneMergeWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")
        stepOneMergeSub = Subscription(workflow = stepOneMergeWorkflow, fileset = stepOneUnmergedRAWFileset)
        stepOneMergeSub.loadData()
        self.assertEqual(stepOneMergeSub["type"], "Merge",
                         "Error: Step one sub has wrong type.")

        stepTwoWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                   task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc")
        stepTwoWorkflow.load()
        self.assertTrue("RECODEBUGoutput" in stepTwoWorkflow.outputMap.keys(),
                        "Error: Step two missing output module.")
        self.assertTrue("DQMoutput" in stepTwoWorkflow.outputMap.keys(),
                        "Error: Step two missing output module.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutput"][0]["merged_output_fileset"].id, stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutput"][0]["output_fileset"].id, stepTwoUnmergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutput"][0]["merged_output_fileset"].id, stepTwoMergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutput"][0]["output_fileset"].id, stepTwoUnmergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        stepTwoSub = Subscription(workflow = stepTwoWorkflow, fileset = stepOneMergedRAWFileset)
        stepTwoSub.loadData()
        self.assertEqual(stepTwoSub["type"], "Processing",
                         "Error: Step two sub has wrong type.")

        for outputMod in stepTwoWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoCleanupDQMWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcCleanupUnmergedDQMoutput")
        stepTwoCleanupDQMWorkflow.load()
        self.assertEqual(len(stepTwoCleanupDQMWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupDQMSub = Subscription(workflow = stepTwoCleanupDQMWorkflow, fileset = stepTwoUnmergedDQMFileset)
        stepTwoCleanupDQMSub.loadData()
        self.assertEqual(stepTwoCleanupDQMSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoCleanupRECOWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                              task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcCleanupUnmergedRECODEBUGoutput")
        stepTwoCleanupRECOWorkflow.load()
        self.assertEqual(len(stepTwoCleanupRECOWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupRECOSub = Subscription(workflow = stepTwoCleanupRECOWorkflow, fileset = stepTwoUnmergedRECOFileset)
        stepTwoCleanupRECOSub.loadData()
        self.assertEqual(stepTwoCleanupRECOSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoLogCollectWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcLogCollect")
        stepTwoLogCollectWorkflow.load()
        self.assertEqual(len(stepTwoLogCollectWorkflow.outputMap.keys()), 0,
                         "Error: LogCollect shouldn't have any output.")
        stepTwoLogCollectSub = Subscription(workflow = stepTwoLogCollectWorkflow, fileset = stepTwoLogArchiveFileset)
        stepTwoLogCollectSub.loadData()
        self.assertEqual(stepTwoLogCollectSub["type"], "LogCollect",
                         "Error: Step two sub has wrong type.")

        stepTwoMergeRECOWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                            task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput")
        stepTwoMergeRECOWorkflow.load()
        self.assertTrue("Merged" in stepTwoMergeRECOWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in stepTwoMergeRECOWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        stepTwoMergeRECOSub = Subscription(workflow = stepTwoMergeRECOWorkflow, fileset = stepTwoUnmergedRECOFileset)
        stepTwoMergeRECOSub.loadData()
        self.assertEqual(stepTwoMergeRECOSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in stepTwoMergeRECOWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoMergeRECOWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoMergeDQMWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                           task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeDQMoutput")
        stepTwoMergeDQMWorkflow.load()
        self.assertTrue("Merged" in stepTwoMergeDQMWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in stepTwoMergeDQMWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        stepTwoMergeDQMSub = Subscription(workflow = stepTwoMergeDQMWorkflow, fileset = stepTwoUnmergedDQMFileset)
        stepTwoMergeDQMSub.loadData()
        self.assertEqual(stepTwoMergeDQMSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in stepTwoMergeDQMWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoMergeDQMWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepThreeWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                     task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc")
        stepThreeWorkflow.load()
        self.assertTrue("aodOutputModule" in stepThreeWorkflow.outputMap.keys(),
                        "Error: Step three missing output module.")
        self.assertTrue("logArchive" in stepThreeWorkflow.outputMap.keys(),
                        "Error: Step three missing output module.")
        self.assertEqual(stepThreeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepThreeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepThreeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeWorkflow.outputMap["aodOutputModule"][0]["merged_output_fileset"].id, stepThreeMergedAODFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepThreeWorkflow.outputMap["aodOutputModule"][0]["output_fileset"].id, stepThreeUnmergedAODFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        stepThreeSub = Subscription(workflow = stepThreeWorkflow, fileset = stepTwoMergedRECOFileset)
        stepThreeSub.loadData()
        self.assertEqual(stepThreeSub["type"], "Processing",
                         "Error: Step three sub has wrong type.")
        for outputMod in stepThreeWorkflow.outputMap.keys():
            self.assertTrue(len(stepThreeWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepThreeCleanupWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                            task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcCleanupUnmergedaodOutputModule")
        stepThreeCleanupWorkflow.load()
        self.assertEqual(len(stepThreeCleanupWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup should have no output.")
        stepThreeCleanupSub = Subscription(workflow = stepThreeCleanupWorkflow, fileset = stepThreeUnmergedAODFileset)
        stepThreeCleanupSub.loadData()
        self.assertEqual(stepThreeCleanupSub["type"], "Cleanup",
                         "Error: Step three sub has wrong type.")

        stepThreeLogCollectWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                               task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcLogCollect")
        stepThreeLogCollectWorkflow.load()
        self.assertEqual(len(stepThreeLogCollectWorkflow.outputMap.keys()), 0,
                         "Error: LogCollect should have no output.")
        stepThreeLogCollectSub = Subscription(workflow = stepThreeLogCollectWorkflow, fileset = stepThreeLogArchiveFileset)
        stepThreeLogCollectSub.loadData()
        self.assertEqual(stepThreeLogCollectSub["type"], "LogCollect",
                         "Error: Step three sub has wrong type.")

        stepThreeMergeWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                          task = "/TestWorkload/StepOneProc/StepOneProcMergeRAWDEBUGoutput/StepTwoProc/StepTwoProcMergeRECODEBUGoutput/StepThreeProc/StepThreeProcMergeaodOutputModule")
        stepThreeMergeWorkflow.load()
        self.assertTrue("Merged" in stepThreeMergeWorkflow.outputMap.keys(),
                        "Error: Step three merge missing output module.")
        self.assertTrue("logArchive" in stepThreeMergeWorkflow.outputMap.keys(),
                        "Error: Step three merge missing output module.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepThreeMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepThreeMergeLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepThreeMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        self.assertEqual(stepThreeMergeWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepThreeMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        stepThreeMergeSub = Subscription(workflow = stepThreeMergeWorkflow, fileset = stepThreeUnmergedAODFileset)
        stepThreeMergeSub.loadData()
        self.assertEqual(stepThreeMergeSub["type"], "Merge",
                         "Error: Step three sub has wrong type.")
        for outputMod in stepThreeMergeWorkflow.outputMap.keys():
            self.assertTrue(len(stepThreeMergeWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        return

    def verifyDiscardRAW(self):
        """
        _verifyDiscardRAW_

        Verify that a workflow that discards the RAW was installed into WMBS
        correctly.
        """
        topLevelFileset = Fileset(name = "TestWorkload-StepOneProc-SomeBlock")
        topLevelFileset.loadData()

        stepTwoUnmergedDQMFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-DQMoutput")
        stepTwoUnmergedDQMFileset.loadData()
        stepTwoUnmergedRECOFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-RECODEBUGoutput")
        stepTwoUnmergedRECOFileset.loadData()
        stepTwoMergedDQMFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-Merged")
        stepTwoMergedDQMFileset.loadData()
        stepTwoMergedRECOFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-Merged")
        stepTwoMergedRECOFileset.loadData()
        stepTwoLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-logArchive")
        stepTwoLogArchiveFileset.loadData()
        stepTwoMergeDQMLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput/merged-logArchive")
        stepTwoMergeDQMLogArchiveFileset.loadData()
        stepTwoMergeRECOLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput/merged-logArchive")
        stepTwoMergeRECOLogArchiveFileset.loadData()

        stepTwoWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                   task = "/TestWorkload/StepOneProc")
        stepTwoWorkflow.load()
        self.assertTrue("RECODEBUGoutput" in stepTwoWorkflow.outputMap.keys(),
                        "Error: Step two missing output module.")
        self.assertTrue("DQMoutput" in stepTwoWorkflow.outputMap.keys(),
                        "Error: Step two missing output module.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutput"][0]["merged_output_fileset"].id, stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["RECODEBUGoutput"][0]["output_fileset"].id, stepTwoUnmergedRECOFileset.id,
                         "Error: RECODEBUG output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutput"][0]["merged_output_fileset"].id, stepTwoMergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["DQMoutput"][0]["output_fileset"].id, stepTwoUnmergedDQMFileset.id,
                         "Error: DQM output fileset is wrong.")
        stepTwoSub = Subscription(workflow = stepTwoWorkflow, fileset = topLevelFileset)
        stepTwoSub.loadData()
        self.assertEqual(stepTwoSub["type"], "Processing",
                         "Error: Step two sub has wrong type.")

        for outputMod in stepTwoWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoCleanupDQMWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedDQMoutput")
        stepTwoCleanupDQMWorkflow.load()
        self.assertEqual(len(stepTwoCleanupDQMWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupDQMSub = Subscription(workflow = stepTwoCleanupDQMWorkflow, fileset = stepTwoUnmergedDQMFileset)
        stepTwoCleanupDQMSub.loadData()
        self.assertEqual(stepTwoCleanupDQMSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoCleanupRECOWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                              task = "/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedRECODEBUGoutput")
        stepTwoCleanupRECOWorkflow.load()
        self.assertEqual(len(stepTwoCleanupRECOWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupRECOSub = Subscription(workflow = stepTwoCleanupRECOWorkflow, fileset = stepTwoUnmergedRECOFileset)
        stepTwoCleanupRECOSub.loadData()
        self.assertEqual(stepTwoCleanupRECOSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoLogCollectWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/LogCollect")
        stepTwoLogCollectWorkflow.load()
        self.assertEqual(len(stepTwoLogCollectWorkflow.outputMap.keys()), 0,
                         "Error: LogCollect shouldn't have any output.")
        stepTwoLogCollectSub = Subscription(workflow = stepTwoLogCollectWorkflow, fileset = stepTwoLogArchiveFileset)
        stepTwoLogCollectSub.loadData()
        self.assertEqual(stepTwoLogCollectSub["type"], "LogCollect",
                         "Error: Step two sub has wrong type.")

        stepTwoMergeRECOWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                            task = "/TestWorkload/StepOneProc/StepOneProcMergeRECODEBUGoutput")
        stepTwoMergeRECOWorkflow.load()
        self.assertTrue("Merged" in stepTwoMergeRECOWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in stepTwoMergeRECOWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoMergeRECOLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeRECOWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepTwoMergedRECOFileset.id,
                         "Error: RECODEBUG merge output fileset is wrong.")
        stepTwoMergeRECOSub = Subscription(workflow = stepTwoMergeRECOWorkflow, fileset = stepTwoUnmergedRECOFileset)
        stepTwoMergeRECOSub.loadData()
        self.assertEqual(stepTwoMergeRECOSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in stepTwoMergeRECOWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoMergeRECOWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoMergeDQMWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                           task = "/TestWorkload/StepOneProc/StepOneProcMergeDQMoutput")
        stepTwoMergeDQMWorkflow.load()
        self.assertTrue("Merged" in stepTwoMergeDQMWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in stepTwoMergeDQMWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoMergeDQMLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeDQMWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepTwoMergedDQMFileset.id,
                         "Error: DQM merge output fileset is wrong.")
        stepTwoMergeDQMSub = Subscription(workflow = stepTwoMergeDQMWorkflow, fileset = stepTwoUnmergedDQMFileset)
        stepTwoMergeDQMSub.loadData()
        self.assertEqual(stepTwoMergeDQMSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in stepTwoMergeDQMWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoMergeDQMWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")
        return

    def verifyKeepAOD(self):
        """
        _verifyKeepAOD_

        Verify that a workflow that only produces AOD in a single step was
        installed correctly into WMBS.
        """
        topLevelFileset = Fileset(name = "TestWorkload-StepOneProc-SomeBlock")
        topLevelFileset.loadData()

        stepTwoUnmergedAODFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-aodOutputModule")
        stepTwoUnmergedAODFileset.loadData()
        stepTwoMergedAODFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-Merged")
        stepTwoMergedAODFileset.loadData()
        stepTwoLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/unmerged-logArchive")
        stepTwoLogArchiveFileset.loadData()
        stepTwoMergeAODLogArchiveFileset = Fileset(name = "/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule/merged-logArchive")
        stepTwoMergeAODLogArchiveFileset.loadData()

        stepTwoWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                   task = "/TestWorkload/StepOneProc")
        stepTwoWorkflow.load()
        self.assertTrue("aodOutputModule" in stepTwoWorkflow.outputMap.keys(),
                        "Error: Step two missing output module.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["aodOutputModule"][0]["merged_output_fileset"].id, stepTwoMergedAODFileset.id,
                         "Error: AOD output fileset is wrong.")
        self.assertEqual(stepTwoWorkflow.outputMap["aodOutputModule"][0]["output_fileset"].id, stepTwoUnmergedAODFileset.id,
                         "Error: AOD output fileset is wrong.")
        stepTwoSub = Subscription(workflow = stepTwoWorkflow, fileset = topLevelFileset)
        stepTwoSub.loadData()
        self.assertEqual(stepTwoSub["type"], "Processing",
                         "Error: Step two sub has wrong type.")

        for outputMod in stepTwoWorkflow.outputMap.keys():
            self.assertTrue(len(stepTwoWorkflow.outputMap[outputMod]) == 1,
                            "Error: more than one destination for output mod.")

        stepTwoCleanupAODWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/StepOneProcCleanupUnmergedaodOutputModule")
        stepTwoCleanupAODWorkflow.load()
        self.assertEqual(len(stepTwoCleanupAODWorkflow.outputMap.keys()), 0,
                         "Error: Cleanup shouldn't have any output.")
        stepTwoCleanupAODSub = Subscription(workflow = stepTwoCleanupAODWorkflow, fileset = stepTwoUnmergedAODFileset)
        stepTwoCleanupAODSub.loadData()
        self.assertEqual(stepTwoCleanupAODSub["type"], "Cleanup",
                         "Error: Step two sub has wrong type.")

        stepTwoLogCollectWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                             task = "/TestWorkload/StepOneProc/LogCollect")
        stepTwoLogCollectWorkflow.load()
        self.assertEqual(len(stepTwoLogCollectWorkflow.outputMap.keys()), 0,
                         "Error: LogCollect shouldn't have any output.")
        stepTwoLogCollectSub = Subscription(workflow = stepTwoLogCollectWorkflow, fileset = stepTwoLogArchiveFileset)
        stepTwoLogCollectSub.loadData()
        self.assertEqual(stepTwoLogCollectSub["type"], "LogCollect",
                         "Error: Step two sub has wrong type.")

        stepTwoMergeAODWorkflow = Workflow(spec = "somespec", name = "TestWorkload",
                                           task = "/TestWorkload/StepOneProc/StepOneProcMergeaodOutputModule")
        stepTwoMergeAODWorkflow.load()
        self.assertTrue("Merged" in stepTwoMergeAODWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertTrue("logArchive" in stepTwoMergeAODWorkflow.outputMap.keys(),
                        "Error: Step two merge missing output module.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["logArchive"][0]["merged_output_fileset"].id, stepTwoMergeAODLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["logArchive"][0]["output_fileset"].id, stepTwoMergeAODLogArchiveFileset.id,
                         "Error: logArchive fileset is wrong.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["Merged"][0]["merged_output_fileset"].id, stepTwoMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        self.assertEqual(stepTwoMergeAODWorkflow.outputMap["Merged"][0]["output_fileset"].id, stepTwoMergedAODFileset.id,
                         "Error: AOD merge output fileset is wrong.")
        stepTwoMergeAODSub = Subscription(workflow = stepTwoMergeAODWorkflow, fileset = stepTwoUnmergedAODFileset)
        stepTwoMergeAODSub.loadData()
        self.assertEqual(stepTwoMergeAODSub["type"], "Merge",
                         "Error: Step two sub has wrong type.")
        for outputMod in stepTwoMergeAODWorkflow.outputMap.keys():
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
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["KeepStepOneOutput"] = False

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")

        # Verify that pileup is configured for both of the cmsRun steps in the
        # top level task.
        topLevelTask = testWorkload.getTopLevelTask()[0]
        cmsRun1Helper = topLevelTask.getStepHelper("cmsRun1")
        cmsRun2Helper = topLevelTask.getStepHelper("cmsRun2")
        cmsRun1PileupConfig = cmsRun1Helper.getPileup()
        cmsRun2PileupConfig = cmsRun2Helper.getPileup()

        self.assertTrue(cmsRun1PileupConfig.mc.dataset, "/some/cosmics/dataset")
        self.assertTrue(cmsRun2PileupConfig.mc.dataset, "/some/cosmics/dataset")
        
        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath = self.testInit.testDir)
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
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]
        defaultArguments["KeepStepOneOutput"] = False
        defaultArguments["KeepStepTwoOutput"] = False

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")

        self.assertTrue(len(testWorkload.getTopLevelTask()) == 1,
                        "Error: Wrong number of top level tasks.")
        topLevelTask = testWorkload.getTopLevelTask()[0]
        topLevelStep = topLevelTask.steps()
        cmsRun2Step = topLevelStep.getStep("cmsRun2").getTypeHelper()
        self.assertTrue(len(cmsRun2Step.listOutputModules()) == 2,
                        "Error: Wrong number of output modules in cmsRun2.")

        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath = self.testInit.testDir)
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
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase, combinedStepOne = True)
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[2]
        defaultArguments["StepThreeConfigCacheID"] = None
        defaultArguments["StepOneOutputModuleName"] = "RECODEBUGoutput"

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")
        
        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath = self.testInit.testDir)

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
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = injectReDigiConfigs(self.configDatabase)
        defaultArguments["StepOneConfigCacheID"] = configs[2]
        defaultArguments["StepTwoConfigCacheID"] = None
        defaultArguments["StepThreeConfigCacheID"] = None

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")
        
        testWMBSHelper = WMBSHelper(testWorkload, "StepOneProc", "SomeBlock", cachepath = self.testInit.testDir)

        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.verifyKeepAOD()
        return

if __name__ == '__main__':
    unittest.main()
