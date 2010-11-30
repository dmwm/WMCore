#!/usr/bin/env python
"""
_SetupCMSSWPset_t.py

Tests for the PSet configuration code.
"""

import unittest
import pickle
import os

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Job import Job
from WMCore.Configuration import ConfigSection

from WMCore.WMSpec.WMStep import WMStep
from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSWStepHelper
from WMCore.WMSpec.Steps import StepFactory
from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset

from WMQuality.TestInit import TestInit

class SetupCMSSWPsetTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        self.testInit.delWorkDir()
        return

    def createTestStep(self):
        """
        _createTestStep_

        Create a test step that can be passed to the setup script.
        """
        newStep = WMStep("cmsRun1")
        newStepHelper = CMSSWStepHelper(newStep)
        newStepHelper.setStepType("CMSSW")
        newStepHelper.setGlobalTag("SomeGlobalTag")
        stepTemplate = StepFactory.getStepTemplate("CMSSW")
        stepTemplate(newStep)
        newStep.application.command.configuration = "PSet.pkl"
        return newStepHelper

    def createTestJob(self):
        """
        _createTestJob_

        Create a test job that has parents for each input file.
        """
        newJob = Job(name = "TestJob")
        newJob.addFile(File(lfn = "/some/file/one",
                            parents = set([File(lfn = "/some/parent/one")])))
        newJob.addFile(File(lfn = "/some/file/two",
                            parents = set([File(lfn = "/some/parent/two")])))
        return newJob
    
    def testPSetFixup(self):
        """
        _testPSetFixup_

        Verify that all necessary parameters are set in the PSet.
        """
        setupScript = SetupCMSSWPset()
        setupScript.step = self.createTestStep()
        setupScript.stepSpace = ConfigSection(name = "stepSpace")
        setupScript.stepSpace.location = self.testDir
        setupScript.job = self.createTestJob()
        setupScript()

        fixedPSetHandle = open(os.path.join(self.testDir, "PSet.pkl"))
        fixedPSet = pickle.load(fixedPSetHandle)
        fixedPSetHandle.close()

        self.assertEqual(fixedPSet.GlobalTag.globaltag.value, "SomeGlobalTag",
                         "Error: Wrong global tag.")

        self.assertEqual(fixedPSet.source.cacheSize.value, 100000000,
                         "Error: Wrong cache size.")
        self.assertEqual(len(fixedPSet.source.fileNames.value), 2,
                         "Error: Wrong number of files.")
        self.assertEqual(len(fixedPSet.source.secondaryFileNames.value), 2,
                         "Error: Wrong number of secondary files.")
        self.assertEqual(fixedPSet.source.fileNames.value[0], "/some/file/one",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.fileNames.value[1], "/some/file/two",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.secondaryFileNames.value[0], "/some/parent/one",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.secondaryFileNames.value[1], "/some/parent/two",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.skipEvents.value, 0,
                         "Error: Wrong number of skip events.")
        self.assertEqual(fixedPSet.source.firstRun.value, 0,
                         "Error: Wrong first run")
        self.assertEqual(fixedPSet.source.lumisToProcess, [],
                         "Error: Wrong first run")        
        self.assertEqual(fixedPSet.maxEvents.input.value, -1,
                         "Error: Wrong maxEvents.")

        return

if __name__ == "__main__":
    unittest.main()
