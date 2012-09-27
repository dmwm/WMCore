#!/usr/bin/env python
"""
_SetupCMSSWPset_t.py

Tests for the PSet configuration code.

"""

import imp
import nose
import pickle
import os
import sys
import unittest

import WMCore.WMBase

from WMCore.Configuration import ConfigSection
from WMCore.WMSpec.Steps import StepFactory
from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSWStepHelper
from WMCore.WMSpec.WMStep import WMStep
from WMQuality.TestInit import TestInit

class SetupCMSSWPsetTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()
        sys.path.insert(0, os.path.join(WMCore.WMBase.getTestBase(),
                                        "WMCore_t/WMRuntime_t/Scripts_t"))

    def tearDown(self):
        sys.path.remove(os.path.join(WMCore.WMBase.getTestBase(),
                                     "WMCore_t/WMRuntime_t/Scripts_t"))
        del sys.modules["WMTaskSpace"]
        self.testInit.delWorkDir()

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
        newStep.application.command.configuration = "PSet.py"
        newStep.application.multicore.numberOfCores = "auto"
        return newStepHelper

    def loadProcessFromPSet(self):
        """
        _loadProcessFromPSet_

        This requires changing the working directory,
        do so in a safe manner to encapsulate the change to this method only
        """

        currentPath = os.getcwd()
        loadedProcess = None
        try:
            if not os.path.isdir(self.testDir):
                raise
            os.chdir(self.testDir)
            testFile = "PSet.py"
            pset = imp.load_source('process', testFile)
            loadedProcess = pset.process
        except Exception, ex:
            self.fail("An exception was caught while trying to load the PSet, %s" % str(ex))
        finally:
            os.chdir(currentPath)

        return loadedProcess

    def testBuildPset(self):
        """
        _testBuildPset_

        Verify that multicore parameters are set in the PSet.

        """
        from WMCore.WMRuntime.Scripts.SetupCMSSWMulticore import SetupCMSSWMulticore
        setupScript = SetupCMSSWMulticore()
        setupScript.step = self.createTestStep()
        setupScript.stepSpace = ConfigSection(name = "stepSpace")
        setupScript.stepSpace.location = self.testDir
        setupScript.files = {'file1': {'events':1000}}
        setupScript.buildPSet()

        fixedPSet = self.loadProcessFromPSet()

        self.assertTrue(int(fixedPSet.options.multiProcesses.maxChildProcesses.value) > 0)
        self.assertTrue(int(fixedPSet.options.multiProcesses.maxSequentialEventsPerChild.value) > 0)


if __name__ == "__main__":
    unittest.main()
