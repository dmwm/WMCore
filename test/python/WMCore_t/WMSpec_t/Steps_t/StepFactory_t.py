#!/usr/bin/env python
"""
_StepFactory_

Unittests for StepFactory module

"""

import unittest

import WMCore.WMSpec.Steps.StepFactory as StepFactory


class StepFactoryTest(unittest.TestCase):
    """
    TestCase for StepFactory module. Load CMSSW implementation from
    each factory to test the mechanics

    """

    def testA(self):
        """template"""
        try:
            cmssw = StepFactory.getStepTemplate("CMSSW")
        except Exception as ex:
            msg = "Error loading Step Template of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """bad template"""
        self.assertRaises(ImportError,
                          StepFactory.getStepTemplate, "UtterGuff")

    def testC(self):
        """builder"""
        try:
            cmssw = StepFactory.getStepBuilder("CMSSW")
        except Exception as ex:
            msg = "Error loading Step Builder of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        """bad buildere"""
        self.assertRaises(ImportError,
                          StepFactory.getStepBuilder, "UtterGuff")

    def testExecutor(self):
        """executor"""
        try:
            cmssw = StepFactory.getStepExecutor("CMSSW")
        except Exception as ex:
            msg = "Error loading Step Executor of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)

    def testBadExecutor(self):
        """bad executor"""
        self.assertRaises(ImportError,
                          StepFactory.getStepExecutor, "UtterGuff")

    def testEmulator(self):
        """emulator"""
        try:
            cmssw = StepFactory.getStepEmulator("CMSSW")
        except Exception as ex:
            msg = "Error loading Step Emulator of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)

    def testBadEmulator(self):
        """bad emu"""
        self.assertRaises(ImportError,
                          StepFactory.getStepEmulator, "UtterGuff")


if __name__ == '__main__':
    unittest.main()
