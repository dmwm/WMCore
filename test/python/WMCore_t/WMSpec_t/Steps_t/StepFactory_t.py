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
        except Exception, ex:
            msg = "Error loading Step Template of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)




    def testB(self):
        """bad template"""
        self.assertRaises(StepFactory.StepFactoryException,
                          StepFactory.getStepTemplate, "UtterGuff")


    def testC(self):
        """builder"""
        try:
            cmssw = StepFactory.getStepBuilder("CMSSW")
        except Exception, ex:
            msg = "Error loading Step Builder of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)




    def testD(self):
        """bad buildere"""
        self.assertRaises(StepFactory.StepFactoryException,
                          StepFactory.getStepBuilder, "UtterGuff")


    def testC(self):
        """executor"""
        try:
            cmssw = StepFactory.getStepExecutor("CMSSW")
        except Exception, ex:
            msg = "Error loading Step Executor of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)




    def testD(self):
        """bad executor"""
        self.assertRaises(StepFactory.StepFactoryException,
                          StepFactory.getStepExecutor, "UtterGuff")


    def testC(self):
        """emulator"""
        try:
            cmssw = StepFactory.getStepEmulator("CMSSW")
        except Exception, ex:
            msg = "Error loading Step Emulator of Type CMSSW\n"
            msg += str(ex)
            self.fail(msg)




    def testG(self):
        """bad emu"""
        self.assertRaises(StepFactory.StepFactoryException,
                          StepFactory.getStepEmulator, "UtterGuff")











if __name__ == '__main__':
    unittest.main()
