#!/usr/bin/env python
"""
Unittest for WMStep
"""


import unittest
from WMCore.WMSpec.WMStep import WMStep, makeWMStep


class WMStepTest(unittest.TestCase):
    """
    TestCase for WMStep class
    """
    def testA(self):
        """instantiation"""

        try:
            wmStep = WMStep("step1")
        except Exception, ex:
            msg = "Failed to instantiate WMStep:\n"
            msg += str(ex)
            self.fail(msg)

        try:
            wmStep2 = makeWMStep("step2")
        except Exception, ex:
            msg = "Failed to instantiate WMStep via makeWMStep:\n"
            msg += str(ex)
            self.fail(msg)



    def testB(self):
        """tree building"""

        wmStep1 = makeWMStep("step1")
        wmStep1.setStepType("TYPE1")

        wmStep2a = wmStep1.addStep("step2a")
        wmStep2a.setStepType("TYPE2")
        wmStep2b = wmStep1.addStep("step2b")
        wmStep2b.setStepType("TYPE3")
        wmStep2c = wmStep1.addStep("step2c")
        wmStep2c.setStepType("TYPE4")

        wmStep3a = wmStep2a.addStep("step3a")
        wmStep3a.setStepType("TYPE5")
        wmStep3b = wmStep2a.addStep("step3b")
        wmStep3b.setStepType("TYPE6")
        wmStep3c = wmStep2a.addStep("step3c")
        wmStep3c.setStepType("TYPE7")

        wmStep3d = wmStep2b.addStep("step3d")
        wmStep3d.setStepType("TYPE8")
        wmStep3e = wmStep2b.addStep("step3e")
        wmStep3e.setStepType("TYPE9")
        wmStep3f = wmStep2b.addStep("step3f")
        wmStep3f.setStepType("TYPE10")

        wmStep3g = wmStep2c.addStep("step3g")
        wmStep3g.setStepType("TYPE11")
        wmStep3h = wmStep2c.addStep("step3h")
        wmStep3h.setStepType("TYPE12")
        wmStep3i = wmStep2c.addStep("step3i")
        wmStep3i.setStepType("TYPE13")

        nameOrder = ['step1', 'step2a', 'step3a', 'step3b', 'step3c',
                     'step2b', 'step3d', 'step3e', 'step3f', 'step2c',
                     'step3g', 'step3h', 'step3i']
        typeOrder = ['TYPE1', 'TYPE2', 'TYPE5', 'TYPE6', 'TYPE7',
                     'TYPE3', 'TYPE8', 'TYPE9', 'TYPE10', 'TYPE4',
                     'TYPE11', 'TYPE12', 'TYPE13']

        checkType = [ x.stepType for x in wmStep1.nodeIterator()]
        checkOrder = [ x._internal_name for x in wmStep1.nodeIterator()]

        self.assertEqual(nameOrder, checkOrder)
        self.assertEqual(typeOrder, checkType)


if __name__ == '__main__':
    unittest.main()
