#!/usr/bin/env python
"""
Unittest for WMStep
"""

import unittest

import WMCore.WMSpec.Steps.StepFactory as StepFactory
from Utils.PythonVersion import PY3
from WMCore.WMSpec.WMStep import WMStep, makeWMStep


class WMStepTest(unittest.TestCase):
    """
    TestCase for WMStep class
    """

    def setUp(self):
        """set up"""
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        """clean up"""
        pass

    def testA(self):
        """instantiation"""

        try:
            wmStep = WMStep("step1")
        except Exception as ex:
            msg = "Failed to instantiate WMStep:\n"
            msg += str(ex)
            self.fail(msg)

        try:
            wmStep2 = makeWMStep("step2")
        except Exception as ex:
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

        checkType = [x.stepType for x in wmStep1.nodeIterator()]
        checkOrder = [x._internal_name for x in wmStep1.nodeIterator()]

        self.assertEqual(nameOrder, checkOrder)
        self.assertEqual(typeOrder, checkType)

    def testC_testGetSetOverrides(self):
        """
        Test whether we can use the override manipulation tools in StepTypeHelper

        """

        wmStep = makeWMStep("step2")

        output = wmStep.getOverrides()

        # This should be empty since we haven't put anything in it
        self.assertEqual(output, {})

        wmStep.addOverride(override='test', overrideValue='nonsense')

        self.assertTrue(hasattr(wmStep.data, 'override'))
        self.assertTrue(hasattr(wmStep.data.override, 'test'))
        self.assertEqual(wmStep.data.override.test, 'nonsense')

        output = wmStep.getOverrides()

        self.assertEqual(output, {'test': 'nonsense'})

        return

    def testD_getOutputModule(self):
        """
        Test our ability to get an output module

        """

        wmStep = makeWMStep("step2")

        wmStep.data.output.section_('modules')
        wmStep.data.output.modules.section_('test')
        setattr(wmStep.data.output.modules.test, 'tester', 'nonsense')

        testModule = wmStep.getOutputModule(moduleName='test')

        self.assertEqual(testModule.tester, 'nonsense')

        return

    def testE_Properties(self):
        """
        _Properties_

        Test the various properties that we have set in the step
        """

        wmStep = makeWMStep("step2")

        # errorDestinatio
        self.assertEqual(wmStep.getErrorDestinationStep(), None)
        wmStep.setErrorDestinationStep(stepName='testStep')
        self.assertEqual(wmStep.getErrorDestinationStep(), 'testStep')

        self.assertEqual(wmStep.getConfigInfo(), (None, None, None))

        wmStep.data.application.configuration.configCacheUrl = 'test1'
        wmStep.data.application.configuration.cacheName = 'test2'
        wmStep.data.application.configuration.configId = 'test3'

        self.assertEqual(wmStep.getConfigInfo(), ('test1', 'test2', 'test3'))
        return

    def testGPUSettings(self):
        """
        Test GPU settings and the 'getGPURequired' and 'getGPURequirements' methods
        """
        # create a standard step object - without the CMSSW template applied
        wmStep = makeWMStep("step1")
        self.assertIsNone(wmStep.stepType())
        self.assertFalse(hasattr(wmStep.data, "gpu"))
        self.assertIsNone(wmStep.getGPURequired())

        # now apply the CMSSW template
        wmStep.setStepType("CMSSW")
        self.assertEqual(wmStep.stepType(), "CMSSW")
        template = StepFactory.getStepTemplate("CMSSW")
        template(wmStep.data)
        wmStepHelper = wmStep.getTypeHelper()
        self.assertEqual(wmStepHelper.getGPURequired(), "forbidden")
        self.assertIsNone(wmStepHelper.getGPURequirements())

        gpuParams = {"GPUMemoryMB": 1234, "CUDARuntime": "11.2.3", "CUDACapabilities": ["7.5", "8.0"]}
        wmStepHelper.setGPUSettings("required", gpuParams)
        self.assertEqual(wmStepHelper.getGPURequired(), "required")
        self.assertItemsEqual(wmStepHelper.getGPURequirements(), gpuParams)

        return


if __name__ == '__main__':
    unittest.main()
