#!/usr/bin/env python
# encoding: utf-8
"""
CMSSWTemplate_t.py

Created by Dave Evans on 2010-03-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import str
import unittest

from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSW as CMSSWTemplate
from WMCore.WMSpec.WMWorkload import newWorkload


class CMSSWTemplateTest(unittest.TestCase):
    """
    Unittest for CMSSW Template and Helper

    Builds the step from scratch, applies and checks the template
    Tests the helper methods

    """

    def testA(self):
        """
        instantiate & apply template
        """
        workload = newWorkload("UnitTests")
        task = workload.newTask("CMSSWTemplate")
        stepHelper = step = task.makeStep("TemplateTest")
        step = stepHelper.data

        # first up, create the template
        try:
            template = CMSSWTemplate()
        except Exception as ex:
            msg = "Failed to instantiate the CMSSW Step Template"
            msg += str(ex)
            self.fail(msg)

        # now apply it to the step
        try:
            template(step)
        except Exception as ex:
            msg = "Failed to apply template to step"
            msg += str(ex)
            self.fail(msg)

        # TODO: Check the step has the appropriate attributes expected for CMSSW
        self.assertTrue(
            hasattr(step, "application"))
        self.assertTrue(
            hasattr(step.application, "setup"))

    def testB(self):
        """

        try using the helper API to set and get information in the step

        """
        workload = newWorkload("UnitTests")
        task = workload.newTask("CMSSWTemplate")
        stepHelper = step = task.makeStep("TemplateTest")
        step = stepHelper.data
        template = CMSSWTemplate()
        template(step)
        try:
            helper = template.helper(step)
        except Exception as ex:
            msg = "Failure to create CMSSW Step Helper"
            msg += str(ex)
            self.fail(msg)

        helper.cmsswSetup("CMSSW_X_Y_Z", scramArch="slc5_ia32_gcc443")
        helper.addOutputModule(
            "outputModule1", primaryDataset="Primary",
            processedDataset='Processed',
            dataTier='Tier',
            lfnBase="/store/unmerged/whatever"
        )

    def testMulticoreSettings(self):
        """
        test multicore related methods
        """
        workload = newWorkload("UnitTests")
        task = workload.newTask("CMSSWTemplate")
        stepHelper = step = task.makeStep("TemplateTest")
        step = stepHelper.data
        template = CMSSWTemplate()
        template(step)

        helper = template.helper(step)

        self.assertEqual(helper.getNumberOfCores(), 1)
        self.assertEqual(helper.getEventStreams(), 0)
        helper.setNumberOfCores(8)
        self.assertEqual(helper.getNumberOfCores(), 8)
        self.assertEqual(helper.getEventStreams(), 0)
        helper.setNumberOfCores(8, 6)
        self.assertEqual(helper.getNumberOfCores(), 8)
        self.assertEqual(helper.getEventStreams(), 6)

    def testChainedProcessing(self):
        """

        check the chained processing set up is correct

        """
        workload = newWorkload("UnitTests")
        task = workload.newTask("CMSSWTemplate")
        step = task.makeStep("TemplateTest")
        template = CMSSWTemplate()
        helper = template.helper(step.data)
        inputStepName = "some_inputStepName"
        inputOutputModule = "some_inputOutputModule"
        helper.setupChainedProcessing(inputStepName, inputOutputModule)

        self.assertEqual(helper.data.input.chainedProcessing, True)
        self.assertEqual(helper.data.input.inputStepName, "some_inputStepName")
        self.assertEqual(helper.data.input.inputOutputModule, "some_inputOutputModule")


if __name__ == '__main__':
    unittest.main()
