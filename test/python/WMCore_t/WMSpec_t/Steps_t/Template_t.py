#!/usr/bin/env python
"""
_Template_

Unittest for WMCore.WMSpec.Steps.Template module

"""

import unittest

from WMCore.WMSpec.WMStep import WMStep, WMStepHelper
from WMCore.WMSpec.Steps.Template import Template, CoreHelper



class TemplateTest(unittest.TestCase):
    """
    TestCase for Template base object

    """

    def testA(self):
        """instantiation"""
        try:
            template = Template()
        except Exception as ex:
            msg = "Failed to instantiate Template object:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """test coreInstall method"""
        step = WMStep("TestStep")

        template = Template()

        try:
            template.coreInstall(step)
        except Exception as ex:
            msg = "Error calling Template.coreInstall(step)\n"
            msg += str(ex)
            self.fail(msg)

        # check environment installation
        self.assertTrue(getattr(step, "environment", None) != None)
        env = getattr(step, "environment")
        self.assertTrue(getattr(env, "paths", None) != None)
        self.assertTrue(getattr(env, "variables", None) != None)


        # check build installation
        self.assertTrue(getattr(step, "build", None) != None)
        build = getattr(step, "build")
        self.assertTrue(getattr(build, "directories", None) != None)

    def testC(self):
        """exceptions"""

        template = Template()
        step = WMStep("TestStep")

        self.assertRaises(NotImplementedError, template.install, step)
        self.assertRaises(NotImplementedError, template.helper, step)



class CoreHelperTest(unittest.TestCase):
    """
    TestCase for CoreHelper object
    """

    def testA(self):
        """instantiation"""
        try:
            helper = CoreHelper(WMStep("TestHelper"))
        except Exception as ex:
            msg = "Failed to instantiate CoreHelper object:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """test stepName"""
        stepName = "StepNameGoesHere"
        helper = CoreHelper(WMStep(stepName))

        try:
            name = helper.stepName()
        except Exception as ex:
            msg = "Failed to call CoreHelper.stepName:\n"
            msg += str(ex)
            self.fail(msg)

        self.assertEqual(name, stepName)


    def testC(self):
        """environment setting manipulators"""

        step = WMStep("CoreHelperTest")
        template = Template()
        template.coreInstall(step)
        helper = CoreHelper(step)

        try:
            helper.addEnvironmentVariable("Variable1", "Value1")
        except Exception as ex:
            msg = "Failed to call CoreHelper.addEnvironmentVariable:\n"
            msg += str(ex)
            self.fail(msg)

        env = helper.environment()
        self.assertTrue(getattr(env.variables, "Variable1", None) != None)
        self.assertEqual(getattr(env.variables, "Variable1"), "Value1")



        helper.addEnvironmentVariable("Variable2", "Value2")
        self.assertTrue(getattr(env.variables, "Variable2", None) != None)
        self.assertEqual(getattr(env.variables, "Variable2"), "Value2")

        try:
            helper.addEnvironmentPath("Path1", "Entry1")
            helper.addEnvironmentPath("Path1", "Entry2")
            helper.addEnvironmentPath("Path1", "Entry3")
        except Exception as ex:
            msg = "Failed to call CoreHelper.addEnvironmentPath:\n"
            msg += str(ex)
            self.fail(msg)


        path1 = getattr(env.paths, "Path1", None)
        self.assertTrue(path1 != None)
        self.assertEqual(path1, ['Entry1', 'Entry2', 'Entry3'])








    def testD(self):
        """build/dir/file structure manipulators"""
        step = WMStep("CoreHelperTest")
        template = Template()
        template.coreInstall(step)
        helper = CoreHelper(step)

        try:
            helper.addDirectory("dir1")
            helper.addDirectory("dir1/dir2")
            helper.addDirectory("dir1/dir3")
            helper.addDirectory("dir1/dir4")
            helper.addDirectory("/dir1/dir5")
        except Exception as ex:
            msg = "Error calling CoreHelper.addDirectory\n"
            msg += str(ex)
            self.fail(msg)

        dirs = helper.directoryStructure()
        self.assertTrue(hasattr(dirs, helper.stepName()))

        stepDir = getattr(dirs, helper.stepName())
        self.assertTrue(hasattr(stepDir, "dir1"))
        dir1 = getattr(stepDir, "dir1")
        dir2 = getattr(dir1, "dir2")

        self.assertTrue(hasattr(dir1, "dir2"))
        self.assertTrue(hasattr(dir1, "dir3"))
        self.assertTrue(hasattr(dir1, "dir4"))
        self.assertTrue(hasattr(dir1, "dir5"))



        helper.addFile("file1")
        helper.addFile("file2", "dir1/dir2/file2")

        self.assertTrue(hasattr(stepDir, "file1"))
        self.assertTrue(hasattr(dir2, "file2"))

        file1 = getattr(stepDir, "file1")
        file2 = getattr(dir2, "file2")

        self.assertEqual(file1['Source'], "file1")
        self.assertEqual(file1['Target'], "file1")

        self.assertEqual(file2['Source'], "file2")
        self.assertEqual(file2['Target'], "file2")






if __name__ == '__main__':
    unittest.main()
