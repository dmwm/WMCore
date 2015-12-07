#!/usr/bin/env python

'''
Unittest for Builder.py
'''





import unittest
import tempfile
import shutil
from WMCore.WMSpec.Steps.Builder import Builder
from WMCore.WMSpec.WMStep import makeWMStep

class WMBuilderTest(unittest.TestCase):
    '''unittest for WMBuilder'''
    def setUp(self):
        """instantiation"""
        try:
            self.testBuilder = Builder()
        except Exception as ex:
            msg = "Failed to instantiate Builder:\n"
            msg += str(ex)
            self.fail(msg)

    def testBuild(self):
        '''make sure the build method throws an exception'''
        mystep = makeWMStep("demostep")
        self.assertRaises(NotImplementedError, self.testBuilder.build, mystep,
                           '/')

    def testInstallWorkingArea(self):
        '''make sure installWorkingArea changes the attr in the data object'''
        mystep = makeWMStep("demostep")
        self.testBuilder.installWorkingArea(mystep.data,"workingdir")
        self.assertEqual(mystep.data.builder.workingDir, "workingdir")



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
