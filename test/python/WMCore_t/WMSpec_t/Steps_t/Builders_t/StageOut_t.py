#!/usr/bin/env python

'''
Unittest for StageOut.py
'''





import unittest
import tempfile
import shutil
import os
import WMCore.WMSpec.Steps.StepFactory as StepFactory
import WMCore.WMSpec.WMStep as WMStep
import WMCore.WMSpec.Steps as TemplateNS


class StageOutTest(unittest.TestCase):
    ''' unittests for the StageOut builder'''
    def setUp(self):
        '''create the builder object and the temporary directory'''
        self.tempDir = tempfile.mkdtemp()

        try:
            self.testBuilder = StepFactory.getStepBuilder("StageOut")
        except Exception as ex:
            msg = "Failed to instantiate Builder:\n"
            msg += str(ex)
            self.fail(msg)

    def tearDown(self):
        '''remove the temp directory we created'''
        shutil.rmtree( self.tempDir )

    def testBuild(self):
        ''' build a directory and verify it exists'''
        mytemplate = StepFactory.getStepTemplate("StageOut")
        mystep = WMStep.makeWMStep("DummyStagingStep")
        mytemplate(mystep.data)
        self.testBuilder(mystep.data, "testTask", self.tempDir)
        self.assertTrue(os.path.exists(self.tempDir))
        self.assertTrue(os.path.exists("%s/DummyStagingStep/__init__.py"
                                       % self.tempDir))

    def testCustomBuild(self):
        ''' add in a custom directory and verify it gets created'''
        mytemplate = StepFactory.getStepTemplate("StageOut")
        mystep = WMStep.makeWMStep("DummyStagingStep")
        mytemplate(mystep.data)
        helper = TemplateNS.Template.CoreHelper(mystep.data)
        helper.addDirectory( 'testdirectory1' )
        helper.addDirectory( 'testdirectory2/testsubdir' )
        self.testBuilder(mystep.data, "testTask", self.tempDir)
        self.assertTrue(os.path.exists(self.tempDir))
        self.assertTrue(os.path.exists("%s/DummyStagingStep/__init__.py"
                                       % self.tempDir))
        self.assertTrue(os.path.exists("%s/DummyStagingStep/testdirectory1"
                                       % self.tempDir))
        self.assertTrue(os.path.exists("%s/%s/testdirectory2/testsubdir"
                                       % (self.tempDir, 'DummyStagingStep')))





if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
