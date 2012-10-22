#!/usr/bin/env python
"""
    _SandboxCreator_t_

    Tests sandbox production
"""

import unittest

import tempfile
import os.path
import tarfile
import pickle
import shutil
import sys
import copy


import WMCore_t.WMSpec_t.TestWorkloads as TestWorkloads
import WMCore.WMRuntime.SandboxCreator as SandboxCreator
import WMCore.WMSpec.WMTask as WMTask

class SandboxCreator_t(unittest.TestCase):

    def testMakeSandbox(self):
        creator  = SandboxCreator.SandboxCreator()
        workload = TestWorkloads.twoTaskTree()
        tempdir  = tempfile.mkdtemp()
        boxpath  = creator.makeSandbox(tempdir, workload)

        # extract our sandbox to test it
        extractDir = tempfile.mkdtemp()
        tarHandle  = tarfile.open(boxpath, 'r:bz2')
        tarHandle.extractall( extractDir )

        self.fileExistsTest( extractDir + "/WMSandbox")
        self.fileExistsTest( extractDir + "/WMSandbox/WMWorkload.pkl")
        self.fileExistsTest( extractDir + "/WMSandbox/__init__.py")
        self.fileExistsTest( extractDir + "/WMSandbox/FirstTask/__init__.py")

        self.fileExistsTest( extractDir + "/WMSandbox/FirstTask/cmsRun1")
        self.fileExistsTest( extractDir + "/WMSandbox/FirstTask/stageOut1")
        self.fileExistsTest( extractDir + "/WMSandbox/FirstTask/cmsRun1/__init__.py")
        self.fileExistsTest( extractDir + "/WMSandbox/FirstTask/stageOut1/__init__.py")

        self.fileExistsTest( extractDir + "/WMSandbox/SecondTask/__init__.py")
        self.fileExistsTest( extractDir + "/WMSandbox/SecondTask/cmsRun2")
        self.fileExistsTest( extractDir + "/WMSandbox/SecondTask/stageOut2")
        self.fileExistsTest( extractDir + "/WMSandbox/SecondTask/cmsRun2/__init__.py")
        self.fileExistsTest( extractDir + "/WMSandbox/SecondTask/stageOut2/__init__.py")

        # make sure the sandbox is there
        self.fileExistsTest( extractDir + '/WMCore.zip')

        # now test that zipimport works
        # This gets replaced in setup/teardown
        sys.path.insert(0, os.path.join(extractDir, 'WMCore.zip'))
        os.system('ls -lah %s' % extractDir)
        # Gotta remove this since python caches subpackage folders in package.__path__
        del sys.modules['WMCore']
        if 'WMCore.ZipImportTestModule' in sys.modules:
            del sys.modules['WMCore.ZipImportTestModule']

        import WMCore.ZipImportTestModule
        sys.modules = copy.copy(self.modulesBackup)
        self.assertTrue( 'WMCore.zip' in WMCore.ZipImportTestModule.__file__ )

        # make sure the pickled file is the same
        pickleHandle = open( extractDir + "/WMSandbox/WMWorkload.pkl")
        pickledWorkload = pickle.load( pickleHandle )
        self.assertEqual( workload.data, pickledWorkload )
        self.assertEqual( pickledWorkload.sandbox, boxpath )

        #TODO:This test will be deprecated when task.data.input.sandbox property is removed
        for task in workload.taskIterator():
            for t in task.nodeIterator():
                t = WMTask.WMTaskHelper(t)
                self.assertEqual(t.data.input.sandbox, boxpath)

        pickleHandle.close()

        pickledWorkload.section_("test_section")
        self.assertNotEqual( workload.data, pickledWorkload )
        shutil.rmtree( extractDir )
        shutil.rmtree( tempdir )

    def fileExistsTest(self,file,msg = None):
        if (msg == None):
            msg = "Failed file existence test for (%s)" % file
        self.assertEquals(os.path.exists(file),True,msg)

    def setUp(self):
        # need to take a slice to make a real copy
        self.backupPath    = sys.path[:]
        self.modulesBackup = copy.copy(sys.modules)

    def tearDown(self):
        sys.path    = self.backupPath[:]
        sys.modules = copy.copy(self.modulesBackup)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
