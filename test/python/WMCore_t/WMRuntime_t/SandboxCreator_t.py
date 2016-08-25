#!/usr/bin/env python
"""
    _SandboxCreator_t_

    Tests sandbox production
"""

import os
import os.path
import pickle
import shutil
import tarfile
import tempfile
import unittest

import WMCore_t.WMSpec_t.TestWorkloads as TestWorkloads

import WMCore.WMRuntime.SandboxCreator as SandboxCreator
import WMCore.WMSpec.WMTask as WMTask


class SandboxCreator_t(unittest.TestCase):

    def testMakeSandbox(self):
        creator  = SandboxCreator.SandboxCreator()
        workload = TestWorkloads.twoTaskTree()
        tempdir  = tempfile.mkdtemp()
        # test that the existing path is deleted else it will crash as in issue #5130
        os.makedirs('%s/%s/WMSandbox' % (tempdir, workload.name()))
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

        # EWV removed this code and related code that was mucking with sys.modules because it broke mock emulators
        # now test that zipimport works
        # This gets replaced in setup/teardown
        # sys.path.append(os.path.join(extractDir, 'WMCore.zip'))
        # os.system('ls -lah %s' % extractDir)
        # Gotta remove this since python caches subpackage folders in package.__path__
        # del sys.modules['WMCore']
        # if 'WMCore.ZipImportTestModule' in sys.modules:
        #     del sys.modules['WMCore.ZipImportTestModule']
        # import WMCore.ZipImportTestModule as zipImport
        # sys.modules = copy.copy(self.modulesBackup)
        # self.assertTrue( 'WMCore.zip' in zipImport.__file__ )
        # EWV: See if we can find a way to put similar code back in

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
        self.assertEqual(os.path.exists(file),True,msg)

    # EWV: No need for this code right now, but if we need to do this, this is the way rather than using tearDown()
    # def setUp(self):
    #     """
    #     Backup sys.path and sys.modules, add a cleanup to guarantee they restore
    #     """
    #
    #     # self.backupPath = copy.deepcopy(sys.path)
    #     # self.modulesBackup = copy.copy(sys.modules)
    #     # self.addCleanup(self.restorePathModules)
    #
    # def restorePathModules(self):
    #     """
    #     Restore sys.path and sys.modules from backups
    #     """
    #
    #     # sys.path = copy.deepcopy(self.backupPath)
    #     # sys.modules = copy.copy(self.modulesBackup)


if __name__ == "__main__":
    unittest.main()
