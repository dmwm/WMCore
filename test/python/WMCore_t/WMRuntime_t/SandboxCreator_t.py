#!/usr/bin/env python
"""
    _SandboxCreator_t_

    Tests sandbox production
"""

import os
import os.path
import pickle
import shutil
import subprocess
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

        # Test that zipimport works on the dummy module that SandboxCreator inserts
        output = subprocess.check_output(['python', '-m', 'WMCore.ZipImportTestModule'],
                                         env={'PYTHONPATH': os.path.join(extractDir, 'WMCore.zip')})
        self.assertIn(b'ZIPIMPORTTESTOK', output)

        # make sure the pickled file is the same
        pickleHandle = open( extractDir + "/WMSandbox/WMWorkload.pkl", "rb")
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

    def fileExistsTest(self, file, msg=None):
        if msg is None:
            msg = "Failed file existence test for (%s)" % file
        self.assertEqual(os.path.exists(file), True, msg)


if __name__ == "__main__":
    unittest.main()
