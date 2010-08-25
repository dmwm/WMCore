#!/usr/bin/env python
"""
    _SandboxCreator_t_
    
    Tests sandbox production
"""

__revision__ = "$Id: SandboxCreator_t.py,v 1.5 2010/04/09 20:36:41 sryu Exp $"
__version__ = "$Revision: 1.5 $"
import unittest
import WMCore.WMRuntime.SandboxCreator as SandboxCreator
import tempfile
import WMCore_t.WMSpec_t.TestWorkloads as TestWorkloads
import os.path
import tarfile
import pickle
import shutil
import WMCore.WMSpec.WMTask as WMTask

class SandboxCreator_t(unittest.TestCase):
    
    def testMakeSandbox(self):
        creator  = SandboxCreator.SandboxCreator()
        creator.disableWMCorePackaging()
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

        
        
        
    def testName(self):
        pass
    
    def fileExistsTest(self,file,msg = None):
        if (msg == None):
            msg = "Failed file existence test for (%s)" % file
        self.assertEquals(os.path.exists(file),True,msg)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()