#!/usr/bin/env python
"""
    _SandboxCreator_t_
    
    Tests sandbox production
"""
import unittest
import WMCore.WMRuntime.SandboxCreator as SandboxCreator
import tempfile
import WMCore_t.WMSpec_t.TestWorkloads as TestWorkloads
import os.path
import tarfile
import pickle

class SandboxCreator_t(unittest.TestCase):
    
    def testMakeSandbox(self):
        creator = SandboxCreator.SandboxCreator()
        tempdir = tempfile.mkdtemp()
        print "using %s as tempdir" % tempdir
        workload = TestWorkloads.oneTaskTwoStep()
        task     = workload.getTask("FirstTask")
        boxpath = creator.makeSandbox(tempdir, workload, task)
        
        # extract our sandbox to test it
        extractDir = tempfile.mkdtemp()
        tarHandle  = tarfile.open(boxpath, 'r:bz2')
        tarHandle.extractall( extractDir )
        
        self.fileExistsTest( extractDir + "/WMSandbox")
        self.fileExistsTest( extractDir + "/WMSandbox/WMWorkload.pcl")
        self.fileExistsTest( extractDir + "/WMSandbox/__init__.py")
        self.fileExistsTest( extractDir + "/WMSandbox/cmsRun1")
        self.fileExistsTest( extractDir + "/WMSandbox/stageOut1")
        self.fileExistsTest( extractDir + "/WMSandbox/cmsRun1/__init__.py")
        self.fileExistsTest( extractDir + "/WMSandbox/stageOut1/__init__.py")
        
        # make sure the pickled file is the same
        pickleHandle = open( extractDir + "/WMSandbox/WMWorkload.pcl")
        pickledWorkload = pickle.load( pickleHandle )
        self.assertEqual( workload.data, pickledWorkload )
        pickleHandle.close()
        
        pickledWorkload.section_("test_section")
        self.assertNotEqual( workload.data, pickledWorkload )

        
        
        
    def testName(self):
        pass
    
    def fileExistsTest(self,file,msg = None):
        if (msg == None):
            msg = "Failed file existence test for (%s)" % file
        self.assertEquals(os.path.exists(file),True,msg)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()