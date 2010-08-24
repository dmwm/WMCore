import os
import logging
logging.basicConfig(level = logging.DEBUG)
import unittest
import mox
import tempfile
import os.path
import WMCore_t.Storage_t.Plugins_t.PluginTestBase_t
from WMCore.Storage.Plugins.SRMV2Impl import SRMV2Impl as ourPlugin

import WMCore.Storage.Plugins.SRMV2Impl
moduleWeAreTesting = WMCore.Storage.Plugins.SRMV2Impl

import subprocess
from WMCore.WMBase import getWMBASE
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure

class popenMockHelper(object):
    def Popen(self,args,**kwargs):
        # an empty thing so I can mock this
        print "This should never be called if mox is working properly"
        pass


    
class SRMV2ImplTest(unittest.TestCase):
    
    def setUp(self):
        self.my_mox = mox.Mox()
        self.my_mox.StubOutWithMock(moduleWeAreTesting.os.path, 'getsize')
        self.my_mox.StubOutWithMock(moduleWeAreTesting,'runCommand')
        self.my_mox.StubOutWithMock(moduleWeAreTesting,'tempfile')
        self.popenMocker = self.my_mox.CreateMock(popenMockHelper)
        self.popenBackup = moduleWeAreTesting.Popen
        
        
        self.temporaryFiles = []
        self.rules          = []
    def tearDown(self):
        moduleWeAreTesting.Popen = self.popenBackup
        self.my_mox.UnsetStubs()
        for file in self.temporaryFiles:
            try:
                os.remove(file)
            except:
                pass
        
    def testFailSrmCopy(self): 

        # copy a file and have it fail
        
        # set up the SRM report
        (tempHandle, tempFilename) = tempfile.mkstemp()
        self.temporaryFiles.extend([tempFilename])
        fdObj = os.fdopen(tempHandle, 'w')
        # we get the exit status from the 3rd column in the output
        fdObj.write("Exit Status: 9001")
        fdObj.flush()
        os.fsync(tempHandle)
        moduleWeAreTesting.tempfile.mkstemp().AndReturn((tempHandle,tempFilename))


        # Actually run the test
        self.my_mox.ReplayAll()       
        testObject = ourPlugin()
        # copy normally and have it work
        self.assertRaises(StageOutFailure, testObject.doTransfer,'file:///store/NONEXISTANTSOURCE',
                              'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.my_mox.VerifyAll()
        
    def testFailOnFileSize(self): 

        # copy a file and have it succeed
        
        # set up the SRM report
        (tempHandle, tempFilename) = tempfile.mkstemp()
        self.temporaryFiles.extend([tempFilename])
        fdObj = os.fdopen(tempHandle, 'w')
        # we get the exit status from the 3rd column in the output
        fdObj.write("Exit Status: 0")
        fdObj.flush()
        os.fsync(tempHandle)

        moduleWeAreTesting.tempfile.mkstemp().AndReturn((tempHandle,tempFilename))
        os.path.getsize('/store/NONEXISTANTSOURCE').AndReturn(9001)
        
        # this stub will either pass popen calls to mock or run them normal
        # it eats rules to say either way (I just need to inject data to the
        # first bit of a pipeline)
        self.rules = [#first three lines are for the initial copy
                      #pass them through to popen so I can make sure the
                      # error code stuff works
                      'SKIP', # skip the cat
                      'SKIP', # skip the cut
                      'SKIP', # skip the grep
                      'DOIT', # process the srmls on the filesize] 
                      'SKIP',
                      'SKIP',
                      'SKIP'
                    ]
        def PopenStub(cmd, **kwargs):
            if self.rules:
                currentRule = self.rules.pop(0)
                if currentRule != 'SKIP':
                    return self.popenMocker.Popen(cmd, **kwargs)
                else:
                    return subprocess.Popen(cmd, **kwargs)
            else:
                return self.popenMocker.Popen(cmd,**kwargs)
        
        # intercept calls to Popen
        moduleWeAreTesting.Popen = PopenStub
        
        # stub out the stdout
        class tempPopenObjectType:
            stdout = tempfile.TemporaryFile()
        
        tempPopenObject = tempPopenObjectType()
        tempPopenObject.stdout.write('test test test\n')
        tempPopenObject.stdout.write('9002 /store/NONEXISTANTTARGET\n')
        tempPopenObject.stdout.write('test test test\n')
        tempPopenObject.stdout.seek(0)

        
        self.popenMocker.Popen(["srmls", '-recursion_depth=0','-retry_num=0',\
                                 'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET'],\
                         stdout=subprocess.PIPE).AndReturn(tempPopenObject)


        # Actually run the test
        self.my_mox.ReplayAll()       
        testObject = ourPlugin()
        # copy normally and have it work
        self.assertRaises(StageOutFailure, testObject.doTransfer,'file:///store/NONEXISTANTSOURCE',
                              'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.my_mox.VerifyAll()
    
    def testWin(self): 

        # copy a file and have it succeed
        
        # set up the SRM report
        (tempHandle, tempFilename) = tempfile.mkstemp()
        self.temporaryFiles.extend([tempFilename])
        fdObj = os.fdopen(tempHandle, 'w')
        # we get the exit status from the 3rd column in the output
        fdObj.write("Exit Status: 0")
        fdObj.flush()
        os.fsync(tempHandle)

        moduleWeAreTesting.tempfile.mkstemp().AndReturn((tempHandle,tempFilename))
        os.path.getsize('/store/NONEXISTANTSOURCE').AndReturn(9001)
        
        # this stub will either pass popen calls to mock or run them normal
        # it eats rules to say either way (I just need to inject data to the
        # first bit of a pipeline)
        self.rules = [#first three lines are for the initial copy
                      #pass them through to popen so I can make sure the
                      # error code stuff works
                      'SKIP', # skip the cat
                      'SKIP', # skip the cut
                      'SKIP', # skip the grep
                      'DOIT', # process the srmls on the filesize] 
                      'SKIP',
                      'SKIP',
                      'SKIP'
                    ]
        def PopenStub(cmd, **kwargs):
            if self.rules:
                currentRule = self.rules.pop(0)
                if currentRule != 'SKIP':
                    return self.popenMocker.Popen(cmd, **kwargs)
                else:
                    return subprocess.Popen(cmd, **kwargs)
            else:
                return self.popenMocker.Popen(cmd,**kwargs)
        
        # intercept calls to Popen
        moduleWeAreTesting.Popen = PopenStub
        
        # stub out the stdout
        class tempPopenObjectType:
            stdout = tempfile.TemporaryFile()
        
        tempPopenObject = tempPopenObjectType()
        tempPopenObject.stdout.write('test test test\n')
        tempPopenObject.stdout.write('9001 /store/NONEXISTANTTARGET\n')
        tempPopenObject.stdout.write('test test test\n')
        tempPopenObject.stdout.seek(0)

        
        self.popenMocker.Popen(["srmls", '-recursion_depth=0','-retry_num=0',\
                                 'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET'],\
                         stdout=subprocess.PIPE).AndReturn(tempPopenObject)


        # Actually run the test
        self.my_mox.ReplayAll()       
        testObject = ourPlugin()
        # copy normally and have it work
        newPfn = testObject.doTransfer('file:///store/NONEXISTANTSOURCE',
                              'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.assertEqual(newPfn, 'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET')
        self.my_mox.VerifyAll()
    
    def testMkdir(self):
        testObject = ourPlugin()
        self.my_mox.StubOutWithMock(testObject, 'runCommandWarnOnError')
        commandList = \
            [['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f/g/h'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f/g'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b'],
            ['srmls', '-recursion_depth=0', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f/g'],
            ['srmmkdir', '-retry_num=5', 'srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f/g/h']]
        for command in commandList:
            testObject.runCommandWarnOnError(command).AndReturn(('asd', 'SRM_FAILURE'))
#        def testFunc(args):
#            print args
#            return ("asd","SRM_FAILURE")
#        testObject.runCommandWarnOnError = testFunc
        self.my_mox.ReplayAll()       
        testObject.createOutputDirectory('srm://host:8443/srm/managerv2?SFN=/a/b/c/d/e/f/g/h/i',True)
        self.my_mox.VerifyAll()

if __name__ == "__main__":
    unittest.main()

