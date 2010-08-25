'''
Created on Jun 18, 2009

@author: meloam
'''
import WMCore_t.WMSpec_t.samples.BasicProductionWorkload as testWorkloads
import WMCore.WMSpec.Steps.Templates.StageOut as StageOutTemplate
import WMCore.WMSpec.Steps.Executors.StageOut as StageOutExecutor
import WMCore.Storage.StageOutError as StageOutError
import unittest
import os
import tempfile
import time

class StageOutTest(unittest.TestCase):
        
    def setUp(self):
        # shut up SiteLocalConfig
        os.environ['CMS_PATH'] = os.getcwd()
        workload = testWorkloads.workload
        task = workload.getTask("Production")
        step = task.getStep("stageOut1")
        realstep = StageOutTemplate.StageOutStepHelper(step.data)
        realstep.disableRetries()
        self.realstep = realstep
        
    def testUnitTestBackend(self):
        executor = StageOutExecutor.StageOut()
        self.realstep.addFile("testin1", "testout1")
        # let's ride the win-train
        testOverrides = { "command" : "test-win",
            "option"  : "",
            "se-name" : "se-name",
            "lfn-prefix" : "I don't need a stinking prefix"}
        executor.execute( self.realstep.data, None,**testOverrides)
        
        # ride the fail whale, hope we get a fail wail.
        testOverrides["command"] = "test-fail"
        self.assertRaises(StageOutError.StageOutFailure,
                          executor.execute,
                          self.realstep.data,
                           None,
                            **testOverrides)
    
    def testCPBackend(self):
        executor = StageOutExecutor.StageOut()
        testOverrides = { "command" : "cp",
            "option"  : "",
            "se-name" : "se-name",
            "lfn-prefix" : ""}
        
        pfn = "/etc/hosts"
        lfn = "/tmp/stageOutTest-%s" % int(time.time())
        self.realstep.addFile(pfn, lfn)
        executor.execute( self.realstep.data, None,**testOverrides)
        self.assert_( os.path.exists(lfn) )
        os.remove(lfn)
            
    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()