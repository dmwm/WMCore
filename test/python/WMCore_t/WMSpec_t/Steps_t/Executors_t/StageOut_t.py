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
        realstep.data.section_('stepSpace')
        self.realstep = realstep
        
    def testUnitTestBackend(self):
        executor = StageOutExecutor.StageOut()
        self.realstep.addFile("testin1", "testout1")
        # let's ride the win-train
        testOverrides = { "command" : "test-win",
            "option"  : "",
            "se-name" : "se-name",
            "lfn-prefix" : "I don't need a stinking prefix"}
        self.realstep.addOverride(override = 'command', overrideValue='test-win')
        self.realstep.addOverride(override = 'option', overrideValue='test-win')
        self.realstep.addOverride(override = 'se-name', overrideValue='test-win')
        self.realstep.addOverride(override = 'lfn-prefix', overrideValue='test-win')
        executor.step = self.realstep.data
        #executor.initialise( self.realstep.data, {'id': 1})
        executor.execute( )
        return
        # ride the fail whale, hope we get a fail wail.
        testOverrides["command"] = "test-fail"
        self.realstep.data.override = testOverrides  
        executor.step = self.realstep.data      
        self.assertRaises(StageOutError.StageOutFailure,
                          executor.execute)
    
    def CPBackend(self):
        executor = StageOutExecutor.StageOut()

        
        pfn = "/etc/hosts"
        lfn = "/tmp/stageOutTest-%s" % int(time.time())
        self.realstep.addFile(pfn, lfn)
        self.realstep.addOverride(override = 'command', overrideValue='cp')
        self.realstep.addOverride(override = 'option', overrideValue='')
        self.realstep.addOverride(override = 'se-name', overrideValue='se-name')
        self.realstep.addOverride(override = 'lfn-prefix', overrideValue='')
        executor.step = self.realstep        
        executor.execute( )
        self.assert_( os.path.exists(lfn) )
        os.remove(lfn)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()