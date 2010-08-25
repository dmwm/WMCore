'''
Created on Jun 18, 2009

@author: meloam
'''
import WMCore_t.WMSpec_t.samples.BasicProductionWorkload as testWorkloads
import WMCore.WMSpec.Steps.Templates.StageOut as StageOutTemplate
import WMCore.WMSpec.Steps.Executors.StageOut as StageOutExecutor
import WMCore.WMSpec.Steps.Builders.StageOut  as StageOutBuilder
import WMCore.WMSpec.Steps.Builders.CMSSW  as CMSSWBuilder
import WMCore.WMSpec.Steps.Templates.CMSSW  as CMSSWTemplate
import logging
import WMCore.Storage.StageOutError as StageOutError
import unittest
import os
import tempfile
import time
import unittest
import shutil
import copy
from WMQuality.TestInit import TestInit

from WMCore.WMSpec.WMStep import WMStep
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.DataStructs.Job import Job

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
import WMCore.WMSpec.Steps.StepFactory as StepFactory
import WMCore.WMRuntime.TaskSpace as TaskSpace

import os.path
import sys
import inspect

import WMCore_t.WMSpec_t.Steps_t as ModuleLocator
from WMCore.FwkJobReport.Report             import Report
from WMCore.FwkJobReport.ReportEmu          import ReportEmu


#
class StageOutTest():
        
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()

        # shut up SiteLocalConfig
        os.environ['CMS_PATH'] = os.getcwd()
        workload = copy.deepcopy(testWorkloads.workload)
        task = workload.getTask("Production")
        step = task.getStep("stageOut1")
        # want to get the cmsstep so I can make the Report
        cmsstep = task.getStep('cmsRun1')
        self.cmsstepdir = os.path.join( self.testDir, 'cmsRun1')
        os.mkdir( self.cmsstepdir )
        open( os.path.join( self.cmsstepdir, '__init__.py'),'w').close()
        open( os.path.join( self.cmsstepdir, 'Report.pkl'),'w').close()

        cmsbuilder = CMSSWBuilder.CMSSW()
        cmsbuilder( cmsstep.data, 'Production', self.cmsstepdir )
        realstep = StageOutTemplate.StageOutStepHelper(step.data)
        realstep.disableRetries()
        self.realstep = realstep
        self.stepDir = os.path.join( self.testDir, 'stepdir')
        os.mkdir( self.stepDir )
        builder = StageOutBuilder.StageOut()
        builder( step.data, 'Production', self.stepDir)
        
        
    def makeReport(self, fileName):
        myReport = Report('oneitem')
        myReport.addStep('stageOut1')
        mod1 = myReport.addOutputModule('module1')
        mod2 = myReport.addOutputModule('module2')
        file1 = myReport.addOutputFile('module1', {'lfn': 'FILE1', 'size' : 1, 'events' : 1})
        file2 = myReport.addOutputFile('module2', {'lfn': 'FILE2', 'size' : 1, 'events' : 1})
        file3 = myReport.addOutputFile('module2', {'lfn': 'FILE3', 'size' : 1, 'events' : 1})
        myReport.persist( fileName )
            
    def tearDown(self):
        self.testInit.delWorkDir()        
        
    def atestUnitTestBackend(self):
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
#        # ride the fail whale, hope we get a fail wail.
#        testOverrides["command"] = "test-fail"
#        self.realstep.data.override = testOverrides  
#        executor.step = self.realstep.data      
#        self.assertRaises(StageOutError.StageOutFailure,
#                          executor.execute)    
        
    def setLocalOverride(self, step):
        step.section_('override')
        step.override.command    = 'cp'
        step.override.option     = ''
        step.override.__setattr__('lfn-prefix', self.testDir +"/")
        step.override.__setattr__('se-name','DUMMYSE')
        
    def atestCPBackendStageOutOld(self):
        executor = StageOutExecutor.StageOut()
        self.realstep.addFile("/etc/hosts", "OLDOUTPUTFILE")
        self.realstep.addFile("/etc/hosts", "/DUMMYDIR/OLDOUTPUTFILE2")
        self.setLocalOverride(self.realstep.data)
        executor.step = self.realstep.data
        executor.execute( )
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'OLDOUTPUTFILE')))
        # there isn'ta  leading / because os.path assumes you want an absolute path. lame
        self.assertTrue(os.path.exists( os.path.join(self.testDir,'DUMMYDIR', 'OLDOUTPUTFILE2')))
    
    def atestCPBackendStageOutNew(self):
        executor = StageOutExecutor.StageOut()
        self.realstep.addFile("/etc/hosts", "NEWOUTPUTFILE")
        self.realstep.addFile("/etc/hosts", "/DUMMYDIR/NEWOUTPUTFILE2")
        self.setLocalOverride(self.realstep.data)
        self.realstep.data.override.newStageOut = True
        executor.step = self.realstep.data
        executor.execute( )
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'NEWOUTPUTFILE')))
        # there isn'ta  leading / because os.path assumes you want an absolute path. lame
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'DUMMYDIR', 'NEWOUTPUTFILE2')))
        
class otherStageOutTest(unittest.TestCase):       

    def setUp(self):
        # stolen from CMSSWExecutor_t. thanks, dave
        self.oldpath = sys.path[:]
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()
        self.job = Job(name = "/UnitTests/DeleterTask/DeleteTest-test-job")
        shutil.copyfile('/etc/hosts', os.path.join(self.testDir, 'testfile'))
        
        self.workload = newWorkload("UnitTests")
        self.task = self.workload.newTask("DeleterTask")
        
        cmsswHelper = self.task.makeStep("cmsRun1")
        cmsswHelper.setStepType('CMSSW')
        stepHelper = cmsswHelper.addStep("DeleteTest")
        stepHelper.setStepType('StageOut')

        self.cmsswstep = cmsswHelper.data
        self.cmsswHelper = cmsswHelper        
        

        self.stepdata = stepHelper.data
        self.stephelp = stepHelper
        print "GO GREEN RANGER"
        self.task.applyTemplates()

        self.executor = StepFactory.getStepExecutor(self.stephelp.stepType())
        taskMaker = TaskMaker(self.workload, self.testDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()
        
        
        self.task.build(self.testDir)

        sys.path.append(self.testDir)
        sys.path.append(os.path.join(self.testDir, 'UnitTests'))
        print "path is sys.path %s" % sys.path

        
#        binDir = inspect.getsourcefile(ModuleLocator)
#        binDir = binDir.replace("__init__.py", "bin")
#
#        if not binDir in os.environ['PATH']:
#            os.environ['PATH'] = "%s:%s" % (os.environ['PATH'], binDir)
        
        shutil.copyfile( os.path.join( os.path.dirname( __file__ ), 'MergeSuccess.pkl'), 
                         os.path.join( self.testDir, 'WMTaskSpace', 'cmsRun1' , 'Report.pkl'))
            
    def tearDown(self):
        print "tearing down"
        sys.path = self.oldpath[:]
        self.stephelp = None
        self.stepdata = None
        self.workload = None
        self.task     = None
        self.stepdata = None
        self.executor = None
        self.cmsswHelper = None
        self.cmsswStep = None

        print dir(self)
    
     
    def testCPBackendStageOutAgainstReportNew(self):
        myReport = Report('cmsRun1')
        myReport.unpersist(os.path.join( self.testDir, 'WMTaskSpace', 'cmsRun1' , 'Report.pkl'))
        myReport.data.cmsRun1.status = 0
        myReport.persist(os.path.join( self.testDir, 'WMTaskSpace', 'cmsRun1' , 'Report.pkl'))
        executor = StageOutExecutor.StageOut()
        executor.initialise( self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        self.stepdata.override.newStageOut = True
        executor.step = self.stepdata
        print "I think testdir is %s" % self.testDir
        executor.execute( )
        self.assertTrue( os.path.exists( os.path.join( self.testDir, 'hosts' )))
        self.assertTrue( os.path.exists( os.path.join( self.testDir, 'test1', 'hosts')))
    
    def testCPBackendStageOutAgainstReportFailedStep(self):
        myReport = Report('cmsRun1')
        myReport.unpersist(os.path.join( self.testDir, 'WMTaskSpace', 'cmsRun1' , 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        myReport.persist(os.path.join( self.testDir, 'WMTaskSpace', 'cmsRun1' , 'Report.pkl'))
        
        executor = StageOutExecutor.StageOut()
        executor.initialise( self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        self.stepdata.override.newStageOut = True
        executor.step = self.stepdata
        executor.execute( )
        self.assertFalse( os.path.exists( os.path.join( self.testDir, 'hosts' )))
        self.assertFalse( os.path.exists( os.path.join( self.testDir, 'test1', 'hosts')))

    def setLocalOverride(self, step):
        step.section_('override')
        step.override.command    = 'cp'
        step.override.option     = ''
        step.override.__setattr__('lfn-prefix', self.testDir +"/")
        step.override.__setattr__('se-name','DUMMYSE')        
        
#    
#    def CPBackend(self):
#        executor = StageOutExecutor.StageOut()
#
#        
#        pfn = "/etc/hosts"
#        lfn = "/tmp/stageOutTest-%s" % int(time.time())
#        self.realstep.addFile(pfn, lfn)
#        self.realstep.addOverride(override = 'command', overrideValue='cp')
#        self.realstep.addOverride(override = 'option', overrideValue='')
#        self.realstep.addOverride(override = 'se-name', overrideValue='se-name')
#        self.realstep.addOverride(override = 'lfn-prefix', overrideValue='')
#        executor.step = self.realstep        
#        executor.execute( )
#        self.assert_( os.path.exists(lfn) )
#        os.remove(lfn)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()