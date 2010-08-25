#!/usr/bin/env python
# encoding: utf-8
"""
CMSSWExecutor_t.py

Created by Dave Evans on 2010-03-15.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import sys, os, inspect
from WMQuality.TestInit import TestInit

from WMCore.WMSpec.WMStep import WMStep
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.DataStructs.Job import Job

from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSW as CMSSWTemplate
from WMCore.WMSpec.Steps.Executors.CMSSW import CMSSW as CMSSWExecutor
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker


import WMCore_t.WMSpec_t.Steps_t as ModuleLocator


class CMSSWExecutor_t(unittest.TestCase):
    def setUp(self):
        """
        build a step for testing purposes
        
        """
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()
        
        self.workload = newWorkload("UnitTests")
        self.task = self.workload.newTask("CMSSWExecutor")
        stepHelper = step = self.task.makeStep("ExecutorTest")
        self.step = stepHelper.data
        template = CMSSWTemplate()
        template(self.step)
        self.helper = template.helper(self.step)
        self.step.application.setup.scramCommand = "scramulator.py"
        self.step.application.command.executable = "cmsRun.py"
        self.step.application.setup.scramProject = "CMSSW"
        self.step.application.setup.scramArch = "slc5_ia32_gcc434"
        self.step.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
        self.step.application.setup.softwareEnvironment = "echo \"Software Setup...\";"
        
        taskMaker = TaskMaker(self.workload, self.testDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()
        
        self.sandboxDir = "%s/UnitTests/CMSSWExecutor" % self.testDir
        
        self.task.build(self.testDir)
        sys.path.append(self.testDir)
        sys.path.append(self.sandboxDir)
        
        
        self.job = Job(name = "/UnitTest/CMSSWExecutor/ExecutorTest-test-job")
        
        binDir = inspect.getsourcefile(ModuleLocator)
        binDir = binDir.replace("__init__.py", "bin")

        if not binDir in os.environ['PATH']:
            os.environ['PATH'] = "%s:%s" % (os.environ['PATH'], binDir)
        
    def tearDown(self):
        """
        clean up
        """
        self.testInit.delWorkDir()
        sys.path.remove(self.testDir)
        sys.path.remove(self.sandboxDir)   
        
    def testA(self):
        """
        instantiate the executor
        """
    
        executor = CMSSWExecutor()
        
        executor.initialise(self.step, self.job)
        
        executor.pre()
        
        executor.execute()
        
        executor.post()
        
        print executor.report

    
if __name__ == '__main__':
    unittest.main()
