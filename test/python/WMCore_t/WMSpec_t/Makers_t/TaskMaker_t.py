#!/usr/bin/env python
"""
_TaskMaker Test_

Unittest for TaskMaker class

"""

import os
import os.path
import unittest
import threading
import tempfile

from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.WMInit import getWMBASE



class TaskMakerTest(unittest.TestCase):
    """
    TaskMaker test class

    """

    def setUp(self):

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        self.testDir = self.testInit.generateWorkDir()

        self.testFileName = os.path.join(self.testDir, 'dummyfile.pkl')

        return


    def tearDown(self):

        myThread = threading.currentThread()

        self.testInit.clearDatabase(modules = ['WMCore.WMBS'])

        self.testInit.delWorkDir()
        
        return


    def twoTaskTree(self):
        """
        Make a task tree workload
        """
        #Shamelessly stolen from testWorkloads.py

        workload = WMWorkloadHelper(WMWorkload("TwoTaskTree"))
        
        
        task1 = workload.newTask("FirstTask")
        
        task2 = task1.addTask("SecondTask")
        
        step1 = task1.makeStep("cmsRun1")
        step1.setStepType("CMSSW")
        
        step2 = step1.addStep("stageOut1")
        step2.setStepType("StageOut")
        
        step3 = task2.makeStep("cmsRun2")
        step3.setStepType("CMSSW")
        
        step4 = step3.addStep("stageOut2")
        step4.setStepType("StageOut")
        
        
        
        return workload


    def basicProdWorkflow(self):

        workload = {}

        return workload



    def testA(self):

        workload = WMWorkloadHelper(WMWorkload("workload1"))
        workload.newTask("Processing")
        workload.newTask("Merge")
        workload.getTask("Processing").makeStep("step1")
        workload.getTask("Processing").makeStep("step2")
        workload.getTask("Merge").makeStep("step3")
        workload.getTask("Merge").makeStep("step4")

        tempdir  = tempfile.mkdtemp()

        workload.save(self.testFileName)

        TM     = TaskMaker(self.testFileName, tempdir)
        result = TM.processWorkload()

        self.assertEqual(result, True)

        return

if __name__ == '__main__':
    unittest.main()
