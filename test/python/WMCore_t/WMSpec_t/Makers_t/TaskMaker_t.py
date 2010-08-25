#!/usr/bin/env python
"""
_TaskMaker Test_

Unittest for TaskMaker class

"""


__revision__ = "$Id: TaskMaker_t.py,v 1.3 2009/10/13 23:06:13 meloam Exp $"
__version__ = "$Revision: 1.3 $"

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

        self.testFileName = 'dummyfile.pkl'

        return


    def tearDown(self):

        myThread = threading.currentThread()
        
        if self._teardown:
            return

        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        
        self._teardown = True

        if os.path.exists(self.testFileName):
            os.remove(self.testFileName)

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


    def testB(self):

        myThread = threading.currentThread()

        tempdir  = tempfile.mkdtemp()

        if not os.path.exists('basicWorkload.pcl'):
            print "This test depends on a basicWorkload.pcl WMWorkload file"
            print "One can be created using WMSpec_t/BasicProductionWorkload.py"
            

        TM     = TaskMaker('basicWorkload.pcl', tempdir)
        result = TM.processWorkload()

        self.assertEqual(result, True)

        result = myThread.dbi.processData('SELECT * FROM wmbs_fileset')
        self.assertEqual(result[0].fetchall()[0][1].find('Processing') != -1, True)
        self.assertEqual(result[0].fetchall()[1][1].find('Merge')      != -1, True)


        result = myThread.dbi.processData('SELECT * FROM wmbs_subscription')
        self.assertEqual(len(result[0].fetchall()), 2)

        result = myThread.dbi.processData('SELECT * FROM wmbs_workflow')
        self.assertEqual(result[0].fetchall()[0][1].find('Processing') != -1, True)
        self.assertEqual(result[0].fetchall()[1][1].find('Merge')      != -1, True)

        return




if __name__ == '__main__':
    unittest.main()
