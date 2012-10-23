'''
Created on May 22, 2009

@author: meloam
'''
import unittest
import WMCore_t.WMSpec_t.TestWorkloads as TestSpecs
import WMCore.WMSpec.WMWorkload
from WMCore.WMSpec.WMWorkload import getWorkloadFromTask
from WMCore.WMSpec.WMTask import  getTaskFromStep

class WMWorkloadTest(unittest.TestCase):

    def testOneTaskTwoSTep(self):
        workload = TestSpecs.oneTaskTwoStep()
        tasks = workload.listAllTaskNames()
        task = workload.getTask(tasks[0])
        findWorkload = getWorkloadFromTask(task)
        self.assertEqual(id(findWorkload.data), id(workload.data))
        self.assertTrue( isinstance( workload, WMCore.WMSpec.WMWorkload.WMWorkloadHelper ) )
        self.assertTrue( isinstance( task, WMCore.WMSpec.WMTask.WMTaskHelper ) )
        steps = task.listAllStepNames()
        step = task.getStep(steps[0])
        self.assertTrue( isinstance( step, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step2= task.getStep(steps[1])
        self.assertTrue( isinstance( step2, WMCore.WMSpec.WMStep.WMStepHelper ) )
        self.assertEqual( id(getTaskFromStep(step)), id(getTaskFromStep(step2)) )

    def testOneTaskFourStep(self):
        workload = TestSpecs.oneTaskFourStep()
        tasks = workload.listAllTaskNames()
        task = workload.getTask(tasks[0])
        findWorkload = getWorkloadFromTask(task)
        self.assertEqual(id(findWorkload.data), id(workload.data))
        self.assertTrue( isinstance( workload, WMCore.WMSpec.WMWorkload.WMWorkloadHelper ) )
        self.assertTrue( isinstance( task, WMCore.WMSpec.WMTask.WMTaskHelper ) )

        steps = task.listAllStepNames()
        step = task.getStep(steps[0])
        self.assertTrue( isinstance( step, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step2= task.getStep(steps[1])
        self.assertTrue( isinstance( step2, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step3= task.getStep(steps[2])
        self.assertTrue( isinstance( step3, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step4= task.getStep(steps[3])
        self.assertTrue( isinstance( step4, WMCore.WMSpec.WMStep.WMStepHelper ) )
        #print step4
        self.assertEqual( id(getTaskFromStep(step)), id(getTaskFromStep(step2)) )

    def testTwoTaskTree(self):
        workload = TestSpecs.twoTaskTree()
        tasks = workload.listAllTaskNames()
        parenttask = workload.getTask("FirstTask")

        findWorkload = getWorkloadFromTask(parenttask)
        self.assertEqual(id(findWorkload.data), id(workload.data))
        self.assertTrue( isinstance( workload, WMCore.WMSpec.WMWorkload.WMWorkloadHelper ) )
        self.assertTrue( isinstance( parenttask, WMCore.WMSpec.WMTask.WMTaskHelper ) )

        task = workload.getTask("SecondTask")
        #print task.data
        self.assertTrue( isinstance( task, WMCore.WMSpec.WMTask.WMTaskHelper ) )
        steps = task.listAllStepNames()

        # there should be a way to do this with iteration that would be neater
        step = task.getStep(steps[0])
        self.assertTrue( isinstance( step, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step2= task.getStep(steps[1])
        self.assertTrue( isinstance( step2, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step3= task.getStep(steps[2])
        self.assertTrue( isinstance( step3, WMCore.WMSpec.WMStep.WMStepHelper ) )
        step4= task.getStep(steps[3])
        self.assertTrue( isinstance( step4, WMCore.WMSpec.WMStep.WMStepHelper ) )
        self.assertEqual( id(getTaskFromStep(step)), id(getTaskFromStep(step2)) )


if __name__ == '__main__':
    unittest.main()
