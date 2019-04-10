'''
Created on Aug 10, 2010

@author: meloam
'''
import unittest
import shutil
from WMQuality.TestInit import TestInit

from WMCore.WMSpec.WMStep import WMStep
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.DataStructs.Job import Job

from WMCore.WMSpec.Steps.Templates.DeleteFiles import DeleteFiles as DeleteTemplate
from WMCore.WMSpec.Steps.Executors.DeleteFiles import DeleteFiles as DeleteExecutor
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
import WMCore.WMSpec.Steps.StepFactory as StepFactory

import os.path
import sys
import inspect
from nose.plugins.attrib import attr

import WMCore_t.WMSpec_t.Steps_t as ModuleLocator

class deleteFileTest(unittest.TestCase):


    def setUp(self):
        # stolen from CMSSWExecutor_t. thanks, dave
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()
        shutil.copyfile('/etc/hosts', os.path.join(self.testDir, 'testfile'))

        self.workload = newWorkload("UnitTests")
        self.task = self.workload.newTask("DeleterTask")
        stepHelper = step = self.task.makeStep("DeleteTest")
        self.step = stepHelper.data
        self.actualStep = stepHelper
        template = DeleteTemplate()
        template(self.step)
        self.helper = template.helper(self.step)
        self.executor = StepFactory.getStepExecutor(self.actualStep.stepType())

        taskMaker = TaskMaker(self.workload, self.testDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        self.sandboxDir = "%s/UnitTests" % self.testDir

        self.task.build(self.testDir)
        sys.path.insert(0, self.testDir)
        sys.path.insert(0, self.sandboxDir)


        self.job = Job(name = "/UnitTest/DeleterTask/DeleteTest-test-job")

        binDir = inspect.getsourcefile(ModuleLocator)
        binDir = binDir.replace("__init__.py", "bin")

        if not binDir in os.environ['PATH']:
            os.environ['PATH'] = "%s:%s" % (os.environ['PATH'], binDir)

    def tearDown(self):
        self.testInit.delWorkDir()
        sys.path.remove(self.testDir)
        sys.path.remove(self.sandboxDir)

    def setLocalOverride(self, step):
        step.section_('override')
        step.override.command    = 'cp'
        step.override.option     = ''
        step.override.__setattr__('lfn-prefix', '')
        step.override.__setattr__('phedex-node','DUMMYPNN')


    @attr('integration')
    def testManualDeleteOld(self):
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'testfile')))
        self.step.section_('filesToDelete')
        self.step.filesToDelete.file1 = os.path.join(self.testDir, 'testfile')
        self.setLocalOverride(self.step)
        self.executor.initialise(self.step, self.job)
        self.executor.execute()
        self.assertFalse(os.path.exists( os.path.join(self.testDir, 'testfile')))
        return

    @attr('integration')
    def testManualDeleteNew(self):
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'testfile')))
        self.step.section_('filesToDelete')
        self.step.filesToDelete.file1 = os.path.join(self.testDir, 'testfile')
        self.setLocalOverride(self.step)
        self.step.override.newStageOut = True
        self.executor.initialise(self.step, self.job)
        self.executor.execute()
        self.assertFalse(os.path.exists( os.path.join(self.testDir, 'testfile')))
        return

    @attr('integration')
    def testJobDeleteOld(self):
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'testfile')))
        self.setLocalOverride(self.step)
        self.job['input_files'] = [ {'lfn': os.path.join(self.testDir, 'testfile') } ]
        self.executor.initialise(self.step, self.job)
        self.executor.execute()
        self.assertFalse(os.path.exists( os.path.join(self.testDir, 'testfile')))
        return

    @attr('integration')
    def testJobDeleteNew(self):
        self.assertTrue(os.path.exists( os.path.join(self.testDir, 'testfile')))
        self.setLocalOverride(self.step)
        self.step.override.newStageOut = True
        self.job['input_files'] = [ {'lfn': os.path.join(self.testDir, 'testfile') } ]
        self.executor.initialise(self.step, self.job)
        self.executor.execute()
        self.assertFalse(os.path.exists( os.path.join(self.testDir, 'testfile')))
        return

if __name__ == "__main__":
    unittest.main()
