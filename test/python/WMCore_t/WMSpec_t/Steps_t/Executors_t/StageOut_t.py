"""
Created on Jun 18, 2009

@author: meloam
"""
from __future__ import print_function

try:
    # https://pylint.pycqa.org/en/latest/technical_reference/features.html
    # W1626: the `reload` built-in function is missing in python3
    # we can use imp.reload (deprecated) or importlib.reload
    from importlib import reload
except:
    pass


import copy
import logging
import os
import os.path
import shutil
import sys
import threading
import time
import unittest

from nose.plugins.attrib import attr

import WMCore.Storage.StageOutError as StageOutError
import WMCore.WMSpec.Steps.Builders.CMSSW as CMSSWBuilder
import WMCore.WMSpec.Steps.Builders.StageOut as StageOutBuilder
import WMCore.WMSpec.Steps.Executors.StageOut as StageOutExecutor
import WMCore.WMSpec.Steps.StepFactory as StepFactory
import WMCore.WMSpec.Steps.Templates.StageOut as StageOutTemplate
import WMCore_t.WMSpec_t.samples.BasicProductionWorkload as testWorkloads
from WMCore.DataStructs.Job import Job
from WMCore.FwkJobReport.Report import Report
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMSpec.WMWorkload import newWorkload
from WMQuality.TestInit import TestInit


class StageOutTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()

        # shut up SiteLocalConfig
        os.environ['CMS_PATH'] = os.getcwd()
        os.environ['SITECONFIG_PATH'] = os.getcwd()
        workload = copy.deepcopy(testWorkloads.workload)
        task = workload.getTask("Production")
        step = task.getStep("stageOut1")
        # want to get the cmsstep so I can make the Report
        cmsstep = task.getStep('cmsRun1')
        self.cmsstepdir = os.path.join(self.testDir, 'cmsRun1')
        os.mkdir(self.cmsstepdir)
        open(os.path.join(self.cmsstepdir, '__init__.py'), 'w').close()
        open(os.path.join(self.cmsstepdir, 'Report.pkl'), 'w').close()

        cmsbuilder = CMSSWBuilder.CMSSW()
        cmsbuilder(cmsstep.data, 'Production', self.cmsstepdir)
        realstep = StageOutTemplate.StageOutStepHelper(step.data)
        realstep.disableRetries()
        self.realstep = realstep
        self.stepDir = os.path.join(self.testDir, 'stepdir')
        os.mkdir(self.stepDir)
        builder = StageOutBuilder.StageOut()
        builder(step.data, 'Production', self.stepDir)

        # stolen from CMSSWExecutor_t. thanks, dave

        # first, delete all the sandboxen and taskspaces
        #    because of caching, this leaks from other tests in other files
        #    this sucks because the other tests are using sandboxen that
        #    are deleted after the test is over, which causes theses tests
        #    to break
        modsToDelete = []
        # not sure what happens if you delete from
        # an arrey you're iterating over. doing it in
        # two steps
        for modname in sys.modules:
            # need to blow away things in sys.modules, otherwise
            # they are cached and we look at old taskspaces
            if modname.startswith('WMTaskSpace'):
                modsToDelete.append(modname)
            if modname.startswith('WMSandbox'):
                modsToDelete.append(modname)
        for modname in modsToDelete:
            try:
                reload(sys.modules[modname])
            except Exception:
                pass
            del sys.modules[modname]

        self.oldpath = sys.path[:]
        self.testInit = TestInit(__file__)

        self.testDir = self.testInit.generateWorkDir()
        self.job = Job(name="/UnitTests/DeleterTask/DeleteTest-test-job")
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
        self.stephelp = StageOutTemplate.StageOutStepHelper(stepHelper.data)
        self.task.applyTemplates()

        self.executor = StepFactory.getStepExecutor(self.stephelp.stepType())
        taskMaker = TaskMaker(self.workload, os.path.join(self.testDir))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        self.task.build(os.path.join(self.testDir, 'UnitTests'))

        sys.path.insert(0, self.testDir)
        sys.path.insert(0, os.path.join(self.testDir, 'UnitTests'))

        #        binDir = inspect.getsourcefile(ModuleLocator)
        #        binDir = binDir.replace("__init__.py", "bin")
        #
        #        if not binDir in os.environ['PATH']:
        #            os.environ['PATH'] = "%s:%s" % (os.environ['PATH'], binDir)
        open(os.path.join(self.testDir, 'UnitTests', '__init__.py'), 'w').close()
        shutil.copyfile(os.path.join(os.path.dirname(__file__), 'MergeSuccess.pkl'),
                        os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))

    def tearDown(self):
        sys.path = self.oldpath[:]
        self.testInit.delWorkDir()

        # making double sure WMTaskSpace and WMSandbox are gone
        modsToDelete = []
        # not sure what happens if you delete from
        # an arrey you're iterating over. doing it in
        # two steps
        for modname in sys.modules:
            # need to blow away things in sys.modules, otherwise
            # they are cached and we look at old taskspaces
            if modname.startswith('WMTaskSpace'):
                modsToDelete.append(modname)
            if modname.startswith('WMSandbox'):
                modsToDelete.append(modname)
        for modname in modsToDelete:
            try:
                reload(sys.modules[modname])
            except Exception:
                pass
            del sys.modules[modname]
        myThread = threading.currentThread()
        if hasattr(myThread, "factory"):
            myThread.factory = {}

    def makeReport(self, fileName):
        myReport = Report('oneitem')
        myReport.addStep('stageOut1')
        myReport.addOutputModule('module1')
        myReport.addOutputModule('module2')
        myReport.addOutputFile('module1', {'lfn': 'FILE1', 'size': 1, 'events': 1})
        myReport.addOutputFile('module2', {'lfn': 'FILE2', 'size': 1, 'events': 1})
        myReport.addOutputFile('module2', {'lfn': 'FILE3', 'size': 1, 'events': 1})
        myReport.persist(fileName)

    def testExecutorDoesntDetonate(self):
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        myReport.persist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))

        executor = StageOutExecutor.StageOut()

        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        executor.step = self.stepdata
        executor.execute()
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'hosts')))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'test1', 'hosts')))
        return

    def testUnitTestBackend(self):
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        myReport.persist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))

        executor = StageOutExecutor.StageOut()
        helper = StageOutTemplate.StageOutStepHelper(self.stepdata)
        helper.addOverride(override='command', overrideValue='test-win')
        helper.addOverride(override='option', overrideValue='')
        helper.addOverride(override='phedex-node', overrideValue='charlie.sheen.biz')
        helper.addOverride(override='lfn-prefix', overrideValue='test-win')

        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        executor.step = self.stepdata
        executor.execute()
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'hosts')))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'test1', 'hosts')))

    def testUnitTestBackendNew(self):
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        myReport.persist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))

        executor = StageOutExecutor.StageOut()
        helper = StageOutTemplate.StageOutStepHelper(self.stepdata)
        helper.addOverride(override='command', overrideValue='test-win')
        helper.addOverride(override='option', overrideValue='')
        helper.addOverride(override='phedex-node', overrideValue='charlie.sheen.biz')
        helper.addOverride(override='lfn-prefix', overrideValue='test-win')
        helper.setNewStageoutOverride(True)

        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        executor.step = self.stepdata
        executor.execute()
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'hosts')))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'test1', 'hosts')))

    def setLocalOverride(self, step):
        step.section_('override')
        step.override.command = 'cp'
        step.override.option = ''
        step.override.__setattr__('lfn-prefix', self.testDir + "/")
        step.override.__setattr__('phedex-node', 'DUMMYPNN')


class otherStageOutTexst(unittest.TestCase):

    def setUp(self):
        # stolen from CMSSWExecutor_t. thanks, dave

        # first, delete all the sandboxen and taskspaces
        #    because of caching, this leaks from other tests in other files
        #    this sucks because the other tests are using sandboxen that
        #    are deleted after the test is over, which causes theses tests
        #    to break
        modsToDelete = []
        # not sure what happens if you delete from
        # an arrey you're iterating over. doing it in
        # two steps
        for modname in sys.modules:
            # need to blow away things in sys.modules, otherwise
            # they are cached and we look at old taskspaces
            if modname.startswith('WMTaskSpace'):
                modsToDelete.append(modname)
            if modname.startswith('WMSandbox'):
                modsToDelete.append(modname)
        for modname in modsToDelete:
            try:
                reload(sys.modules[modname])
            except Exception:
                pass
            del sys.modules[modname]

        self.oldpath = sys.path[:]
        self.testInit = TestInit(__file__)

        self.testDir = self.testInit.generateWorkDir()
        self.job = Job(name="/UnitTests/DeleterTask/DeleteTest-test-job")
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
        self.stephelp = StageOutTemplate.StageOutStepHelper(stepHelper.data)
        self.task.applyTemplates()

        self.executor = StepFactory.getStepExecutor(self.stephelp.stepType())
        taskMaker = TaskMaker(self.workload, os.path.join(self.testDir))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        self.task.build(os.path.join(self.testDir, 'UnitTests'))

        sys.path.insert(0, self.testDir)
        sys.path.insert(0, os.path.join(self.testDir, 'UnitTests'))

        #        binDir = inspect.getsourcefile(ModuleLocator)
        #        binDir = binDir.replace("__init__.py", "bin")
        #
        #        if not binDir in os.environ['PATH']:
        #            os.environ['PATH'] = "%s:%s" % (os.environ['PATH'], binDir)
        open(os.path.join(self.testDir, 'UnitTests', '__init__.py'), 'w').close()
        shutil.copyfile(os.path.join(os.path.dirname(__file__), 'MergeSuccess.pkl'),
                        os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))

    def tearDown(self):
        sys.path = self.oldpath[:]
        self.testInit.delWorkDir()

        # making double sure WMTaskSpace and WMSandbox are gone
        modsToDelete = []
        # not sure what happens if you delete from
        # an arrey you're iterating over. doing it in
        # two steps
        for modname in sys.modules:
            # need to blow away things in sys.modules, otherwise
            # they are cached and we look at old taskspaces
            if modname.startswith('WMTaskSpace'):
                modsToDelete.append(modname)
            if modname.startswith('WMSandbox'):
                modsToDelete.append(modname)
        for modname in modsToDelete:
            try:
                reload(sys.modules[modname])
            except Exception:
                pass
            del sys.modules[modname]
        myThread = threading.currentThread()
        if hasattr(myThread, "factory"):
            myThread.factory = {}

    def testCPBackendStageOutAgainstReportNew(self):
        myReport = Report()
        reportPath = os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl')
        myReport.unpersist(reportPath)
        myReport.data.cmsRun1.status = 0
        myReport.persist(reportPath)
        executor = StageOutExecutor.StageOut()
        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        self.stepdata.override.newStageOut = True
        executor.step = self.stepdata
        # It should fail with:
        # AssertionError: LFN candidate: hosts doesn't match any of the following regular expressions:
        with self.assertRaises(AssertionError):
            executor.execute()

        # now fix those output file names to pass the Lexicon check, and execute it again
        myReport.unpersist(reportPath)
        # cmsRun1.output.FEVT.files.file0.lfn = 'hosts'
        # cmsRun1.output.ALCARECOStreamCombined.files.file0.lfn = '/test1/hosts'
        myReport.data.cmsRun1.output.FEVT.files.file0.lfn = "/store/mc/acqera/pd/FEVT/procstr/abc123.root"
        myReport.data.cmsRun1.output.ALCARECOStreamCombined.files.file0.lfn = "/store/mc/acqera/pd/ALCARECO/procstr/abc123.root"
        myReport.persist(reportPath)
        executor.execute()

        self.assertTrue(os.path.exists(os.path.join(self.testDir, "store", "mc", "acqera", "pd", "FEVT")))
        self.assertTrue(os.path.exists(os.path.join(self.testDir, "store", "mc", "acqera", "pd", "ALCARECO")))

    def testCPBackendStageOutAgainstReportFailedStepNew(self):
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        myReport.persist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        executor = StageOutExecutor.StageOut()
        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        self.stepdata.override.newStageOut = True
        executor.step = self.stepdata
        executor.execute()
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'hosts')))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'test1', 'hosts')))
        return

    def testCPBackendStageOutAgainstReportOld(self):

        myReport = Report()
        reportPath = os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl')
        myReport.unpersist(reportPath)
        myReport.data.cmsRun1.status = 0
        # print("myReport.data.cmsRun1: {}, dir: {}".format(myReport.data.cmsRun1, dir(myReport.data.cmsRun1)))
        myReport.persist(reportPath)
        executor = StageOutExecutor.StageOut()
        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        executor.step = self.stepdata
        # It should fail with:
        # AssertionError: LFN candidate: hosts doesn't match any of the following regular expressions:
        with self.assertRaises(AssertionError):
            executor.execute()

        # now fix those output file names to pass the Lexicon check, and execute it again
        myReport.unpersist(reportPath)
        # cmsRun1.output.FEVT.files.file0.lfn = 'hosts'
        # cmsRun1.output.ALCARECOStreamCombined.files.file0.lfn = '/test1/hosts'
        myReport.data.cmsRun1.output.FEVT.files.file0.lfn = "/store/mc/acqera/pd/FEVT/procstr/abc123.root"
        myReport.data.cmsRun1.output.ALCARECOStreamCombined.files.file0.lfn = "/store/mc/acqera/pd/ALCARECO/procstr/abc123.root"
        myReport.persist(reportPath)
        executor.execute()

        self.assertTrue(os.path.exists(os.path.join(self.testDir, "store", "mc", "acqera", "pd", "FEVT")))
        self.assertTrue(os.path.exists(os.path.join(self.testDir, "store", "mc", "acqera", "pd", "ALCARECO")))
        return

    def testCPBackendStageOutAgainstReportFailedStepOld(self):
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        myReport.persist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))

        executor = StageOutExecutor.StageOut()
        executor.initialise(self.stepdata, self.job)
        self.setLocalOverride(self.stepdata)
        executor.step = self.stepdata
        executor.execute()
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'hosts')))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'test1', 'hosts')))
        return

    @attr('workerNodeTest')
    def testOnWorkerNodes(self):
        raise RuntimeError
        # Stage a file out, stage it back in, check it, delete it
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        myReport.data.cmsRun1.status = 1
        del myReport.data.cmsRun1.output
        myReport.data.cmsRun1.section_('output')
        myReport.data.cmsRun1.output.section_('stagingTestOutput')
        myReport.data.cmsRun1.output.stagingTestOutput.section_('files')
        myReport.data.cmsRun1.output.stagingTestOutput.fileCount = 0
        targetFiles = ['/store/temp/WMAgent/storetest-%s' % time.time(),
                       '/store/unmerged/WMAgent/storetest-%s' % time.time()]

        for file in targetFiles:
            print("Adding file for StageOut %s" % file)
            self.addStageOutFile(myReport, file)

        myReport.persist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        executor = StageOutExecutor.StageOut()

        executor.initialise(self.stepdata, self.job)
        executor.step = self.stepdata
        print("beginning stageout")
        executor.execute()
        print("stageout done")

        # pull in the report with the stage out info
        myReport = Report()
        myReport.unpersist(os.path.join(self.testDir, 'UnitTests', 'WMTaskSpace', 'cmsRun1', 'Report.pkl'))
        print("Got the stage out data back")
        print(myReport.data)

        # now, transfer them back
        # TODO make a stagein step in the task - Melo
        import WMCore.Storage.FileManager as FileManagerModule
        fileManager = FileManagerModule.FileManager(numberOfRetries=10, retryPauseTime=1)
        for file in targetFiles:
            print("Staging in %s" % file)

            fileManager.stageOut(fileToStage={'LFN': file, 'PFN': '%s/%s' % (self.testDir, file)})
            self.assertTrue(os.path.exists('%s/%s' % (self.testDir, file)))
            # self.assertEqual(os.path.getsize('/etc/hosts', '%s/%s' % (self.testDir, file))) This makes no sense - EWV

        # now, should delete the files we made
        for file in targetFiles:
            print("deleting %s" % file)
            fileManager.deleteLFN(file)

        # try staging in again to make sure teh files are gone
        for file in targetFiles:
            print("Staging in (should fail) %s" % file)
            self.assertRaises(StageOutError,
                              FileManagerModule.FileManager.stageOut,
                              fileManager, fileToStage={'LFN': file,
                                                        'PFN': '%s/%s' % (self.testDir, file)},
                              stageOut=False)

        # need to make sure files didn't show up
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'hosts')))
        self.assertFalse(os.path.exists(os.path.join(self.testDir, 'test1', 'hosts')))

    def addStageOutFile(self, myReport, lfn):
        myId = myReport.data.cmsRun1.output.stagingTestOutput.fileCount
        mySection = myReport.data.cmsRun1.output.stagingTestOutput.section_('file%s' % myId)
        mySection.section_('runs')
        setattr(mySection.runs, '114475', [33])
        mySection.section_('branches')
        mySection.lfn = lfn
        mySection.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'Calo',
                             'processedDataset': 'Commissioning09-PromptReco-v8', 'dataTier': 'ALCARECO',
                             'applicationVersion': 'CMSSW_3_2_7'}
        mySection.module_label = 'ALCARECOStreamCombined'
        mySection.parents = []
        mySection.location = 'srm-cms.cern.ch'
        mySection.checksums = {'adler32': 'bbcf2215', 'cksum': '2297542074'}
        mySection.pfn = '/etc/hosts'
        mySection.events = 20000
        mySection.merged = False
        mySection.size = 37556367
        myReport.data.cmsRun1.output.stagingTestOutput.fileCount = myId + 1

    def setLocalOverride(self, step):
        step.section_('override')
        step.override.command = 'cp'
        step.override.option = ''
        step.override.__setattr__('lfn-prefix', self.testDir + "/")
        step.override.__setattr__('phedex-node', 'DUMMYPNN')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
