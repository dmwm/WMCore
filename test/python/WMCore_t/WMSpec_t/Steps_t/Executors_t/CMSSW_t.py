#!/usr/bin/env python
# encoding: utf-8
"""
CMSSW_t.py

Test the CMSSW Executor and its associated Diagnostics object
under different simulated circumstances.

Created by Dave Evans on 2010-03-15.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import inspect
import os
import shutil
import sys
import unittest

import WMCore_t.WMSpec_t.Steps_t as ModuleLocator

from WMCore.DataStructs.Job import Job
from WMCore.FwkJobReport.Report import Report
from WMCore.WMBase import getTestBase
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMSpec.Steps import StepFactory
from WMCore.WMSpec.Steps.Executors.CMSSW import CMSSW as CMSSWExecutor
from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSW as CMSSWTemplate
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.WMSpec.WMWorkload import newWorkload
from WMQuality.TestInit import TestInit


class CMSSW_t(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Build a testing environment similar to a WN
        """
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()

        # Build a workload/task/step with the basic required information
        self.workload = newWorkload("UnitTests")
        self.task = self.workload.newTask("CMSSWExecutor")
        stepHelper = self.task.makeStep("ExecutorTest")
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
        self.step.output.jobReport = "FrameworkJobReport.xml"
        self.helper.addOutputModule("outputRECORECO", primaryDataset="Bogus",
                                    processedDataset="Test-Era-v1",
                                    dataTier="DATA")
        self.helper.addOutputModule("outputALCARECORECO", primaryDataset="Bogus",
                                    processedDataset="Test-Era-v1",
                                    dataTier="DATA")
        self.helper.setGlobalTag("Bogus")
        taskMaker = TaskMaker(self.workload, self.testDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        # Build the TaskSpace/StepSpace
        self.sandboxDir = os.path.join(self.testDir, "UnitTests")
        self.task.build(self.testDir)
        sys.path.append(self.testDir)
        sys.path.append(self.sandboxDir)

        # Copy the files that cmsRun would have generated in the step space
        open(os.path.join(self.step.builder.workingDir, "outputRECORECO.root"), "w").close()
        open(os.path.join(self.step.builder.workingDir, "outputALCARECORECO.root"), "w").close()
        shutil.copy(os.path.join(getTestBase(),
                                 "WMCore_t/FwkJobReport_t/CMSSWProcessingReport.xml"),
                    os.path.join(self.step.builder.workingDir, "FrameworkJobReport.xml"))

        # Create a job
        self.job = Job(name="/UnitTest/CMSSWExecutor/ExecutorTest-test-job")
        self.job["id"] = 1

        # Set the PATH
        binDir = inspect.getsourcefile(ModuleLocator)
        binDir = binDir.replace("__init__.py", "bin")

        if not binDir in os.environ['PATH']:
            os.environ['PATH'] = "%s:%s" % (os.environ['PATH'], binDir)

        self.oldCwd = os.getcwd()

    def tearDown(self):
        """
        _tearDown_

        Clean up
        """
        self.testInit.delWorkDir()
        sys.path.remove(self.testDir)
        sys.path.remove(self.sandboxDir)
        try:
            sys.modules.pop("WMTaskSpace")
            sys.modules.pop("WMTaskSpace.ExecutorTest")
            sys.modules.pop("WMTaskSpace.ExecutorTest.WMCore")
            sys.modules.pop("WMSandbox")
            sys.modules.pop("WMSandbox.CMSSWExecutor")
            sys.modules.pop("WMSandbox.CMSSWExecutor.ExecutorTest")
        except KeyError:
            return

        return

    def testA_Execute(self):
        """
        _Execute_

        Test the full path, including execute.
        """
        try:
            os.chdir(self.step.builder.workingDir)
            executor = CMSSWExecutor()
            executor.initialise(self.step, self.job)
            executor.pre()
            # Remove the preScripts, as it is not needed for this test
            executor.step.runtime.preScripts.remove("SetupCMSSWPset")
            executor.execute()
            executor.post()
            # Check that there were no errors
            self.assertEqual(0, executor.report.getExitCode())
            # Check that we processed the XML
            self.assertEqual(2, len(executor.report.getAllFiles()))
            # Check that the executable was really executed
            self.assertTrue(os.path.isfile(os.path.join(self.step.builder.workingDir, "BogusFile.txt")))
        except Exception as ex:
            self.fail("Failure encountered, %s" % str(ex))
        finally:
            os.chdir(self.oldCwd)
        return

    def testB_ExecuteNonZeroExit(self):
        """
        _ExecuteNonZeroExit_

        Test the execution of a script
        which exits with non-zero code.
        """
        self.step.application.command.executable = "brokenCmsRun.py"
        shutil.copy(os.path.join(getTestBase(),
                                 "WMCore_t/FwkJobReport_t/CMSSWFailReport.xml"),
                    os.path.join(self.step.builder.workingDir, "FrameworkJobReport.xml"))
        try:
            os.chdir(self.step.builder.workingDir)
            executor = StepFactory.getStepExecutor("CMSSW")
            executor.initialise(self.step, self.job)
            executor.pre()
            executor.step.runtime.preScripts.remove("SetupCMSSWPset")
            try:
                executor.execute()
                self.fail("An exception should have been raised")
            except WMExecutionFailure as ex:
                executor.diagnostic(ex.code, executor, ExceptionInstance=ex)
                self.assertEqual(8001, executor.report.getExitCode())
                report = Report()
                report.load("Report.pkl")
                self.assertEqual(8001, report.getExitCode())
        except Exception as ex:
            self.fail("Failure encountered, %s" % str(ex))
        finally:
            os.chdir(self.oldCwd)
        return

    def testC_ExecuteSegfault(self):
        """
        _ExecuteSegfault_

        Test the execution of a script
        which raises a ABRT signal which
        is the normal CMSSW response
        to a SEGFAULT.
        """
        self.step.application.command.executable = "test.sh"
        # CMSSW leaves an empty FWJR when a SEGFAULT is present
        open(os.path.join(self.step.builder.workingDir, "FrameworkJobReport.xml"), "w").close()
        try:
            os.chdir(self.step.builder.workingDir)
            executor = StepFactory.getStepExecutor("CMSSW")
            executor.initialise(self.step, self.job)
            executor.pre()
            executor.step.runtime.preScripts.remove("SetupCMSSWPset")
            try:
                executor.execute()
                self.fail("An exception should have been raised")
            except WMExecutionFailure as ex:
                executor.diagnostic(ex.code, executor, ExceptionInstance=ex)
                #self.assertEqual(50115, executor.report.getExitCode())
                self.assertEqual(134, executor.report.getExitCode())
                report = Report()
                report.load("Report.pkl")
                self.assertEqual(134, report.getExitCode())
        except Exception as ex:
            self.fail("Failure encountered, %s" % str(ex))
        finally:
            os.chdir(self.oldCwd)
        return

    def testD_ExecuteNoOutput(self):
        """
        _ExecuteNoOutput_

        Test what happens when no output is produced,
        the proper error should be included.
        """
        self.step.application.command.executable = "cmsRun.py"
        shutil.copy(os.path.join(getTestBase(),
                                 "WMCore_t/FwkJobReport_t/CMSSWSkippedAll.xml"),
                    os.path.join(self.step.builder.workingDir, "FrameworkJobReport.xml"))
        try:
            os.chdir(self.step.builder.workingDir)
            executor = StepFactory.getStepExecutor("CMSSW")
            executor.initialise(self.step, self.job)
            executor.pre()
            executor.step.runtime.preScripts.remove("SetupCMSSWPset")
            executor.execute()
            executor.post()
            self.assertEqual(60450, executor.report.getExitCode())
        except Exception as ex:
            self.fail("Failure encountered, %s" % str(ex))
        finally:
            os.chdir(self.oldCwd)
        return


if __name__ == '__main__':
    unittest.main()
