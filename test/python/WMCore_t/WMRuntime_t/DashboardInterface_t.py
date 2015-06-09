#!/usr/bin/env python
"""
_DashboardInterface_t_

Unit tests for the DashboardInterface module
"""

import unittest
import socket
import os
import os.path

from WMQuality.TestInit import TestInit

from WMCore.DataStructs.Job  import Job
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run  import Run
from WMCore.FwkJobReport.Report     import Report
from WMCore.WMRuntime.DashboardInterface import DashboardInfo, getUserProxyDN
from WMCore.WMBase import getTestBase

from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator

from nose.plugins.attrib import attr

class DashboardInterfaceTest(unittest.TestCase):
    """
    Test for the dashboard interface and its monitoring interaction

    Well, once I've written them it will be
    """


    def setUp(self):
        """
        Basically, do nothing

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()


        self.testDir = self.testInit.generateWorkDir()

        return


    def tearDown(self):
        """
        Clean up the test directory

        """

        self.testInit.delWorkDir()

        return


    def createWorkload(self):
        """
        Create a workload in order to test things

        """
        generator = WMSpecGenerator()
        workload = generator.createReRecoSpec("Tier1ReReco")
        return workload

    def createTestJob(self):
        """
        Create a test job to pass to the DashboardInterface

        """

        job = Job(name = "ThisIsASillyName")

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(1, *[46]))

        job.addFile(testFileA)
        job.addFile(testFileB)

        job['id'] = 1

        return job


    def createReport(self, outcome = 0):
        """
        Create a test report

        """

        jobReport = Report()
        jobReport.addStep('cmsRun1')
        jobReport.setStepStartTime(stepName = 'cmsRun1')
        jobReport.setStepStopTime(stepName = 'cmsRun1')
        if outcome:
            jobReport.addError('cmsRun1', 200, 'FakeError', 'FakeError')

        return jobReport


    def setupJobEnvironment(self, name = 'test'):
        """
        _setupJobEnvironment_

        Make some sort of environment in which to run tests
        """

        os.environ['WMAGENT_SITE_CONFIG_OVERRIDE'] = os.path.join(getTestBase(),
                                            "WMCore_t/Storage_t",
                                            "T1_US_FNAL_SiteLocalConfig.xml")
        return

    def testASuccessfulJobMonitoring(self):
        """
        _testASuccessfulJobMonitoring_

        Check that the data packets make sense when a job completes successfully
        """

        # Get the necessary objects
        name     = 'testA'
        job      = self.createTestJob()
        workload = self.createWorkload()
        task     = workload.getTask(taskName = "DataProcessing")
        report   = self.createReport()

        # Fill the job environment
        self.setupJobEnvironment(name = name)

        # Instantiate DBInfo
        dbInfo   = DashboardInfo(job = job, task = task)
        dbInfo.addDestination('127.0.0.1', 8884)

        # Check jobStart information
        data = dbInfo.jobStart()
        self.assertEqual(data['MessageType'], 'JobStatus')
        self.assertEqual(data['StatusValue'], 'running')
        self.assertEqual(data['StatusDestination'], "T1_US_FNAL")
        self.assertEqual(data['taskId'], 'wmagent_Tier1ReReco')

        # Do the first step
        step = task.getStep(stepName = "cmsRun1")

        # Do the step start
        data = dbInfo.stepStart(step = step.data)
        self.assertNotEqual(data['jobStart'], None)
        self.assertEqual(data['jobStart']['ExeStart'], step.name())
        self.assertEqual(data['jobStart']['WNHostName'], socket.gethostname())
        self.assertEqual(data['1_ExeStart'], step.name())

        #Do the step end
        data = dbInfo.stepEnd(step = step.data, stepReport = report)
        self.assertEqual(data['1_ExeEnd'], step.name())
        self.assertEqual(data['1_ExeExitCode'], 0)
        self.assertTrue(data['1_ExeWCTime'] >= 0)
        self.assertEqual(data['1_NCores'], 1)
        self.assertEqual(report.retrieveStep("cmsRun1").counter, 1)

        #Do a second step
        step = task.getStep(stepName = "cmsRun1")

        #Do the step start (It's not the first step)
        data = dbInfo.stepStart(step = step.data)
        self.assertEqual(data['jobStart'], None)
        self.assertEqual(data['2_ExeStart'], step.name())

        #Do the step end
        data = dbInfo.stepEnd(step = step.data, stepReport = report)
        self.assertEqual(data['2_ExeEnd'], step.name())
        self.assertEqual(data['2_ExeExitCode'], 0)
        self.assertTrue(data['2_ExeWCTime'] >= 0)
        self.assertEqual(data['2_NCores'], 1)
        self.assertEqual(report.retrieveStep("cmsRun1").counter, 2)

        # End the job!
        data = dbInfo.jobEnd()
        self.assertEqual(data['ExeEnd'], "cmsRun1")
        self.assertEqual(data['JobExitCode'], 0)
        self.assertEqual(data['WrapperCPUTime'], 0)
        self.assertTrue(data['WrapperWCTime'] >= 0)
        self.assertNotEqual(data['JobExitReason'], "")

        return

    def testMultithreadedApplication(self):
        """
        _testMultithreadedApplication_

        Check that the data packets have NCores and it picks it up successfully from the CMSSW step
        """

        # Get the necessary objects
        name     = 'testMT'
        job      = self.createTestJob()
        workload = self.createWorkload()
        task     = workload.getTask(taskName = "DataProcessing")
        report   = self.createReport()

        # Fill the job environment
        self.setupJobEnvironment(name = name)

        # Instantiate DBInfo
        dbInfo   = DashboardInfo(job = job, task = task)
        dbInfo.addDestination('127.0.0.1', 8884)

        # Modify the first step
        step = task.getStep(stepName = "cmsRun1")
        step.getTypeHelper().setMulticoreCores(8)

        # Check jobStart information
        data = dbInfo.jobStart()
        self.assertEqual(data['NCores'], 8)

        # Do the first step
        step = task.getStep(stepName = "cmsRun1")

        # Do the step start
        data = dbInfo.stepStart(step = step.data)

        #Do the step end
        data = dbInfo.stepEnd(step = step.data, stepReport = report)
        self.assertEqual(data['1_NCores'], 8)
        self.assertEqual(report.retrieveStep("cmsRun1").counter, 1)

        # End the job and test the final NCores report
        data = dbInfo.jobEnd()
        self.assertEqual(data['NCores'], 8)

        return



    def testAFailedJobMonitoring(self):
        """
        _TestAFailedJobMonitoring_

        Simulate a job that completes but fails, check that the data sent is
        correct
        """

        # Get the necessary objects
        name     = 'testB'
        job      = self.createTestJob()
        workload = self.createWorkload()
        task     = workload.getTask(taskName = "DataProcessing")
        report   = self.createReport(outcome = 1)

        # Fill the job environment
        self.setupJobEnvironment(name = name)

        # Instantiate DBInfo
        dbInfo   = DashboardInfo(job = job, task = task)
        dbInfo.addDestination('127.0.0.1', 8884)

        # Check jobStart information
        data = dbInfo.jobStart()
        self.assertEqual(data['MessageType'], 'JobStatus')
        self.assertEqual(data['StatusValue'], 'running')
        self.assertEqual(data['StatusDestination'], "T1_US_FNAL")
        self.assertEqual(data['taskId'], 'wmagent_Tier1ReReco')

        # Do the first step
        step = task.getStep(stepName = "cmsRun1")

        # Do the step start
        data = dbInfo.stepStart(step = step.data)
        self.assertNotEqual(data['jobStart'], None)
        self.assertEqual(data['jobStart']['ExeStart'], step.name())
        self.assertEqual(data['jobStart']['WNHostName'], socket.gethostname())
        self.assertEqual(data['1_ExeStart'], step.name())

        #Do the step end
        data = dbInfo.stepEnd(step = step.data, stepReport = report)
        self.assertEqual(data['1_ExeEnd'], step.name())
        self.assertNotEqual(data['1_ExeExitCode'], 0)
        self.assertTrue(data['1_ExeWCTime'] >= 0)
        self.assertEqual(report.retrieveStep("cmsRun1").counter, 1)

        # End the job!
        data = dbInfo.jobEnd()
        self.assertEqual(data['ExeEnd'], "cmsRun1")
        self.assertNotEqual(data['JobExitCode'], 0)
        self.assertEqual(data['WrapperCPUTime'], 0)
        self.assertTrue(data['WrapperWCTime'] >= 0)
        self.assertNotEqual(data['JobExitReason'].find('cmsRun1'), -1)

        return

    def testAKilledJobMonitoring(self):
        """
        _TestAKilledJobMonitoring_

        Simulate a job that is killed check that the data sent is
        correct
        """

        # Get the necessary objects
        name     = 'testC'
        job      = self.createTestJob()
        workload = self.createWorkload()
        task     = workload.getTask(taskName = "DataProcessing")
        report   = self.createReport(outcome = 1)

        # Fill the job environment
        self.setupJobEnvironment(name = name)

        # Instantiate DBInfo
        dbInfo   = DashboardInfo(job = job, task = task)
        dbInfo.addDestination('127.0.0.1', 8884)

        # Check jobStart information
        data = dbInfo.jobStart()
        self.assertEqual(data['MessageType'], 'JobStatus')
        self.assertEqual(data['StatusValue'], 'running')
        self.assertEqual(data['StatusDestination'], "T1_US_FNAL")
        self.assertEqual(data['taskId'], 'wmagent_Tier1ReReco')

        # Do the first step
        step = task.getStep(stepName = "cmsRun1")

        # Do the step start
        data = dbInfo.stepStart(step = step.data)
        self.assertNotEqual(data['jobStart'], None)
        self.assertEqual(data['jobStart']['ExeStart'], step.name())
        self.assertEqual(data['jobStart']['WNHostName'], socket.gethostname())
        self.assertEqual(data['1_ExeStart'], step.name())

        #Do the step end
        data = dbInfo.stepEnd(step = step.data, stepReport = report)
        self.assertEqual(data['1_ExeEnd'], step.name())
        self.assertNotEqual(data['1_ExeExitCode'], 0)
        self.assertTrue(data['1_ExeWCTime'] >= 0)

        # Kill the job!
        data = dbInfo.jobKilled()
        self.assertEqual(data['ExeEnd'], "cmsRun1")
        self.assertNotEqual(data['JobExitCode'], 0)
        self.assertEqual(data['WrapperCPUTime'], 0)
        self.assertTrue(data['WrapperWCTime'] >= 0)
        self.assertNotEqual(data['JobExitReason'].find('killed'), -1)

        return

    @attr('integration')
    def testGetDN(self):
        """
        _testGetDN_

        Checks that we can get a DN
        """
        dn = getUserProxyDN()
        if 'X509_USER_PROXY' in os.environ:
            self.assertNotEqual(dn, None, 'Error: This should get a DN, if you have set one')
        else:
            self.assertEqual(dn, None, 'Error: There is no proxy in the environment, it should not get one')


if __name__ == "__main__":
    unittest.main()
