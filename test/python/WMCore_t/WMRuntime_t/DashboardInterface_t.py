#!/usr/bin/env python
"""
_DashboardInterface_t_

"""

import threading
import logging
import unittest
import os

from WMQuality.TestInit import TestInit

# We need the DataStructs versions of these
from WMCore.DataStructs.Job  import Job
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run  import Run

# We're going to need one of these
from WMCore.WMSpec.StdSpecs.ReReco  import rerecoWorkload, getTestArguments
from WMCore.FwkJobReport.Report     import Report


from WMCore.WMRuntime.DashboardInterface import DashboardInfo
from WMCore.WMRuntime.Bootstrap          import setupMonitoring

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
        workload = rerecoWorkload("Tier1ReReco", getTestArguments())
        rereco = workload.getTask("DataProcessing")
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


    def createReport(self):
        """
        Create a test report

        """

        jobReport = Report()
        jobReport.addStep('cmsRun1')

        return jobReport


    def setupJobEnvironment(self, name = 'test'):
        """
        _setupJobEnvironment_

        Make some sort of environment in which to run tests
        """

        os.environ['CONDOR_JOBID']            = name
        os.environ['GLOBUS_GRAM_JOB_CONTACT'] = "https://%s:test" %(name) 



        return


    def DISABLEDtestA_createDashboardInfo(self):
        """
        _createDashboardInfo_

        Can we create the dashboardInfo and fill it with
        local information?
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


        # Check some defaults
        self.assertEqual(dbInfo.get('TaskType', None), task.taskType())
        self.assertEqual(dbInfo.get('User', None), 'sfoulkes@fnal.gov')
        self.assertEqual(dbInfo.get('JSTool', None), 'WMAgent')
        self.assertEqual(dbInfo.get('jobName', None),
                         'WMAgent_1_0_ThisIsASillyName')
        self.assertEqual(dbInfo.get('taskName', None),
                         'ProdAgent_-Tier1ReReco-DataProcessing_WMAgentPrimary')


        dbInfo.jobStart()

        self.assertEqual(dbInfo.get('GridJobID', None), name)
        self.assertEqual(dbInfo.get('SyncCE', None), name)


        # Do a step
        step = task.getStep(stepName = "cmsRun1")

        # Do the step start
        dbInfo.stepStart(step = step.data)
        self.assertEqual(dbInfo.get('ExeStart', None), step.name())
        self.assertEqual(dbInfo.get('ApplicationVersion', None),
                         'CMSSW_3_5_8')


        # Do the step end
        dbInfo.stepEnd(step = step.data, stepReport = report)
        self.assertEqual(dbInfo.get('ExeEnd', None), step.name())
        self.assertEqual(dbInfo.get('ExeExitStatus', None), False)




        # End the job!
        dbInfo.jobEnd()
        self.assertFalse(dbInfo.get('JobFinished', None) == None,
                         'Did not assign finish time in jobEnd()')

        return


    def DISABLEDtestB_TestMonitoring(self):
        """
        _TestMonitoring_

        See if you can run the whole monitoring system
        """

        # Get the necessary objects
        name     = 'testB'
        job      = self.createTestJob()
        workload = self.createWorkload()
        task     = workload.getTask(taskName = "DataProcessing")
        report   = self.createReport()

        # Fill the job environment
        self.setupJobEnvironment(name = name)

        step = task.getStep(stepName = "cmsRun1")


        monitor = setupMonitoring()
        myThread = threading.currentThread

        myThread.watchdogMonitor.setupMonitors(task = task,
                                               wmbsJob = job)
        myThread.watchdogMonitor.notifyJobStart(task)
        
        myThread.watchdogMonitor.notifyStepStart(step.data)
        myThread.watchdogMonitor.notifyStepEnd(step = step.data,
                                               stepReport = report)

        myThread.watchdogMonitor.notifyJobEnd(task)


        # Base a test on the idea that there's only one monitor
        mon = myThread.watchdogMonitor._Monitors[0]
        dbInfo = mon.dashboardInfo


        # Do some basic checks
        self.assertEqual(dbInfo.get('TaskType', None), task.taskType())
        self.assertEqual(dbInfo.get('User', None), 'sfoulkes@fnal.gov')
        self.assertEqual(dbInfo.get('JSTool', None), 'WMAgent')
        self.assertEqual(dbInfo.get('jobName', None),
                         'WMAgent_1_0_ThisIsASillyName')
        self.assertEqual(dbInfo.get('taskName', None),
                         'ProdAgent_-Tier1ReReco-DataProcessing_WMAgentPrimary')
        
        self.assertEqual(dbInfo.get('GridJobID', None), name)
        self.assertEqual(dbInfo.get('SyncCE', None), name)
        
        self.assertEqual(dbInfo.get('ExeStart', None), step.name())
        self.assertEqual(dbInfo.get('ApplicationVersion', None),
                         'CMSSW_3_5_8')


        return





if __name__ == "__main__":
    unittest.main()
