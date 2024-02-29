#!/bin/env python

"""
WorkerNode unittest for WMRuntime/WMSpec

"""
from __future__ import print_function

import os
import os.path
import random
import shutil
# Basic libraries
import unittest

import sys
# from WMCore.WMSpec.StdSpecs.ReReco  import rerecoWorkload, getTestArguments
from WMCore_t.WMSpec_t.TestSpec import createTestWorkload

import WMCore.WMRuntime.Bootstrap as Bootstrap
# DataStructs
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.DataStructs.Subscription import Subscription
from WMCore.FwkJobReport.Report import Report
# Factories
from WMCore.JobSplitting.Generators.GeneratorFactory import GeneratorFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
# Misc WMCore
from WMCore.Services.UUIDLib import makeUUID
# WMRuntime
from WMCore.WMRuntime.Unpacker import runUnpacker as RunUnpacker
# Builders
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
# Init junk
from WMQuality.TestInit import TestInit


def getListOfTasks(workload):
    """
    _getListOfTasks_

    Get a complete list of tasks in a workload
    Returns a list of task helpers
    """
    listOfTasks = []

    for primeTask in workload.taskIterator():
        for task in primeTask.taskIterator():
            listOfTasks.append(task)

    return listOfTasks


def miniStartup(thisDir=os.getcwd()):
    """
    This is an imitation of the startup script
    I don't want to try sourcing the main() of the startup
    Or run this in subprocess

    """
    Bootstrap.setupLogging(thisDir)
    job = Bootstrap.loadJobDefinition()
    task = Bootstrap.loadTask(job)
    Bootstrap.createInitialReport(job=job,
                                  reportName="Report.0.pkl")
    monitor = Bootstrap.setupMonitoring(logName="Report.0.pkl")

    task.build(thisDir)
    task.execute(job)

    task.completeTask(jobLocation=os.path.join(thisDir, 'WMTaskSpace'),
                      reportName="Report.0.pkl")

    if monitor.isAlive():
        monitor.shutdown()

    return


class RuntimeTest(unittest.TestCase):
    """
    _RuntimeTest_

    A unittest to test the WMRuntime/WMSpec/Storage/etc tree
    """

    def setUp(self):
        """
        Basic setUp

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testDir = self.testInit.generateWorkDir()

        # Random variables
        self.workloadDir = None
        self.unpackDir = None
        self.initialDir = os.getcwd()
        self.origPath = sys.path
        self.thisDirPath = os.path.dirname(__file__)

        # Create some dirs
        os.makedirs(os.path.join(self.testDir, 'packages'))

        return

    def tearDown(self):
        """
        _tearDown_

        Remove any references you put directly into the modules
        """

        self.testInit.delWorkDir()

        # Clean up imports
        if 'WMSandbox' in sys.modules.keys():
            del sys.modules['WMSandbox']
        if 'WMSandbox.JobIndex' in sys.modules.keys():
            del sys.modules['WMSandbox.JobIndex']

        return

    def setupTestWorkload(self, workloadName='Test', emulator=True):
        """
        Creates a test workload for us to run on, hold the basic necessities.
        """

        workloadDir = os.path.join(self.testDir, workloadName)

        # arguments = getTestArguments()

        # workload = rerecoWorkload("Tier1ReReco", arguments)
        # rereco = workload.getTask("ReReco")

        workload = createTestWorkload(emulation=emulator)
        rereco = workload.getTask("ReReco")

        # Set environment and site-local-config
        siteConfigPath = os.path.join(workloadDir, 'SITECONF/local/JobConfig/')
        if not os.path.exists(siteConfigPath):
            os.makedirs(siteConfigPath)
        shutil.copy(os.path.join(self.thisDirPath, 'site-local-config.xml'), siteConfigPath)
        shutil.copy(os.path.join(self.thisDirPath, 'storage.json'), os.path.join(siteConfigPath, '..'))
        environment = rereco.data.section_('environment')
        environment.CMS_PATH = workloadDir
        environment.SITECONFIG_PATH = os.path.join(workloadDir, 'SITECONF/local')

        taskMaker = TaskMaker(workload, workloadDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def unpackComponents(self, workload):
        """
        Run the unpacker to build the directories
        IMPORTANT NOTE:
          This is not how we do things on the worker node
          On the worker node we do not run multiple tasks
          So here we create multiple tasks in different directories
          To mimic running on multiple systems

        """

        listOfTasks = getListOfTasks(workload=workload)

        self.unpackDir = os.path.join(self.testDir, 'unpack')

        if not os.path.exists(self.unpackDir):
            os.makedirs(self.unpackDir)

        os.chdir(self.unpackDir)

        sandbox = workload.data.sandbox

        for task in listOfTasks:
            # We have to create a directory, unpack in it, and then get out
            taskName = task.name()
            taskDir = os.path.join(self.unpackDir, taskName)
            if not os.path.exists(taskDir):
                # Well then we have to make it
                os.makedirs(taskDir)
            os.chdir(taskDir)
            # Now that we're here, run the unpacker

            package = os.path.join(self.testDir, 'packages', '%sJobPackage.pkl' % (taskName))
            jobIndex = 1

            RunUnpacker(sandbox=sandbox, package=package,
                        jobIndex=jobIndex, jobname=taskName)

            # And go back to where we started
            os.chdir(self.unpackDir)

        os.chdir(self.initialDir)

        return

    def createWMBSComponents(self, workload):
        """
        Create the WMBS Components for this job

        """

        listOfTasks = []

        for primeTask in workload.taskIterator():
            for task in primeTask.taskIterator():
                listOfTasks.append(task)

        for task in listOfTasks:
            fileset = self.getFileset()
            sub = self.createSubscriptions(task=task,
                                           fileset=fileset)

        return

    def createSubscriptions(self, task, fileset):
        """
        Create a subscription based on a task


        """
        taskType = task.taskType()
        work = task.makeWorkflow()

        sub = Subscription(fileset=fileset,
                           workflow=work,
                           split_algo="FileBased",
                           type=taskType)

        package = self.createWMBSJobs(subscription=sub,
                                      task=task)

        packName = os.path.join(self.testDir, 'packages',
                                '%sJobPackage.pkl' % (task.name()))
        package.save(packName)

        return sub

    def createWMBSJobs(self, subscription, task):
        """
        Create the jobs for WMBS Components
        Send a subscription/task, get back a package.

        """

        splitter = SplitterFactory()
        geneFac = GeneratorFactory()
        jobfactory = splitter(subscription=subscription,
                              package="WMCore.DataStructs",
                              generators=geneFac.makeGenerators(task))
        params = task.jobSplittingParameters()
        jobGroups = jobfactory(**params)

        jobID = 1
        package = JobPackage()
        for group in jobGroups:
            for job in group.jobs:
                job['id'] = jobID
                jobID += 1
                package[job['id']] = job

        return package

    def getFileset(self):
        """
        Get a fileset based on the task

        """

        fileset = Fileset(name='Merge%s' % (type))

        for i in range(0, random.randint(15, 25)):
            # Use the testDir to generate a random lfn
            inpFile = File(lfn="%s/%s.root" % (self.testDir, makeUUID()),
                           size=random.randint(200000, 1000000),
                           events=random.randint(1000, 2000))
            inpFile.setLocation('Megiddo')
            fileset.addFile(inpFile)

        return fileset

    def runJobs(self, workload):
        """
        This might actually run the job.  Who knows?


        """
        listOfTasks = []

        for primeTask in workload.taskIterator():
            listOfTasks.append(primeTask)
            # Only run primeTasks for now

        for task in listOfTasks:
            jobName = task.name()
            taskDir = os.path.join(self.unpackDir, jobName, 'job')
            os.chdir(taskDir)
            sys.path.append(taskDir)

            # Scream, run around in panic, blow up machine
            print("About to run jobs")
            print(taskDir)
            # SITECONFIG_PATH is not available here so set it up so that site config can be loaded in Bootstrap.createInitialReport inside miniStartup
            os.environ['SITECONFIG_PATH'] = os.path.realpath(
                os.path.join(taskDir, '../../../basicWorkload/SITECONF/local'))
            miniStartup(thisDir=taskDir)
            # When exiting, go back to where you started
            os.chdir(self.initialDir)
            sys.path.remove(taskDir)

        return

    def testA_CreateWorkload(self):
        """
        _CreateWorkload_

        Create a workload
        Unpack the workload
        Check for consistency
        """

        workloadName = 'basicWorkload'
        workload = self.setupTestWorkload(workloadName=workloadName)

        self.createWMBSComponents(workload=workload)

        taskNames = []
        for task in getListOfTasks(workload=workload):
            taskNames.append(task.name())

        workloadPath = os.path.join(self.testDir, workloadName, "TestWorkload")
        siteConfigDir = os.path.join(self.testDir, workloadName, 'SITECONF/local/JobConfig/')

        # Pre-run checks

        # Does it have the right directories?
        dirList = os.listdir(workloadPath)
        self.assertCountEqual(dirList, ['WMSandbox', 'TestWorkload-Sandbox.tar.bz2'])
        dirList = os.listdir(os.path.join(workloadPath, 'WMSandbox'))
        for taskName in taskNames:
            self.assertTrue(taskName in dirList)

        # Do we have job packages
        for task in taskNames:
            self.assertTrue('%sJobPackage.pkl' % (task) in os.listdir(os.path.join(self.testDir, 'packages')))

        # Does it have the SITECONF?
        self.assertTrue('site-local-config.xml' in os.listdir(siteConfigDir))

        # Now actually see if you can unpack it.
        self.unpackComponents(workload=workload)

        # Check for proper unpacking
        # Check the the task has the right directories,
        # and that the PSetTweaks and WMSandbox directories
        # have the right contents
        taskContents = ['WMSandbox', 'WMCore', 'PSetTweaks']
        PSetContents = ['PSetTweak.pyc', 'CVS', 'PSetTweak.py',
                        '__init__.pyc', 'WMTweak.py', '__init__.py']
        taskSandbox = ['JobPackage.pcl', 'JobIndex.py', '__init__.py', 'WMWorkload.pkl']
        taskSandbox.extend(taskNames)  # Should have a directory for each task

        for task in taskNames:
            self.assertTrue(task in os.listdir(os.path.join(self.testDir, 'unpack')))
            taskDir = os.path.join(self.testDir, 'unpack', task, 'job')
            self.assertTrue(os.path.isdir(taskDir))
            self.assertEqual(os.listdir(taskDir).sort(), taskContents.sort())
            self.assertEqual(os.listdir(os.path.join(taskDir, 'WMSandbox')).sort(),
                             taskSandbox.sort())
            self.assertEqual(os.listdir(os.path.join(taskDir, 'PSetTweaks')).sort(),
                             PSetContents.sort())

        # And we're done.
        # Assume if we got this far everything is good

        # At the end, copy the directory
        # if os.path.exists('tmpDir'):
        #    shutil.rmtree('tmpDir')
        # shutil.copytree(self.testDir, 'tmpDir')

        return

    def testB_EmulatorTest(self):
        """
        _EmulatorTest_

        This is where things get scary.  We need to not only unpack the job,
        but also ascertain whether it can run locally in emulator mode.

        This requires...uh...emulator emulation.
        """

        # Assume all this works, because we tested it in testA
        workloadName = 'basicWorkload'
        workload = self.setupTestWorkload(workloadName=workloadName)

        self.createWMBSComponents(workload=workload)

        self.unpackComponents(workload=workload)

        self.runJobs(workload=workload)

        # Check the report
        taskDir = os.path.join(self.testDir, 'unpack/ReReco/job/WMTaskSpace')
        report = Report()
        report.load(os.path.join(taskDir, 'Report.0.pkl'))
        cmsReport = report.data.cmsRun1

        # Now validate the report
        self.assertEqual(report.getSiteName(), 'T1_US_FNAL')
        # self.assertEqual(report.data.hostName, socket.gethostname())
        self.assertTrue(report.data.completed)

        # Should have status 0 (emulator job)
        self.assertEqual(cmsReport.status, 0)

        # Should have one output module
        self.assertEqual(cmsReport.outputModules, ['TestOutputModule'])

        # It should have one file for input and output
        self.assertEqual(cmsReport.input.PoolSource.files.fileCount, 1)
        self.assertEqual(cmsReport.output.TestOutputModule.files.fileCount, 1)

        # So, um, I guess we're done

        # At the end, copy the directory
        # if os.path.exists('tmpDir'):
        #    shutil.rmtree('tmpDir')
        # shutil.copytree(self.testDir, 'tmpDir')

        return


if __name__ == "__main__":
    unittest.main()
