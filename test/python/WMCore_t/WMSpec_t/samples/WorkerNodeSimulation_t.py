#!/bin/env python

"""
WorkerNode unittest for WMRuntime/WMSpec

"""

__revision__ = "$Id: WorkerNodeSimulation_t.py,v 1.4 2010/04/14 16:53:54 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

# Basic libraries
import unittest
import threading
import logging
import os
import os.path
import shutil
import re
import random
import inspect
import sys

# Init junk
from WMQuality.TestInit import TestInit
from WMCore.WMInit      import getWMBASE

# Builders
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMSpec.StdSpecs.ReReco  import rerecoWorkload, outputModule
from WMCore.DataStructs.JobPackage  import JobPackage

# Factories
from WMCore.JobSplitting.Generators.GeneratorFactory import makeGenerators
from WMCore.JobSplitting.SplitterFactory             import SplitterFactory


# DataStructs
from WMCore.DataStructs.File         import File
from WMCore.DataStructs.Fileset      import Fileset
from WMCore.DataStructs.Workflow     import Workflow
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.JobGroup     import JobGroup
from WMCore.DataStructs.Job          import Job


#WMRuntime
from WMCore.WMRuntime.Unpacker import runUnpacker as RunUnpacker
import WMCore.WMRuntime.Bootstrap as Bootstrap

# Misc WMCore
from WMCore.DataStructs.Run     import Run
from WMCore.Services.UUID       import makeUUID
from WMCore.FwkJobReport.Report import Report


# DBS
from DBSAPI.dbsApi import DbsApi


def miniStartup(dir = os.getcwd()):
    """
    This is the startup script
    Because I don't want to try and source main

    """

    job = Bootstrap.loadJobDefinition()
    task = Bootstrap.loadTask(job)
    monitor = Bootstrap.setupMonitoring()

    Bootstrap.setupLogging(dir)

    task.build(dir)

    task.execute(job)

    if monitor.isAlive():
        monitor.shutdown()


    return





class basicWNTest(unittest.TestCase):
    """
    This serves as a test for what you should encounter on the
    WorkerNode.

    """

    # This is an integration test
    __integration__ = "Any old bollocks"


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
        self.unpackDir   = None
        self.initialDir  = os.getcwd()
        self.origPath    = sys.path

        return


    def tearDown(self):
        """
        Basic tearDown

        """

        self.testInit.delWorkDir()

        # Clean up imports
        if 'WMSandbox' in sys.modules.keys():
                del sys.modules['WMSandbox']
        if 'WMSandbox.JobIndex' in sys.modules.keys():
                del sys.modules['WMSandbox.JobIndex']
        

        return


    def createWorkload(self, workloadName = 'basicWorkload', emulator = True):
        """
        Create a basic WMWorkload for testing purposes.

        """

        workloadDir = os.path.join(self.testDir, workloadName)
        self.workloadDir = workloadDir
        
        if not os.path.isdir(workloadDir):
            os.makedirs(workloadDir)
        


        # Create a new workload using StdSpecs.ReReco
        arguments = {
            "OutputTiers" : ['RECO', 'ALCARECO', 'AOD'],
            "AcquisitionEra" : "Teatime09",
            "GlobalTag" :"GR09_P_V7::All",
            "LFNCategory" : "/store/data",
            "ProcessingVersion" : "v99",
            "Scenario" : "cosmics",
            "CMSSWVersion" : "CMSSW_3_3_5_patch3",
            "InputDatasets" : "/MinimumBias/BeamCommissioning09-v1/RAW",
            "Emulate" : emulator,
            }

        workload = rerecoWorkload("Tier1ReReco", arguments)
        rereco = workload.getTask("ReReco")

        # Set monitoring
        monitoring  = rereco.data.section_('watchdog')
        monitoring.monitors = ['WMRuntimeMonitor', 'TestMonitor']
        monitoring.section_('TestMonitor')
        monitoring.TestMonitor.connectionURL = "dummy.cern.ch:99999/CMS"
        monitoring.TestMonitor.password      = "ThisIsTheWorld'sStupidestPassword"
        monitoring.TestMonitor.softTimeOut   = 30000
        monitoring.TestMonitor.hardTimeOut   = 60000

        # Set environment and site-local-config
        siteConfigPath = '%s/SITECONF/local/JobConfig/' %(workloadDir)
        if not os.path.exists(siteConfigPath):
            os.makedirs(siteConfigPath)
        shutil.copy('site-local-config.xml', siteConfigPath)
        environment = rereco.data.section_('environment')
        environment.CMS_PATH = workloadDir

        for primeTask in workload.taskIterator():
            for task in primeTask.taskIterator():
                task.setSplittingAlgorithm("FileBased", files_per_job = 1)
        
        taskMaker = TaskMaker(workload, workloadDir)
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        return workload



    def createDBSFileset(self, dataset = "/store/data/BeamCommissioning09/MinimumBias/RAW/*"):
        """
        Get list of files from DBS, create files for them,
        and put them into DBS

        """

        dbs = DbsApi({"url" : "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
                      "version" : "DBS_2_0_8" })

        fileset = Fileset(name = 'SkimFiles')

        listOfFiles = dbs.listFiles(patternLFN = dataset,
                                    retriveList = ['retrive_lumi', 'retrive_run'])


        if len(listOfFiles) > 20:
            listOfFiles = listOfFiles[:20]

        for dbsF in listOfFiles:
            runs = {}
            for lumi in dbsF['LumiList']:
                runNum = lumi['RunNumber']
                if runNum not in runs.keys():
                    runs[runNum] = Run(runNum)
                runs[runNum].lumis.append(lumi['LumiSectionNumber'])



            wmbsF = File(lfn = dbsF['LogicalFileName'],
                         size = dbsF['FileSize'],
                         events = dbsF['NumberOfEvents'])
            for run in runs.values():
                wmbsF.addRun(run)
            fileset.addFile(wmbsF)
            
        return fileset


    def createMergeFileset(self, task, rereco):
        """
        Given the rereco task and the task in question
        Generate a fileset with proper naming

        """

        # This is sort of awkward
        type    = task.name().split('Merge')[1]
        helper  = rereco.getStep('cmsRun1').getTypeHelper()
        output  = helper.getOutputModule(outputModule(key = type.upper()))
        lfnBase = output.lfnBase

        fileset = Fileset(name = 'Merge%s' %(type))


        for i in range(0, random.randint(15,25)):
            inpFile = File(lfn = "%s/%s.root" %(lfnBase, makeUUID()),
                           size = random.randint(200000, 1000000),
                           events = random.randint(1000,2000) )


        return fileset

        



    def getFileset(self, task, rereco):
        """
        Get a fileset based on the task

        """

        if task.getPathName() == rereco.getPathName():
            # Then we have the rereco prime task
            return self.createDBSFileset()

        # If we're a merge job, create a merge fileset
        if task.taskType() == 'Merge':
            return self.createMergeFileset(task = task, rereco = rereco)


        else:
            # Well, then I don't know what to do
            return Fileset(name = 'Empty')






    def createWMBSComponents(self, workload):
        """
        Create the WMBS Components for this job

        """

        listOfTasks = []
        listOfSubs  = []

        rerecoTask  = None
        
        for primeTask in workload.taskIterator():
            # There should only be one prime task, and it should be the rerecoTask
            rerecoTask = primeTask
            for task in primeTask.taskIterator():
                listOfTasks.append(task)

        for task in listOfTasks:
            fileset = self.getFileset(task = task, rereco = rerecoTask)
            sub = self.createSubscriptions(task = task,
                                           fileset = fileset)
            listOfSubs.append(sub)

        

        return


    def createSubscriptions(self, task, fileset):
        """
        Create a subscription based on a task


        """
        type = task.taskType()
        work = task.makeWorkflow()
        
        sub = Subscription(fileset = fileset,
                           workflow = work,
                           split_algo = "FileBased",
                           type = type)

        package = self.createWMBSJobs(subscription = sub,
                                      task = task)        

        packName = os.path.join(self.workloadDir,
                                '%sJobPackage.pkl' %(task.name()))
        package.save(packName)

        return sub


    def createWMBSJobs(self, subscription, task):
        """
        Create the jobs for WMBS Components
        Send a subscription/task, get back a package.

        """

        splitter = SplitterFactory()
        jobfactory = splitter(subscription = subscription,
                              package = "WMCore.DataStructs",
                              generators = makeGenerators(task))
        params = task.jobSplittingParameters()
        jobGroups = jobfactory(**params)

        package = JobPackage()
        for group in jobGroups:
            package.extend(group.getJobs())

        return package



    def unpackComponents(self, workload):
        """
        Run the unpacker to build the directories
        IMPORTANT NOTE:
          This is not how we do things on the worker node
          On the worker node we do not run multiple tasks
          So here we create multiple tasks in different directories
          To mimic running on multiple systems

        """

        listOfTasks = []

        self.unpackDir = os.path.join(self.testDir, 'unpack')

        if not os.path.exists(self.unpackDir):
            os.makedirs(self.unpackDir)

        os.chdir(self.unpackDir)

        for primeTask in workload.taskIterator():
            for task in primeTask.taskIterator():
                listOfTasks.append(task)

        sandbox  = workload.data.sandbox

        for task in listOfTasks:
            # We have to create a directory, unpack in it, and then get out
            jobName = task.name()
            taskDir = os.path.join(self.unpackDir, jobName)
            if not os.path.exists(taskDir):
                # Well then we have to make it
                os.makedirs(taskDir)
            os.chdir(taskDir)
            # Now that we're here, run the unpacker

            package  = os.path.join(self.workloadDir, '%sJobPackage.pkl' %(jobName))
            jobIndex = 0

            RunUnpacker(sandbox = sandbox, package = package,
                        jobIndex = jobIndex, jobname = jobName)

            # And go back to where we started
            os.chdir(self.unpackDir)


        os.chdir(self.initialDir)

        return



    def runJobs(self, workload):
        """
        This might actually run the job.  Who knows?


        """

        listOfTasks = []

        for primeTask in workload.taskIterator():
            listOfTasks.append(primeTask)
            for task in primeTask.taskIterator():
                #listOfTasks.append(task)
                # For now, only run prime task
                pass


        for task in listOfTasks:
            jobName = task.name()
            taskDir = os.path.join(self.unpackDir, jobName, 'job')
            os.chdir(taskDir)
            sys.path.append(taskDir)

            # Scream, run around in panic, blow up machine
            print "About to run jobs"
            print taskDir
            miniStartup(dir = taskDir)
            

            # When exiting, go back to where you started
            os.chdir(self.initialDir)
            sys.path.remove(taskDir)
            
        return

        



    def testA_ComponentTest(self):
        """
        Test whether the components work

        """

        #return

        workloadName = 'basicWorkload'

        workload = self.createWorkload(workloadName = workloadName)

        self.createWMBSComponents(workload = workload)

        workloadDir   = os.path.join(self.testDir, workloadName)
        siteConfigDir = os.path.join(workloadDir, 'SITECONF/local/JobConfig/')
        # A list of files we expect in the workloadDir
        listOfWorkloadDirFiles = ['SITECONF', 'Tier1ReReco',
                                  'Tier1ReReco-Sandbox.tar.bz2',
                                  'ReRecoJobPackage.pkl',
                                  'MergeRecoJobPackage.pkl',
                                  'MergeAlcaRecoJobPackage.pkl',
                                  'MergeAodJobPackage.pkl']

        # Check setup
        self.assertTrue(os.path.isdir(workloadDir))
        self.assertEqual(os.listdir(workloadDir), listOfWorkloadDirFiles)
        self.assertTrue('site-local-config.xml' in os.listdir(siteConfigDir))

        print os.listdir(workloadDir)

        self.unpackComponents(workload = workload)

        sandboxContents = ['stageOut1', 'logArch1', '__init__.py']
        # 'WMWorkload.pkl', 'JobPackage.pcl', 'JobIndex.py']
        WMCoreContents  = os.listdir(os.path.join(getWMBASE(), 'src/python/WMCore'))
        PSetContents    = ['CVS', '__init__.pyc', 'PSetTweak.py',
                           'WMTweak.py', '__init__.py', 'PSetTweak.pyc']

        self.assertEqual(os.listdir(self.unpackDir),
                         ['ReReco', 'MergeReco', 'MergeAlcaReco', 'MergeAod'])
        for dir in os.listdir(self.unpackDir):
            taskPath = os.path.join(self.unpackDir, dir, 'job')
            self.assertEqual(os.listdir(taskPath), ['WMSandbox', 'WMCore', 'PSetTweaks'])
            for item in sandboxContents:
                # All the files in sandboxContents should be here
                print "printing item"
                print item
                print os.listdir(os.path.join(taskPath, 'WMSandbox', dir))
                self.assertTrue(item in os.listdir(os.path.join(taskPath, 'WMSandbox', dir)))
            # WMCore should be the same as WMCore
            self.assertEqual(os.listdir(os.path.join(taskPath, 'WMCore')), WMCoreContents)
            self.assertEqual(os.listdir(os.path.join(taskPath, 'PSetTweaks')),
                             PSetContents)


        if os.path.exists('tmpDir'):
            shutil.rmtree('tmpDir')

        shutil.copytree(self.testDir, os.path.join(os.getcwd(), 'tmpDir'))
            
            


        return


    def testB_EmulatorTest(self):
        """
        It runs through a set of tests to see what they do with the emulator

        """

        return
        
        workloadName = 'basicWorkload'

        workload = self.createWorkload(workloadName = workloadName)

        self.createWMBSComponents(workload = workload)

        self.unpackComponents(workload = workload)

        self.runJobs(workload = workload)

        # Check what came out
        rerecoDir = os.path.join(self.unpackDir, 'ReReco', 'job')
        self.assertTrue('WMTaskSpace' in os.listdir(rerecoDir))
        self.assertEqual(len(os.listdir(rerecoDir)), 5)
        wmTaskDir = os.path.join(rerecoDir, 'WMTaskSpace')
        # Did we get the right files in the WMTaskSpace
        self.assertEqual(os.listdir(wmTaskDir), ['__init__.py', 'cmsRun1', 'stageOut1',
                                                 'logArch1', '__init__.pyc', 'Report.pkl'])
        # Get rid of the ReportEmuTestFile
        os.remove(os.path.join(wmTaskDir, 'cmsRun1', 'ReportEmuTestFile.txt'))
        for dir in ['cmsRun1', 'logArch1', 'stageOut1']:
            path = os.path.join(wmTaskDir, dir)
            self.assertEqual(os.listdir(path), ['__init__.py','__init__.pyc', 'Report.pkl'])

        report = Report()
        report.load(os.path.join(wmTaskDir, 'cmsRun1', 'Report.pkl'))
        reportCMSRun = report.data.cmsRun1

        # Parse the report
        self.assertTrue(os.path.isabs(reportCMSRun.output.outputRECORECO.files.file0.PFN))
        self.assertEqual(reportCMSRun.output.outputRECORECO.files.file0.LFN,
                         "/store/unmerged/Teatime09/MinimumBias/RECO/rereco_GR09_P_V7_All_v99/ThisIsGUID.root")
        self.assertEqual(reportCMSRun.output.outputRECORECO.files.file0.datasetPath,
                         'MinimumBias/rereco_GR09_P_V7_All_v99/RECO')
        self.assertEqual(reportCMSRun.output.outputRECORECO.files.file0.size, 10)
        self.assertEqual(reportCMSRun.output.outputRECORECO.files.file0.checksums,
                         {'adler32': '11e003a6', 'cksum': '2658792579'})

        if os.path.exists('tmpDir'):
            shutil.rmtree('tmpDir')

        shutil.copytree(self.testDir, os.path.join(os.getcwd(), 'tmpDir'))



        return



    def testC_RealTest(self):
        """
        Checks to see if the real code does anything
        
        NOTE: This shouldn't work for you.  To do it, I had to disable CMSSW
        Right now this works WITHOUT the CMSSW step
        Adding the CMSSW test causes trouble.
        Waiting for future work on that.
        """

        #return

        workloadName = 'basicWorkload'

        workload = self.createWorkload(workloadName = workloadName, emulator = False)

        

        self.createWMBSComponents(workload = workload)

        self.unpackComponents(workload = workload)

        self.runJobs(workload = workload)

        

        # Check what came out
        rerecoDir = os.path.join(self.unpackDir, 'ReReco', 'job')
        self.assertTrue('WMTaskSpace' in os.listdir(rerecoDir))
        self.assertEqual(len(os.listdir(rerecoDir)), 5)
        logArchDir = os.path.join(rerecoDir, 'WMTaskSpace', 'logArch1')
        # Did we produce a jobReport
        self.assertTrue('Report.pkl' in os.listdir(logArchDir))
        # Did we bundle the logs?
        self.assertTrue('logArchive.tar.gz' in os.listdir(logArchDir))
        # Did stageOut kick in and execute a local stageOut?
        self.assertTrue('logArchive.tar.gz2' in os.listdir(logArchDir))


        if os.path.exists('tmpDir'):
            shutil.rmtree('tmpDir')

        shutil.copytree(self.testDir, os.path.join(os.getcwd(), 'tmpDir'))


        return




if __name__ == "__main__":

    unittest.main()
