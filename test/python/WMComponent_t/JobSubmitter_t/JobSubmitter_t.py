#!/bin/env python



import unittest
import threading
import os
import os.path
import time
import shutil
import pickle

import WMCore.WMInit
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job
from WMComponent.JobSubmitter.JobSubmitter import JobSubmitter
from WMCore.JobStateMachine.ChangeState import ChangeState
from subprocess import Popen, PIPE

from WMCore.Agent.Configuration             import loadConfigurationFile, Configuration
from WMCore.ResourceControl.ResourceControl import ResourceControl


#Workload stuff
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker

class JobSubmitterTest(unittest.TestCase):
    """
    Test class for the JobSubmitter

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']

    def setUp(self):
        """
        Standard setup


        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.testInit.clearDatabase(modules = ['WMCore.WMBS', 'WMCore.MsgService', 'WMCore.ResourceControl'])
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.MsgService', 'WMCore.ResourceControl'],
                                useDefault = False)
        
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationSlots  = daofactory(classname = "Locations.SetJobSlots")

        for site in self.sites:
            locationAction.execute(siteName = site)
            locationSlots.execute(siteName = site, jobSlots = 1000)

        #Create sites in resourceControl
        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName = site, seName = site, ceName = site)
            resourceControl.insertThreshold(thresholdName = 'ProcessingThreshold', \
                                            thresholdValue = 1000, siteNames = site)

        self.testDir = self.testInit.generateWorkDir()
            
        return

    def tearDown(self):
        """
        Standard tearDown

        """
        self.testInit.clearDatabase()

        self.testInit.delWorkDir()



    def createJobGroup(self, nSubs, config, instance = '', workloadSpec = None, task = None, nJobs = 100):
        """
        This function acts to create a number of test job group with jobs that we can submit

        """

        myThread = threading.currentThread()

        jobGroupList = []

        changeState = ChangeState(config)

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                name = "wf001", task="basicWorkload/Production")
        testWorkflow.create()

        cacheDir = os.path.join(self.testDir, 'CacheDir')

        if not os.path.exists(cacheDir):
            os.makedirs(cacheDir)

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            if not workloadSpec:
                workloadSpec = "TestSingleWorkload%s/TestHugeTask" %(nameStr)

            myThread.transaction.begin()


        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,nJobs):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/singleLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)


            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()

            index = 0
            for file in testFileset.getFiles():
                index+=1
                testJob = Job(name = "test-%s" %(file["lfn"]))
                testJob.addFile(file)
                testJob["location"]  = file.getLocations()[0]
                testJob['task']    = task.getPathName()
                testJob['sandbox'] = task.data.input.sandbox
                testJob['spec']    = os.path.join(self.testDir, 'basicWorkload.pcl')
                jobCache = os.path.join(cacheDir, 'Job_%i' %(index))
                os.makedirs(jobCache)
                testJob.create(testJobGroup)
                testJobGroup.add(testJob)
                testJob['cache_dir'] = jobCache
                testJob.save()
                output = open(os.path.join(jobCache, 'job.pkl'),'w')
                pickle.dump(testJob, output)
                output.close()
            
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

            myThread.transaction.commit()

            changeState.propagate(testJobGroup.jobs, 'created', 'new')

        return jobGroupList
        

    def getConfig(self, configPath = os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMComponent/JobSubmitter/DefaultConfig.py')):
        """
        _getConfig_

        Gets a basic config from default location
        """

        myThread = threading.currentThread()

        config = Configuration()

        config.component_("WMAgent")
        config.WMAgent.WMSpecDirectory = self.testDir


        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        config.component_("JobSubmitter")
        # The log level of the component. 
        config.JobSubmitter.logLevel = 'INFO'
        # maximum number of threads we want to deal
        # with messages per pool.
        config.JobSubmitter.maxThreads = 1
        #
        # JobSubmitter
        #
        config.JobSubmitter.pollInterval  = 10
        config.JobSubmitter.pluginName    = 'TestPlugin'
        config.JobSubmitter.pluginDir     = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir     = os.path.join(self.testDir, 'submit')
        config.JobSubmitter.submitNode    = os.getenv("HOSTNAME", 'badtest.fnal.gov')
        config.JobSubmitter.submitScript  = os.path.join(WMCore.WMInit.getWMBASE(), 'test/python/WMComponent_t/JobSubmitter_t', 'submit.sh')
        config.JobSubmitter.componentDir  = os.path.join(os.getcwd(), 'Components')
        config.JobSubmitter.workerThreads = 1
        config.JobSubmitter.jobsPerWorker = 100
        config.JobSubmitter.inputFile     = os.path.join(WMCore.WMInit.getWMBASE(), 'test/python/WMComponent_t/JobSubmitter_t', 'FrameworkJobReport-4540.xml')

        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'mnorman:theworst@cmssrv52.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"


        # Needed, because this is a test
        os.makedirs(config.JobSubmitter.submitDir)


        return config
    
    def createTestWorkload(self, workloadName = None):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        if not workloadName:
            workloadName = os.path.join(self.testDir, 'basicWorkload.pcl')

        if os.path.isdir(workloadName):
            raise
        if os.path.isfile(workloadName):
            os.remove(workloadName)

        #Basic workload definition
        workload = newWorkload("BasicProduction")
        workload.setStartPolicy('MonteCarlo')
        workload.setEndPolicy('SingleShot')

        #Basic production step
        production = workload.newTask("Production")
        production.addProduction(totalevents = 1000)
        prodCmssw = production.makeStep("cmsRun1")
        prodCmssw.setStepType("CMSSW")
        prodStageOut = prodCmssw.addStep("stageOut1")
        prodStageOut.setStepType("StageOut")
        prodLogArch = prodCmssw.addStep("logArch1")
        prodLogArch.setStepType("LogArchive")
        production.applyTemplates()
        production.setSplittingAlgorithm("FileBased", files_per_job = 10)

        #Basic Merge step
        merge = workload.newTask("Merge")
        mergeCmssw = merge.makeStep("cmsRun1")
        mergeCmssw.setStepType("CMSSW")
        mergeStageOut = mergeCmssw.addStep("stageOut1")
        mergeStageOut.setStepType("StageOut")
        merge.applyTemplates()
        merge.setSplittingAlgorithm("FileBased", files_per_job = 10)


        prodCmsswHelper = prodCmssw.getTypeHelper()
        prodCmsswHelper.data.section_('emulator')
        prodCmsswHelper.data.emulator.emulatorName = "CMSSW"
        prodCmsswHelper.data.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
        prodCmsswHelper.data.application.setup.softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
        #prodCmsswHelper.data.application.configuration.configCacheUrl = "http://whatever"
        prodCmsswHelper.addOutputModule("writeData", primaryDataset = "Primary",
                                        processedDataset = "Processed",
                                        dataTier = "TIER",
                                        lfnBase = "/this/is/a/test/LFN")


        prodStageOutHelper = prodStageOut.getTypeHelper()
        prodStageOutHelper.data.section_('emulator')
        prodStageOutHelper.data.emulator.emulatorName = "StageOut"
        prodLogArchHelper = prodLogArch.getTypeHelper()
        prodLogArchHelper.data.section_('emulator')
        prodLogArchHelper.data.emulator.emulatorName = "LogArchive"
        merge.setInputReference(prodCmssw, outputModule = "writeData")


        monitoring  = production.data.section_('watchdog')
        monitoring.monitors = ['WMRuntimeMonitor', 'TestMonitor']
        monitoring.section_('TestMonitor')
        monitoring.TestMonitor.connectionURL = "dummy.cern.ch:99999/CMS"
        monitoring.TestMonitor.password      = "ThisIsTheWorld'sStupidestPassword"
        monitoring.TestMonitor.softTimeOut   = 300
        monitoring.TestMonitor.hardTimeOut   = 600
        
        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def testA_BasicSubmission(self):
        """
        This is the simplest of tests

        """

        #return

        #This submits a job to the local condor_q
        #It basically does nothing, so there's little to test
        
        myThread = threading.currentThread()

        workload = self.createTestWorkload()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))

        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'first', workloadSpec = 'basicWorkload', task = workload.getTask('Production'))


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        print "Killing"
        time.sleep(10)
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)


        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)

        output = pipe.communicate()[0]

        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        #print "You must check that you have 100 NEW jobs in the condor_q manually."
        #print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        #os.popen3('rm %s/*.jdl' %(config.JobSubmitter.submitDir))

        return


    def testB_TestLongSubmission(self):
        """
        See if you can get Burt to kill you.

        """

        #return

        myThread = threading.currentThread()

        workload = self.createTestWorkload()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))


        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'second', workloadSpec = 'basicWorkload', task = workload.getTask('Production'), nJobs = 500)


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)


        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)

        output = pipe.communicate()[0]

        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        #print "You must check that you have 3000 NEW jobs in the condor_q manually."
        #print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        

        return



    def testC_ThisWillBreakEverything(self):
        """
        This test will fail.  I cannot make it work.

        """

        return

        myThread = threading.currentThread()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))
        if not os.path.isfile('basicWorkloadWithSandbox.pcl'):
            self.assertEqual(True, False, 'basicWorkloadWithSandbox.pcl must be present in working directory')

        #Right now only works with 1
        jobGroupList = self.createJobGroup(2, config, 'second', workloadSpec = 'basicWorkloadWithSandbox', task = workload.getTask('Production'), nJobs = 5000)


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)


        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)

        output = pipe.communicate()[0]

        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        #print "You must check that you have 1000 NEW jobs in the condor_q manually."
        #print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        

        return


    def testD_shadowPoolSubmit(self):


        #return

        workload = self.createTestWorkload()

        myThread = threading.currentThread()

        #if os.path.exists(os.path.join(os.getcwd(), 'FrameworkJobReport.xml')):
        #    os.remove(os.path.join(os.getcwd(), 'FrameworkJobReport.xml'))

        config = self.getConfig()
        config.JobSubmitter.pluginName    = 'ShadowPoolPlugin'
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))

        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'second', workloadSpec = 'basicWorkload', task = workload.getTask('Production'), nJobs = 10)


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        #Give it three minutes to get on a node and do its 120 second of sleeping
        time.sleep(90)
        
        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)
        output = pipe.communicate()[0]
        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        time.sleep(180)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)

        self.assertEqual(os.path.isfile('%s/CacheDir/Job_2/Report.pkl' %self.testDir), True, "Job did not return file successfully")
        self.assertEqual(os.path.isfile('%s/CacheDir/Job_8/Report.pkl' %self.testDir), True, "Job did not return file successfully")

        shutil.copy('%s/CacheDir/Job_8/Report.pkl' %self.testDir, os.getcwd())

        return



if __name__ == "__main__":

    unittest.main() 
