#!/bin/env python

import os
import os.path
import unittest
import threading
import logging
import time
import random
import WMCore.WMInit
from WMQuality.TestInit         import TestInit
from WMCore.DAOFactory          import DAOFactory
from WMCore.WMFactory           import WMFactory
from WMCore.Agent.Configuration import Configuration
from WMCore.Services.UUID       import makeUUID

from WMCore.DataStructs.Run     import Run

from WMCore.JobStateMachine.ChangeState import ChangeState

#WMBS Objects
from WMCore.WMBS.Subscription   import Subscription
from WMCore.WMBS.Fileset        import Fileset
from WMCore.WMBS.Workflow       import Workflow
from WMCore.WMBS.JobGroup       import JobGroup
from WMCore.WMBS.File           import File
from WMCore.WMBS.Job            import Job


#Now get the components
from WMComponent.JobCreator.JobCreator       import JobCreator
from WMComponent.JobSubmitter.JobSubmitter   import JobSubmitter
from WMComponent.JobAccountant.JobAccountant import JobAccountant
from WMComponent.JobTracker.JobTracker       import JobTracker
from WMComponent.ErrorHandler.ErrorHandler   import ErrorHandler
from WMComponent.RetryManager.RetryManager   import RetryManager
from WMComponent.JobArchiver.JobArchiver     import JobArchiver

#Workload stuff
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper



class FullRunthroughTest(unittest.TestCase):

    #This is an integration test
    __integration__ = "Any old bollocks"


    def setUp(self):
        """
        _setUp_

        Setup all the databases, tables, factories, and locations needed
        """
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.MsgService', \
                                                 'WMCore.ThreadPool','WMCore.ResourceControl',\
                                                 "WMComponent.DBSBuffer.Database"], \
                                useDefault = False)


        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = myThread.logger, dbinterface = myThread.dbi)
        
        #Create the locations
        self.sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']
        locationAction = self.daoFactory(classname = "Locations.New")
        locationSlots  = self.daoFactory(classname = "Locations.SetJobSlots")
        for site in self.sites:
            locationAction.execute(siteName = site)
            locationSlots.execute(siteName = site, jobSlots = 1000)


        #Just in case we lose our way
        self.cwd = os.getcwd()

        #Set the log verbosity
        self.logLevel = 'INFO'

        return


    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        
        myThread = threading.currentThread()

        self.testInit.clearDatabase(modules = ['WMCore.WMBS', 'WMCore.MsgService', 'WMCore.ResourceControl', \
                                               'WMCore.ThreadPool', "WMComponent.DBSBuffer.Database"])
        os.popen3('rm -r test/Test*')
        os.popen3('rm -r test/Basic*')

        if hasattr(myThread, 'workerThreadManager'):
            myThread.workerThreadManager.terminateWorkers()
        
        return


    def createTestWorkload(self):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

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
        prodCmsswHelper.data.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
        prodCmsswHelper.data.application.setup.softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
        prodCmsswHelper.data.application.configuration.configCacheUrl = "http://whatever"
        prodCmsswHelper.addOutputModule("writeData", primaryDataset = "Primary",
                                        processedDataset = "Processed",
                                        dataTier = "TIER")


        prodStageOutHelper = prodStageOut.getTypeHelper()
        merge.setInputReference(prodCmssw, outputModule = "writeData")

        workload.save('basicWorkload.pcl')

        return workload



    def getConfig(self):
        """
        _getConfig_
        
        For now, build a config file from the ground up.
        Later, use this as a model for the JSM master config
        """

        myThread = threading.currentThread()

        config = Configuration()

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        if not config.CoreDatabase.dialect or not config.CoreDatabase.connectUrl:
            msg1 = "No database or dialect in environment!"
            msg2 = "Database set to %s" %(config.CoreDatabase.connectUrl)
            msg3 = "Dialect set to %s" %(config.CoreDatabase.dialect)
            print msg1
            print msg2
            print msg3
            raise Exception (msg1)


        #General config options
        config.section_("WMAgent")
        config.WMAgent.WMSpecDirectory    = os.getcwd()  #Where are the WMSpecs by default?


        #Now we go by component

        #First the JobCreator
        config.component_("JobCreator")
        config.JobCreator.namespace                 = 'WMComponent.JobCreator.JobCreator'
        config.JobCreator.logLevel                  = self.logLevel
        config.JobCreator.maxThreads                = 1
        config.JobCreator.UpdateFromResourceControl = True
        config.JobCreator.pollInterval              = 10
        config.JobCreator.jobCacheDir               = os.path.join(self.cwd, 'test')
        config.JobCreator.defaultJobType            = 'processing' #Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads             = 2
        config.JobCreator.componentDir              = os.path.join(os.getcwd(), 'Components/JobCreator')
        config.JobCreator.useWorkQueue              = False

        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'mnorman:theworst@cmssrv48.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"


        #JobSubmitter
        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel      = self.logLevel
        config.JobSubmitter.maxThreads    = 1
        config.JobSubmitter.pollInterval  = 10
        config.JobSubmitter.pluginName    = 'ShadowPoolPlugin'
        config.JobSubmitter.pluginDir     = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir     = os.path.join(os.getcwd(), 'submit')
        config.JobSubmitter.submitNode    = os.getenv("HOSTNAME", 'badtest.fnal.gov')
        config.JobSubmitter.submitScript  = os.path.join(os.getcwd(), 'submit.sh')
        config.JobSubmitter.componentDir  = os.path.join(os.getcwd(), 'Components/JobSubmitter')
        config.JobSubmitter.inputFile     = os.path.join(os.getcwd(), 'FrameworkJobReport-4540.xml')
        config.JobSubmitter.workerThreads = 1
        config.JobSubmitter.jobsPerWorker = 100


        #JobTracker
        config.component_("JobTracker")
        config.JobTracker.logLevel      = self.logLevel
        config.JobTracker.pollInterval  = 10
        config.JobTracker.trackerName   = 'TestTracker'
        config.JobTracker.pluginDir     = 'WMComponent.JobTracker.Plugins'
        config.JobTracker.runTimeLimit  = 7776000 #Jobs expire after 90 days
        config.JobTracker.idleTimeLimit = 7776000
        config.JobTracker.heldTimeLimit = 7776000
        config.JobTracker.unknTimeLimit = 7776000
        

        #ErrorHandler
        config.component_("ErrorHandler")
        config.ErrorHandler.logLevel     = self.logLevel
        config.ErrorHandler.namespace    = 'WMComponent.ErrorHandler.ErrorHandler'
        config.ErrorHandler.maxRetries   = 10
        config.ErrorHandler.pollInterval = 10
        

        #RetryManager
        config.component_("RetryManager")
        config.RetryManager.logLevel     = self.logLevel
        config.RetryManager.namespace    = 'WMComponent.RetryManager.RetryManager'
        config.RetryManager.pollInterval = 10
        config.RetryManager.coolOffTime  = {'create': 10, 'submit': 10, 'job': 10}
        config.RetryManager.pluginPath   = 'WMComponent.RetryManager.PlugIns'
        config.RetryManager.pluginName   = ''
        config.RetryManager.WMCoreBase   = WMCore.WMInit.getWMBASE()
        

        #JobAccountant
        config.component_("JobAccountant")
        config.JobAccountant.logLevel      = self.logLevel
        config.JobAccountant.pollInterval  = 10
        config.JobAccountant.workerThreads = 1
        config.JobAccountant.componentDir  = os.path.join(os.getcwd(), 'Components/JobAccountant')


        #JobArchiver
        config.component_("JobArchiver")
        config.JobArchiver.pollInterval  = 10
        config.JobArchiver.logLevel      = self.logLevel
        config.JobArchiver.logDir        = os.path.join(os.getcwd(), 'logs')




        return config


    def createSimpleFiles(self):
        """
        _createSimpleJobs_
        
        Create a simple job group that can be used for basic testing
        """

        testWorkflow = Workflow(spec = os.getcwd() + "/basicWorkload.pcl", owner = "Simon",
                                name = "wf001", task="Merge")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('T2_US_UCSD')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('T2_US_UCSD')
        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()

        return


    def createFileset(self, nFiles = 100):

        """
        _createFileset_
        
        Create a larger simple fileset that can be used for basic testing
        """

        testWorkflow = Workflow(spec = os.getcwd() + "/basicWorkload.pcl", owner = "Simon",
                                name = "wf001", task="Merge")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        for i in range(1, nFiles):
            site = random.choice(self.sites)
            testFileA = File(lfn = "/this/is/a/lfn%i"%(i), size = 1024, events = 10)
            testFileA.addRun(Run(i, *[i]))
            testFileA.setLocation(site)
            testFileA.create()
            testWMBSFileset.addFile(testFileA)


        testWMBSFileset.commit()
        testSubscription = Subscription(fileset = testWMBSFileset, workflow = testWorkflow)
        testSubscription.create()

        return


    def stupidJobEmulator(self, flag = False):

        config = self.getConfig()

        changeState = ChangeState(config)

        jobFinder = self.daoFactory(classname = "Jobs.GetAllJobs")

        listOfJobIDs = jobFinder.execute(state = 'executing')

        jobList = []
        for id in listOfJobIDs:
            job = Job(id = id)
            job.load()
            jobList.append(job)

        changeState.propagate(jobList, 'complete', 'executing')
        changeState.propagate(jobList, 'jobfailed', 'complete')
    


        return


    def jobEmulator(self, goodFJR = None, badFJR = os.path.join(os.getcwd(), 'SkimFailure.xml'), fractionPassed = 0.0):

        #This has to be triggered before the jobs are removed from the queue


        myThread = threading.currentThread()

        config = self.getConfig()

        changeState = ChangeState(config)

        jobFinder = self.daoFactory(classname = "Jobs.GetAllJobs")

        listOfJobIDs = jobFinder.execute(state = 'executing')

        listOfGoodReports = os.listdir(goodFJR)
        #Get rid of CVS, Repack report
        listOfGoodReports.pop()
        listOfGoodReports.pop()

        myThread.transaction.begin()

        jobList  = []
        passed   = 0
        failed   = 0
        for id in listOfJobIDs:
            job = Job(id = id)
            job.load()

            if random.uniform(0.0,1.0) < fractionPassed:
                job['fwjr'] = os.path.join(goodFJR, listOfGoodReports.pop())
                passed += 1
            else:
                job['fwjr'] = badFJR
                failed += 1
            print job['fwjr']
            job.setFWJRPath(job['fwjr'])
            jobList.append(job)

        myThread.transaction.commit()

        print "Have set job report paths"
        print listOfJobIDs
        print myThread.dbi.processData("SELECT fwjr_path FROM wmbs_job")[0].fetchall()

        #changeState.propagate(jobList, 'complete', 'executing')


        return passed, failed


    def testA_StartupSequence(self):
        """
        _testA_StartupSequence_
        
        This test does nothing except start, and then stop, the components.
        It's been included here so I can make sure that components actually start up properly.
        It's been left here because it's useful to run before everything else.
        """

        return
        
        myThread = threading.currentThread()

        self.createTestWorkload()

        config = self.getConfig()

        #Initialize components
        testJobCreator    = JobCreator(config)
        testJobSubmitter  = JobSubmitter(config)
        testErrorHandler  = ErrorHandler(config)
        testRetryManager  = RetryManager(config)
        testJobAccountant = JobAccountant(config)
        testJobTracker    = JobTracker(config)
        testJobArchiver   = JobArchiver(config)


        #Start components
        testJobCreator.prepareToStart()
        testJobSubmitter.prepareToStart()
        testErrorHandler.prepareToStart()
        testRetryManager.prepareToStart()
        testJobAccountant.prepareToStart()
        testJobTracker.prepareToStart()
        testJobArchiver.prepareToStart()

        #time.sleep(60)


        print "About to kill"

        myThread.workerThreadManager.terminateWorkers()


        #At the end, wait if threads are still active
        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)


        return


    def testB_SimpleEndToEnd(self):
        """
        testB_SimpleEndToEnd
        
        This should do the simplest possible runthrough of a simple set of jobs.
        It basically tests to see if things are actually working.
        """

        #return

        myThread = threading.currentThread()

        config = self.getConfig()

        self.createTestWorkload()

        self.createSimpleFiles()

        
        #Initialize components
        testJobCreator    = JobCreator(config)
        testJobSubmitter  = JobSubmitter(config)
        testErrorHandler  = ErrorHandler(config)
        testRetryManager  = RetryManager(config)
        testJobAccountant = JobAccountant(config)
        testJobArchiver   = JobArchiver(config)
        testJobTracker    = JobTracker(config)


        #Start components
        testJobCreator.prepareToStart()
        testJobSubmitter.prepareToStart()
        testErrorHandler.prepareToStart()
        testRetryManager.prepareToStart()
        testJobAccountant.prepareToStart()
        testJobArchiver.prepareToStart()
        testJobTracker.prepareToStart()


        time.sleep(60)

        jobs      = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        jobgroups = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        subs      = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        files     = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()

        print "First state printing"
        print jobs
        print jobgroups
        print subs
        print files
        print state

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobgroups), 1)
        self.assertEqual(len(files), 2)

        self.assertEqual(os.path.isdir('test/BasicProduction/Merge/JobCollection_1_0/job_1'), True)
        #self.assertEqual(os.path.isfile('DeleteThisFile.txt'), True)

        #All jobs should be in the same that
        
        self.assertEqual(state[0].values()[0], 'executing')

        #passed, failed = self.jobEmulator(goodFJR = "%s/test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports/" %(WMCore.WMInit.getWMBASE()),
        #                                  fractionPassed = 0.0)


        time.sleep(420)


        jobs      = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        jobgroups = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        subs      = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        files     = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()


        #This checks the RetryManager
        #Under the defaults, after waiting this long, jobs should have re-entered the executing phase
        print "These are the states"
        print state
        self.assertEqual(state[0].values()[0], 'cleanout')

        time.sleep(2)


        print "About to kill"

        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        #At the end, wait if threads are still active
        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)


        #os.popen3('condor_rm %s' %(os.getenv('USER')))


        return


    def testC_RecyclingEndToEnd(self):
        """
        testC_RecyclingEndToEnd
        
        This should go through several iterations where it keeps emulating a series of failed jobs
        Hopefully we will see some exhaustions as I fine tune this.
        """

        return

        myThread = threading.currentThread()

        config = self.getConfig()
        self.createTestWorkload()
        config.JobSubmitter.pluginName    = 'TestPlugin'

        self.createFileset()

        
        #Initialize components
        testJobCreator    = JobCreator(config)
        testJobSubmitter  = JobSubmitter(config)
        testErrorHandler  = ErrorHandler(config)
        testRetryManager  = RetryManager(config)
        testJobAccountant = JobAccountant(config)
        testJobTracker    = JobTracker(config)
        testJobArchiver   = JobArchiver(config)


        #Start components
        testJobCreator.prepareToStart()
        testJobSubmitter.prepareToStart()
        testErrorHandler.prepareToStart()
        testRetryManager.prepareToStart()
        testJobAccountant.prepareToStart()
        testJobTracker.prepareToStart()
        testJobArchiver.prepareToStart()
        #testJobAccountant.startComponent()


        time.sleep(40)

        jobs      = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        jobgroups = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        subs      = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        files     = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        file_acq  = myThread.dbi.processData("SELECT * FROM wmbs_sub_files_acquired")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()


        #Check that jobs are executing, cache directories created
        self.assertEqual(os.path.isdir('test/BasicProduction/Merge/JobCollection_1_0/job_1'), True)
        self.assertEqual(state[0].values()[0], 'executing')
        self.assertEqual(os.path.isdir('test/BasicProduction/Merge/JobCollection_1_0/job_2'), True)
        listOfFiles = os.listdir('test/BasicProduction/Merge/JobCollection_1_0/job_2')
        self.assertEqual('baggage.pcl' in listOfFiles, True)


        jobstate  = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()

        stateList = []
        for i in jobstate:
            stateList.append(i.values()[0])
        print "All jobs should be executing"
        print stateList
        print myThread.dbi.processData("SELECT * FROM wmbs_job_state")[0].fetchall()


        #First tell emulator to randomly pass/fail jobs
        passed, failed = self.jobEmulator(goodFJR = "%s/test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports/" %(WMCore.WMInit.getWMBASE()), 
                                          fractionPassed = 0.5)
        #Remove jobs to trigger tracker
        os.popen3('condor_rm %s' %(os.getenv('USER')))
        time.sleep(20)

        
        #Once tracker has triggered, poll
        #testJobAccountant.pollForJobs()

        time.sleep(0.1)

        jobstate  = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()

        stateList = []
        for i in jobstate:
            stateList.append(i.values()[0])
        print "Have states; should have passed jobs"
        print stateList
        print passed
        print failed
        print myThread.dbi.processData("SELECT * FROM wmbs_job_state")[0].fetchall()
        self.assertEqual(stateList.count(5) + stateList.count(11), passed)
        self.assertEqual(stateList.count(7) + stateList.count(9)  + stateList.count(3) + stateList.count(14), failed)


        #Wait for jobs to recycle through ErrorHandling
        time.sleep(60)

        #Re-emulate jobs
        passed, failed = self.jobEmulator(goodFJR = "%s/test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports/" %(WMCore.WMInit.getWMBASE()),
                                          fractionPassed = 1.0)

        #Remove jobs to trigger tracker
        os.popen3('condor_rm %s' %(os.getenv('USER')))
        time.sleep(20)

        #Trigger poll
        #testJobAccountant.pollForJobs()

        time.sleep(0.1)

        jobstate  = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()

        stateList = []
        for i in jobstate:
            stateList.append(i.values()[0])
        print "Have states after emulator round 2"
        print stateList

        #Wait to trigger archiver
        time.sleep(20)


        jobstate  = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()
        state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()

        stateList = []
        for i in jobstate:
            stateList.append(i.values()[0])
        print "Everything should now be cleaned out"
        print stateList

        
        print "Executing final directory check"
        jobNames = []
        for name in myThread.dbi.processData("SELECT name FROM wmbs_job")[0].fetchall():
            jobNames.append(name.values()[0])
        for name in jobNames:
            self.assertEqual(os.path.isfile('logs/Job_%s.tar' %(name)), True, "Could not find file Job_%s.tar" %(name))
        #self.assertEqual(os.path.isdir('test/BasicProduction/Merge/JobCollection_1_0/job_2'), True)
        listOfDirs = os.listdir('test/BasicProduction/Merge/JobCollection_1_0/')
        for dir in listOfDirs:
            print os.listdir(os.path.join('test/BasicProduction/Merge/JobCollection_1_0/', dir))




        print "About to kill"

        myThread.workerThreadManager.terminateWorkers()


        #At the end, wait if threads are still active
        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)


        return



    def testD_LargeEndToEnd(self):
        """
        testD_LargeEndToEnd
        
        This is a test of a very large dataset; mostly it's a timing test
        """

        #return

        myThread = threading.currentThread()

        config = self.getConfig()

        self.createTestWorkload()

        print "About to start creating the fileset"

        self.createFileset(nFiles = 50)
        #self.createSimpleFiles()

        print "Finished creating the fileset"

        #Initialize components
        testJobCreator    = JobCreator(config)
        testJobSubmitter  = JobSubmitter(config)
        testErrorHandler  = ErrorHandler(config)
        testRetryManager  = RetryManager(config)
        testJobAccountant = JobAccountant(config)
        testJobTracker    = JobTracker(config)
        testJobArchiver   = JobArchiver(config)

        startTime = time.time()

        #Start components
        testJobCreator.prepareToStart()
        testJobSubmitter.prepareToStart()
        testErrorHandler.prepareToStart()
        testRetryManager.prepareToStart()
        testJobAccountant.prepareToStart()
        testJobTracker.prepareToStart()
        testJobArchiver.prepareToStart()

        #time.sleep(10)
        #myThread.workerThreadManager.terminateWorkers()
        #return



        finishedJobs = False
        count = 0
        while not finishedJobs:
            #Wait, and then see if all jobs are in closeout state
            time.sleep(10)
            #passed, failed = self.jobEmulator(goodFJR = "/home/mnorman/WMCORE/test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports/", 
            #                                  fractionPassed = 1.0)
            #print "About to check state"
            state     = myThread.dbi.processData("SELECT name FROM wmbs_job_state WHERE id IN (SELECT state FROM wmbs_job)")[0].fetchall()
            #print "About to check jobstate"
            jobstate  = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

            if jobstate == []:
                continue

            #print state
            #print jobstate
            
            finishedJobs = True
            #print "About to check to see if jobs are not done"
            for i in jobstate:
                if not i.values()[0] == 11:
                    #print i.values()[0]
                    finishedJobs = False
                    break



            count += 1

            #if count == 100:
            #    finishedJobs = True






        


        #time.sleep(10)

        



        totalTime = time.time() - startTime

        print "Completed Long test"
        print "This took me %f seconds" %(totalTime)
        print count

        myThread.workerThreadManager.terminateWorkers()

        return


if __name__ == "__main__":

    unittest.main() 


        
    
