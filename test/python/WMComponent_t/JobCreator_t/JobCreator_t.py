#!/bin/env python

__revision__ = "$Id: JobCreator_t.py,v 1.13 2010/03/22 15:04:40 mnorman Exp $"
__version__ = "$Revision: 1.13 $"

import unittest
import random
import threading
import time
import os
import shutil
import logging
import cProfile
import pstats

from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory


from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job

from WMCore.Agent.Configuration              import loadConfigurationFile, Configuration
from WMComponent.JobCreator.JobCreator       import JobCreator
from WMComponent.JobCreator.JobCreatorPoller import JobCreatorPoller

from WMCore.WMSpec.WMWorkload                import WMWorkload, WMWorkloadHelper

from WMCore.WMSpec.WMTask                    import WMTask, WMTaskHelper
from WMCore.ResourceControl.ResourceControl  import ResourceControl

#Workload stuff
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker


class JobCreatorTest(unittest.TestCase):
    """
    Test case for the JobCreator

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']


    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        #Stolen from Subscription_t.py

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ['WMCore.WMBS', 
                                                 'WMCore.MsgService',
                                                 'WMCore.ThreadPool',
                                                 'WMCore.ResourceControl',
                                                 'WMCore.WorkQueue.Database'], useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        for site in self.sites:
            locationAction.execute(siteName = site)



        #Create sites in resourceControl

        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName = site, seName = site, ceName = site)
            resourceControl.insertThreshold(siteName = site, taskType = 'Processing', \
                                            minSlots = 1000, maxSlots = 10000)

        self.resourceControl = resourceControl



        self._setup = True
        self._teardown = False

        self.testDir = self.testInit.generateWorkDir()
        self.cwd = os.getcwd()


        return





    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        
        myThread = threading.currentThread()

        #self.testInit.clearDatabase(modules = ['WMCore.ThreadPool'])
        self.testInit.clearDatabase(modules = ['WMCore.WMBS', 'WMCore.MsgService',
                                               'WMCore.ThreadPool', 'WMCore.ResourceControl',
                                               'WMCore.WorkQueue.Database'])
        #self.testInit.clearDatabase()
        
        time.sleep(2)

        self.testInit.delWorkDir()
        
        self._teardown = True

        return






    def createBigJobCollection(self, instance, nSubs):
        """

        Creates a giant block of jobs


        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec = "TestHugeWorkload/TestHugeTask", owner = "mnorman",
                                name = "wf001", task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        
            for j in range(0,100):
                #pick a random site
                site = random.choice(self.sites)
                testFile = File(lfn = "/this/is/a/lfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()
        return



    def createSingleSiteCollection(self, instance, nSubs, workloadSpec = None):
        """
        Creates a giant block of jobs at one site
        """



        myThread = threading.currentThread()

        if not workloadSpec:
            logging.error("Should never be assigning workloadSpec")
            workloadSpec = "TestSingleWorkload/TestHugeTask"


        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman", name = "wf001", task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,100):
                #pick the first site
                site = self.sites[0]
                testFile = File(lfn = "/singleLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()

        return

    def createMutlipleSiteCollection(self, instance, nSubs, workloadSpec = None):
        """

        Creates a giant block of jobs at multiple sites


        """



        myThread = threading.currentThread()

        nameStr = str(instance)


        if not workloadSpec:
            workloadSpec = "TestSingleWorkload%s/TestHugeTask" %(nameStr)


        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman2",
                                name = "wf001"+nameStr, task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,100):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/multLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(self.sites)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()


        return


    def getAbsolutelyMassiveJobGroup(self, instance, nSubs, workloadSpec = None):
        """

        Creates a giant block of jobs at multiple sites


        """



        myThread = threading.currentThread()

        if not workloadSpec:
            workloadSpec = "TestSingleWorkload/TestReallyHugeTask" 

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman3",
                                name = "afmwf001", task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()
        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()

            for j in range(0,5000):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/multLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(self.sites)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()


        return


    def getConfig(self):
        """
        _getConfig_

        Creates a common config.
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

        config.component_("JobCreator")
        config.JobCreator.namespace = 'WMComponent.JobCreator.JobCreator'
        #The log level of the component. 
        #config.JobCreator.logLevel = 'SQLDEBUG'
        config.JobCreator.logLevel = 'INFO'

        # maximum number of threads we want to deal
        # with messages per pool.
        config.JobCreator.maxThreads                = 1
        config.JobCreator.UpdateFromResourceControl = True
        config.JobCreator.pollInterval              = 10
        config.JobCreator.jobCacheDir               = os.path.join(self.testDir)
        config.JobCreator.defaultJobType            = 'processing' #Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads             = 2
        config.JobCreator.componentDir              = self.testDir
        config.JobCreator.useWorkQueue              = True
        config.JobCreator.WorkQueueParam            = {}
        
        # We now call the JobMaker from here
        config.component_('JobMaker')
        config.JobMaker.logLevel        = 'INFO'
        config.JobMaker.namespace       = 'WMCore.WMSpec.Makers.JobMaker'
        config.JobMaker.maxThreads      = 1
        config.JobMaker.makeJobsHandler = 'WMCore.WMSpec.Makers.Handlers.MakeJobs'
        
        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'mnorman:theworst@cmssrv52.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"

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


    def testA(self):
        """
        Test for whether or not the job will actually run

        """

        #return

        myThread = threading.currentThread()

        nSubs = 5

        self.createBigJobCollection("first", nSubs)

        print "Should have database by now"
        print myThread.dbi.processData("SELECT * FROM rc_threshold")[0].fetchall()
        print self.resourceControl.listThresholdsForCreate()



        
        config = self.getConfig()

        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        time.sleep(30)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()


        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), len(self.sites) * nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        assert len(result[0].fetchall()) > nSubs * 20, "Not enough jobs!"

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)

        #self.teardown = True


        return


    def testB(self):
        """
        This one actually tests something

        """

        #return
        
        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs)

        config = self.getConfig()


        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(10)
        

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 20)

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)

        #os.chdir(self.cwd)

        return


    def testC(self):
        """
        This one actually tests whether we can read the WMSpec

        """

        #return

        wmWorkload = self.createTestWorkload()


        if os.path.exists("basicWorkloadUpdated.pcl"):
            os.remove("basicWorkloadUpdated.pcl")

        wmTask     = wmWorkload.getTask("Merge")
        #print wmTask.data

        wmTask.data.section_("seeders")
        wmTask.data.seeders.section_("RandomSeeder")
        wmTask.data.seeders.section_("RunAndLumiSeeder")
        wmTask.data.seeders.RandomSeeder.simMuonRPCDigis            = None
        wmTask.data.seeders.RandomSeeder.simEcalUnsuppressedDigis   = None
        wmTask.data.seeders.RandomSeeder.simCastorDigis             = None
        wmTask.data.seeders.RandomSeeder.generator                  = None 
        wmTask.data.seeders.RandomSeeder.simSiStripDigis            = None
        wmTask.data.seeders.RandomSeeder.LHCTransport               = None

        wmWorkload.save(os.path.join(self.testDir, "basicWorkloadUpdated.pcl"))
        


        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection(instance = "first", nSubs = nSubs, workloadSpec = os.path.join(self.testDir, "basicWorkloadUpdated.pcl"))

        config = self.getConfig()


        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(20)


        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 10)

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)

        return


    def testD(self):
        """
        This one tests whether or not we can choose a site from a list in the file
        This is not well tested, since I don't know which location it will end up in.

        """

        #return


        wmWorkload = self.createTestWorkload()

        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs, self.testDir + "/basicWorkload.pcl")
        

        config = self.getConfig()

        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(20)


        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT id FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 10)

        self.assertEqual(os.listdir('%s/BasicProduction/Merge/JobCollection_1_0/job_1' %self.testDir), ['job.pkl'])

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)


        return


    def testE_Profile(self):
        """
        Profile your performance
        You shouldn't be running this normally because it doesn't do anything

        """

        #return

        wmWorkload = self.createTestWorkload()

        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs, self.testDir + "/basicWorkload.pcl")
        

        config = self.getConfig()

        testJobCreator = JobCreatorPoller(config = config)
        cProfile.runctx("testJobCreator.algorithm()", globals(), locals(), filename = "testStats.stat")

        p = pstats.Stats('testStats.stat')
        p.sort_stats('time')
        p.print_stats()

        return
        


    def testAbsoFuckingLoutelyHugeJob(self):
        """
        This one takes a long time to run, but it runs
        """

        return

        print "This should take about twelve to fifteen minutes"

        
        wmWorkload = self.createTestWorkload()
        
        myThread = threading.currentThread()

        nSubs = 5

        self.getAbsolutelyMassiveJobGroup("first", nSubs, self.testDir + "/basicWorkload.pcl")

        config = self.getConfig()

        startTime = time.clock()
        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()
        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            #print "Waiting for threads to finish"
            time.sleep(0.1)
        stopTime = time.clock()

        #time.sleep(90)

        print "Time taken: "
        print stopTime - startTime

        if os.path.exists('tmpDir'):
            shutil.rmtree('tmpDir')
        shutil.copytree('%s' %self.testDir, os.path.join(os.getcwd(), 'tmpDir'))

        dirs = os.listdir(os.path.join(self.testDir, 'BasicProduction/Merge'))

        self.assertEqual(len(dirs), (nSubs*500)/500)

        result = myThread.dbi.processData('SELECT id FROM wmbs_job')[0].fetchall()
        print "Have wmbs_job results"
        print len(result)
        
        for dir in dirs:
            self.assertEqual(len(os.listdir('%s/BasicProduction/Merge/%s' %(self.testDir, dir))), 500)


        result = myThread.dbi.processData('SELECT id FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 500)

        

        return


if __name__ == "__main__":

    unittest.main() 
