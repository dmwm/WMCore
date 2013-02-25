#!/bin/env python
#pylint: disable-msg=E1101, W0201, W0142, E1103
# E1101: reference config file variables
# W0142: ** magic
# W0201: Don't much around with __init__
# E1103: Use thread members




import unittest
import random
import threading
import time
import os
import cProfile
import pstats
import cPickle

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMCore.DAOFactory import DAOFactory


from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.DataStructs.Run   import Run

from WMCore.Agent.Configuration              import Configuration
from WMComponent.JobCreator.JobCreatorPoller import JobCreatorPoller

from WMCore.Services.UUID import makeUUID

from WMCore.ResourceControl.ResourceControl  import ResourceControl
from WMCore.Agent.HeartbeatAPI               import HeartbeatAPI

#Workload stuff
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore_t.WMSpec_t.TestSpec     import testWorkload

from nose.plugins.attrib import attr


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

        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ['WMCore.WMBS',
                                                 'WMCore.ResourceControl',
                                                 'WMCore.Agent.Database'], useDefault = False)
        self.couchdbname = "jobcreator_t"
        self.testInit.setupCouch("%s/jobs" % self.couchdbname, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % self.couchdbname, "FWJRDump")


        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        locationAction = self.daoFactory(classname = "Locations.New")
        for site in self.sites:
            locationAction.execute(siteName = site, seName = site)

        #Create sites in resourceControl

        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName = site, seName = site, ceName = site)
            resourceControl.insertThreshold(siteName = site, taskType = 'Processing', \
                                            maxSlots = 10000, pendingSlots = 10000)

        self.resourceControl = resourceControl

        self._setup = True
        self._teardown = False

        self.testDir = self.testInit.generateWorkDir()
        self.cwd = os.getcwd()

        # Set heartbeat
        self.componentName = 'JobCreator'
        self.heartbeatAPI  = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """

        myThread = threading.currentThread()

        self.testInit.clearDatabase(modules = ['WMCore.WMBS', 'WMCore.ResourceControl',
                                               'WMCore.Agent.Database'])

        self.testInit.delWorkDir()

        self._teardown = True

        self.testInit.tearDownCouch()


        return



    def createJobCollection(self, name, nSubs, nFiles, workflowURL = 'test'):
        """
        _createJobCollection_

        Create a collection of jobs
        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec = workflowURL, owner = "mnorman",
                                name = name, task="/TestWorkload/ReReco")
        testWorkflow.create()

        for sub in range(nSubs):

            nameStr = '%s-%i' % (name, sub)

            myThread.transaction.begin()

            testFileset = Fileset(name = nameStr)
            testFileset.create()

            for f in range(nFiles):
                # pick a random site
                site = random.choice(self.sites)
                testFile = File(lfn = "/lfn/%s/%i" % (nameStr, f), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
                                            split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()


        return

    def createWorkload(self, workloadName = 'Test', emulator = True, priority = 1):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload("Tier1ReReco")
        rereco = workload.getTask("ReReco")
        seederDict = {"generator.initialSeed": 1001, "evtgenproducer.initialSeed": 1001}
        rereco.addGenerator("PresetSeeder", **seederDict)


        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        return workload

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

        config.section_("Agent")
        config.Agent.componentName   = self.componentName

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
        #config.JobCreator.jobCacheDir               = self.testDir
        config.JobCreator.defaultJobType            = 'processing' #Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads             = 4
        config.JobCreator.componentDir              = self.testDir
        config.JobCreator.useWorkQueue              = True
        config.JobCreator.WorkQueueParams           = {'emulateDBSReader': True}

        # We now call the JobMaker from here
        config.component_('JobMaker')
        config.JobMaker.logLevel        = 'INFO'
        config.JobMaker.namespace       = 'WMCore.WMSpec.Makers.JobMaker'
        config.JobMaker.maxThreads      = 1
        config.JobMaker.makeJobsHandler = 'WMCore.WMSpec.Makers.Handlers.MakeJobs'

        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'cmssrv52.fnal.gov:5984')
        config.JobStateMachine.couchDBName     = self.couchdbname

        return config

    def testA_VerySimpleTest(self):
        """
        _VerySimpleTest_

        Just test that everything works...more or less
        """

        #return

        myThread = threading.currentThread()

        config = self.getConfig()

        name         = makeUUID()
        nSubs        = 5
        nFiles       = 10
        workloadName = 'TestWorkload'

        workload = self.createWorkload(workloadName = workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name = name, nSubs = nSubs, nFiles = nFiles, workflowURL = workloadPath)

        testJobCreator = JobCreatorPoller(config = config)

        # First, can we run once without everything crashing?
        testJobCreator.algorithm()

        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")

        self.assertEqual(len(result), nSubs*nFiles)

        # Count database objects
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')[0].fetchall()
        self.assertEqual(len(result), nSubs * nFiles)


        # Find the test directory
        testDirectory = os.path.join(self.testDir, 'jobCacheDir', 'TestWorkload', 'ReReco')
        # It should have at least one jobGroup
        self.assertTrue('JobCollection_1_0' in os.listdir(testDirectory))
        # But no more then twenty
        self.assertTrue(len(os.listdir(testDirectory)) <= 20)

        groupDirectory = os.path.join(testDirectory, 'JobCollection_1_0')

        # First job should be in here
        listOfDirs = []
        for tmpDirectory in os.listdir(testDirectory):
            listOfDirs.extend(os.listdir(os.path.join(testDirectory, tmpDirectory)))
        self.assertTrue('job_1' in listOfDirs)
        self.assertTrue('job_2' in listOfDirs)
        self.assertTrue('job_3' in listOfDirs)
        jobDir = os.listdir(groupDirectory)[0]
        jobFile = os.path.join(groupDirectory, jobDir, 'job.pkl')
        self.assertTrue(os.path.isfile(jobFile))
        f = open(jobFile, 'r')
        job = cPickle.load(f)
        f.close()

        self.assertEqual(job.baggage.PresetSeeder.generator.initialSeed, 1001)
        self.assertEqual(job.baggage.PresetSeeder.evtgenproducer.initialSeed, 1001)


        self.assertEqual(job['workflow'], name)
        self.assertEqual(len(job['input_files']), 1)
        self.assertEqual(os.path.basename(job['sandbox']), 'TestWorkload-Sandbox.tar.bz2')

        return

    @attr('performance')
    def testB_ProfilePoller(self):
        """
        Profile your performance
        You shouldn't be running this normally because it doesn't do anything

        """

        return

        myThread = threading.currentThread()

        name         = makeUUID()
        nSubs        = 5
        nFiles       = 1500
        workloadName = 'TestWorkload'


        workload = self.createWorkload(workloadName = workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name = name, nSubs = nSubs, nFiles = nFiles, workflowURL = workloadPath)

        config = self.getConfig()

        testJobCreator = JobCreatorPoller(config = config)
        cProfile.runctx("testJobCreator.algorithm()", globals(), locals(), filename = "testStats.stat")


        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")

        time.sleep(10)

        self.assertEqual(len(result), nSubs*nFiles)

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(.2)

        return


    def testC_ProfileWorker(self):
        """
        Profile where the work actually gets done
        You shouldn't be running this one either, since it doesn't test anything.
        """

        return

        myThread = threading.currentThread()

        name         = makeUUID()
        nSubs        = 5
        nFiles       = 500
        workloadName = 'TestWorkload'


        workload = self.createWorkload(workloadName = workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name = name, nSubs = nSubs, nFiles = nFiles, workflowURL = workloadPath)

        config = self.getConfig()

        configDict = {"couchURL": config.JobStateMachine.couchurl,
                      "couchDBName": config.JobStateMachine.couchDBName,
                      'jobCacheDir': config.JobCreator.jobCacheDir,
                      'defaultJobType': config.JobCreator.defaultJobType}

        input = [{"subscription": 1}, {"subscription": 2}, {"subscription": 3}, {"subscription": 4}, {"subscription": 5}]

        testJobCreator = JobCreatorPoller(**configDict)
        cProfile.runctx("testJobCreator.algorithm(parameters = input)", globals(), locals(), filename = "workStats.stat")


        p = pstats.Stats('workStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(.2)

        return


    def testD_HugeTest(self):
        """
        Don't run this one either

        """

        return


        myThread = threading.currentThread()

        config = self.getConfig()

        name         = makeUUID()
        nSubs        = 10
        nFiles       = 5000
        workloadName = 'Tier1ReReco'

        workload = self.createWorkload(workloadName = workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name = name, nSubs = nSubs, nFiles = nFiles, workflowURL = workloadPath)




        testJobCreator = JobCreatorPoller(config = config)


        # First, can we run once without everything crashing?
        startTime = time.time()
        testJobCreator.algorithm()
        stopTime  = time.time()

        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")

        self.assertEqual(len(result), nSubs*nFiles)


        print("Job took %f seconds to run" %(stopTime - startTime))


        # Count database objects
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')[0].fetchall()
        self.assertEqual(len(result), nSubs * nFiles)

        return


    def stuffWMBS(self, workflowURL, name):
        """
        _stuffWMBS_

        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test job creation.  Three completed job groups each
        containing several files are injected.  Another incomplete job group is
        also injected.  Also files are added to the "Mergeable" subscription as
        well as to the output fileset for their jobgroups.
        """
        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "s1", seName = "somese.cern.ch")

        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")

        mergeFileset = Fileset(name = "mergeFileset")
        mergeFileset.create()
        bogusFileset = Fileset(name = "bogusFileset")
        bogusFileset.create()

        mergeWorkflow = Workflow(spec = workflowURL, owner = "mnorman",
                                 name = name, task="/TestWorkload/ReReco")
        mergeWorkflow.create()

        mergeSubscription = Subscription(fileset = mergeFileset,
                                         workflow = mergeWorkflow,
                                         split_algo = "ParentlessMergeBySize")
        mergeSubscription.create()
        bogusSubscription = Subscription(fileset = bogusFileset,
                                         workflow = mergeWorkflow,
                                         split_algo = "ParentlessMergeBySize")

        file1 = File(lfn = "file1", size = 1024, events = 1024, first_event = 0,
                     locations = set(["somese.cern.ch"]))
        file1.addRun(Run(1, *[45]))
        file1.create()
        file2 = File(lfn = "file2", size = 1024, events = 1024,
                     first_event = 1024, locations = set(["somese.cern.ch"]))
        file2.addRun(Run(1, *[45]))
        file2.create()
        file3 = File(lfn = "file3", size = 1024, events = 1024,
                     first_event = 2048, locations = set(["somese.cern.ch"]))
        file3.addRun(Run(1, *[45]))
        file3.create()
        file4 = File(lfn = "file4", size = 1024, events = 1024,
                     first_event = 3072, locations = set(["somese.cern.ch"]))
        file4.addRun(Run(1, *[45]))
        file4.create()

        fileA = File(lfn = "fileA", size = 1024, events = 1024,
                     first_event = 0, locations = set(["somese.cern.ch"]))
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileB = File(lfn = "fileB", size = 1024, events = 1024,
                     first_event = 1024, locations = set(["somese.cern.ch"]))
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileC = File(lfn = "fileC", size = 1024, events = 1024,
                     first_event = 2048, locations = set(["somese.cern.ch"]))
        fileC.addRun(Run(1, *[46]))
        fileC.create()

        fileI = File(lfn = "fileI", size = 1024, events = 1024,
                     first_event = 0, locations = set(["somese.cern.ch"]))
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileII = File(lfn = "fileII", size = 1024, events = 1024,
                      first_event = 1024, locations = set(["somese.cern.ch"]))
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileIII = File(lfn = "fileIII", size = 1024, events = 102400,
                       first_event = 2048, locations = set(["somese.cern.ch"]))
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIV = File(lfn = "fileIV", size = 102400, events = 1024,
                      first_event = 3072, locations = set(["somese.cern.ch"]))
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()

        for file in [file1, file2, file3, file4, fileA, fileB, fileC, fileI,
                     fileII, fileIII, fileIV]:
            mergeFileset.addFile(file)
            bogusFileset.addFile(file)

        mergeFileset.commit()
        bogusFileset.commit()

        return



    def testE_TestNonProxySplitting(self):
        """
        _TestNonProxySplitting_

        Test and see if we can split things without
        a proxy.
        """



        myThread = threading.currentThread()

        config = self.getConfig()
        config.JobCreator.workerThreads = 1

        name         = makeUUID()
        workloadName = 'TestWorkload'


        workload = self.createWorkload(workloadName = workloadName)

        # Change the file splitting algo
        procTask = workload.getTask("ReReco")
        procTask.setSplittingAlgorithm("ParentlessMergeBySize", min_merge_size = 1, max_merge_size = 100000,
                            max_merge_events = 200000)

        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')


        self.stuffWMBS(workflowURL = workloadPath, name = name)


        testJobCreator = JobCreatorPoller(config = config)

        testJobCreator.algorithm()

        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 1)

        result = getJobsAction.execute(state = 'Created', jobType = "Merge")
        self.assertEqual(len(result), 0)

        return


if __name__ == "__main__":

    unittest.main()
    deleteConfig(ConfigFile)
