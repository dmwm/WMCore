#!/usr/bin/env python

"""
Resubmission test:

This is an integration test that tests the resubmission chain

"""

import os
import threading
import time
import unittest

from WMComponent.ErrorHandler.ErrorHandlerPoller import ErrorHandlerPoller
from WMComponent.JobCreator.JobCreatorPoller     import JobCreatorPoller

import WMCore.WMInit
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.DAOFactory          import DAOFactory
from WMCore.Services.UUID       import makeUUID


from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.JobGroup     import JobGroup

from WMCore.DataStructs.Run             import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Agent.Configuration         import Configuration
from WMCore.WMSpec.Makers.TaskMaker     import TaskMaker
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.ACDC.Resubmitter            import Resubmitter, resubmitWorkflow

from WMCore_t.WMSpec_t.TestSpec         import testWorkload





class ResubmissionTest(unittest.TestCase):
    """
    TestCase for Resubmission system
    """
    
    def setUp(self):
        """
        setup for test.
        """
        myThread = threading.currentThread()
        
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)
        self.testInit.setupCouch("resubmission_t", "GroupUser", "ACDC", "JobDump")

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")
        self.setJobTime = self.daoFactory(classname = "Jobs.SetStateTime")
        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "malpaquet", seName = "malpaquet")
        self.testDir = self.testInit.generateWorkDir()
        self.nJobs = 10

        self.dataCS = DataCollectionService(url = self.testInit.couchUrl,
                                            database = self.testInit.couchDbName)
        
        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        #self.testInit.tearDownCouch()
        return

    def getConfig(self):
        """
        _getConfig_

        """
        config = Configuration()

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        # ErrorHandler
        config.component_("ErrorHandler")
        config.ErrorHandler.logLevel = 'DEBUG'
        config.ErrorHandler.namespace = 'WMComponent.ErrorHandler.ErrorHandler'
        config.ErrorHandler.maxThreads = 30
        config.ErrorHandler.maxRetries = 1
        config.ErrorHandler.pollInterval = 60

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', None)
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "errorhandler_t"

        # JobCreator
        config.component_("JobCreator")
        config.JobCreator.namespace = 'WMComponent.JobCreator.JobCreator'
        config.JobCreator.logLevel = 'INFO'
        config.JobCreator.maxThreads                = 1
        config.JobCreator.UpdateFromResourceControl = True
        config.JobCreator.pollInterval              = 10
        config.JobCreator.jobCacheDir               = self.testDir
        config.JobCreator.defaultJobType            = 'processing' #Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads             = 4
        config.JobCreator.componentDir              = os.path.join(os.getcwd(), 'Components')
        config.JobCreator.useWorkQueue              = True
        config.JobCreator.WorkQueueParams           = {'emulateDBSReader': True}

        # ACDC
        config.section_('ACDC')
        config.ACDC.couchurl         = self.testInit.couchUrl
        config.ACDC.database         = self.testInit.couchDbName
        config.ACDC.resubmitCacheDir = os.path.join(self.testDir, 'wf')

        return config


    def createWorkload(self, workloadName = 'Test', emulator = True):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload("Tier1ReReco")
        rereco = workload.getTask("ReReco")

        # Add RequestManager stuff
        workload.data.request.section_('schema')
        workload.data.request.schema.Requestor = 'nobody'
        workload.data.request.schema.Group     = 'testers'
        
        
        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def createTestHeaders(self, workloadPath = 'test', fileEnder = ''):
        """
        _createTestHeaders_

        Create test files, workflow and subscription
        """
        testWorkflow = Workflow(spec = workloadPath, owner = "Simon",
                                name = "wf001", task="/TestWorkload/ReReco")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFile0 = File(lfn = "/this/is/a/parent", size = 1024, events = 10)
        testFile0.addRun(Run(10, *[12312]))
        testFile0.setLocation('malpaquet')

        testFileA = File(lfn = "/this/is/a/lfnA" , size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileB.setLocation('malpaquet')

        testFile0.create()
        testFileA.create()
        testFileB.create()

        testFileA.addParent(lfn = "/this/is/a/parent")
        testFileB.addParent(lfn = "/this/is/a/parent")

        testSubscription.acquireFiles(files = [testFileA, testFileB])
        testSubscription.save()

        return testSubscription, [testFileA, testFileB]
        
    

    def createTestJobGroup(self, testSubscription, testFiles, nJobs = 10, retry_count = 1, ):
        """
        _createTestJobGroup_
        
        Creates a group of several jobs
        """
        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        

        for i in range(0, nJobs):
            testJob = Job(name = makeUUID())
            testJob['retry_count'] = retry_count
            testJob['retry_max'] = 1
            testJobGroup.add(testJob)
            testJob.create(group = testJobGroup)
            for f in testFiles:
                testJob.addFile(f)
            testJob.save()

        
        testJobGroup.commit()

        return testJobGroup


    def testA_SimpleSubscription(self):
        """
        SimpleSubscription
        
        Mimics the failure of a job, and then resubmits it
        """

        workloadName = 'TestWorkload'

        workload = self.createWorkload(workloadName = workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload',
                                    'WMSandbox', 'WMWorkload.pkl')

        testSub, testFiles  = self.createTestHeaders(workloadPath = workloadPath)
        testJobGroup        = self.createTestJobGroup(nJobs = self.nJobs,
                                                      testSubscription = testSub,
                                                      testFiles = testFiles)
        
        config  = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')

        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), self.nJobs)

        # Now exhaust them
        for job in testJobGroup.jobs:
            job['retry_count'] = 6
            job.save()

        testErrorHandler = ErrorHandlerPoller(config)
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state = 'Exhausted')
        self.assertEqual(len(idList), self.nJobs)

        # Check that it showed up in ACDC
        collList = self.dataCS.listDataCollections()

        self.assertEqual(len(collList), 1)

        collection = collList[0]
        self.assertEqual(collection['database'], self.testInit.couchDbName)
        self.assertEqual(collection['url'], self.testInit.couchUrl)
        self.assertEqual(collection['collection_type'], 'ACDC.CollectionTypes.DataCollection')
        self.assertEqual(collection['name'], workloadName)

        # Now look at what's inside
        for fileset in self.dataCS.listFilesets(collection):
            for f in fileset.files():
                self.assertTrue(f['lfn'] in ["/this/is/a/lfnA", "/this/is/a/lfnB"])
                self.assertEqual(f['events'], 10)
                self.assertEqual(f['size'], 1024)
                self.assertEqual(f['parents'], [u'/this/is/a/parent'])

        # Make sure there's only one subscription so far
        sub = Subscription(id = 2)
        self.assertFalse(sub.exists())
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 0)

        testFileC = File(lfn = "/this/is/a/lfnC" , size = 1024, events = 10)
        testFileC.addRun(Run(10, *[12312]))
        testFileC.setLocation('malpaquet')

        testFileD = File(lfn = "/this/is/a/lfnD", size = 1024, events = 10)
        testFileD.addRun(Run(10, *[12312]))
        testFileD.setLocation('malpaquet')

        testFileC.create()
        testFileD.create()
        testFileC.addParent(lfn = "/this/is/a/parent")
        testFileD.addParent(lfn = "/this/is/a/parent")
        
        testSub.acquireFiles(files = [testFileC, testFileD])
        testSub.save()
        
        testJobGroup2 = self.createTestJobGroup(nJobs = self.nJobs,
                                                testSubscription = testSub,
                                                testFiles = [testFileC, testFileD])
        changer.propagate(testJobGroup2.jobs, 'createfailed', 'new')
        # Now exhaust them
        for job in testJobGroup2.jobs:
            job['retry_count'] = 6
            job.save()
        testErrorHandler.algorithm(None)

        # Check that it showed up in ACDC
        collList = self.dataCS.listDataCollections()
        self.assertEqual(len(collList), 1)


        # Check that we can change the dataset
        wmSpec = resubmitWorkflow(wmSpec = workload)

        self.assertTrue(wmSpec.getResubmitFlag())
        self.assertEqual(wmSpec.name(), '%s_resubmit' % workloadName)
        self.assertEqual(wmSpec.listOutputDatasets(), ['/MinimumBias/Commissioning10-v4_resubmit/RAW'])


        # Get ready to split things into chunks
        chunkSize = 1
        resubmitter = Resubmitter(config = config)
        chunks = resubmitter.chunkWorkflow(wmSpec = wmSpec,
                                           taskName = '/TestWorkload/ReReco',
                                           chunkSize = chunkSize)
        self.assertEqual(len(chunks), 4)
        offset = 0
        for chunk in chunks:
            self.assertEqual(chunk['files'], 1)
            self.assertEqual(chunk['events'], 10)
            self.assertEqual(chunk['offset'], offset)
            offset += 1

        for chunk in chunks:
            files = resubmitter.loadWorkflowChunk(wmSpec = wmSpec, taskName = '/TestWorkload/ReReco',
                                                  chunkOffset = chunk['offset'], chunkSize = chunkSize)
            self.assertEqual(len(files), 1)
            self.assertTrue(files[0]['lfn'] in ["/this/is/a/lfnA", "/this/is/a/lfnB",
                                                "/this/is/a/lfnC", "/this/is/a/lfnD"])
            self.assertEqual(files[0]['events'], 10)
        #files = resubmitter.loadWorkflow(wmSpec = wmSpec, taskName = '/TestWorkload/ReReco')

        #self.assertEqual(files.keys(), [frozenset(['malpaquet'])])
        #self.assertEqual(len(files[frozenset(['malpaquet'])]), 4)
        #for f in files[frozenset(['malpaquet'])]:
        #    self.assertTrue(f['lfn'] in ["/this/is/a/lfnA", "/this/is/a/lfnB", "/this/is/a/lfnC", "/this/is/a/lfnD"])
        #    self.assertEqual(f['events'], 10)
        #    self.assertEqual(f['size'], 1024)
        #    self.assertEqual(f['parents'], [u'/this/is/a/parent'])
        

        ## Now there should be a second subscription
        ## containing the files from the first
        #sub.load()
        #self.assertEqual(sub.exists(), sub['id'])
        #sub.loadData()
        #self.assertEqual(sub['workflow'].task, '/%s/ReReco' % workloadName)
        #self.assertTrue(os.path.isfile(sub['workflow'].spec))
        #self.assertTrue(os.path.isfile(os.path.join(config.ACDC.resubmitCacheDir, workloadName,
        #                                            '%s-Sandbox.tar.bz2' % workloadName)))
        #for f in sub['fileset'].files:
        #    self.assertTrue(f['lfn'] in ["/this/is/a/lfnA", "/this/is/a/lfnB"])
        #    self.assertEqual(f['events'], 10)
        #    self.assertEqual(f['size'], 1024)
        #    self.assertEqual(f['parents'].pop()['lfn'], '/this/is/a/parent')
        #
        #
        ## At this point, everything should be inserted and ready
        ## for job creation
        #
        #jobCreator = JobCreatorPoller(config = config)
        #jobCreator.algorithm()
        #
        #
        #result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        #
        #self.assertEqual(len(result), 2)  # Should be one job per file
        #
        #for jobID in result:
        #    job = Job(id = jobID)
        #    job.loadData()
        #    self.assertEqual(len(job['input_files']), 1)
        #    self.assertTrue(job['input_files'][0] in testFiles)
            
        return


if __name__ == '__main__':
    unittest.main()
    
