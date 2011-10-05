#!/usr/bin/env python
#pylint: disable-msg=E1101, W0142, C0103
# E1101: Use configuration files
# W0142: Use ** magic
# C0103: Different naming conventions apply for tests
"""
RetryManager test for module and the harness
"""




import os
import os.path
import threading
import time
import unittest

from WMComponent.RetryManager.RetryManagerPoller import RetryManagerPoller

import WMCore.WMInit

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
#from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.JobGroup     import JobGroup

from WMCore.DataStructs.Run   import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Agent.Configuration import Configuration
from WMCore.FwkJobReport.Report import Report

class RetryManagerTest(unittest.TestCase):
    """
    TestCase for TestRetryManager module 
    """
    def setUp(self):
        """
        setup for test.
        """
        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setupCouch("retry_manager_t/jobs", "JobDump")
        self.testInit.setupCouch("retry_manager_t/fwjrs", "FWJRDump")
        
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")
        self.setJobTime = self.daofactory(classname = "Jobs.SetStateTime")
        self.testDir = self.testInit.generateWorkDir()
        self.nJobs = 10
        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
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

        config.component_("RetryManager")
        config.RetryManager.logLevel     = 'DEBUG'
        config.RetryManager.namespace    = 'WMComponent.RetryManager.RetryManager'
        config.RetryManager.maxRetries   = 10
        config.RetryManager.pollInterval = 10
        # These are the cooloff times for the RetryManager, the times it waits
        # Before attempting resubmission
        config.RetryManager.coolOffTime  = {'create': 120, 'submit': 120, 'job': 120}
        # Path to plugin directory
        config.RetryManager.pluginPath   = 'WMComponent.RetryManager.PlugIns'
        #config.RetryManager.pluginName   = ''
        config.RetryManager.WMCoreBase   = WMCore.WMInit.getWMBASE()
        config.RetryManager.componentDir = os.path.join(os.getcwd(), 'Components')

        # ErrorHandler
        # Not essential, but useful for ProcessingAlgo
        config.component_("ErrorHandler")
        config.ErrorHandler.maxRetries = 5

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', None)
        config.JobStateMachine.couchDBName     = "retry_manager_t"

        return config

    def createTestJobGroup(self, nJobs):
        """
        _createTestJobGroup_
        
        Creates a group of several jobs
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = makeUUID(), task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        for i in range(0, nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['cache_dir'] = os.path.join(self.testDir, testJob['name'])
            os.mkdir(testJob['cache_dir'])
            testJobGroup.add(testJob)
        
        testJobGroup.commit()
        return testJobGroup

    def testA_Create(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testCreate()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'New')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testB_Submit(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testSubmit()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)


        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)        

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testC_Job(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testJob()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)
        
        config = self.getConfig()
        config.RetryManager.pluginName   = 'DefaultRetryAlgo'
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)
        return



    def testD_SquaredAlgo(self):
        """
        _testSquaredAlgo_

        Test the squared algorithm to make sure it loads and works
        """

        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        config.RetryManager.pluginName   = 'SquaredAlgo'
        config.RetryManager.coolOffTime  = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'submitcooloff')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')


        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 5)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 12)

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)



    def testE_ExponentialAlgo(self):
        """
        _testExponentialAlgo_

        Test the exponential algorithm to make sure it loads and works
        """

        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        config.RetryManager.pluginName   = 'ExponentialAlgo'
        config.RetryManager.coolOffTime  = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'submitcooloff')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')


        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 5)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 12)

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)


    def testF_LinearAlgo(self):
        """
        _testLinearAlgo_

        Test the linear algorithm to make sure it loads and works
        """

        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        config.RetryManager.pluginName   = 'ExponentialAlgo'
        config.RetryManager.coolOffTime  = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'submitcooloff')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')


        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 5)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 12)

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)

        return


    def testG_ProcessingAlgo(self):
        """
        _ProcessingAlgo_

        Test for the ProcessingAlgo Prototype
        """

        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        config.RetryManager.pluginName   = 'ProcessingAlgo'
        config.RetryManager.coolOffTime  = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        fwjrPath = os.path.abspath("../JobAccountant_t/fwjrs/badBackfillJobReport.pkl")
        report = Report()
        report.load(fwjrPath)
        for job in testJobGroup.jobs:
            job['fwjr'] = report
            job['retry_count'] = 0
            report.save(os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count']))
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.algorithm()

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs) 

        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup.jobs:
            j = Job(id = job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 1)
            report.save(os.path.join(j['cache_dir'], "Report.%i.pkl" % j['retry_count']))

        config.RetryManager.ProcessingAlgoOneMoreErrorCodes = [8020]
        testRetryManager2 = RetryManagerPoller(config)
        testRetryManager2.algorithm()

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs) 

        for job in testJobGroup.jobs:
            j = Job(id = job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 5)


        # Now test timeout
        testJobGroup2 = self.createTestJobGroup(nJobs = self.nJobs)

        # Cycle jobs
        for job in testJobGroup2.jobs:
            job['fwjr'] = report
            job['retry_count'] = 0
            report.save(os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count']))
        changer.propagate(testJobGroup2.jobs, 'created', 'new')
        changer.propagate(testJobGroup2.jobs, 'executing', 'created')
        changer.propagate(testJobGroup2.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup2.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup2.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup2.jobs:
            j = Job(id = job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 0)

        config.RetryManager.ProcessingAlgoOneMoreErrorCodes = []
        config.RetryManager.ProcessingAlgoMaxRuntime = 1
        testRetryManager3 = RetryManagerPoller(config)
        testRetryManager3.algorithm()

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs * 2)

        for job in testJobGroup2.jobs:
            j = Job(id = job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 5)
        

        return



    def testY_MultipleIterations(self):
        """
        _MultipleIterations_
        
        Paranoia based check to see if I'm saving class instances correctly
        """

        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'New')
        self.assertEqual(len(idList), self.nJobs)



        # Make a new jobGroup for a second run
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        # Set job state
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        # Set them to go off
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 200)


        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'New')
        self.assertEqual(len(idList), self.nJobs * 2)   


        return


    def testZ_Profile(self):
        """
        _Profile_

        Do a basic profiling of the algo
        """

        return

        import cProfile, pstats

        nJobs = 1000

        testJobGroup = self.createTestJobGroup(nJobs = nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)

        startTime = time.time()
        #cProfile.runctx("testRetryManager.algorithm()", globals(), locals(), filename = "profStats.stat")
        testRetryManager.algorithm(None)
        stopTime  = time.time()
        
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'New')
        self.assertEqual(len(idList), nJobs)


        print("Took %f seconds to run polling algo" % (stopTime - startTime))

        p = pstats.Stats('profStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(0.2)

        return

if __name__ == '__main__':
    unittest.main()
