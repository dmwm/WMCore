#!/usr/bin/env python

"""
ErrorHandler test TestErrorHandler module and the harness
"""

__revision__ = "$Id: ErrorHandler_t.py,v 1.17 2010/07/02 14:25:53 mnorman Exp $"
__version__ = "$Revision: 1.17 $"

import os
import threading
import time
import unittest

from WMComponent.ErrorHandler.ErrorHandlerPoller import ErrorHandlerPoller

import WMCore.WMInit
from WMQuality.TestInit   import TestInit
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

class ErrorHandlerTest(unittest.TestCase):
    """
    TestCase for TestErrorHandler module 
    """
    def setUp(self):
        """
        setup for test.
        """
        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")
        self.setJobTime = self.daofactory(classname = "Jobs.SetStateTime")
        locationAction = self.daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "malpaquet", seName = "malpaquet")
        self.testDir = self.testInit.generateWorkDir()
        self.nJobs = 10
        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
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

        config.component_("ErrorHandler")
        # The log level of the component. 
        config.ErrorHandler.logLevel = 'DEBUG'
        # The namespace of the component
        config.ErrorHandler.namespace = 'WMComponent.ErrorHandler.ErrorHandler'
        # maximum number of threads we want to deal
        # with messages per pool.
        config.ErrorHandler.maxThreads = 30
        # maximum number of retries we want for job
        config.ErrorHandler.maxRetries = 10
        # The poll interval at which to look for failed jobs
        config.ErrorHandler.pollInterval = 60

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', None)
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "errorhandler_t"

        return config

    def createTestJobGroup(self, nJobs = 10):
        """
        Creates a group of several jobs
        """


        myThread = threading.currentThread()
        myThread.transaction.begin()
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
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
        testFileB.setLocation('malpaquet')
        
        testFileA.create()
        testFileB.create()

        for i in range(0, nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJobGroup.add(testJob)
        
        testJobGroup.commit()


        testSubscription.acquireFiles(files = [testFileA, testFileB])
        testSubscription.save()
        myThread.transaction.commit()
        
        return testJobGroup


    def testCreate(self):
        """
        WMComponent_t.ErrorHandler_t.ErrorHandler_t:testCreate()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)
        
        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')

        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), self.nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testSubmit(self):
        """
        WMComponent_t.ErrorHandler_t.ErrorHandler_t:testSubmit()
        
        Mimics creation of component and test jobs failed in submit stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')

        idList = self.getJobs.execute(state = 'SubmitFailed')
        self.assertEqual(len(idList), self.nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state = 'SubmitFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testJobs(self):
        """
        WMComponent_t.ErrorHandler_t.ErrorHandler_t.testJobs()

        Mimics creation of component and test jobs failed in execute stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)
        
        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        idList = self.getJobs.execute(state = 'JobFailed')
        self.assertEqual(len(idList), self.nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state = 'JobFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)
        return


    def testExhausted(self):
        """
        _testExhausted_

        Test that the system can exhaust jobs correctly
        """

        testJobGroup = self.createTestJobGroup(nJobs = self.nJobs)

        config = self.getConfig()
        config.ErrorHandler.maxRetries = 1
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        testSubscription = Subscription(id = 1) # You should only have one
        testSubscription.load()
        testSubscription.loadData()

        # Do we have files to start with?
        self.assertEqual(len(testSubscription.filesOfStatus("Acquired")), 2)


        testErrorHandler = ErrorHandlerPoller(config)
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state = 'JobFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Exhausted')
        self.assertEqual(len(idList), self.nJobs)

        

        # Did we fail the files?
        self.assertEqual(len(testSubscription.filesOfStatus("Acquired")), 0)
        self.assertEqual(len(testSubscription.filesOfStatus("Failed")), 2)



    def testZ_Profile(self):
        """
        _testProfile_

        Do a full profile of the poller
        """

        return

        import cProfile, pstats

        nJobs = 1000

        testJobGroup = self.createTestJobGroup(nJobs = nJobs)
        
        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')

        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        testErrorHandler.setup(None)
        startTime = time.time()
        #cProfile.runctx("testErrorHandler.algorithm()", globals(), locals(), filename = "profStats.stat")
        testErrorHandler.algorithm()
        stopTime = time.time()

        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), nJobs)

        print("Took %f seconds to run polling algo" % (stopTime - startTime))

        p = pstats.Stats('profStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(0.2)
        
        return

if __name__ == '__main__':
    unittest.main()

