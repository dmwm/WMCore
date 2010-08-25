#!/usr/bin/env python

"""
ErrorHandler test TestErrorHandler module and the harness
"""

__revision__ = "$Id: ErrorHandler_t.py,v 1.13 2010/02/02 20:50:30 mnorman Exp $"
__version__ = "$Revision: 1.13 $"
__author__ = "fvlingen@caltech.edu"

import os
import threading
import time
import unittest

from WMComponent.ErrorHandler.ErrorHandler import ErrorHandler

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

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)

        
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        self.testDir = self.testInit.generateWorkDir()


        self.nJobs = 10

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()

        self.testInit.delWorkDir()

        return



    def getConfig(self, configPath=None):
        if configPath == None:
            configPath = os.path.join(os.getenv('WMCOREBASE'), \
                                                'src/python/WMComponent/ErrorHandler/DefaultConfig.py')

        config = Configuration()

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)

        # Now the CoreDatabase information
        # This should be the dialect, dburl, etc

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")


        config.component_("JobAccountant")
        #The log level of the component. 
        config.JobAccountant.logLevel = 'INFO'
        config.JobAccountant.pollInterval = 10


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
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'mnorman:theworst@cmssrv52.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"


        return config



    def createTestJobGroup(self):
        """
        Creates a group of several jobs

        """

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
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        for i in range(0,self.nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJobGroup.add(testJob)
        
        testJobGroup.commit()

        return testJobGroup




    def testCreate(self):
        """
        Mimics creation of component and test jobs failed in create stage.
        """
        ErrorHandlerTest._teardown = True
        # read the default config first.
        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')

        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), self.nJobs)



        # we set the maxRetries to 10 for testing purposes
        config.ErrorHandler.maxRetries = 10

        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()

        testErrorHandler = ErrorHandler(config)
        testErrorHandler.prepareToStart()


        time.sleep(20)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        idList = self.getJobs.execute(state = 'CreateFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        return
    

    def testSubmit(self):
        """
        Mimics creation of component and test jobs failed in submit stage.
        """
        ErrorHandlerTest._teardown = True
        # read the default config first.
        config = self.getConfig()


        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')

        idList = self.getJobs.execute(state = 'SubmitFailed')
        self.assertEqual(len(idList), self.nJobs)


        # we set the maxRetries to 10 for testing purposes
        config.ErrorHandler.maxRetries = 10

        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()



        testErrorHandler = ErrorHandler(config)
        testErrorHandler.prepareToStart()


        time.sleep(20)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        idList = self.getJobs.execute(state = 'SubmitFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        return


    def testJobs(self):
        """
        Mimics creation of component and test jobs failed in execute stage.
        """
        ErrorHandlerTest._teardown = True
        # read the default config first.
        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        idList = self.getJobs.execute(state = 'JobFailed')
        self.assertEqual(len(idList), self.nJobs)


        # we set the maxRetries to 10 for testing purposes
        config.ErrorHandler.maxRetries = 10

        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()



        testErrorHandler = ErrorHandler(config)
        testErrorHandler.prepareToStart()


        time.sleep(20)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        idList = self.getJobs.execute(state = 'JobFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        return


if __name__ == '__main__':
    unittest.main()

