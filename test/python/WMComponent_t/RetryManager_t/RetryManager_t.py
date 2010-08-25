#!/usr/bin/env python

"""
RetryManager test for module and the harness
"""

__revision__ = "$Id: RetryManager_t.py,v 1.2 2009/10/13 22:04:43 meloam Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import os
import threading
import time
import unittest

from WMComponent.RetryManager.RetryManager import RetryManager


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

class RetryManagerTest(unittest.TestCase):
    """
    TestCase for TestRetryManager module 
    """

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMCore.MsgService"],
                                useDefault = False)
        
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")


        self.nJobs = 10

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()



    def getConfig(self, configPath=os.path.join(os.getenv('WMCOREBASE'), \
                                                'src/python/WMComponent/RetryManager/DefaultConfig.py')):

        config = self.testInit.getConfiguration(configPath)
       
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
            testJobGroup.add(testJob)
        
        testJobGroup.commit()

        return testJobGroup



    def testCreate(self):
        """
        Mimics creation of component and test jobs failed in create stage.
        """

        myThread = threading.currentThread()

        # read the default config first.
        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)



        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()

        testRetryManager = RetryManager(config)
        testRetryManager.prepareToStart()

        time.sleep(50)

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        time.sleep(100)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'New')
        self.assertEqual(len(idList), self.nJobs)

        return


    def testSubmit(self):
        """
        Mimics creation of component and test jobs failed in create stage.
        """
        # read the default config first.
        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)



        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()

        testRetryManager = RetryManager(config)
        testRetryManager.prepareToStart()

        time.sleep(50)

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        time.sleep(100)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)

        return


    def testJob(self):
        """
        Mimics creation of component and test jobs failed in create stage.
        """
        # read the default config first.
        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)



        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()

        testRetryManager = RetryManager(config)
        testRetryManager.prepareToStart()

        time.sleep(50)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        time.sleep(100)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)


        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)

        return

if __name__ == '__main__':
    unittest.main()
