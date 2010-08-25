#!/usr/bin/env python

"""
ErrorHandler test TestErrorHandler module and the harness
"""

__revision__ = "$Id: ErrorHandler_t.py,v 1.9 2009/07/28 21:28:24 mnorman Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "fvlingen@caltech.edu"

import commands
import logging
import os
import threading
import time
import unittest

from WMComponent.ErrorHandler.ErrorHandler import ErrorHandler

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory     import WMFactory
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

class ErrorHandlerTest(unittest.TestCase):
    """
    TestCase for TestErrorHandler module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        ErrorHandlerTest._setup_done = True
        
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")


        self.nJobs = 10

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()

        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()

        ErrorHandlerTest._teardown = False



    def getConfig(self, configPath=os.path.join(os.getenv('WMCOREBASE'), \
                                                'src/python/WMComponent/ErrorHandler/DefaultConfig.py')):


        if os.path.isfile(configPath):
            # read the default config first.
            config = loadConfigurationFile(configPath)
        else:
            config = Configuration()
            config.component_("JobAccountant")
            #The log level of the component. 
            config.JobAccountant.logLevel = 'INFO'
            config.JobAccountant.pollInterval = 10

        myThread = threading.currentThread()

        config.section_("General")
        
        config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            myThread.database = os.getenv("DATABASE")



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




    def runTest(self):
        """
        Run error handler test.
        """
        self.testA()

if __name__ == '__main__':
    unittest.main()

