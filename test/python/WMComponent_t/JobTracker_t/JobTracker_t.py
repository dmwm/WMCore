#!/usr/bin/env python

"""
JobTracker test 
"""

__revision__ = "$Id: JobTracker_t.py,v 1.1 2009/10/02 21:23:59 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import os
import logging
import threading
import unittest
import time


from WMCore.Agent.Configuration import loadConfigurationFile

from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.WMFactory     import WMFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

from WMCore.DataStructs.Run   import Run

from WMComponent.JobTracker.JobTracker import JobTracker

from WMCore.JobStateMachine.ChangeState import ChangeState

class JobTrackerTest(unittest.TestCase):
    """
    TestCase for TestJobTracker module 
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
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        locationAction = self.daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "malpaquet") 


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
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()

        factory = WMFactory("MsgService", "WMCore.MsgService")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()

        factory = WMFactory("ThreadPool", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()
        
        

    def getConfig(self, configPath=os.path.join(os.getenv('WMCOREBASE'), \
                                                'src/python/WMComponent/JobTracker/DefaultConfig.py')):


        if os.path.isfile(configPath):
            # read the default config first.
            config = loadConfigurationFile(configPath)
        else:
            config = Configuration()
            config.component_("JobTracker")
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
            testJob['location'] = 'malpaquet'
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJobGroup.add(testJob)
            testJob.create(testJobGroup)

            
        
        testJobGroup.commit()

        return testJobGroup

    def testA_ComponentTest(self):
        """
        Tests the components, and sees if it passes non-existant jobs
        Otherwise does nothing.
        """

        myThread = threading.currentThread()

        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')

        testJobTracker = JobTracker(config)
        testJobTracker.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        jobListAction = self.daofactory(classname = "Jobs.GetAllJobs")
        jobList       = jobListAction.execute(state = "Executing")

        self.assertEqual(len(jobList), 0, "Should be no executing jobs left, but instead there are %i" %(len(jobList)))

        jobListAction = self.daofactory(classname = "Jobs.GetAllJobs")
        jobList       = jobListAction.execute(state = "Complete")

        #The jobs should be complete.
        #If the tracker can't find them, it assumes the job finished.
        self.assertEqual(len(jobList), self.nJobs, "Should be %i complete jobs, but instead there are %i" %(self.nJobs, len(jobList)))

        return


if __name__ == '__main__':
    unittest.main()

