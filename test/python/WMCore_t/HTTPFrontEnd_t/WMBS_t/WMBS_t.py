#!/usr/bin/env python

import unittest, os, logging, commands, random, threading
from sets import Set

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.Job      import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine import DefaultConfig

from WMCore.Services.Requests import Requests, JSONRequests
import urllib

class WMBSServiceTest(unittest.TestCase):
    _setup = False
    _teardown = False

    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        
        This doesn't start server automatically.
        You need to start server before - make sure change self.server_url,
        if it is not the same as given one - localhost:8080.
        
        WMCORE/src/python/WMCore/WebTools/Root.py --ini=WMCORE/src/python/WMCore/HTTPFrontEnd/WMBSDefaultConfig.py
        """
        self.server_url = 'localhost:8080'
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        
        locationAction = self.daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "test.site.ch")
        locationAction.execute(siteName = "base.site.ch")
        testSubscription, testFileA, testFileB, testFileC = \
           self.createSubscriptionWithFileABC()
        self.createTestJob(testSubscription, 'TestJob1', testFileA)
        self.createTestJob(testSubscription, 'TestJob2', testFileB)
        self.createTestJob(testSubscription, 'TestJob3', testFileC)
        
        return

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()
        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        
        
    def createSubscriptionWithFileABC(self):
        """"
        _createSubscriptionWithFileABC_

        Create a subscription where the input fileset has three files.  Also
        create a second subscription that has acquired two of the files.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testWorkflow2 = Workflow(spec = "specBOGUS.xml", owner = "Simon",
                                name = "wfBOGUS", task = "Test")
        testWorkflow2.create()        

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 20,
                         locations = Set(["test.site.ch"]))
        testFileA.addRun(Run(1, *[45]))
                                 
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 20,
                         locations = Set(["test.site.ch"]))                         
        testFileB.addRun(Run(1, *[46]))
        
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 20,
                         locations = Set(["test.site.ch"]))
        testFileC.addRun(Run(2, *[48]))
         
        testFileA.create()
        testFileB.create()
        testFileC.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()
        testSubscription2 = Subscription(fileset = testFileset,
                                         workflow = testWorkflow2)
        testSubscription2.create()
        testSubscription2.acquireFiles([testFileA, testFileB])
        
        #return (testSubscription, testFileset, testWorkflow, testFileA,
        #        testFileB, testFileC)
        
        return (testSubscription, testFileA, testFileB, testFileC)
        
    def createTestJob(self, testSubscription, jobName, *testFiles):
        """
        _createTestJob_

        Create a test job with two files as input.  This will also create the
        appropriate workflow, jobgroup and subscription.
        """

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testFiles = list(testFiles)
        testJob = Job(name = jobName, files = testFiles)
        testJob["couch_record"] = "somecouchrecord"
        testJob["location"] = "test.site.ch"
        testJob.create(group = testJobGroup)
        
    def wmbsServiceSetup(self, argstring, kargs={}, returnType='text'):
        
        if returnType == 'json':
            request = JSONRequests(self.server_url)
        else:
            request = Requests(self.server_url)
        results = request.get("/wmbs/%s/" % argstring, kargs)
        
        return results
    
    def testAllMethods(self):
        pass
        
    def testJobs(self):
        print "\nTesting jobs service: Should return all the job id and state of jobs" 
        print self.wmbsServiceSetup('jobs')
        
    def testJobCount(self):
        print "\nTesting job count service: Should return the job count by and state of jobs" 
        
        print self.wmbsServiceSetup('jobcount')
    
    def testJobsBySubs(self):
        print "\nTesting jobsbysubs service: Should return the jobs by given fileset and workflow and specified time" 
        param = {"fileset_name": 'TestFileset', 'workflow_name':'wf001', 'state_time': 0}
        print self.wmbsServiceSetup('jobsbysubs', param)
    
    def testJobCountBySubsAndRun(self):
        print "\nTesting jobcountbysubs service: Should return the job count by given subscription and run" 
        param = {"fileset_name": 'TestFileset', 'workflow_name':'wf001', 'run':1 }
        print self.wmbsServiceSetup('jobcountbysubs', param)
        
if __name__ == "__main__":
    unittest.main()
    