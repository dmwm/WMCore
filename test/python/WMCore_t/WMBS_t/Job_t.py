#!/usr/bin/env python
"""
_Job_t_

Unit tests for the WMBS job class.
"""

__revision__ = "$Id: Job_t.py,v 1.34 2009/12/16 17:45:45 sfoulkes Exp $"
__version__ = "$Revision: 1.34 $"

import unittest
import logging
import os
import commands
import threading
import random
import time

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset as Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMFactory import WMFactory
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine import DefaultConfig
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID import makeUUID

from WMQuality.TestInit import TestInit

class JobTest(unittest.TestCase):


    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """


        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()        
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", 
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        locationNew = self.daoFactory(classname = "Locations.New")
        locationNew.execute(siteName = "test.site.ch", jobSlots = 300)
        locationNew.execute(siteName = "test2.site.ch", jobSlots = 300)

        return
          
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        
    def createTestJob(self):
        """
        _createTestJob_

        Create a test job with two files as input.  This will also create the
        appropriate workflow, jobgroup and subscription.
        """
        testWorkflow = Workflow(spec = makeUUID(), owner = "Simon",
                                name = makeUUID(), task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow,
                                        type = "Merge")
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(1, *[46]))
        testFileA.create()
        testFileB.create()

        testJob = Job(name = makeUUID(), files = [testFileA, testFileB])
        testJob["couch_record"] = "somecouchrecord"
        testJob["location"] = "test.site.ch"
        testJob.create(group = testJobGroup)
        testJob.associateFiles()

        return testJob
            
    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Create and then delete a job.  Use the job class's exists() method to
        determine if the job has been written to the database before it is
        created, after it has been created and after it has been deleted.
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
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileA.create()
        testFileB.create()

        testJob = Job(name = "TestJob", files = [testFileA, testFileB])
       
        assert testJob.exists() == False, \
               "ERROR: Job exists before it was created"

        testJob.create(group = testJobGroup)

        assert testJob.exists() >= 0, \
               "ERROR: Job does not exist after it was created"

        testJob.delete()

        assert testJob.exists() == False, \
               "ERROR: Job exists after it was delete"

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a job and save it to the database.  Roll back the database
        transaction and verify that the job is no longer in the database.
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
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileA.create()
        testFileB.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJob = Job(name = "TestJob", files = [testFileA, testFileB])

        assert testJob.exists() == False, \
               "ERROR: Job exists before it was created"

        testJob.create(group = testJobGroup)

        assert testJob.exists() >= 0, \
               "ERROR: Job does not exist after it was created"

        myThread.transaction.rollback()

        assert testJob.exists() == False, \
               "ERROR: Job exists after transaction was rolled back."

        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a new job and commit it to the database.  Start a new transaction
        and delete the file from the database.  Verify that the file has been
        deleted.  After that, roll back the transaction and verify that the
        job is once again in the database.
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
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileA.create()
        testFileB.create()

        testJob = Job(name = "TestJob", files = [testFileA, testFileB])

        assert testJob.exists() == False, \
               "ERROR: Job exists before it was created"

        testJob.create(group = testJobGroup)

        assert testJob.exists() >= 0, \
               "ERROR: Job does not exist after it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJob.delete()

        assert testJob.exists() == False, \
               "ERROR: Job exists after it was delete"

        myThread.transaction.rollback()

        assert testJob.exists() >= 0, \
               "ERROR: Job does not exist after transaction was rolled back."

        return

    def testCreateDeleteExistsNoFiles(self):
        """
        _testCreateDeleteExistsNoFiles_

        Create and then delete a job but don't add any input files to it.
        Use the job class's exists() method to determine if the job has been
        written to the database before it is created, after it has been created
        and after it has been deleted.
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
        
        testJob = Job(name = "TestJob")

        assert testJob.exists() == False, \
               "ERROR: Job exists before it was created"

        testJob.create(group = testJobGroup)

        assert testJob.exists() >= 0, \
               "ERROR: Job does not exist after it was created"

        testJob.delete()

        assert testJob.exists() == False, \
               "ERROR: Job exists after it was delete"

        return    

    def testLoad(self):
        """
        _testLoad_

        Create a job and save it to the database.  Load it back from the
        database using the name and the id and then verify that all information
        was loaded correctly.
        """
        testJobA = self.createTestJob()
        testJobB = Job(id = testJobA["id"])
        testJobC = Job(name = testJobA["name"])
        testJobB.load()
        testJobC.load()

        assert type(testJobB["id"]) == int or \
               type(testJobB["id"]) == long, \
               "ERROR: Job id is not an int."

        assert type(testJobC["id"]) == int or \
               type(testJobC["id"]) == long, \
               "ERROR: Job id is not an int."

        assert type(testJobB["jobgroup"]) == int or \
               type(testJobB["jobgroup"]) == long, \
               "ERROR: Job group id is not an int."

        assert type(testJobC["jobgroup"]) == int or \
               type(testJobC["jobgroup"]) == long, \
               "ERROR: Job group id is not an int."        

        assert type(testJobB["retry_count"]) == int or \
               type(testJobB["retry_count"]) == long, \
               "ERROR: retry_count is not an int."

        assert type(testJobC["retry_count"]) == int or \
               type(testJobC["retry_count"]) == long, \
               "ERROR: retry_count is not an int."

        assert (testJobA["id"] == testJobB["id"]) and \
               (testJobA["name"] == testJobB["name"]) and \
               (testJobA["jobgroup"] == testJobB["jobgroup"]) and \
               (testJobA["couch_record"] == testJobB["couch_record"]) and \
               (testJobA["location"] == testJobB["location"]), \
               "ERROR: Load from ID didn't load everything correctly"

        assert (testJobA["id"] == testJobC["id"]) and \
               (testJobA["name"] == testJobC["name"]) and \
               (testJobA["jobgroup"] == testJobC["jobgroup"]) and \
               (testJobA["couch_record"] == testJobC["couch_record"]) and \
               (testJobA["location"] == testJobC["location"]), \
               "ERROR: Load from name didn't load everything correctly"

        return

    def testLoadData(self):
        """
        _testLoadData_

        Create a job and save it to the database.  Load it back from the
        database using the name and the id.  Verify that all job information
        is correct including input files and the job mask.
        """
        testJobA = self.createTestJob()

        testJobA["mask"]["FirstEvent"] = 1
        testJobA["mask"]["LastEvent"] = 2
        testJobA["mask"]["FirstLumi"] = 3
        testJobA["mask"]["LastLumi"] = 4
        testJobA["mask"]["FirstRun"] = 5
        testJobA["mask"]["LastRun"] = 6

        testJobA.save()

        testJobB = Job(id = testJobA["id"])
        testJobC = Job(name = testJobA["name"])
        testJobB.loadData()
        testJobC.loadData()

        assert type(testJobB["id"]) == int or \
               type(testJobB["id"]) == long, \
               "ERROR: Job id is not an int."

        assert type(testJobC["id"]) == int or \
               type(testJobC["id"]) == long, \
               "ERROR: Job id is not an int."        

        assert type(testJobB["jobgroup"]) == int or \
               type(testJobB["jobgroup"]) == long, \
               "ERROR: Job group id is not an int."

        assert type(testJobC["jobgroup"]) == int or \
               type(testJobC["jobgroup"]) == long, \
               "ERROR: Job group id is not an int."        

        assert (testJobA["id"] == testJobB["id"]) and \
               (testJobA["name"] == testJobB["name"]) and \
               (testJobA["jobgroup"] == testJobB["jobgroup"]) and \
               (testJobA["couch_record"] == testJobB["couch_record"]) and \
               (testJobA["location"] == testJobB["location"]), \
               "ERROR: Load from ID didn't load everything correctly"

        assert (testJobA["id"] == testJobC["id"]) and \
               (testJobA["name"] == testJobC["name"]) and \
               (testJobA["jobgroup"] == testJobC["jobgroup"]) and \
               (testJobA["couch_record"] == testJobC["couch_record"]) and \
               (testJobA["location"] == testJobC["location"]), \
               "ERROR: Load from name didn't load everything correctly"

        assert testJobA["mask"] == testJobB["mask"], \
               "ERROR: Job mask did not load properly"

        assert testJobA["mask"] == testJobC["mask"], \
               "ERROR: Job mask did not load properly"        

        goldenFiles = testJobA.getFiles()
        for testFile in testJobB.getFiles():
            assert testFile in goldenFiles, \
                   "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Job didn't load all files"

        goldenFiles = testJobA.getFiles()
        for testFile in testJobC.getFiles():
            assert testFile in goldenFiles, \
                   "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Job didn't load all files"        
        
        return

    def testGetFiles(self):
        """
        _testGetFiles_

        Test the Job's getFiles() method.  This should load the files from
        the database if they haven't been loaded already.
        """
        testJobA = self.createTestJob()
        
        testJobB = Job(id = testJobA["id"])
        testJobB.loadData()

        goldenFiles = testJobA.getFiles()
        for testFile in testJobB.getFiles():
            assert testFile in goldenFiles, \
                   "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Job didn't load all files"

        return

    def testSaveTransaction(self):
        """
        _testSaveTransaction_

        Create a job and a job mask and save them both to the database.  Load
        the job from the database and verify that everything was written
        correctly.  Begin a new transaction and update the job mask again.
        Load the mask and verify that it's correct.  Finally, rollback the
        transaction and reload the mask to verify that it is in the correct
        state.
        """
        testJobA = self.createTestJob()

        testJobA["mask"]["FirstEvent"] = 1
        testJobA["mask"]["LastEvent"] = 2
        testJobA["mask"]["FirstLumi"] = 3
        testJobA["mask"]["LastLumi"] = 4
        testJobA["mask"]["FirstRun"] = 5
        testJobA["mask"]["LastRun"] = 6

        testJobA.save()

        testJobB = Job(id = testJobA["id"])        
        testJobB.loadData()

        assert testJobA["mask"] == testJobB["mask"], \
               "ERROR: Job mask did not load properly"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJobA["mask"]["FirstEvent"] = 7
        testJobA["mask"]["LastEvent"] = 8
        testJobA["mask"]["FirstLumi"] = 9
        testJobA["mask"]["LastLumi"] = 10
        testJobA["mask"]["FirstRun"] = 11
        testJobA["mask"]["LastRun"] = 12
        testJobA["name"] = "stevesJob"
        testJobA["couch_record"] = "someCouchRecord"
        testJobA["location"] = "test2.site.ch"

        testJobA.save()
        testJobC = Job(id = testJobA["id"])
        testJobC.loadData()

        assert testJobA["mask"] == testJobC["mask"], \
            "ERROR: Job mask did not load properly"

        assert testJobC["name"] == "stevesJob", \
            "ERROR: Job name did not save"

        assert testJobC["couch_record"] == "someCouchRecord", \
            "ERROR: Job couch record did not save"

        assert testJobC["location"] == "test2.site.ch", \
            "ERROR: Job site did not save"

        myThread.transaction.rollback()

        testJobD = Job(id = testJobA["id"])
        testJobD.loadData()

        assert testJobB["mask"] == testJobD["mask"], \
               "ERROR: Job mask did not load properly"        
        
        return


    def testJobState(self):
        """
        _testJobState_

        Unittest to see if we can figure out what the jobState actually is and set it
        """

        testJobA = self.createTestJob()

        value = testJobA.getState()

        self.assertEqual(value, 'new')

        return

    def testNewestStateChangeDAO(self):
        """
        _testNewestStateChangeDAO_

        Test the Jobs.NewsetStateChangeForSub DAO that will return the current
        state and time of state transition that last occured for a job created
        by the given subscription.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        newestStateDAO = self.daoFactory(classname = "Jobs.NewestStateChangeForSub")
        result = newestStateDAO.execute(subscription = testSubscription["id"])

        assert len(result) == 0, \
               "ERROR: DAO returned more than 0 jobs..."

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testJobA = Job(name = "TestJobA")
        testJobA.create(group = testJobGroup)

        stateChanger = ChangeState(DefaultConfig.config,
                                   "job_t_jsm_database")
        stateChanger.propagate([testJobA], "created", "new")

        result = newestStateDAO.execute(subscription = testSubscription["id"])

        assert len(result) == 1, \
               "ERROR: Wrong number of jobs returned: %s" % len(result)

        assert result[0]["name"] == "created", \
               "ERROR: Job returned in wrong state: %s" % result[0]["name"]

        testJobB = Job(name = "TestJobB")
        testJobB.create(group = testJobGroup)

        # We need to wait a little bit otherwise both jobs could be returned by
        # the DAO as their state changes happened within the same second.
        time.sleep(5)

        stateChanger.propagate([testJobB], "createfailed", "new")

        result = newestStateDAO.execute(subscription = testSubscription["id"])

        assert len(result) == 1, \
               "ERROR: Wrong number of jobs returned: %s" % len(result)

        assert result[0]["name"] == "createfailed", \
               "ERROR: Job returned in wrong state: %s" % result[0]["name"]

        return

    def testJobCacheDir(self):
        """
        _testJobCacheDir_
        
        Check retrieval of the jobCache directory.
        """
        testJobA = self.createTestJob()
        value = testJobA.getCache()

        self.assertEqual(value, None)

        testJobA.setCache('UnderTheDeepBlueSea')
        value = testJobA.getCache()

        self.assertEqual(value, 'UnderTheDeepBlueSea')

        return
   
    def testGetOutputParentLFNs(self):
        """
        _testGetOutputParentLFNs_

        Verify that the getOutputDBSParentLFNs() method returns the correct
        parent LFNs.
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
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10, 
                         merged = True)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10, 
                         merged = True)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10, 
                         merged = False)
        testFileD = File(lfn = "/this/is/a/lfnD", size = 1024, events = 10, 
                         merged = False)
        testFileE = File(lfn = "/this/is/a/lfnE", size = 1024, events = 10, 
                         merged = True)
        testFileF = File(lfn = "/this/is/a/lfnF", size = 1024, events = 10, 
                         merged = True)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFileE.create()
        testFileF.create()

        testFileE.addChild(testFileC["lfn"])
        testFileF.addChild(testFileD["lfn"])

        testJobA = Job(name = "TestJob", files = [testFileA, testFileB])
        testJobA["couch_record"] = "somecouchrecord"
        testJobA["location"] = "test.site.ch"
        testJobA.create(group = testJobGroup)
        testJobA.associateFiles()

        testJobB = Job(name = "TestJobB", files = [testFileC, testFileD])
        testJobB["couch_record"] = "somecouchrecord"
        testJobB["location"] = "test.site.ch"
        testJobB.create(group = testJobGroup)
        testJobB.associateFiles()

        goldenLFNs = ["/this/is/a/lfnA", "/this/is/a/lfnB"]
        
        parentLFNs = testJobA.getOutputDBSParentLFNs()
        for parentLFN in parentLFNs:
            assert parentLFN in goldenLFNs, \
                "ERROR: Unknown lfn: %s" % parentLFN
            goldenLFNs.remove(parentLFN)

        assert len(goldenLFNs) == 0, \
            "ERROR: LFNs are missing: %s" % goldenLFNs

        goldenLFNs = ["/this/is/a/lfnE", "/this/is/a/lfnF"]
        
        parentLFNs = testJobB.getOutputDBSParentLFNs()
        for parentLFN in parentLFNs:
            assert parentLFN in goldenLFNs, \
                "ERROR: Unknown lfn: %s" % parentLFN
            goldenLFNs.remove(parentLFN)

        assert len(goldenLFNs) == 0, \
            "ERROR: LFNs are missing..."
        
        return

    def testJobFWJRPath(self):
        """
        _testJobFWJRPath_

        Verify the correct operation of the Jobs.SetFWJRPath and
        Jobs.GetFWJRByState DAOs.
        """
        testJobA = self.createTestJob()
        testJobA["state"] = "complete"
        testJobB = self.createTestJob()
        testJobB["state"] = "executing"
        testJobC = self.createTestJob()
        testJobC["state"] = "complete"

        myThread = threading.currentThread()
        setFWJRAction = self.daoFactory(classname = "Jobs.SetFWJRPath")
        setFWJRAction.execute(jobID = testJobA["id"], fwjrPath = "NonsenseA",
                              conn = myThread.transaction.conn,
                              transaction = True)                              
        setFWJRAction.execute(jobID = testJobB["id"], fwjrPath = "NonsenseB",
                              conn = myThread.transaction.conn,
                              transaction = True)                              
        setFWJRAction.execute(jobID = testJobC["id"], fwjrPath = "NonsenseC",
                              conn = myThread.transaction.conn,
                              transaction = True)                              

        changeStateAction = self.daoFactory(classname = "Jobs.ChangeState")
        changeStateAction.execute(jobs = [testJobA, testJobB, testJobC],
                                  conn = myThread.transaction.conn,
                                  transaction = True)

        getJobsAction = self.daoFactory(classname = "Jobs.GetFWJRByState")
        jobs = getJobsAction.execute(state = "complete",
                                     conn = myThread.transaction.conn,
                                     transaction = True)

        goldenIDs = [testJobA["id"], testJobC["id"]]
        for job in jobs:
            assert job["id"] in goldenIDs, \
                   "Error: Unknown job: %s" % job["id"]

            goldenIDs.remove(job["id"])

            if job["id"] == testJobA["id"]:
                assert job["fwjr_path"] == "NonsenseA", \
                       "Error: Wrong fwjr path: %s" % job["fwjr_path"]
            else:
                assert job["fwjr_path"] == "NonsenseC", \
                       "Error: Wrong fwjr path: %s" % job["fwjr_path"]

        assert len(goldenIDs) == 0, \
               "Error: Jobs missing: %s" % len(goldenIDs)

        return

    def testFailJobInput(self):
        """
        _testFailJobInput_

        Test the Jobs.FailInput DAO and verify that it doesn't affect other
        jobs/subscriptions that run over the same files.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test")
        bogusWorkflow = Workflow(spec = "spec1.xml", owner = "Steve",
                                name = "wf002", task="Test")
        testWorkflow.create()
        bogusWorkflow.create()

        testFileset = Fileset(name = "TestFileset")
        bogusFileset = Fileset(name = "BogusFileset")
        testFileset.create()
        bogusFileset.create()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        bogusSubscription = Subscription(fileset = bogusFileset,
                                         workflow = bogusWorkflow)
        testSubscription.create()
        bogusSubscription.create()

        testFileA = File(lfn = makeUUID(), locations = "test.site.ch")
        testFileB = File(lfn = makeUUID(), locations = "test.site.ch")
        testFileC = File(lfn = makeUUID(), locations = "test.site.ch")
        testFileA.create()
        testFileB.create()
        testFileC.create()
                         
        testFileset.addFile([testFileA, testFileB, testFileC])
        bogusFileset.addFile([testFileA, testFileB, testFileC])        
        testFileset.commit()
        bogusFileset.commit()

        testSubscription.completeFiles([testFileA, testFileB, testFileC])
        bogusSubscription.acquireFiles([testFileA, testFileB, testFileC])

        testJobGroup = JobGroup(subscription = testSubscription)
        bogusJobGroup = JobGroup(subscription = bogusSubscription)
        testJobGroup.create()
        bogusJobGroup.create()

        testJob = Job(name = "TestJob", files = [testFileA, testFileB, testFileC])
        bogusJob = Job(name = "BogusJob", files = [testFileA, testFileB, testFileC])
        testJob.create(group = testJobGroup)
        bogusJob.create(group = bogusJobGroup)
        
        testJob.failInputFiles()

        availFiles = len(testSubscription.filesOfStatus("Available"))
        assert availFiles == 0, \
               "Error: test sub has wrong number of available files: %s" % availFiles

        acqFiles = len(testSubscription.filesOfStatus("Acquired"))
        assert acqFiles == 0, \
               "Error: test sub has wrong number of acquired files: %s" % acqFiles

        compFiles = len(testSubscription.filesOfStatus("Completed"))
        assert compFiles == 0, \
               "Error: test sub has wrong number of complete files: %s" % compFiles

        failFiles = len(testSubscription.filesOfStatus("Failed"))
        assert failFiles == 3, \
               "Error: test sub has wrong number of failed files: %s" % failFiles

        availFiles = len(bogusSubscription.filesOfStatus("Available"))
        assert availFiles == 0, \
               "Error: test sub has wrong number of available files: %s" % availFiles

        acqFiles = len(bogusSubscription.filesOfStatus("Acquired"))
        assert acqFiles == 3, \
               "Error: test sub has wrong number of acquired files: %s" % acqFiles

        compFiles = len(bogusSubscription.filesOfStatus("Completed"))
        assert compFiles == 0, \
               "Error: test sub has wrong number of complete files: %s" % compFiles

        failFiles = len(bogusSubscription.filesOfStatus("Failed"))
        assert failFiles == 0, \
               "Error: test sub has wrong number of failed files: %s" % failFiles        
        
        return

    def testCompleteJobInput(self):
        """
        _testCompleteJobInput_

        Verify the correct output of the CompleteInput DAO.  This should mark
        the input for a job as complete once all the jobs that run over a
        particular file have complete successfully.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test")
        bogusWorkflow = Workflow(spec = "spec1.xml", owner = "Steve",
                                name = "wf002", task="Test")
        testWorkflow.create()
        bogusWorkflow.create()

        testFileset = Fileset(name = "TestFileset")
        bogusFileset = Fileset(name = "BogusFileset")
        testFileset.create()
        bogusFileset.create()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        bogusSubscription = Subscription(fileset = bogusFileset,
                                         workflow = bogusWorkflow)
        testSubscription.create()
        bogusSubscription.create()

        testFileA = File(lfn = makeUUID(), locations = "test.site.ch")
        testFileB = File(lfn = makeUUID(), locations = "test.site.ch")
        testFileA.create()
        testFileB.create()
                         
        testFileset.addFile([testFileA, testFileB])
        bogusFileset.addFile([testFileA, testFileB])        
        testFileset.commit()
        bogusFileset.commit()

        testSubscription.acquireFiles([testFileA, testFileB])
        bogusSubscription.acquireFiles([testFileA, testFileB])

        testJobGroup = JobGroup(subscription = testSubscription)
        bogusJobGroup = JobGroup(subscription = bogusSubscription)
        testJobGroup.create()
        bogusJobGroup.create()

        testJobA = Job(name = "TestJobA", files = [testFileA])
        testJobB = Job(name = "TestJobB", files = [testFileA])
        testJobC = Job(name = "TestJobC", files = [testFileB])        
        bogusJob = Job(name = "BogusJob", files = [testFileA, testFileB])
        testJobA.create(group = testJobGroup)
        testJobB.create(group = testJobGroup)
        testJobC.create(group = testJobGroup)
        bogusJob.create(group = bogusJobGroup)

        testJobA["outcome"] = "success"
        testJobB["outcome"] = "failure"
        testJobC["outcome"] = "success"
        testJobA.save()
        testJobB.save()
        testJobC.save()

        testJobA.completeInputFiles()
        
        compFiles = len(testSubscription.filesOfStatus("Completed"))
        assert compFiles == 0, \
               "Error: test sub has wrong number of complete files: %s" % compFiles

        testJobB["outcome"] = "success"
        testJobB.save()
        
        testJobB.completeInputFiles()
        
        availFiles = len(testSubscription.filesOfStatus("Available"))
        assert availFiles == 0, \
               "Error: test sub has wrong number of available files: %s" % availFiles

        acqFiles = len(testSubscription.filesOfStatus("Acquired"))
        assert acqFiles == 1, \
               "Error: test sub has wrong number of acquired files: %s" % acqFiles

        compFiles = len(testSubscription.filesOfStatus("Completed"))
        assert compFiles == 1, \
               "Error: test sub has wrong number of complete files: %s" % compFiles

        failFiles = len(testSubscription.filesOfStatus("Failed"))
        assert failFiles == 0, \
               "Error: test sub has wrong number of failed files: %s" % failFiles

        availFiles = len(bogusSubscription.filesOfStatus("Available"))
        assert availFiles == 0, \
               "Error: test sub has wrong number of available files: %s" % availFiles

        acqFiles = len(bogusSubscription.filesOfStatus("Acquired"))
        assert acqFiles == 2, \
               "Error: test sub has wrong number of acquired files: %s" % acqFiles

        compFiles = len(bogusSubscription.filesOfStatus("Completed"))
        assert compFiles == 0, \
               "Error: test sub has wrong number of complete files: %s" % compFiles

        failFiles = len(bogusSubscription.filesOfStatus("Failed"))
        assert failFiles == 0, \
               "Error: test sub has wrong number of failed files: %s" % failFiles        
        
        return

    def testJobTypeDAO(self):
        """
        _testJobTypeDAO_

        Verify that the Jobs.GetType DAO returns the correct job type.  The
        job type is retrieved from the subscription type.
        """
        testJob = self.createTestJob()

        jobTypeAction = self.daoFactory(classname = "Jobs.GetType")
        jobType = jobTypeAction.execute(jobID = testJob["id"])

        assert jobType == "Merge", \
               "Error: GetJobType DAO returned the wrong job type."

        return        

    def testGetOutputMapDAO(self):
        """
        _testGetOutputMapDAO_

        Verify the proper behavior of the GetOutputMapDAO for a variety of
        different processing chains.
        """
        recoOutputFileset = Fileset(name = "RECO")
        recoOutputFileset.create()
        mergedRecoOutputFileset = Fileset(name = "MergedRECO")
        mergedRecoOutputFileset.create()        
        alcaOutputFileset = Fileset(name = "ALCA")
        alcaOutputFileset.create()
        dqmOutputFileset = Fileset(name = "DQM")
        dqmOutputFileset.create()        

        testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                name = "TestWF", task = "None")
        testWorkflow.create()
        testWorkflow.addOutput("output", recoOutputFileset)
        testWorkflow.addOutput("ALCARECOStreamCombined", alcaOutputFileset)
        testWorkflow.addOutput("DQM", dqmOutputFileset)        

        testRecoMergeWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                         name = "TestRecoMergeWF", task = "None")
        testRecoMergeWorkflow.create()
        testRecoMergeWorkflow.addOutput("anything", mergedRecoOutputFileset)

        testRecoProcWorkflow = Workflow(spec = "wf004.xml", owner = "Steve",
                                         name = "TestRecoProcWF", task = "None")
        testRecoProcWorkflow.create()

        testAlcaChildWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                         name = "TestAlcaChildWF", task = "None")
        testAlcaChildWorkflow.create()

        inputFile = File(lfn = "/path/to/some/lfn", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFile.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "EventBased",
                                        type = "Processing")

        testMergeRecoSubscription = Subscription(fileset = recoOutputFileset,
                                                 workflow = testRecoMergeWorkflow,
                                                 split_algo = "WMBSMergeBySize",
                                                 type = "Merge")
        testProcRecoSubscription = Subscription(fileset = recoOutputFileset,
                                                workflow = testRecoProcWorkflow,
                                                split_algo = "FileBased",
                                                type = "Processing")

        testChildAlcaSubscription = Subscription(fileset = alcaOutputFileset,
                                                 workflow = testAlcaChildWorkflow,
                                                 split_algo = "FileBased",
                                                 type = "Processing")
        testSubscription.create()
        testMergeRecoSubscription.create()
        testProcRecoSubscription.create()
        testChildAlcaSubscription.create()
        testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testJob = Job(name = "SplitJobA", files = [inputFile])
        testJob.create(group = testJobGroup)
        testJob["state"] = "complete"
        testJob.save()

        outputMapAction = self.daoFactory(classname = "Jobs.GetOutputMap")
        outputMap = outputMapAction.execute(jobID = testJob["id"])

        assert len(outputMap.keys()) == 3, \
               "Error: Wrong number of outputs for primary workflow."

        assert outputMap.has_key("output"), \
               "Error: Output map is missing 'output' key."
        assert outputMap.has_key("ALCARECOStreamCombined"), \
               "Error: Output map is missing 'ALCARECOStreamCombined' key."
        assert outputMap.has_key("DQM"), \
               "Error: Output map is missing 'DQM' key."        

        dqmMap = outputMap["DQM"]
        assert len(dqmMap["children"]) == 0, \
               "Error: DQM output map shouldn't have any child workflows."
        assert dqmMap["fileset"] == dqmOutputFileset.id

        alcaMap = outputMap["ALCARECOStreamCombined"]
        assert len(alcaMap["children"]) == 1, \
               "Error: ALCA output map should have one child workflow."
        assert alcaMap["fileset"] == alcaOutputFileset.id, \
               "Error: Wrong output fileset for ALCA."

        alcaChild = alcaMap["children"][0]
        assert alcaChild["child_sub_output_id"] == None, \
               "Error: ALCA workflow shouldn't have child subscription."
        assert alcaChild["child_sub_output_fset"] == None, \
               "Error: ALCA workflow shouldn't have child output fileset."
        assert alcaChild["child_sub_type"] == "Processing", \
               "Error: ALCA child subscription type should be processing."

        recoMap = outputMap["output"]
        assert len(recoMap["children"]) == 2, \
               "Error: RECO output map should have two children."
        assert recoMap["fileset"] == recoOutputFileset.id, \
               "Error: Wrong output fileset for RECO."

        goldenRecoOutput = [{"child_sub_output_id": None,
                             "child_sub_output_fset": None,
                             "child_sub_type": "Processing"},
                            {"child_sub_output_id": "anything",
                             "child_sub_output_fset": mergedRecoOutputFileset.id,
                             "child_sub_type": "Merge"}]
        for outputChild in recoMap["children"]:
            assert outputChild in goldenRecoOutput, \
                   "Error: Extra output for RECO: %s" % outputChild

            goldenRecoOutput.remove(outputChild)

        assert len(goldenRecoOutput) == 0, \
               "Error: Missing outputs in output map for RECO."
        
        return

if __name__ == "__main__":
    unittest.main() 
