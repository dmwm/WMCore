#!/usr/bin/env python
"""
_Job_t_

Unit tests for the WMBS job class.
"""

__revision__ = "$Id: Job_t.py,v 1.19 2009/05/12 16:16:25 sfoulkes Exp $"
__version__ = "$Revision: 1.19 $"

import unittest
import logging
import os
import commands
import threading
import random
from sets import Set

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset as WMBSFileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DataStructs.Run import Run

class JobTest(unittest.TestCase):
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
        WMBS tables.
        """
        if self._setup:
            return

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
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

        self._setup = True
        return
          
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return
        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
            
        self._teardown = True
        
    def createTestJob(self):
        """
        _createTestJob_

        Create a test job with two files as input.  This will also create the
        appropriate workflow, jobgroup and subscription.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(1, *[46]))
        testFileA.create()
        testFileB.create()

        testJob = Job(name = "TestJob", files = [testFileA, testFileB])
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
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
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
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
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
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
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
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
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

        # test loading state, state_time, retry count, etc...

        testJobA = self.createTestJob()
        testJobB = Job(id = testJobA["id"])
        testJobC = Job(name = "TestJob")
        testJobB.load()
        testJobC.load()

        assert type(testJobB["id"]) == int, \
               "ERROR: Job id is not an int."

        assert type(testJobC["id"]) == int, \
               "ERROR: Job id is not an int."

        assert type(testJobB["jobgroup"]) == int, \
               "ERROR: Job group id is not an int."

        assert type(testJobC["jobgroup"]) == int, \
               "ERROR: Job group id is not an int."        

        assert type(testJobB["retry_count"]) == int, \
               "ERROR: retry_count is not an int."

        assert type(testJobC["retry_count"]) == int, \
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
        testJobC = Job(name = "TestJob")
        testJobB.loadData()
        testJobC.loadData()

        assert type(testJobB["id"]) == int, \
               "ERROR: Job id is not an int."

        assert type(testJobC["id"]) == int, \
               "ERROR: Job id is not an int."        

        assert type(testJobB["jobgroup"]) == int, \
               "ERROR: Job group id is not an int."

        assert type(testJobC["jobgroup"]) == int, \
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
    
if __name__ == "__main__":
    unittest.main() 
