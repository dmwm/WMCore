#!/usr/bin/env python
"""
_JobGroup_t_

Unit tests for the WMBS JobGroup class.
"""




import unittest
import logging
import os
import commands
import threading
import random
import time

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

class JobGroupTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """

        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        #We need to set sites in the locations table
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "site1", seName = "goodse.cern.ch")
        locationAction.execute(siteName = "site2", seName = "malpaquet")
        locationAction.execute(siteName = "site3", seName = "badse.cern.ch")
    
               
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
    
    def createTestJobGroup(self, commitFlag = True):
        """
        _createTestJobGroup_
        
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
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation("goodse.cern.ch")
        testFileA.setLocation("malpaquet")

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileB.setLocation("goodse.cern.ch")
        testFileB.setLocation("malpaquet")

        testFileA.create()
        testFileB.create()

        testJobA = Job(name = "TestJobA")
        testJobA.addFile(testFileA)
        
        testJobB = Job(name = "TestJobB")
        testJobB.addFile(testFileB)
        
        testJobGroup.add(testJobA)
        testJobGroup.add(testJobB)

        if commitFlag:
            testJobGroup.commit()
        
        return testJobGroup


    def createLargerTestJobGroup(self, commitFlag = True):
        """
        _createTestJobGroup_
        
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

        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10)
        testFileC.addRun(Run(10, *[12312]))
        testFileC.setLocation("goodse.cern.ch")
        testFileC.setLocation("malpaquet")

        testFileD = File(lfn = "/this/is/a/lfnD", size = 1024, events = 10)
        testFileD.addRun(Run(10, *[12312]))
        testFileD.setLocation("goodse.cern.ch")
        testFileD.setLocation("malpaquet")

        testFileC.create()
        testFileD.create()

        testJobA = Job(name = "TestJobA1")
        testJobA.addFile(testFileC)
        
        testJobB = Job(name = "TestJobB1")
        testJobB.addFile(testFileD)

        testJobGroup.add(testJobA)
        testJobGroup.add(testJobB)

        for i in range(0,100):
            testJob = Job(name = "TestJob%i" %(i))
            testJob.addFile(testFileC)
            testJobGroup.add(testJob)

        if commitFlag:
            testJobGroup.commit()
        
        return testJobGroup
    
    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Create a JobGroup and then delete it.  Use the JobGroup's exists()
        method to determine if it exists before it is created, after it is
        created and after it is deleted.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testFileset = WMBSFileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)

        assert testJobGroup.exists() == False, \
               "ERROR: Job group exists before it was created"
        
        testJobGroup.create()

        assert testJobGroup.exists() >= 0, \
               "ERROR: Job group does not exist after it was created"
        
        testJobGroup.delete()

        assert testJobGroup.exists() == False, \
               "ERROR: Job group exists after it was deleted"

        testSubscription.delete()
        testFileset.delete()
        testWorkflow.delete()
        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a JobGroup and commit it to the database.  Rollback the database
        transaction and verify that the JobGroup is no longer in the database.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testFileset = WMBSFileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)

        assert testJobGroup.exists() == False, \
               "ERROR: Job group exists before it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testJobGroup.create()

        assert testJobGroup.exists() >= 0, \
               "ERROR: Job group does not exist after it was created"
        
        myThread.transaction.rollback()

        assert testJobGroup.exists() == False, \
               "ERROR: Job group exists after transaction was rolled back."

        testSubscription.delete()
        testFileset.delete()
        testWorkflow.delete()
        return    

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a JobGroup and then commit it to the database.  Begin a
        transaction and the delete the JobGroup from the database.  Using the
        exists() method verify that the JobGroup is not in the database.
        Finally, roll back the transaction and verify that the JobGroup is
        in the database.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testFileset = WMBSFileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)

        assert testJobGroup.exists() == False, \
               "ERROR: Job group exists before it was created"
        
        testJobGroup.create()

        assert testJobGroup.exists() >= 0, \
               "ERROR: Job group does not exist after it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testJobGroup.delete()

        assert testJobGroup.exists() == False, \
               "ERROR: Job group exists after it was deleted"

        myThread.transaction.rollback()

        assert testJobGroup.exists() >= 0, \
               "ERROR: Job group does not exist after transaction was rolled back."        

        testSubscription.delete()
        testFileset.delete()
        testWorkflow.delete()
        return

    def testLoad(self):
        """
        _testLoad_

        Test loading the JobGroup and any associated meta data from the
        database.
        """
        testJobGroupA = self.createTestJobGroup()
        
        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.load()
        testJobGroupC = JobGroup(uid = testJobGroupA.uid)
        testJobGroupC.load()

        assert type(testJobGroupB.id) == int, \
               "ERROR: Job group id is not an int."

        assert type(testJobGroupC.id) == int, \
               "ERROR: Job group id is not an int."        

        assert type(testJobGroupB.subscription["id"]) == int, \
               "ERROR: Job group subscription id is not an int."

        assert type(testJobGroupC.subscription["id"]) == int, \
               "ERROR: Job group subscription id is not an int."        

        assert type(testJobGroupB.output.id) == int, \
               "ERROR: Job group output id is not an int."

        assert type(testJobGroupC.output.id) == int, \
               "ERROR: Job group output id is not an int."        

        assert testJobGroupB.uid == testJobGroupA.uid, \
               "ERROR: Job group did not load uid correctly."

        assert testJobGroupC.id == testJobGroupA.id, \
               "ERROR: Job group did not load id correctly."
        
        assert testJobGroupB.subscription["id"] == \
               testJobGroupA.subscription["id"], \
               "ERROR: Job group did not load subscription correctly"

        assert testJobGroupC.subscription["id"] == \
               testJobGroupA.subscription["id"], \
               "ERROR: Job group did not load subscription correctly"        

        assert testJobGroupB.output.id == testJobGroupA.output.id, \
               "ERROR: Output fileset didn't load properly"

        assert testJobGroupC.output.id == testJobGroupA.output.id, \
               "ERROR: Output fileset didn't load properly"        
        
        return

    def testLoadData(self):
        """
        _testLoadData_

        Test loading the JobGroup, it's meta data and any data associated with
        its output fileset and jobs from the database.
        """
        testJobGroupA = self.createTestJobGroup()

        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.loadData()

        assert testJobGroupB.subscription["id"] == \
               testJobGroupA.subscription["id"], \
               "ERROR: Job group did not load subscription correctly"



        goldenJobs = testJobGroupA.getJobs(type = "list")
        
        for job in testJobGroupB.getJobs(type = "list"):
            assert job in goldenJobs, \
                   "ERROR: JobGroup loaded an unknown job: %s, %s" % (job, goldenJobs)
            goldenJobs.remove(job)

        assert len(goldenJobs) == 0, \
            "ERROR: JobGroup didn't load all jobs"

        assert testJobGroupB.output.id == testJobGroupA.output.id, \
               "ERROR: Output fileset didn't load properly"
        
        return    

    def testCommit(self):
        """
        _testCommit_

        Verify that jobs are not added to a job group until commit() is called
        on the JobGroup.  Also verify that commit() correctly commits the jobs
        to the database.
        """
        testJobGroupA = self.createTestJobGroup(commitFlag = False)

        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.loadData()

        assert len(testJobGroupA.getJobs()) == 0, \
               "ERROR: Original object commited too early"
        assert len(testJobGroupB.getJobs()) == 0, \
               "ERROR: Loaded JobGroup has too many jobs"

        testJobGroupA.commit()
        testJobGroupA.loadData()

        assert len(testJobGroupA.getJobs()) == 2, \
               "ERROR: Original object did not commit jobs"

        testJobGroupC = JobGroup(id = testJobGroupA.id)
        testJobGroupC.loadData()

        assert len(testJobGroupC.getJobs()) == 2, \
               "ERROR: Loaded object has too few jobs."

        return

    def testCommitTransaction(self):
        """
        _testCommitTransaction_

        Create a JobGroup and then add some jobs to it.  Begin a transaction
        and then call commit() on the JobGroup.  Verify that the newly committed
        jobs can be loaded from the database.  Rollback the transaction and then
        verify that the jobs that were committed before are no longer associated
        with the JobGroup.
        """
        testJobGroupA = self.createTestJobGroup(commitFlag = False)
        
        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.loadData()

        assert len(testJobGroupA.getJobs()) == 0, \
               "ERROR: Original object commited too early"

        assert len(testJobGroupB.getJobs()) == 0, \
               "ERROR: Loaded JobGroup has too many jobs"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJobGroupA.commit()

        assert len(testJobGroupA.getJobs()) == 2, \
               "ERROR: Original object did not commit jobs"

        testJobGroupC = JobGroup(id = testJobGroupA.id)
        testJobGroupC.loadData()

        assert len(testJobGroupC.getJobs()) == 2, \
               "ERROR: Loaded object has too few jobs."        

        myThread.transaction.rollback()

        testJobGroupD = JobGroup(id = testJobGroupA.id)
        testJobGroupD.loadData()

        assert len(testJobGroupD.getJobs()) == 0, \
               "ERROR: Loaded object has too many jobs."        

        return


    def testSetGetSite(self):
        """
        _testSetGetSite_

        For the JobCreator we have to be able to specify the site for a JobGroup.
        This function tests the accessors for that information.
        """
        myThread = threading.currentThread()


        testJobGroup = self.createTestJobGroup()
        testJobGroup.setSite("site1")
        result = testJobGroup.getSite()

        self.assertEqual(result, "site1")

        return

    def testCommitBulk(self):
        """
        _testCommitBulk_

        Exactly the same as testCommit, but using commitBulk() instead of commit()
        """

        myThread = threading.currentThread()
        
        testJobGroupA = self.createLargerTestJobGroup(commitFlag = False)

        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.loadData()

        assert len(testJobGroupA.getJobs()) == 0, \
               "ERROR: Original object commited too early"
        assert len(testJobGroupB.getJobs()) == 0, \
               "ERROR: Loaded JobGroup has too many jobs"

        testJobGroupA.commitBulk()
        testJobGroupA.loadData()

        self.assertEqual(len(testJobGroupA.getJobs()), 102)

        testJobGroupC = JobGroup(id = testJobGroupA.id)
        testJobGroupC.loadData()

        self.assertEqual(len(testJobGroupC.getJobs()), 102)

        self.assertEqual(testJobGroupC.jobs[0].getFiles()[0]['lfn'], '/this/is/a/lfnC')
        self.assertEqual(testJobGroupC.jobs[1].getFiles()[0]['lfn'], '/this/is/a/lfnD')

        return


    def testGetLocationsForJobs(self):
        """
        _testGetLocationsForJobs

        Tests the functionality of grabbing locations for a single job

        """

        myThread = threading.currentThread()

        testJobGroup = self.createTestJobGroup()

        result = testJobGroup.getLocationsForJobs()

        self.assertEqual(len(result), 2)
        self.assertEqual("site1" in result, True)
        self.assertEqual("site2" in result, True)

        testJobGroupA = self.createLargerTestJobGroup(commitFlag = True)

        result = testJobGroupA.getLocationsForJobs()

        self.assertEqual(len(result), 2)
        self.assertEqual("site1" in result, True)
        self.assertEqual("site2" in result, True)

        return

    def testGetGroupsByJobStateDAO(self):
        """
        _testGetGroupsByJobStateDAO_

        Verify that the GetGrounsByJobState DAO does what it is supposed to do.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()
        testJobGroupB = JobGroup(subscription = testSubscription)
        testJobGroupB.create()        

        testJobA = Job(name = "TestJobA")
        testJobB = Job(name = "TestJobB")
        
        testJobGroupA.add(testJobA)
        testJobGroupB.add(testJobB)

        testJobGroupA.commit()
        testJobGroupB.commit()

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        stateChangeAction = daofactory(classname = "Jobs.ChangeState")
        testJobA["state"] = "complete"
        testJobB["state"] = "executing"        
        stateChangeAction.execute(jobs = [testJobA, testJobB])

        jobGroupAction = daofactory(classname = "JobGroup.GetGroupsByJobState")
        jobGroups = jobGroupAction.execute(jobState = "complete")

        assert len(jobGroups) == 1, \
               "Error: Wrong number of job groups returned."
        assert jobGroups[0] == testJobGroupA.id, \
               "Error: Wrong job group returned."

        return
    
if __name__ == "__main__":
    unittest.main() 
