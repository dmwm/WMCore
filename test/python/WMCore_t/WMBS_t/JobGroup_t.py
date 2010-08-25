#!/usr/bin/env python
"""
_JobGroup_t_

Unit tests for the WMBS JobGroup class.
"""

__revision__ = "$Id: JobGroup_t.py,v 1.19 2009/05/12 16:17:30 sfoulkes Exp $"
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

class JobGroupTest(unittest.TestCase):
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

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
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
                   "ERROR: JobGroup loaded an unknown job"
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

if __name__ == "__main__":
    unittest.main() 
