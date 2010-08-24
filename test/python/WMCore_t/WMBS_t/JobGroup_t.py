#!/usr/bin/env python2.4
"""
_JobGroup_t_

"""

__revision__ = "$Id: JobGroup_t.py,v 1.5 2008/12/26 15:31:19 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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

class Job_t(unittest.TestCase):
    _setup = False
    _teardown = False
    
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
            
    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
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

    def testLoad(self):
        """
        _testLoad_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.create()
        testFileB.create()

        testFilesetA = Fileset(name = "TestFilesetA", files = Set([testFileA]))
        testFilesetB = Fileset(name = "TestFilesetB", files = Set([testFileB]))
        
        testJobA = Job(name = "TestJobA", files = testFilesetA)
        testJobB = Job(name = "TestJobB", files = testFilesetB)

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)

        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.load()

        assert testJobGroupB.subscription["id"] == testSubscription["id"], \
               "ERROR: Job group did not load subscription correctly"

        goldenJobs = [testJobA.id, testJobB.id]
        for job in testJobGroupB.jobs:
            assert job.id in goldenJobs, \
                   "ERROR: JobGroup loaded an unknown job"
            goldenJobs.remove(job.id)

        assert len(goldenJobs) == 0, \
            "ERROR: JobGroup didn't load all jobs"

        assert testJobGroupB.groupoutput.id == testJobGroupA.groupoutput.id, \
               "ERROR: Output fileset didn't load properly"
        
        return

    def testCommit(self):
        """
        _testCommit_

        Verify that jobs are not added to a job group until commit() is called
        on the JobGroup.  Also verify that commit() correctly commits the jobs
        to the database.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                         cksum=1)
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                         cksum=1)
        testFileB.addRun(Run(1, *[45]))
        testFileA.create()
        testFileB.create()

        testFilesetA = Fileset(name = "TestFilesetA", files = Set([testFileA]))
        testFilesetB = Fileset(name = "TestFilesetB", files = Set([testFileB]))

        testJobA = Job(name = "TestJobA", files = testFilesetA)
        testJobB = Job(name = "TestJobB", files = testFilesetB)

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)

        testJobGroupB = JobGroup(id = testJobGroupA.id)
        testJobGroupB.load()

        assert len(testJobGroupA.jobs) == 0, \
               "ERROR: Original object commited too early"

        assert len(testJobGroupB.jobs) == 0, \
               "ERROR: Loaded JobGroup has too many jobs"

        testJobGroupA.commit()

        assert len(testJobGroupA.jobs) == 2, \
               "ERROR: Original object did not commit jobs"

        testJobGroupC = JobGroup(id = testJobGroupA.id)
        testJobGroupC.load()

        assert len(testJobGroupC.jobs) == 2, \
               "ERROR: Loaded object has too few jobs."

    
    
if __name__ == "__main__":
    unittest.main() 
