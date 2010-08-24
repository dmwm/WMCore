#!/usr/bin/env python2.4
"""
_JobGroup_t_

"""

__revision__ = "$Id: JobGroup_t.py,v 1.3 2008/11/20 17:00:34 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

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

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                         run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                         run = 1, lumi = 45)
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

    
    
if __name__ == "__main__":
    unittest.main() 
