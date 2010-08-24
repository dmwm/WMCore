#!/usr/bin/env python
"""
_Job_t_

"""

__revision__ = "$Id: Job_t.py,v 1.2 2008/12/01 22:16:13 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

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
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                         run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                         run = 1, lumi = 45)        
        testFileA.create()
        testFileB.create()

        testFileset = Fileset(name = "TestFileset", files = Set([testFileA, testFileB]))

        testJob = Job(name = "TestJob", files = testFileset)

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

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                         run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                         run = 1, lumi = 45)        
        testFileA.create()
        testFileB.create()

        testFileset = Fileset(name = "TestFileset", files = Set([testFileA, testFileB]))
        testFileset.commit()

        testJobA = Job(name = "TestJob", files = testFileset)
        testJobA.create(group = testJobGroup)

        testJobA.mask["FirstEvent"] = 1
        testJobA.mask["LastEvent"] = 2
        testJobA.mask["FirstLumi"] = 3
        testJobA.mask["LastLumi"] = 4
        testJobA.mask["FirstRun"] = 5
        testJobA.mask["LastRun"] = 6

        testJobA.save()

        testJobB = Job(id = testJobA.id)
        testJobC = Job(name = "TestJob")
        testJobB.load(method = "Jobs.LoadFromID")
        testJobC.load(method = "Jobs.LoadFromName")

        assert (testJobA.id == testJobB.id) and \
               (testJobA.name == testJobB.name) and \
               (testJobA.job_group == testJobB.job_group), \
               "ERROR: Load from ID didn't load everything correctly"

        assert (testJobA.id == testJobC.id) and \
               (testJobA.name == testJobC.name) and \
               (testJobA.job_group == testJobC.job_group), \
               "ERROR: Load from name didn't load everything correctly"        
               
        assert testJobA.mask == testJobB.mask, \
               "ERROR: Job mask did not load properly"

        assert testJobA.mask == testJobC.mask, \
               "ERROR: Job mask did not load properly"        

        goldenFiles = [testFileA, testFileB]
        for testFile in testJobB.file_set.getFiles():
            assert testFile in goldenFiles, \
                   "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Job didn't load all files"

        goldenFiles = [testFileA, testFileB]
        for testFile in testJobC.file_set.getFiles():
            assert testFile in goldenFiles, \
                   "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Job didn't load all files"        
        
        return

if __name__ == "__main__":
    unittest.main() 
