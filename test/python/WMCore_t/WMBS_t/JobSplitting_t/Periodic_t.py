#!/usr/bin/env python
"""
_Periodic_t_

Periodic job splitting test.
"""

__revision__ = "$Id: Periodic_t.py,v 1.1 2009/08/04 16:39:38 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from sets import Set
import unittest
import os
import threading
import time

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMQuality.TestInit import TestInit

class FileBasedTest(unittest.TestCase):
    """
    _FileBasedTest_

    Test file based job splitting.
    """
    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        
        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("somese.cern.ch")
        locationAction.execute("otherse.cern.ch")
        
        self.testFileset = Fileset(name = "TestFileset1")
        self.testFileset.create()
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1000, events = 100,
                         locations = Set(["somese.cern.ch"]))
        testFileA.create()
        self.testFileset.addFile(testFileA)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1000, events = 100,
                         locations = Set(["somese.cern.ch"]))
        testFileB.create()
        self.testFileset.addFile(testFileB)    
        self.testFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test" )
        testWorkflow.create()
        self.testSubscription = Subscription(fileset = self.testFileset,
                                             workflow = testWorkflow,
                                             split_algo = "Periodic",
                                             type = "Processing")
        self.testSubscription.create()
        return
    
    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        myThread = threading.currentThread()

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
            
        myThread.transaction.begin()
            
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
            
        myThread.transaction.commit()
        return            

    def testPeriodicSplitting(self):
        """
        _testPeriodiciSplitting_


        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.testSubscription)

        # First pass: no jobs exist.  The algorithm should create a job
        # containing all available files.
        jobGroups = jobFactory(job_period = 60)

        assert len(jobGroups) == 1, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: Jobgroup has wrong number of jobs: %s" % len(jobGroups[0].jobs)

        wmbsJob = jobGroups[0].jobs.pop()

        assert len(wmbsJob["input_files"]) == 2, \
               "ERROR: Job has wrong number of files: %s" % len(wmbsJob["files"])

        goldenFiles = ["/this/is/a/lfnA", "/this/is/a/lfnB"]
        for file in wmbsJob["input_files"]:
            assert file["lfn"] in goldenFiles, \
                   "ERROR: Unknown lfn: %s" % file["lfn"]
            goldenFiles.remove(file["lfn"])

        assert len(goldenFiles) == 0, \
               "ERROR: Files are missing from the job."

        time.sleep(5)
        moreJobGroups = jobFactory(job_period = 1)        

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        # Complete the job so that the splitting algorithm will generate
        # another job.
        wmbsJob["state"] = "closeout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")
        changeStateDAO.execute([wmbsJob])

        # All jobs complete, but our period is not up yet.  Should get back
        # no jobs.
        moreJobGroups = jobFactory(job_period = 999999999999)

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        # Call the job splitting code with a short period, we should get a job
        # back.  Sleep for a little bit just in case.
        time.sleep(5)
        jobGroups = jobFactory(job_period = 1)

        assert len(jobGroups) == 1, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: Jobgroup has wrong number of jobs: %s" % len(jobGroups[0].jobs)

        wmbsJob = jobGroups[0].jobs.pop()

        assert len(wmbsJob["input_files"]) == 2, \
               "ERROR: Job has wrong number of files: %s" % len(wmbsJob["input_files"])

        goldenFiles = ["/this/is/a/lfnA", "/this/is/a/lfnB"]
        for file in wmbsJob["input_files"]:
            assert file["lfn"] in goldenFiles, \
                   "ERROR: Unknown lfn: %s" % file["lfn"]
            goldenFiles.remove(file["lfn"])

        assert len(goldenFiles) == 0, \
               "ERROR: Files are missing from the job."

        # The job splitting code will not create jobs if the input fileset is
        # closed.
        self.testFileset.markOpen(False)
        wmbsJob["state"] = "closeout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO.execute([wmbsJob])

        time.sleep(5)
        moreJobGroups = jobFactory(job_period = 999999999999)

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        return

if __name__ == '__main__':
    unittest.main()
