#!/usr/bin/env python
"""
_Periodic_t_

Periodic job splitting test.
"""

__revision__ = "$Id: Periodic_t.py,v 1.6 2009/12/16 18:40:38 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

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

class PeriodicTest(unittest.TestCase):
    """
    _PeriodicTest_

    Test file periodic splitting.
    """

    
    def setUp(self):
        """
        _setUp_

        Create a single subscription with one file.
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
        
        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("somese.cern.ch")
        locationAction.execute("otherse.cern.ch")
        
        self.testFileset = Fileset(name = "TestFileset1")
        self.testFileset.create()
        
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
        self.testInit.clearDatabase()
        return            

    def injectFile(self):
        """
        _injectFile_

        Inject a file into the periodic splitting input fileset.
        """
        testFile = File(lfn = "/this/is/a/lfn%s" % time.time(), size = 1000,
                        events = 100, locations = set(["somese.cern.ch"]))
        testFile.create()
        self.testFileset.addFile(testFile)    
        self.testFileset.commit()

        return

    def verifyFiles(self, wmbsJob):
        """
        _verifyFiles_

        Verify that the input files for the job are the same as the files in the
        input fileset.
        """
        inputFiles = wmbsJob.getFiles()
        filesetFiles = self.testFileset.getFiles()

        for inputFile in inputFiles:
            assert inputFile in filesetFiles, \
                   "ERROR: Unknown file: %s" % inputFile
            filesetFiles.remove(inputFile)

        assert len(filesetFiles) == 0, \
               "ERROR: Not all files included in job."
                
        return
    
    def testPeriodicSplitting(self):
        """
        _testPeriodiciSplitting_

        Manipulate the splitting algorithm to test the corner cases.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.testSubscription)

        # First pass: no jobs exist.  The algorithm should create a job
        # containing all available files.
        self.injectFile()
        jobGroups = jobFactory(job_period = 99999999999)

        assert len(jobGroups) == 1, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: Jobgroup has wrong number of jobs: %s" % len(jobGroups[0].jobs)

        wmbsJob = jobGroups[0].jobs.pop()
        self.verifyFiles(wmbsJob)

        # Verify that no jobs are generated as the previously issued job has not
        # completed yet.
        time.sleep(5)
        self.injectFile()
        moreJobGroups = jobFactory(job_period = 1)    

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        # Complete the job so that the splitting algorithm will generate
        # another job.
        wmbsJob["state"] = "cleanout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")
        changeStateDAO.execute([wmbsJob])

        # Verify that no jobs will be generated if the period has not yet
        # expried.
        self.injectFile()
        moreJobGroups = jobFactory(job_period = 999999999999)

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        # Verify that a job will be generated if the period has expired.
        time.sleep(5)
        self.injectFile()
        jobGroups = jobFactory(job_period = 1)

        assert len(jobGroups) == 1, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: Jobgroup has wrong number of jobs: %s" % len(jobGroups[0].jobs)

        self.verifyFiles(jobGroups[0].jobs.pop())

        # Verify that no jobs will be generated in the case that a periodic job
        # is still running and the fileset has been closed.
        self.testFileset.markOpen(False)
        time.sleep(5)
        self.injectFile()
        jobGroups = jobFactory(job_period = 1)

        assert len(jobGroups) == 0, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        # Complete the outstanding job.
        wmbsJob["state"] = "cleanout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO.execute([wmbsJob])

        # Verify that when the input fileset is closed and all periodic jobs
        # are complete a job will be generated even if the period has not yet
        # expired.
        self.injectFile()
        jobGroups = jobFactory(job_period = 99999999999)

        assert len(jobGroups) == 1, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: Jobgroup has wrong number of jobs: %s" % len(jobGroups[0].jobs)

        self.verifyFiles(jobGroups[0].jobs.pop())

        # Verify that after the final job is complete no more jobs are generated.
        wmbsJob["state"] = "cleanout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO.execute([wmbsJob])

        time.sleep(5)
        self.injectFile()
        moreJobGroups = jobFactory(job_period = 1)

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        return

if __name__ == '__main__':
    unittest.main()
