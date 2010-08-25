#!/usr/bin/env python
"""
_PeriodicEndOfRun_t_

"""

__revision__ = "$Id: PeriodicEndOfRun_t.py,v 1.1 2010/04/19 14:22:29 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

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

class PeriodicEndOfRunTest(unittest.TestCase):
    """
    _PeriodicEndOfRunTest_


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
        locationAction.execute("site1", seName = "somese.cern.ch")
        
        self.testFileset = Fileset(name = "TestFileset1")
        self.testFileset.create()
        
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        self.testSubscription = Subscription(fileset = self.testFileset,
                                             workflow = testWorkflow,
                                             split_algo = "Periodic",
                                             type = "Processing")
        self.testSubscription.create()

        self.testFileset2 = Fileset(name = "TestFileset2")
        self.testFileset2.create()
        testWorkflow.addOutput("Anything", self.testFileset2)

        testFile = File(lfn = "/this/is/a/lfnA", size = 1000,
                        events = 100, locations = set(["somese.cern.ch"]))
        testFile.create()
        self.testFileset2.addFile(testFile)    
        self.testFileset2.commit()

        testWorkflow2 = Workflow(spec = "spec.xml", owner = "Steve",
                                 name = "wf002", task = "Test2")
        testWorkflow2.create()

        self.testSubscription2 = Subscription(fileset = self.testFileset2,
                                              workflow = testWorkflow2,
                                              split_algo = "EndOfRun",
                                              type = "Processing")
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
    
    def testPeriodicEndOfRunSplitting(self):
        """
        _testPeriodicicEndOfRunSplitting_

        Verify that periodic splitting works correctly with the fileset closing
        algorithm.
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

        listClosable = self.daoFactory(classname = "Fileset.ListClosable")
        assert len(listClosable.execute()) == 0, \
               "Error: No filesets should be closed."

        # Verify that no jobs are generated as the previously issued job has not
        # completed yet.
        time.sleep(5)
        self.injectFile()
        moreJobGroups = jobFactory(job_period = 1)    

        assert len(moreJobGroups) == 0, \
               "Error: No jobgroups should be returned."

        # Complete the job so that the splitting algorithm will generate
        # another job.
        wmbsJob["state"] = "cleanout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")
        changeStateDAO.execute([wmbsJob])

        assert len(listClosable.execute()) == 0, \
               "Error: No filesets should be closed."

        # Verify that no jobs will be generated if the period has not yet
        # expried.
        self.injectFile()
        moreJobGroups = jobFactory(job_period = 999999999999)

        assert len(moreJobGroups) == 0, \
               "ERROR: No jobgroups should be returned."

        assert len(listClosable.execute()) == 0, \
               "Error: No filesets should be closed."

        # Verify that a job will be generated if the period has expired.
        time.sleep(5)
        self.injectFile()
        jobGroups = jobFactory(job_period = 1)

        assert len(jobGroups) == 1, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: Jobgroup has wrong number of jobs: %s" % len(jobGroups[0].jobs)

        self.verifyFiles(jobGroups[0].jobs.pop())
        assert len(listClosable.execute()) == 0, \
               "Error: No filesets should be closed."

        # Verify that no jobs will be generated in the case that a periodic job
        # is still running and the fileset has been closed.
        self.testFileset.markOpen(False)
        time.sleep(5)
        self.injectFile()
        jobGroups = jobFactory(job_period = 1)

        assert len(jobGroups) == 0, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(listClosable.execute()) == 0, \
               "Error: No filesets should be closed."

        # Complete the outstanding job.
        wmbsJob["state"] = "cleanout"
        wmbsJob["oldstate"] = "new"
        wmbsJob["couch_record"] = "somejive"
        wmbsJob["retry_count"] = 0
        changeStateDAO.execute([wmbsJob])

        # Verify that when the input fileset is closed and all periodic jobs
        # are complete a job will not be generated.
        self.injectFile()
        jobGroups = jobFactory(job_period = 99999999999)

        assert len(jobGroups) == 0, \
               "ERROR: Wrong number of job groups returned: %s" % len(jobGroups)

        assert len(listClosable.execute()) == 1, \
               "Error: One fileset should be closabled."
        assert self.testFileset2.id == listClosable.execute()[0], \
               "Error: Wrong fileset returned."
        return

if __name__ == '__main__':
    unittest.main()
