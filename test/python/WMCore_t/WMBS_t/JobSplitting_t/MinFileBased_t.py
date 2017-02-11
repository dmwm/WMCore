#!/usr/bin/env python
"""
_FileBased_t_

File based splitting test.
"""


import unittest
import os
import threading
import logging
import time

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow     import Workflow
from WMCore.DataStructs.Run   import Run

from WMCore.DAOFactory                   import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib                import makeUUID
from WMQuality.TestInit                  import TestInit
#from nose.plugins.attrib import attr

class FileBasedTest(unittest.TestCase):
    """
    _FileBasedTest_

    Test file based job splitting.
    """

    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.nSites = 2
        locationAction = daofactory(classname = "Locations.New")
        for site in range(self.nSites):
            locationAction.execute(siteName = "site%i" % site,
                                   pnn = "T2_CH_CERN_%i" % site)

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase(modules = ["WMCore.WMBS"])

        return


    def createTestSubscription(self, nFiles, nSites = 1, closeFileset = False):
        """
        _createTestSubscription_

        Create a set of test subscriptions for testing purposes.
        """

        if nSites > self.nSites:
            nSites = self.nSites

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        # Create a testWorkflow
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test")
        testWorkflow.create()

        # Create the files for each site
        for s in range(nSites):
            for i in range(nFiles):
                newFile = File(makeUUID(), size = 1024, events = 100,
                               locations = set(["T2_CH_CERN_%i" % s]))
                newFile.create()
                testFileset.addFile(newFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "MinFileBased",
                                        type = "Processing")
        testSubscription.create()

        # Close the fileset
        if closeFileset:
            testFileset.markOpen(isOpen = False)

        return testSubscription

    def testA_ExactFiles(self):
        """
        _testExactFiles_

        Test file based job splitting when the number of files per job is
        exactly the same as the number of files in the input fileset.
        """
        nFiles = 5
        sub    = self.createTestSubscription(nFiles = nFiles)
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = sub)

        jobGroups = jobFactory(files_per_job = nFiles)


        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(len(jobGroups[0].jobs[0]['input_files']), nFiles)
        return

    def testB_LessFilesOpen(self):
        """
        _LessFilesOpen_

        Test with less files then required.
        If the fileset is open, this should produce no jobs.
        """

        sub    = self.createTestSubscription(nFiles = 5)
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = sub)

        jobGroups = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroups), 0)
        return

    def testC_LessFilesClosed(self):
        """
        _LessFilesClosed_

        Test with less files then required.
        If the fileset is closed, this should produce one job.
        """

        sub    = self.createTestSubscription(nFiles = 5, closeFileset = True)
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = sub)

        jobGroups = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(len(jobGroups[0].jobs[0]['input_files']), 5)
        return

    def testD_MoreFilesOpen(self):
        """
        _MoreFilesOpen_

        If you pass it more files then files_per_job, it should produce
        jobs until it hits the limit, then stop.
        """

        sub        = self.createTestSubscription(nFiles = 10)
        splitter   = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = sub)

        jobGroups = jobFactory(files_per_job = 3)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 3)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 3)
        return

    def testE_MoreFilesClosed(self):
        """
        _MoreFilesClosed_

        If you pass it more files then files_per_job, it should produce
        jobs enough to hold all the files in the fileset if the
        fileset is closed
        """

        sub        = self.createTestSubscription(nFiles = 10, closeFileset = True)
        splitter   = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = sub)

        jobGroups = jobFactory(files_per_job = 3)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 4)
        for job in jobGroups[0].jobs[:3]:
            self.assertEqual(len(job['input_files']), 3)
        self.assertEqual(len(jobGroups[0].jobs[3]['input_files']), 1)
        return


if __name__ == '__main__':
    unittest.main()
