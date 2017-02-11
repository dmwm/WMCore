#!/usr/bin/env python
"""
_SizeBased_t_

Size based splitting test.
"""

import unittest
import threading
import os

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class SizeBasedTest(unittest.TestCase):
    """
    _SizeBasedTest_

    Test size based job splitting.
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

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "site1", pnn = "T2_CH_CERN")
        locationAction.execute(siteName = "site2", pnn = "T1_US_FNAL_Disk")

        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["T2_CH_CERN"]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = set(["T2_CH_CERN"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()


        self.multipleSiteFileset = Fileset(name = "TestFileset3")
        self.multipleSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation("T2_CH_CERN")
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation(["T2_CH_CERN","T1_US_FNAL_Disk"])
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test")
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "SizeBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "SizeBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()
        self.multipleSiteSubscription = Subscription(fileset = self.multipleSiteFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "SizeBased",
                                                     type = "Processing")
        self.multipleSiteSubscription.create()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(size_per_job = 1000)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."


        return


    def testMultipleFiles(self):
        """
        _testMultipleFiles_

        Tests the mechanism for splitting up multiple files into jobs with
        a variety of different arguments.
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups  = jobFactory(size_per_job = 1000)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 1)

        return


    def testMultipleFiles2000(self):
        """
        _testMultipleFiles2000_

        Tests the mechanism for splitting up multiple files into jobs with
        a variety of different arguments.
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)
        #Test it with two files per job
        jobGroups  = jobFactory(size_per_job = 2000)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)

        return


    def testMultipleFiles2500(self):
        """
        _testMultipleFiles2500_

        Tests the mechanism for splitting up multiple files into jobs with
        a variety of different arguments.
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)


        #Now test it with a size that can't be broken up evenly
        jobGroups  = jobFactory(size_per_job = 2500)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)

        return


    def testMultipleFiles500(self):
        """
        _testMultipleFiles500_

        Tests the mechanism for splitting up multiple files into jobs with
        a variety of different arguments.
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)


        #Test it with something too small to handle; should return one job per file, plus one extra
        #open at the end
        jobGroups  = jobFactory(size_per_job = 500)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)

        return


    def testMultipleSites(self):
        """
        _testMultipleSites_

        Tests how to break up files at different locations
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleSiteSubscription)

        jobGroups  = jobFactory(size_per_job = 1000)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 1)


if __name__ == '__main__':
    unittest.main()
