#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

from __future__ import division


import unittest
import os
import threading
import logging

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory

class EventBasedTest(unittest.TestCase):
    """
    _EventBasedTest_

    Test event based job splitting.
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

        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "T2_CH_CERN")
            newFile.addRun(Run(i, *[45+i]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100, locations = "T2_CH_CERN")
        newFile.addRun(Run(1, *[45]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()


        self.multipleFileRunset = Fileset(name = "TestFileset3")
        self.multipleFileRunset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "T2_CH_CERN")
            newFile.addRun(Run(i/3, *[45]))
            newFile.create()
            self.multipleFileRunset.addFile(newFile)
        self.multipleFileRunset.commit()

        self.singleRunFileset = Fileset(name = "TestFileset4")
        self.singleRunFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "T2_CH_CERN")
            newFile.addRun(Run(1, *[45]))
            newFile.create()
            self.singleRunFileset.addFile(newFile)
        self.singleRunFileset.commit()

        self.singleRunMultipleLumi = Fileset(name = "TestFileset5")
        self.singleRunMultipleLumi.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "T2_CH_CERN")
            newFile.addRun(Run(1, *[45+i]))
            newFile.create()
            self.singleRunMultipleLumi.addFile(newFile)
        self.singleRunMultipleLumi.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "mnorman", name = "wf001", task="Test")
        testWorkflow.create()
        self.multipleFileSubscription                = Subscription(fileset = self.multipleFileFileset,
                                                                    workflow = testWorkflow,
                                                                    split_algo = "RunBased",
                                                                    type = "Processing")
        self.singleFileSubscription                  = Subscription(fileset = self.singleFileFileset,
                                                                    workflow = testWorkflow,
                                                                    split_algo = "RunBased",
                                                                    type = "Processing")
        self.multipleRunSubscription                 = Subscription(fileset = self.multipleFileRunset,
                                                                    workflow = testWorkflow,
                                                                    split_algo = "RunBased",
                                                                    type = "Processing")
        self.singleRunSubscription                   = Subscription(fileset = self.singleRunFileset,
                                                                    workflow = testWorkflow,
                                                                    split_algo = "RunBased",
                                                                    type = "Processing")
        self.singleRunMultipleLumiSubscription       = Subscription(fileset = self.singleRunMultipleLumi,
                                                                    workflow = testWorkflow,
                                                                    split_algo = "RunBased",
                                                                    type = "Processing")


        self.multipleFileSubscription.create()
        self.singleFileSubscription.create()
        self.multipleRunSubscription.create()
        self.singleRunSubscription.create()
        self.singleRunMultipleLumiSubscription.create()


        return

    def tearDown(self):
        """
        _tearDown_

        Tear down WMBS architechture.
        """
        self.testInit.clearDatabase()
        return

    def testExactRuns(self):
        """
        _testExactRuns_

        Test run based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 1)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        return


    def testMoreRuns(self):
        """
        _testMoreEvents_

        Test run based job splitting when the number of runs per job is
        greater than the number of runs in the input file.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 2)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        return

    def testMultipleRuns(self):
        """
        _testMultipleRuns_

        Test run based job splitting when the number of runs is
        equal to the number in each input file, with multiple files

        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 1)

        assert len(jobGroups) == 10, \
               "ERROR: JobFactory didn't return one JobGroup per run."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't put each run in a file."

        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 1)


        return

    def testMultipleRunsCombine(self):
        """
        _testMultipleRunsCombine_

        Test run based job splitting when the number of jobs is
        less then the number of files, with multiple files

        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleRunSubscription)

        jobGroups = jobFactory(files_per_job = 2)



        assert len(jobGroups) == 4, \
               "ERROR: JobFactory didn't return one JobGroup per run."

        assert len(jobGroups[1].jobs) == 2, \
               "ERROR: JobFactory didn't put only one job in the first job"

        #Last one in the queue should have one job, previous two (three files per run)
        self.assertEqual(len(jobGroups[1].jobs.pop().getFiles(type = "lfn")), 1)
        self.assertEqual(len(jobGroups[1].jobs.pop().getFiles(type = "lfn")), 2)


        return

    def testSingleRunsCombineUneven(self):
        """
        _testSingleRunsCombineUneven_

        Test run based job splitting when the number of jobs is
        less then and indivisible by the number of files, with multiple files.

        """

        #This should return two jobs, one with 8 and one with 2 files

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleRunSubscription)

        jobGroups = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 8)


        return


    def testPersistSingleRunsCombineUneven(self):
        """
        _testPerisistSingleRunsCombineUneven_

        Test run based job splitting when the number of jobs is
        less then and indivisible by the number of files, with multiple files.

        """

        #This should return two jobs, one with 8 and one with 2 files

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleRunSubscription)

        jobGroups = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 8)

        return

    def testSingleRunsMultipleLumiCombineUneven(self):
        """
        _testSingleRunsMultipeLumiCombineUneven_

        Test run based job splitting when the number of jobs is
        less then and indivisible by the number of files, with multiple files.

        """

        #This should return two jobs, one with 8 and one with 2 files

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleRunMultipleLumiSubscription)

        jobGroups = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 8)

        return


if __name__ == '__main__':
    unittest.main()
