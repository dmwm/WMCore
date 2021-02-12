#!/usr/bin/env python
"""
_RunBased_t_

RunBased splitting test.
"""
from __future__ import division

from builtins import range

import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID

class RunBasedTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = set(["somese.cern.ch"]))
            newFile.addRun(Run(i, *[45+i]))
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100, locations = set(["somese.cern.ch"]))
        newFile.addRun(Run(1, *[45]))
        self.singleFileFileset.addFile(newFile)


        self.multipleFileRunset = Fileset(name = "TestFileset3")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = set(["somese.cern.ch"]))
            newFile.addRun(Run(i//3, *[45]))
            self.multipleFileRunset.addFile(newFile)

        self.singleRunFileset = Fileset(name = "TestFileset4")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = set(["somese.cern.ch"]))
            newFile.addRun(Run(1, *[45]))
            self.singleRunFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "RunBased",
                                                     type = "Processing")
        self.singleFileSubscription   = Subscription(fileset = self.singleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "RunBased",
                                                     type = "Processing")
        self.multipleRunSubscription  = Subscription(fileset = self.multipleFileRunset,
                                                     workflow = testWorkflow,
                                                     split_algo = "RunBased",
                                                     type = "Processing")
        self.singleRunSubscription    = Subscription(fileset = self.singleRunFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "RunBased",
                                                     type = "Processing")


        return

    def tearDown(self):
        pass

    def testExactRuns(self):
        """
        _testExactRuns_

        Test run based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

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
        jobFactory = splitter(self.singleFileSubscription)

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
        jobFactory = splitter(self.multipleFileSubscription)

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
        jobFactory = splitter(self.multipleRunSubscription)

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
        jobFactory = splitter(self.singleRunSubscription)

        jobGroups = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 2)
        self.assertEqual(len(jobGroups[0].jobs.pop().getFiles(type = "lfn")), 8)


        return




if __name__ == '__main__':
    unittest.main()
