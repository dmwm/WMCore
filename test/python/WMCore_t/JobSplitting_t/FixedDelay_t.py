#!/usr/bin/env python
"""
_FixedDelay_t_

Fixed Delay splitting test.
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
import time
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID

class FixedDelayTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(i, *[45+i]))
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.addRun(Run(1, *[45]))
        self.singleFileFileset.addFile(newFile)

        self.multipleFileLumiset = Fileset(name = "TestFileset3")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45 + i // 3]))
            self.multipleFileLumiset.addFile(newFile)

        self.singleLumiFileset = Fileset(name = "TestFileset4")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45]))
            self.singleLumiFileset.addFile(newFile)


        testWorkflow = Workflow()
        self.multipleFileSubscription  = Subscription(fileset = self.multipleFileFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "FixedDelay",
                                                      type = "Processing")
        self.singleFileSubscription    = Subscription(fileset = self.singleFileFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "FixedDelay",
                                                      type = "Processing")
        self.multipleLumiSubscription  = Subscription(fileset = self.multipleFileLumiset,
                                                      workflow = testWorkflow,
                                                      split_algo = "FixedDelay",
                                                      type = "Processing")
        self.singleLumiSubscription    = Subscription(fileset = self.singleLumiFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "FixedDelay",
                                                      type = "Processing")


        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def testNone(self):
        """
        _testNone_

        Since the time hasn'tpassed, we shouldn't get any jobs back.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        jobGroups = jobFactory(trigger_time = int(time.time()) * 2)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory(trigger_time = int(time.time()) * 2)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory(trigger_time = int(time.time()) * 2)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory(trigger_time = int(time.time()) * 2)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        return

    def testClosed(self):
        """
        _testClosed_
        since the subscriptions are closed and none of the files ahve been
        acquired, all of the files should show up
        """
        splitter = SplitterFactory()
        self.singleFileSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.singleFileSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        self.multipleFileSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs),1)
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEqual(len(myfiles), 10)

        self.multipleLumiSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs),1)
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEqual(len(myfiles), 10)
        #self.assertEqual(jobGroups, [], "Should have returned a null set")

        self.singleLumiSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEqual(len(myfiles), 10)


    def testAllAcquired(self):
        """
        _testAllAcquired_
        should all return no job groups
        """
        splitter = SplitterFactory()
        self.singleFileSubscription.acquireFiles(
                           self.singleFileSubscription.availableFiles())
        jobFactory = splitter(self.singleFileSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(jobGroups, [], "Should have returned a null set: %s" % jobGroups)

        self.multipleFileSubscription.acquireFiles(
                           self.multipleFileSubscription.availableFiles())
        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        self.multipleLumiSubscription.acquireFiles(
                           self.multipleLumiSubscription.availableFiles())
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        self.singleLumiSubscription.acquireFiles(
                           self.singleLumiSubscription.availableFiles())
        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

    def testClosedSomeAcquired(self):
        """
        _testClosedSomeAcquired_
        since the subscriptions are closed and none of the files ahve been
        acquired, all of the files should show up
        """
        splitter = SplitterFactory()
        self.multipleFileSubscription.getFileset().markOpen(False)
        self.singleFileSubscription.acquireFiles(
                           [self.singleFileSubscription.availableFiles().pop()])
        jobFactory = splitter(self.singleFileSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(jobGroups, [], "Should have returned a null set")

        self.multipleFileSubscription.getFileset().markOpen(False)
        self.multipleFileSubscription.acquireFiles(
                           [self.multipleFileSubscription.availableFiles().pop()])
        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(len(jobGroups),1, "Should have gotten one jobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1, \
               "JobFactory should have made one job")
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEqual(len(myfiles), 9, \
                "JobFactory should have provides us with 9 files")

        self.multipleLumiSubscription.getFileset().markOpen(False)
        self.multipleLumiSubscription.acquireFiles(
                           [self.multipleLumiSubscription.availableFiles().pop()])
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(len(jobGroups),1, "Should have gotten one jobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1, \
               "JobFactory should have made one job")
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEqual(len(myfiles), 9, \
                "JobFactory should have provides us with 9 files")

        self.singleLumiSubscription.getFileset().markOpen(False)
        self.singleLumiSubscription.acquireFiles(
                           [self.singleLumiSubscription.availableFiles().pop()])
        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory(trigger_time = 1)
        self.assertEqual(len(jobGroups),1, "Should have gotten one jobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1, \
               "JobFactory should have made one job")
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEqual(len(myfiles), 9, \
                "JobFactory should have provides us with 9 files")

        self.assertEqual(len(myfiles), 9)

if __name__ == '__main__':
    unittest.main()
