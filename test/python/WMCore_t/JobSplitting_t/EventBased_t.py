#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID

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
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation('se01')
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.setLocation('se02')
        self.singleFileFileset.addFile(newFile)

        self.emptyFileFileset = Fileset(name = "TestFileset3")
        newFile = File("/some/file/name", size = 1000, events = 0)
        newFile.setdefault('se03')
        self.emptyFileFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "EventBased",
                                                     type = "Processing")
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "EventBased",
                                                   type = "Processing")
        self.emptyFileSubscription = Subscription(fileset = self.emptyFileFileset,
                                                  workflow = testWorkflow,
                                                  split_algo = "EventBased",
                                                  type = "Processing")

        self.performanceParams = {'timePerEvent' : None,
                                  'memoryRequirement' : 2300,
                                  'sizePerEvent' : 400}

        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def generateFakeMCFile(self, numEvents = 100, firstEvent = 1,
                           lastEvent = 100, firstLumi = 1, lastLumi = 10):
        #MC comes with only one MCFakeFile
        singleMCFileset = Fileset(name = "MCTestFileset")
        newFile = File("MCFakeFileTest", size = 1000, events = numEvents)
        newFile.setLocation('se01')
        newFile.addRun(Run(1, *range(firstLumi, lastLumi + 1)))
        newFile["first_event"] = firstEvent
        newFile["last_event"] = lastEvent
        testWorkflow = Workflow()
        singleMCFileset.addFile(newFile)
        singleMCFileSubscription = Subscription(fileset = singleMCFileset,
                                                workflow = testWorkflow,
                                                split_algo = "EventBased",
                                                type = "Production")
        return singleMCFileSubscription

    def testNoEvents(self):
        """
        _testNoEvents_

        Test event based job splitting where there are no events in the
        input file, make sure the mask events are None
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.emptyFileSubscription)
        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1,
                         "ERROR: JobFactory didn't return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "ERROR: JobFactory didn't create a single job")

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"],
                         "ERROR: Job contains unknown files")
        self.assertEqual(job["mask"].getMaxEvents(), None,
                         "ERROR: Mask maxEvents is not None")


    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        assert job["mask"].getMaxEvents() is None, \
               "ERROR: Job's max events is incorrect."

        assert job["mask"]["FirstEvent"] == 0, \
               "ERROR: Job's first event is incorrect."

        return

    def testMoreEvents(self):
        """
        _testMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 1000,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory created %s jobs not one" % len(jobGroups[0].jobs)

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        assert job["mask"].getMaxEvents() is None, \
               "ERROR: Job's max events is incorrect."

        assert job["mask"]["FirstEvent"] is None, \
               "ERROR: Job's first event is incorrect."

        return

    def test50EventSplit(self):
        """
        _test50EventSplit_

        Test event based job splitting when the number of events per job is
        50, this should result in two jobs.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 2, \
               "ERROR: JobFactory created %s jobs not two" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            assert job["mask"].getMaxEvents() == 50 or job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] in [0, 50], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return

    def test99EventSplit(self):
        """
        _test99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 99,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 2, \
               "ERROR: JobFactory created %s jobs not two" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            self.assertTrue(job["mask"].getMaxEvents() == 99 or job['mask'].getMaxEvents() is None,
                            "ERROR: Job's max events is incorrect.")

            assert job["mask"]["FirstEvent"] in [0, 99], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return

    def test100EventMultipleFileSplit(self):
        """
        _test100EventMultipleFileSplit_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 10, \
               "ERROR: JobFactory created %s jobs not ten" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

            assert job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] == 0, \
                   "ERROR: Job's first event is incorrect."

        return

    def test50EventMultipleFileSplit(self):
        """
        _test50EventMultipleFileSplit_

        Test job splitting into 50 event jobs when the input subscription has
        more than one file available.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not twenty" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

            assert job["mask"].getMaxEvents() == 50 or job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] in [0, 50], \
                   "ERROR: Job's first event is incorrect."

        return

    def test150EventMultipleFileSplit(self):
        """
        _test150EventMultipleFileSplit_

        Test job splitting into 150 event jobs when the input subscription has
        more than one file available.  This test verifies that the job splitting
        code will put at most one file in a job.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 150,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 10, \
               "ERROR: JobFactory created %s jobs not ten" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

            assert job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] is None, \
                   "ERROR: Job's first event is incorrect."
    def testMCExactEvents(self):
        """
        _testMCExactEvents_
        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file and no lumi
        information was supplied.
        """
        singleMCSubscription = self.generateFakeMCFile(firstLumi = 1,
                                                       lastLumi = 1)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["MCFakeFileTest"],
               "Error: Job contains unknown files.")

        self.assertEqual(job["mask"].getMaxEvents(), 100,
               "Error: Job's max events is incorrect.%i" %job["mask"].getMaxEvents() )
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")
        self.assertEqual(len(job["mask"].getRunAndLumis()), 0,
                         "Error: Job's mask has runs and lumis")

    def testMCMoreEvents(self):
        """
        _testMCMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file and no lumi
        information was supplied.
        """
        singleMCSubscription = self.generateFakeMCFile(firstLumi = 1,
                                                       lastLumi = 1)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 1000,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["MCFakeFileTest"],
               "Error: Job contains unknown files.")

        self.assertEqual(job["mask"].getMaxEvents(), 100,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")
        self.assertEqual(len(job["mask"].getRunAndLumis()), 0,
                         "Error: Job's mask has runs and lumis")
    def testMC99EventSplit(self):
        """
        _testMC99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs.
        No lumi information is supplied here.
        """
        singleMCSubscription = self.generateFakeMCFile(firstLumi = 1,
                                                       lastLumi = 2)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 99,
                               lheInputFiles = True,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 99 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == 1)
            secondJobCondition = (job["mask"].getMaxEvents() == 1 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 100)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])
            self.assertTrue(job.getBaggage().lheInputFiles)


    def testMC50EventSplit(self):
        """
        _testMC50EventSplit_

        Test event based job splitting when the number of events per job is
        50, this should result in two jobs.
        No lumi information supplied here.
        """
        singleMCSubscription = self.generateFakeMCFile(firstLumi = 1,
                                                       lastLumi = 2)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 50,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 50 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == 1)
            secondJobCondition = (job["mask"].getMaxEvents() == 50 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 51)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])
            self.assertFalse(job.getBaggage().lheInputFiles)

        return

    def testMCShiftedEventSplit(self):
        """
        _testMCShiftedEventSplit_

        Performs different tests with files that start with event counters
        different than 1, lumi information remains default.
        """
        singleMCSubscription = self.generateFakeMCFile(numEvents = 600,
                                                       firstEvent = 201,
                                                       lastEvent = 800)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 600,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job["mask"].getMaxEvents(), 600,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 201,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")

        singleMCSubscription = self.generateFakeMCFile(numEvents = 600,
                                                       firstEvent = 201,
                                                       lastEvent = 800)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)
        jobGroups = jobFactory(events_per_job = 6000,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job["mask"].getMaxEvents(), 600,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 201,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")

        singleMCSubscription = self.generateFakeMCFile(numEvents = 600,
                                                       firstEvent = 201,
                                                       lastEvent = 800)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)
        jobGroups = jobFactory(events_per_job = 599,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 599 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == 201)
            secondJobCondition = (job["mask"].getMaxEvents() == 1 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 800)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])

        singleMCSubscription = self.generateFakeMCFile(numEvents = 600,
                                                       firstEvent = 201,
                                                       lastEvent = 800)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)
        jobGroups = jobFactory(events_per_job = 300,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 300 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == 201)
            secondJobCondition = (job["mask"].getMaxEvents() == 300 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 501)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])

    def testMCShiftedLumiSplit(self):
        """
        _testMCShiftedLumiSplit

        Perform different tests with files that have lumi counters starting
        in something different than 1, however the splitting algorithm
        splits lumi with it's default value.
        """
        singleMCSubscription = self.generateFakeMCFile(firstLumi = 345,
                                                       lastLumi = 345)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job["mask"].getMaxEvents(), 100,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 345,
                "Error: Job's first lumi is incorrect.")

        singleMCSubscription = self.generateFakeMCFile(firstLumi = 345,
                                                       lastLumi = 345)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)
        jobGroups = jobFactory(events_per_job = 1000,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job["mask"].getMaxEvents(), 100,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 345,
                "Error: Job's first lumi is incorrect.")

        singleMCSubscription = self.generateFakeMCFile(firstLumi = 345,
                                                       lastLumi = 345)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)
        jobGroups = jobFactory(events_per_job = 99,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 99 and
                                 job["mask"]["FirstLumi"] == 345 and
                                 job["mask"]["FirstEvent"] == 1)
            secondJobCondition = (job["mask"].getMaxEvents() == 1 and
                                  job["mask"]["FirstLumi"] == 346 and
                                  job["mask"]["FirstEvent"] == 100)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])

        singleMCSubscription = self.generateFakeMCFile(firstLumi = 345,
                                                       lastLumi = 345)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)
        jobGroups = jobFactory(events_per_job = 50,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 50 and
                                 job["mask"]["FirstLumi"] == 345 and
                                 job["mask"]["FirstEvent"] == 1)
            secondJobCondition = (job["mask"].getMaxEvents() == 50 and
                                  job["mask"]["FirstLumi"] == 346 and
                                  job["mask"]["FirstEvent"] == 51)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])
    def testMCLumiSplit(self):
        """
        _testMCLumiSplit_

        2 tests on lumi splitting are performed:
            1. The number of events per job is a multiple of the events
            per lumi
            2. The number of events per job is not a multiple of the events
            per lumi

        """
        singleMCSubscription = self.generateFakeMCFile(numEvents = 150,
                                                       lastEvent = 150,
                                                       lastLumi = 15)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 100, events_per_lumi = 10,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 50 and
                                 job["mask"]["FirstLumi"] == 11 and
                                 job["mask"]["FirstEvent"] == 101)
            secondJobCondition = (job["mask"].getMaxEvents() == 100 and
                                  job["mask"]["FirstLumi"] == 1 and
                                  job["mask"]["FirstEvent"] == 1)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])

        singleMCSubscription = self.generateFakeMCFile(numEvents = 150,
                                                       lastEvent = 150,
                                                       lastLumi = 15)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 111, events_per_lumi = 10,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 39 and
                                 job["mask"]["FirstLumi"] == 13 and
                                 job["mask"]["FirstEvent"] == 112 and
                                 job["mask"]["LastLumi"] == 17)
            secondJobCondition = (job["mask"].getMaxEvents() == 111 and
                                  job["mask"]["FirstLumi"] == 1 and
                                  job["mask"]["FirstEvent"] == 1)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])
    def testMCEventSplitOver32bit(self):
        """
        _testMCEventSplitOver32bit_

        Make sure that no events will go over a 32 bit unsigned integer
        representation, event counter should be reset in that case.
        Also test is not over cautious.
        """
        firstEvent = 1
        singleMCSubscription = self.generateFakeMCFile(numEvents = 2**32,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 2**32 - 1,
                               events_per_lumi = 2**32 - 1,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 2**32 - 1 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == firstEvent and
                                 job["mask"]["LastEvent"] <= 2**32)
            secondJobCondition = (job["mask"].getMaxEvents() == 1 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 1)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])

        firstEvent = 1
        singleMCSubscription = self.generateFakeMCFile(numEvents = 2**32 - 1,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 2**31, events_per_lumi = 2**32,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 2**31 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == firstEvent and
                                 job["mask"]["LastEvent"] <= 2**32
                                 )
            secondJobCondition = (job["mask"].getMaxEvents() == 2**31 - 1 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 2**31 + 1 and
                                  job["mask"]["LastEvent"] <= 2**32)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])
        firstEvent = 1
        singleMCSubscription = self.generateFakeMCFile(numEvents = 2**32 - 1,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 2**32, events_per_lumi = 2**32,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job["mask"].getMaxEvents(), 2**32 - 1,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")

        firstEvent = 2**32 - 1
        singleMCSubscription = self.generateFakeMCFile(numEvents = 2,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 3, events_per_lumi = 1,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job["mask"].getMaxEvents(), 2,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")

        firstEvent = 2**32
        singleMCSubscription = self.generateFakeMCFile(numEvents = 50,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 60, events_per_lumi = 10,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "Error: JobFactory created %s jobs not one"
                         % len(jobGroups[0].jobs))

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job["mask"].getMaxEvents(), 50,
               "Error: Job's max events is incorrect.")
        self.assertEqual(job["mask"]["FirstEvent"], 1,
               "Error: Job's first event is incorrect.")
        self.assertEqual(job["mask"]["FirstLumi"], 1,
                "Error: Job's first lumi is incorrect.")

        firstEvent = 2**32
        singleMCSubscription = self.generateFakeMCFile(numEvents = 50,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 30, events_per_lumi = 10,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))

        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 30 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == 1 and
                                 job["mask"]["LastEvent"] <= 2**32)
            secondJobCondition = (job["mask"].getMaxEvents() == 20 and
                                  job["mask"]["FirstLumi"] == 4 and
                                  job["mask"]["FirstEvent"] == 31)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])


if __name__ == '__main__':
    unittest.main()
