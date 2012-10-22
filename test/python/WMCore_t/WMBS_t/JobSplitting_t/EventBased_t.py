#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DataStructs.Run import Run

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMQuality.TestInit import TestInit

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
        locationAction.execute(siteName = 's1', seName = "somese.cern.ch")
        locationAction.execute(siteName = 's2', seName = "otherse.cern.ch")

        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        parentFile = File('/parent/lfn/', size = 1000, events = 100,
                          locations = set(["somese.cern.ch"]))
        parentFile.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            newFile.addParent(lfn = parentFile['lfn'])
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = set(["somese.cern.ch"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()


        self.multipleSiteFileset = Fileset(name = "TestFileset3")
        self.multipleSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation("somese.cern.ch")
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation(["somese.cern.ch","otherse.cern.ch"])
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test")
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "EventBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "EventBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()
        self.multipleSiteSubscription = Subscription(fileset = self.multipleSiteFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "EventBased",
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

    def generateFakeMCFile(self, numEvents = 100, firstEvent = 1,
                           lastEvent = 100, firstLumi = 1, lastLumi = 10,
                           index = 1):
        #MC comes with only one MCFakeFile
        singleMCFileset = Fileset(name = "MCTestFileset %i" % index)
        singleMCFileset.create()
        newFile = File("MCFakeFileTest %i" % index, size = 1000,
                       events = numEvents,
                       locations = set(["somese.cern.ch"]))
        newFile.addRun(Run(1, *range(firstLumi, lastLumi + 1)))
        newFile["first_event"] = firstEvent
        newFile["last_event"] = lastEvent
        newFile.create()
        singleMCFileset.addFile(newFile)
        singleMCFileset.commit()
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test")
        testWorkflow.create()

        singleMCFileSubscription = Subscription(fileset = singleMCFileset,
                                                workflow = testWorkflow,
                                                split_algo = "EventBased",
                                                type = "Production")
        singleMCFileSubscription.create()
        return singleMCFileSubscription

    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 100)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 1)

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"),  ["/some/file/name"])

        self.assertEqual(job["mask"].getMaxEvents(), 100)

        self.assertEqual(job["mask"]["FirstEvent"], 0)

        return

    def testMoreEvents(self):
        """
        _testMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 1000)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 1)

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

        self.assertEqual(job["mask"].getMaxEvents(), 100)

        self.assertEqual(job["mask"]["FirstEvent"], 0)

        return

    def test50EventSplit(self):
        """
        _test50EventSplit_

        Test event based job splitting when the number of events per job is
        50, this should result in two jobs.
        """
        splitter = SplitterFactory()

        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 2)

        for job in jobGroups[0].jobs:
            self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

            self.assertEqual(job["mask"].getMaxEvents(), 50)

            assert job["mask"]["FirstEvent"] in [0, 50], \
                   "ERROR: Job's first event is incorrect."

        return

    def test99EventSplit(self):
        """
        _test99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 99)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 2)

        for job in jobGroups[0].jobs:
            self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

            self.assertTrue(job["mask"].getMaxEvents() == 99 or \
                            job["mask"].getMaxEvents() == 1)

            assert job["mask"]["FirstEvent"] in [0, 99], \
                   "ERROR: Job's first event is incorrect."

        return

    def test100EventMultipleFileSplit(self):
        """
        _test100EventMultipleFileSplit_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 100)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 10)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type = "lfn")), 1)

            self.assertEqual(job["mask"].getMaxEvents(), 100)

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
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 20)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type = "lfn")), 1)
            self.assertEqual(job["mask"].getMaxEvents(), 50)
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
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 150)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        self.assertEqual(len(jobGroups[0].jobs[0].getFiles(type = "lfn")), 1)
        self.assertEqual(len(jobGroups[0].jobs[6].getFiles(type = "lfn")), 1)

        for job in jobGroups[0].jobs:
            self.assertEqual(job["mask"].getMaxEvents(), 100)
            self.assertEqual(job["mask"]["FirstEvent"], 0)

        return

    def test100EventMultipleSite(self):
        """
        _test100EventMultipleSite_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available, at different site combinations.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(events_per_job = 100)

        self.assertEqual(len(jobGroups), 2)

        self.assertEqual(len(jobGroups[0].jobs), 5)
        self.assertEqual(len(jobGroups[1].jobs), 5)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type = "lfn")), 1)

            self.assertEqual(job["mask"].getMaxEvents(), 100)

            assert job["mask"]["FirstEvent"] == 0, \
                   "ERROR: Job's first event is incorrect."

        return

    def testMCEventSplitOver32bit(self):
        """
        _testMCEventSplitOver32bit_

        Make sure that no events will go over a 32 bit unsigned integer
        representation, event counter should be reset in that case.
        Also test is not over cautious.
        """
        firstEvent = 3*(2**30) + 1
        singleMCSubscription = self.generateFakeMCFile(numEvents = 2**30,
                                                       firstEvent = firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",subscription = singleMCSubscription)

        jobGroups = jobFactory(events_per_job = 2**30 - 1,
                               events_per_lumi = 2**30 - 1)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            firstJobCondition = (job["mask"].getMaxEvents() == 2**30 - 1 and
                                 job["mask"]["FirstLumi"] == 1 and
                                 job["mask"]["FirstEvent"] == firstEvent and
                                 job["mask"]["LastEvent"] <= 2**32)
            secondJobCondition = (job["mask"].getMaxEvents() == 1 and
                                  job["mask"]["FirstLumi"] == 2 and
                                  job["mask"]["FirstEvent"] == 1)
            self.assertTrue(firstJobCondition or secondJobCondition,
                            "Job mask: %s didn't pass neither of the conditions"
                            % job["mask"])

    def test_addParents(self):
        """
        _addParents_

        Test our ability to add parents to a job
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50, include_parents = True)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 20)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type = "lfn")), 1)
            self.assertEqual(job["mask"].getMaxEvents(), 50)
            self.assertTrue(job["mask"]["FirstEvent"] in [0, 50],
                            "ERROR: Job's first event is incorrect: %i" % job['mask']['FirstEvent'])
            for f in job['input_files']:
                self.assertEqual(len(f['parents']), 1)
                self.assertEqual(list(f['parents'])[0]['lfn'], '/parent/lfn/')

        return


if __name__ == '__main__':
    unittest.main()
