#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

import unittest
import threading
import os

from WMCore.Database.CMSCouch import CouchServer
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInitCouchApp import TestInitCouchApp

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
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.couchUrl = os.environ["COUCHURL"]
        self.couchDBName = "acdc_event_based_t"
        self.testInit.setupCouch(self.couchDBName, "GroupUser", "ACDC")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        couchSever = CouchServer(dburl = self.couchUrl)
        self.couchDB = couchSever.connectDatabase(self.couchDBName)
        self.populateWMBS()

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        return

    def populateWMBS(self):
        """
        _populateWMBS_

        Create files and subscriptions in WMBS
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = 's1', seName = "somese.cern.ch")
        locationAction.execute(siteName = 's2', seName = "otherse.cern.ch")
        self.validLocations = ["somese.cern.ch", "otherse.cern.ch"]

        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        parentFile = File('/parent/lfn/', size = 1000, events = 100,
                          locations = set(["somese.cern.ch"]))
        parentFile.create()
        for _ in range(10):
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
        for _ in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation("somese.cern.ch")
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for _ in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation(["somese.cern.ch", "otherse.cern.ch"])
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
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

    def populateACDCCouch(self, numFiles = 2, lumisPerJob = 35,
                          eventsPerJob = 20000):
        """
        _populateACDCCouch_

        Create production files in couchDB to test the creation
        of ACDC jobs for the EventBased algorithm
        """
        # Define some constants
        workflowName = "ACDC_TestEventBased"
        filesetName = "/%s/Production" % workflowName
        owner = "dballest@fnal.gov"
        group = "unknown"

        lumisPerFile = lumisPerJob * 250
        for i in range(numFiles):
            for j in range(250):
                lfn = "MCFakeFile-some-hash-%s" % str(i).zfill(5)
                acdcFile = File(lfn = lfn, size = 100, events = eventsPerJob, locations = self.validLocations,
                                merged = False, first_event = 1)
                run = Run(1, *range(1 + (i * lumisPerFile) + j * lumisPerJob,
                                    (j + 1) * lumisPerJob + (i * lumisPerFile) + 2))
                acdcFile.addRun(run)
                acdcDoc = {"collection_name" : workflowName,
                           "collection_type" : "ACDC.CollectionTypes.DataCollection",
                           "files" : {lfn : acdcFile},
                           "fileset_name" : filesetName,
                           "owner" : {"user": owner,
                                      "group" : group}}
                self.couchDB.queue(acdcDoc)

        self.couchDB.commit()
        return

    def generateFakeMCFile(self, numEvents = 100, firstEvent = 1,
                           lastEvent = 100, firstLumi = 1, lastLumi = 10,
                           index = 1):
        """
        _generateFakeMCFile_

        Generates a fake MC file for testing production EventBased
        creation of jobs
        """
        # MC comes with only one MCFakeFile
        singleMCFileset = Fileset(name = "MCTestFileset-%i" % index)
        singleMCFileset.create()
        newFile = File("MCFakeFile-some-hash-%s" % str(index).zfill(5), size = 1000,
                       events = numEvents,
                       locations = set(["somese.cern.ch"]))
        newFile.addRun(Run(1, *range(firstLumi, lastLumi + 1)))
        newFile["first_event"] = firstEvent
        newFile["last_event"] = lastEvent
        newFile.create()
        singleMCFileset.addFile(newFile)
        singleMCFileset.commit()
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
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

        self.assertEqual(job["mask"].getMaxEvents(), None)

        self.assertEqual(job["mask"]["FirstEvent"], 0)

        return

    def testMoreEvents(self):
        """
        _testMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file.
        Since the file has less events than the splitting, the job goes without a mask.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 1000)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 1)

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

        self.assertEqual(job["mask"].getMaxEvents(), None)

        self.assertEqual(job["mask"]["FirstEvent"], None)

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

            self.assertTrue((job["mask"].getMaxEvents() == 50 and job["mask"]["FirstEvent"] == 0) or \
                            (job["mask"].getMaxEvents() is None and job["mask"]["FirstEvent"] == 50))

        return

    def test99EventSplit(self):
        """
        _test99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs. Last job shouldn't have a maximum
        number of events, let it run until the end of the file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 99)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 2)

        for job in jobGroups[0].jobs:
            self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

            self.assertTrue((job["mask"].getMaxEvents() == 99 and job["mask"]["FirstEvent"] == 0) or \
                            (job["mask"].getMaxEvents() is None and job["mask"]["FirstEvent"] == 99))
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

            self.assertEqual(job["mask"].getMaxEvents(), None)

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
            self.assertTrue((job["mask"].getMaxEvents() == 50 and job["mask"]["FirstEvent"] == 0) or \
                            (job["mask"].getMaxEvents() is None and job["mask"]["FirstEvent"] == 50))


        return

    def test150EventMultipleFileSplit(self):
        """
        _test150EventMultipleFileSplit_

        Test job splitting into 150 event jobs when the input subscription has
        more than one file available.  This test verifies that the job splitting
        code will put at most one file in a job. Since every job has less events
        than the maximum. the job goes without a mask.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 150)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        self.assertEqual(len(jobGroups[0].jobs[0].getFiles(type = "lfn")), 1)
        self.assertEqual(len(jobGroups[0].jobs[6].getFiles(type = "lfn")), 1)

        for job in jobGroups[0].jobs:
            self.assertEqual(job["mask"].getMaxEvents(), None)
            self.assertEqual(job["mask"]["FirstEvent"], None)

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

            self.assertEqual(job["mask"].getMaxEvents(), None)

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
            self.assertTrue((job["mask"].getMaxEvents() == 50 and job["mask"]["FirstEvent"] == 0) or \
                            (job["mask"].getMaxEvents() is None and job["mask"]["FirstEvent"] == 50))
            for f in job['input_files']:
                self.assertEqual(len(f['parents']), 1)
                self.assertEqual(list(f['parents'])[0]['lfn'], '/parent/lfn/')

        return

    def testACDCProduction(self):
        """
        _testACDCProduction_

        Test the ability of the EventBased algorithm of creating
        jobs from ACDC correctly
        """
        self.populateACDCCouch()
        mcSubscription = self.generateFakeMCFile(20000, 1, 20001, 1, 8750, 0)
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = mcSubscription)

        jobGroups = jobFactory(events_per_job = 50, collectionName = "ACDC_TestEventBased",
                               couchURL = self.couchUrl, couchDB = self.couchDBName,
                               filesetName = "/ACDC_TestEventBased/Production",
                               owner = "dballest@fnal.gov", group = "unknown")

        self.assertEqual(1, len(jobGroups))
        jobGroup = jobGroups[0]
        self.assertEqual(250, len(jobGroup.jobs))

        for job in jobGroup.jobs:
            self.assertEqual(1, len(job["input_files"]))
            self.assertEqual("MCFakeFile-some-hash-00000", job["input_files"][0]["lfn"])
            mask = job["mask"]
            self.assertEqual(35, mask["LastLumi"] - mask["FirstLumi"])
            self.assertEqual(20000, mask["LastEvent"] - mask["FirstEvent"])
            self.assertFalse(mask["runAndLumis"])

        return

if __name__ == '__main__':
    unittest.main()
