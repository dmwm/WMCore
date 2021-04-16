#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""
from __future__ import print_function, division

from builtins import range
import os
import random
import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Database.CMSCouch import CouchServer
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
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
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.couchUrl = os.environ["COUCHURL"]
        self.couchDBName = "acdc_event_based_t"
        self.testInit.setupCouch(self.couchDBName, "GroupUser", "ACDC")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        couchSever = CouchServer(dburl=self.couchUrl)
        self.couchDB = couchSever.connectDatabase(self.couchDBName)
        self.populateWMBS()
        self.performanceParams = {'timePerEvent': 12,
                                  'memoryRequirement': 2300,
                                  'sizePerEvent': 400}
        self.eventsPerJob = 100

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
        daofactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationAction = daofactory(classname="Locations.New")
        locationAction.execute(siteName='s1', pnn="T1_US_FNAL_Disk")
        locationAction.execute(siteName='s2', pnn="T2_CH_CERN")
        self.validLocations = ["T1_US_FNAL_Disk", "T2_CH_CERN"]

        testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                name="wf001", task="Test")
        testWorkflow.create()

        self.multipleFileFileset = Fileset(name="TestFileset1")
        self.multipleFileFileset.create()
        parentFile = File('/parent/lfn/', size=1000, events=100,
                          locations=set(["T1_US_FNAL_Disk"]))
        parentFile.create()
        for _ in range(10):
            newFile = File(makeUUID(), size=1000, events=100,
                           locations=set(["T1_US_FNAL_Disk"]))
            newFile.create()
            newFile.addParent(lfn=parentFile['lfn'])
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()
        self.multipleFileSubscription = Subscription(fileset=self.multipleFileFileset,
                                                     workflow=testWorkflow,
                                                     split_algo="EventBased",
                                                     type="Processing")
        self.multipleFileSubscription.create()

        self.singleFileFileset = Fileset(name="TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size=1000, events=100,
                       locations=set(["T1_US_FNAL_Disk"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()
        self.singleFileSubscription = Subscription(fileset=self.singleFileFileset,
                                                   workflow=testWorkflow,
                                                   split_algo="EventBased",
                                                   type="Processing")
        self.singleFileSubscription.create()

        self.multipleSiteFileset = Fileset(name="TestFileset3")
        self.multipleSiteFileset.create()
        for _ in range(5):
            newFile = File(makeUUID(), size=1000, events=100)
            newFile.setLocation("T1_US_FNAL_Disk")
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for _ in range(5):
            newFile = File(makeUUID(), size=1000, events=100)
            newFile.setLocation(["T1_US_FNAL_Disk", "T2_CH_CERN"])
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()
        self.multipleSiteSubscription = Subscription(fileset=self.multipleSiteFileset,
                                                     workflow=testWorkflow,
                                                     split_algo="EventBased",
                                                     type="Processing")
        self.multipleSiteSubscription.create()

        return

    def populateACDCCouch(self, numFiles=3, lumisPerJob=4,
                          eventsPerJob=100, numberOfJobs=250, acdcVer=None):
        """
        _populateACDCCouch_

        Create production files in couchDB to test the creation
        of ACDC jobs for the EventBased algorithm.
        acdcVer parameter decides whether the ACDC documents have to be created
        following the new behaviour/changes or not, for more details see:
        See issue: https://github.com/dmwm/WMCore/issues/9126
        """
        # Define some constants
        workflowName = "ACDC_TestEventBased"
        filesetName = "/%s/Production" % workflowName

        lumisPerFile = lumisPerJob * numberOfJobs
        for i in range(numFiles):
            lfn = "MCFakeFile-some-hash-%s" % str(i).zfill(5)
            for j in range(numberOfJobs):
                firstEvent = (i * eventsPerJob * numberOfJobs) + (j * eventsPerJob) + 1
                acdcFile = File(lfn=lfn, size=1024, events=eventsPerJob, locations=self.validLocations,
                                merged=False, first_event=firstEvent)
                if acdcVer == 2:
                    # lumi range is really inclusive (process first and last lumi)
                    run = Run(1, *list(range(1 + j * lumisPerJob + (i * lumisPerFile),
                                        1 + (j + 1) * lumisPerJob + (i * lumisPerFile))))
                else:
                    # bigger lumi range, LastLumi is in most of the cases not processed
                    run = Run(1, *list(range(1 + j * lumisPerJob + (i * lumisPerFile),
                                        2 + (j + 1) * lumisPerJob + (i * lumisPerFile))))

                acdcFile.addRun(run)
                acdcDoc = {"collection_name": workflowName,
                           "collection_type": "ACDC.CollectionTypes.DataCollection",
                           "files": {lfn: acdcFile},
                           "acdc_version": 2 if acdcVer else 1,
                           "fileset_name": filesetName}

                self.couchDB.queue(acdcDoc)

        self.couchDB.commit()
        return

    def populateACDCFakeFile(self, acdcVer=1):
        """
        _populateACDCFakeFile_
        Create an ACDC Collection with a MCFakeFile and 5 jobs:
        * 4 jobs with single lumi section
        * last job with 4 lumi sections (or 5 if it's the old ACDC doc)
        """
        eventsPerJob = 700
        workflowName = "ACDC_TestEventBased"
        filesetName = "/%s/Production" % workflowName
        acdcFiles = []
        # create 4 MCFakeFiles with a single lumi section
        for lumi in [337, 197, 529, 421]:
            lfn = "MCFakeFile-some-hash-00000"
            firstEvent = random.randint(1, 100) * eventsPerJob + 1
            acdcFile = File(lfn=lfn, size=1024, events=eventsPerJob, locations=self.validLocations,
                            merged=False, first_event=firstEvent)
            if acdcVer == 2:
                run = Run(1, *list(range(lumi, lumi + 1)))
            else:
                run = Run(1, *list(range(lumi, lumi + 2)))
            acdcFile.addRun(run)
            acdcFiles.append(acdcFile)
        # create one last entry with more lumis
        acdcFile = File(lfn=lfn, size=1024, events=eventsPerJob, locations=self.validLocations,
                        merged=False, first_event=firstEvent)
        if acdcVer == 2:
            run = Run(1, *list(range(277, 277 + 4)))
        else:
            run = Run(1, *list(range(277, 277 + 5)))
        acdcFile.addRun(run)
        acdcFiles.append(acdcFile)

        for f in acdcFiles:
            acdcDoc = {"collection_name": workflowName,
                       "collection_type": "ACDC.CollectionTypes.DataCollection",
                       "files": {f['lfn']: f},
                       "acdc_version": acdcVer,
                       "fileset_name": filesetName}
            self.couchDB.queue(acdcDoc)

        self.couchDB.commit()
        return

    def generateFakeMCFile(self, numEvents=100, firstEvent=1,
                           lastEvent=100, firstLumi=1, lastLumi=10,
                           index=1, existingSub=None):
        """
        _generateFakeMCFile_

        Generates a fake MC file for testing production EventBased
        creation of jobs, it creates a single file subscription if no
        existing subscription is provided.
        """
        # MC comes with MCFakeFile(s)
        newFile = File("MCFakeFile-some-hash-%s" % str(index).zfill(5), size=1000,
                       events=numEvents,
                       locations=set(["T1_US_FNAL_Disk"]))
        newFile.addRun(Run(1, *list(range(firstLumi, lastLumi + 1))))
        newFile["first_event"] = firstEvent
        newFile["last_event"] = lastEvent
        newFile.create()
        if existingSub is None:
            singleMCFileset = Fileset(name="MCTestFileset-%i" % index)
            singleMCFileset.create()
            singleMCFileset.addFile(newFile)
            singleMCFileset.commit()
            testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                    name="wf001", task="Test")
            testWorkflow.create()
            singleMCFileSubscription = Subscription(fileset=singleMCFileset,
                                                    workflow=testWorkflow,
                                                    split_algo="EventBased",
                                                    type="Production")
            singleMCFileSubscription.create()
            return singleMCFileSubscription
        else:
            existingSub['fileset'].addFile(newFile)
            existingSub['fileset'].commit()
            return existingSub

    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job=self.eventsPerJob,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 1)

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type="lfn"), ["/some/file/name"])

        self.assertEqual(job["mask"].getMaxEvents(), self.eventsPerJob)
        self.assertEqual(job["mask"]["FirstEvent"], 0)
        self.assertEqual(job["mask"]["LastEvent"], 99)

        self.assertEqual(job["estimatedJobTime"], 100 * 12)
        self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
        self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def testMoreEvents(self):
        """
        _testMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file.
        Since the file has less events than the splitting, the job goes without a mask.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS", subscription=self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job=1000,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 1)

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type="lfn"), ["/some/file/name"])

        self.assertEqual(job["mask"].getMaxEvents(), 100)
        self.assertEqual(job["mask"]["FirstEvent"], 0)
        self.assertEqual(job["mask"]["LastEvent"], 99)

        self.assertEqual(job["estimatedJobTime"], 100 * 12)
        self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
        self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def test50EventSplit(self):
        """
        _test50EventSplit_

        Test event based job splitting when the number of events per job is
        50, this should result in two jobs.
        """
        splitter = SplitterFactory()

        jobFactory = splitter(package="WMCore.WMBS", subscription=self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job=50,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 2)

        for job in jobGroups[0].jobs:
            self.assertEqual(job.getFiles(type="lfn"), ["/some/file/name"])
            self.assertEqual(job["mask"].getMaxEvents(), 50)

            if job["mask"]["FirstEvent"] == 0:
                self.assertEqual(job["mask"]["LastEvent"], 49)
            elif job["mask"]["FirstEvent"] == 50:
                self.assertEqual(job["mask"]["LastEvent"], 99)
            else:
                self.fail("Unexpected splitting was performed")
            self.assertEqual(job["estimatedJobTime"], 50 * 12)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 50)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def test99EventSplit(self):
        """
        _test99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs. Last job shouldn't have a maximum
        number of events, let it run until the end of the file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS", subscription=self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job=99,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 2)

        for job in jobGroups[0].jobs:
            self.assertEqual(job.getFiles(type="lfn"), ["/some/file/name"])

            if job["mask"].getMaxEvents() == 99:
                self.assertEqual(job["mask"]["FirstEvent"], 0)
                self.assertEqual(job["mask"]["LastEvent"], 98)
                self.assertEqual(job["estimatedJobTime"], 99 * 12)
                self.assertEqual(job["estimatedDiskUsage"], 400 * 99)
                self.assertEqual(job["estimatedMemoryUsage"], 2300)
            elif job["mask"].getMaxEvents() == 1:
                self.assertEqual(job["mask"]["FirstEvent"], 99)
                self.assertEqual(job["mask"]["LastEvent"], 99)
                self.assertEqual(job["estimatedJobTime"], 1 * 12)
                self.assertEqual(job["estimatedDiskUsage"], 400 * 1)
                self.assertEqual(job["estimatedMemoryUsage"], 2300)
            else:
                self.fail("Unexpected splitting was performed")
        return

    def test100EventMultipleFileSplit(self):
        """
        _test100EventMultipleFileSplit_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS", subscription=self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job=self.eventsPerJob,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 10)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type="lfn")), 1)
            self.assertEqual(job["mask"].getMaxEvents(), self.eventsPerJob)
            self.assertEqual(job["mask"]["FirstEvent"], 0)
            self.assertEqual(job["estimatedJobTime"], 100 * 12)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def test50EventMultipleFileSplit(self):
        """
        _test50EventMultipleFileSplit_

        Test job splitting into 50 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS", subscription=self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job=50,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 20)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type="lfn")), 1)
            self.assertEqual(job["mask"].getMaxEvents(), 50)

            if job["mask"]["FirstEvent"] == 0:
                self.assertEqual(job["mask"]["LastEvent"], 49)
            elif job["mask"]["FirstEvent"] == 50:
                self.assertEqual(job["mask"]["LastEvent"], 99)
            else:
                self.fail("Unexpected splitting was performed")
            self.assertEqual(job["estimatedJobTime"], 50 * 12)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 50)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)
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
        jobFactory = splitter(package="WMCore.WMBS", subscription=self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job=150,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type="lfn")), 1)
            self.assertEqual(job["mask"].getMaxEvents(), 100)
            self.assertEqual(job["mask"]["FirstEvent"], 0)
            self.assertEqual(job["mask"]["LastEvent"], 99)
            self.assertEqual(job["estimatedJobTime"], 100 * 12)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def test100EventMultipleSite(self):
        """
        _test100EventMultipleSite_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available, at different site combinations.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.multipleSiteSubscription)

        jobGroups = jobFactory(events_per_job=self.eventsPerJob,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 2)

        self.assertEqual(len(jobGroups[0].jobs), 5)
        self.assertEqual(len(jobGroups[1].jobs), 5)
        self.assertEqual(jobGroups[0].jobs[0]['possiblePSN'], set(['s1', 's2']))
        self.assertEqual(jobGroups[1].jobs[0]['possiblePSN'], set(['s1']))

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type="lfn")), 1)

            self.assertEqual(job["mask"].getMaxEvents(), self.eventsPerJob)

            self.assertEqual(job["mask"]["FirstEvent"], 0)
            self.assertEqual(job["estimatedJobTime"], 100 * 12)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def testMCEventSplitOver32bit(self):
        """
        _testMCEventSplitOver32bit_

        Make sure that no events will go over a 32 bit unsigned integer
        representation, event counter should be reset in that case.
        Also test is not over cautious.
        """
        firstEvent = 3 * (2 ** 30) + 1
        singleMCSubscription = self.generateFakeMCFile(numEvents=2 ** 30,
                                                       firstEvent=firstEvent)
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS", subscription=singleMCSubscription)

        jobGroups = jobFactory(events_per_job=2 ** 30 - 1,
                               events_per_lumi=2 ** 30 - 1,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1,
                         "Error: JobFactory did not return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 2,
                         "Error: JobFactory created %s jobs not two"
                         % len(jobGroups[0].jobs))
        for job in jobGroups[0].jobs:

            if job["mask"].getMaxEvents() == 2 ** 30 - 1:
                self.assertEqual(job["mask"]["FirstLumi"], 1)
                self.assertEqual(job["mask"]["FirstEvent"], firstEvent)
                self.assertTrue(job["mask"]["LastEvent"] <= 2 ** 32)
                self.assertEqual(job["estimatedJobTime"], (2 ** 30 - 1) * 12)
                self.assertEqual(job["estimatedDiskUsage"], 400 * (2 ** 30 - 1))
                self.assertEqual(job["estimatedMemoryUsage"], 2300)
            elif job["mask"].getMaxEvents() == 1:
                self.assertEqual(job["mask"]["FirstLumi"], 2)
                self.assertEqual(job["mask"]["FirstEvent"], 1)
                self.assertEqual(job["estimatedJobTime"], 1 * 12)
                self.assertEqual(job["estimatedDiskUsage"], 400 * 1)
                self.assertEqual(job["estimatedMemoryUsage"], 2300)
            else:
                self.fail("Unexpected splitting was performed")

    def test_addParents(self):
        """
        _addParents_

        Test our ability to add parents to a job
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job=50, include_parents=True,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 20)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles(type="lfn")), 1)

            self.assertEqual(job["mask"].getMaxEvents(), 50)
            if job["mask"]["FirstEvent"] == 0:
                self.assertEqual(job["mask"]["LastEvent"], 49)
            elif job["mask"]["FirstEvent"] == 50:
                self.assertEqual(job["mask"]["LastEvent"], 99)
            else:
                self.fail("Unexpected splitting was performed")

            self.assertEqual(job["estimatedJobTime"], 50 * 12)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 50)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)
            for f in job['input_files']:
                self.assertEqual(len(f['parents']), 1)
                self.assertEqual(list(f['parents'])[0]['lfn'], '/parent/lfn/')

        return

    def testACDCProduction_v1(self):
        """
        _testACDCProduction_v1_

        Test the ability of the EventBased algorithm of creating
        jobs from ACDC correctly. Uses ACDC documents not versioned.
        """
        numFiles = 3
        lumisPerJob = 4
        eventsPerJob = 100
        numberOfJobs = 20
        self.populateACDCCouch(numFiles=numFiles, lumisPerJob=lumisPerJob,
                               eventsPerJob=eventsPerJob, numberOfJobs=numberOfJobs)

        mcSubscription = None
        for idx in range(3):
            mcSubscription = self.generateFakeMCFile(numEvents=eventsPerJob * numberOfJobs,
                                                     firstEvent=idx * eventsPerJob * numberOfJobs + 1,
                                                     lastEvent=(idx + 1) * eventsPerJob * numberOfJobs,
                                                     firstLumi=idx * lumisPerJob * numberOfJobs + 1,
                                                     lastLumi=(idx + 1) * lumisPerJob * numberOfJobs,
                                                     index=idx, existingSub=mcSubscription)

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=mcSubscription)

        jobGroups = jobFactory(events_per_job=eventsPerJob, events_per_lumi=int(eventsPerJob / lumisPerJob),
                               collectionName="ACDC_TestEventBased",
                               couchURL=self.couchUrl, couchDB=self.couchDBName,
                               filesetName="/ACDC_TestEventBased/Production",
                               performance=self.performanceParams)

        self.assertEqual(1, len(jobGroups))
        jobGroup = jobGroups[0]
        self.assertEqual(numFiles * numberOfJobs, len(jobGroup.jobs))

        for job in jobGroup.jobs:
            self.assertEqual(1, len(job["input_files"]))
            mask = job["mask"]
            self.assertEqual(mask.getMaxEvents(), eventsPerJob)
            self.assertEqual(mask.getMax("Event"), eventsPerJob)
            self.assertEqual(mask.getMax("Lumi"), lumisPerJob)
            self.assertEqual(mask.getMax("Run"), 1)
            self.assertEqual(mask["LastLumi"] - mask["FirstLumi"], lumisPerJob - 1)
            self.assertEqual(mask["LastEvent"] - mask["FirstEvent"], eventsPerJob - 1)
            self.assertEqual(mask["runAndLumis"], {})
            self.assertEqual(job["estimatedJobTime"], eventsPerJob * 12)
            self.assertEqual(job["estimatedDiskUsage"], eventsPerJob * 400)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def testACDCProduction_v2(self):
        """
        _testACDCProduction_v2_

        Test the ability of the EventBased algorithm of creating
        jobs from ACDC correctly. Uses the new ACDC document version.
        """
        numFiles = 3
        lumisPerJob = 4
        eventsPerJob = 100
        numberOfJobs = 12  # 200
        self.populateACDCCouch(numFiles=numFiles, lumisPerJob=lumisPerJob, eventsPerJob=eventsPerJob,
                               numberOfJobs=numberOfJobs, acdcVer=2)

        mcSubscription = None
        for idx in range(3):
            mcSubscription = self.generateFakeMCFile(numEvents=eventsPerJob * numberOfJobs,
                                                     firstEvent=idx * eventsPerJob * numberOfJobs + 1,
                                                     lastEvent=(idx + 1) * eventsPerJob * numberOfJobs,
                                                     firstLumi=idx * lumisPerJob * numberOfJobs + 1,
                                                     lastLumi=(idx + 1) * lumisPerJob * numberOfJobs,
                                                     index=idx, existingSub=mcSubscription)

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=mcSubscription)

        jobGroups = jobFactory(events_per_job=eventsPerJob, events_per_lumi=int(eventsPerJob / lumisPerJob),
                               collectionName="ACDC_TestEventBased",
                               couchURL=self.couchUrl, couchDB=self.couchDBName,
                               filesetName="/ACDC_TestEventBased/Production",
                               performance=self.performanceParams)

        self.assertEqual(1, len(jobGroups))
        jobGroup = jobGroups[0]
        self.assertEqual(numFiles * numberOfJobs, len(jobGroup.jobs))

        for job in jobGroup.jobs:
            self.assertEqual(1, len(job["input_files"]))
            mask = job["mask"]
            self.assertEqual(mask.getMaxEvents(), eventsPerJob)
            self.assertEqual(mask.getMax("Event"), eventsPerJob)
            self.assertEqual(mask.getMax("Lumi"), lumisPerJob)
            self.assertEqual(mask.getMax("Run"), 1)
            self.assertEqual(mask["LastLumi"] - mask["FirstLumi"], lumisPerJob - 1)
            self.assertEqual(mask["LastEvent"] - mask["FirstEvent"], eventsPerJob - 1)
            self.assertEqual(mask["runAndLumis"], {})
            self.assertEqual(job["estimatedJobTime"], eventsPerJob * 12)
            self.assertEqual(job["estimatedDiskUsage"], eventsPerJob * 400)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def testACDCNonSequential_v1(self):
        """
        _testACDCNonSequential_v1_
        Test the ability of the EventBased algorithm to create the proper jobs
        given job information from the ACDCServer using non-sequential and irregular
        (diff number of lumis per job) lumi distribution (old version of ACDC docs)
        """
        eventsPerJob = 700
        eventsPerLumi = 200
        lumisPerJob = 4

        self.populateACDCFakeFile()

        mcSubscription = self.generateFakeMCFile(3500, 1, 0, 1, 1000, 0)
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=mcSubscription)

        jobGroups = jobFactory(events_per_job=eventsPerJob, events_per_lumi=eventsPerLumi,
                               collectionName="ACDC_TestEventBased",
                               couchURL=self.couchUrl, couchDB=self.couchDBName,
                               filesetName="/ACDC_TestEventBased/Production",
                               performance=self.performanceParams)

        self.assertEqual(1, len(jobGroups))
        jobGroup = jobGroups[0]
        self.assertEqual(5, len(jobGroup.jobs))

        for jobNum, lRange in enumerate([[197, 198], [277, 281], [337, 338], [421, 422], [529, 530]]):
            job = jobGroup.jobs[jobNum]
            self.assertEqual(1, len(job["input_files"]))
            mask = job["mask"]
            self.assertEqual(mask.getMaxEvents(), eventsPerJob)
            self.assertEqual(mask.getMax("Event"), eventsPerJob)
            if lRange[0] == 277:
                self.assertEqual(mask.getMax("Lumi"), lumisPerJob)
            else:
                self.assertEqual(mask.getMax("Lumi"), 1)
            self.assertEqual(mask.getMax("Run"), 1)
            self.assertEqual(mask["FirstLumi"], lRange[0])
            self.assertEqual(mask["LastLumi"], lRange[1] - 1)
            self.assertEqual(mask["LastEvent"] - mask["FirstEvent"], eventsPerJob - 1)
            self.assertEqual(mask["runAndLumis"], {})
            self.assertEqual(job["estimatedJobTime"], eventsPerJob * 12)
            self.assertEqual(job["estimatedDiskUsage"], eventsPerJob * 400)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return

    def testACDCNonSequential_v2(self):
        """
        _testACDCNonSequential_v2_
        Test the ability of the EventBased algorithm to create the proper jobs
        given job information from the ACDCServer using non-sequential and irregular
        (diff number of lumis per job) lumi distribution (new version of ACDC docs)
        """
        eventsPerJob = 700
        eventsPerLumi = 200
        lumisPerJob = 4

        self.populateACDCFakeFile(acdcVer=2)

        mcSubscription = self.generateFakeMCFile(3500, 1, 0, 1, 1000, 0)
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=mcSubscription)

        jobGroups = jobFactory(events_per_job=eventsPerJob, events_per_lumi=eventsPerLumi,
                               collectionName="ACDC_TestEventBased",
                               couchURL=self.couchUrl, couchDB=self.couchDBName,
                               filesetName="/ACDC_TestEventBased/Production",
                               performance=self.performanceParams)

        self.assertEqual(1, len(jobGroups))
        jobGroup = jobGroups[0]
        self.assertEqual(5, len(jobGroup.jobs))

        for jobNum, lRange in enumerate([[197, 198], [277, 281], [337, 338], [421, 422], [529, 530]]):
            job = jobGroup.jobs[jobNum]
            self.assertEqual(1, len(job["input_files"]))
            mask = job["mask"]
            self.assertEqual(mask.getMaxEvents(), eventsPerJob)
            self.assertEqual(mask.getMax("Event"), eventsPerJob)
            if lRange[0] == 277:
                self.assertEqual(mask.getMax("Lumi"), lumisPerJob)
            else:
                self.assertEqual(mask.getMax("Lumi"), 1)
            self.assertEqual(mask.getMax("Run"), 1)
            self.assertEqual(mask["FirstLumi"], lRange[0])
            self.assertEqual(mask["LastLumi"], lRange[1] - 1)
            self.assertEqual(mask["LastEvent"] - mask["FirstEvent"], eventsPerJob - 1)
            self.assertEqual(mask["runAndLumis"], {})
            self.assertEqual(job["estimatedJobTime"], eventsPerJob * 12)
            self.assertEqual(job["estimatedDiskUsage"], eventsPerJob * 400)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)

        return


if __name__ == '__main__':
    unittest.main()
