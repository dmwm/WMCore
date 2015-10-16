#!/usr/bin/env python
"""
_ParentlessMergeBySize_t_

Unit tests for parentless WMBS merging.
"""

import unittest
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMQuality.TestInit import TestInit


class ParentlessMergeBySizeTest(unittest.TestCase):
    """
    _ParentlessMergeBySizeTest_

    Unit tests for parentless WMBS merging.
    """

    def setUp(self):
        """
        _setUp_

        Boiler plate DB setup.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def stuffWMBS(self):
        """
        _stuffWMBS_

        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test job creation.  Three completed job groups each
        containing several files are injected.  Another incomplete job group is
        also injected.  Also files are added to the "Mergeable" subscription as
        well as to the output fileset for their jobgroups.
        """
        locationAction = self.daoFactory(classname="Locations.New")
        locationAction.execute(siteName="T1_US_FNAL", pnn="T1_US_FNAL_Disk")

        self.mergeFileset = Fileset(name="mergeFileset")
        self.mergeFileset.create()
        self.bogusFileset = Fileset(name="bogusFileset")
        self.bogusFileset.create()

        mergeWorkflow = Workflow(name="mergeWorkflow", spec="bunk2",
                                 owner="Steve", task="Test")
        mergeWorkflow.create()
        markWorkflow = self.daoFactory(classname="Workflow.MarkInjectedWorkflows")
        markWorkflow.execute(names=[mergeWorkflow.name], injected=True)

        self.mergeSubscription = Subscription(fileset=self.mergeFileset,
                                              workflow=mergeWorkflow,
                                              split_algo="ParentlessMergeBySize")
        self.mergeSubscription.create()
        self.bogusSubscription = Subscription(fileset=self.bogusFileset,
                                              workflow=mergeWorkflow,
                                              split_algo="ParentlessMergeBySize")

        file1 = File(lfn = "file1", size = 1024, events = 1024, first_event = 0,
                     locations = set(["T1_US_FNAL_Disk"]))
        file1.addRun(Run(1, *[45]))
        file1.create()
        file2 = File(lfn = "file2", size = 1024, events = 1024,
                     first_event = 1024, locations = set(["T1_US_FNAL_Disk"]))
        file2.addRun(Run(1, *[45]))
        file2.create()
        file3 = File(lfn = "file3", size = 1024, events = 1024,
                     first_event = 2048, locations = set(["T1_US_FNAL_Disk"]))
        file3.addRun(Run(1, *[45]))
        file3.create()
        file4 = File(lfn = "file4", size = 1024, events = 1024,
                     first_event = 3072, locations = set(["T1_US_FNAL_Disk"]))
        file4.addRun(Run(1, *[45]))
        file4.create()

        fileA = File(lfn = "fileA", size = 1024, events = 1024,
                     first_event = 0, locations = set(["T1_US_FNAL_Disk"]))
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileB = File(lfn = "fileB", size = 1024, events = 1024,
                     first_event = 1024, locations = set(["T1_US_FNAL_Disk"]))
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileC = File(lfn = "fileC", size = 1024, events = 1024,
                     first_event = 2048, locations = set(["T1_US_FNAL_Disk"]))
        fileC.addRun(Run(1, *[46]))
        fileC.create()

        fileI = File(lfn = "fileI", size = 1024, events = 1024,
                     first_event = 0, locations = set(["T1_US_FNAL_Disk"]))
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileII = File(lfn = "fileII", size = 1024, events = 1024,
                      first_event = 1024, locations = set(["T1_US_FNAL_Disk"]))
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileIII = File(lfn = "fileIII", size = 1024, events = 102400,
                       first_event = 2048, locations = set(["T1_US_FNAL_Disk"]))
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIV = File(lfn = "fileIV", size = 102400, events = 1024,
                      first_event = 3072, locations = set(["T1_US_FNAL_Disk"]))
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()

        for jobFile in [file1, file2, file3, file4, fileA, fileB, fileC, fileI,
                        fileII, fileIII, fileIV]:
            self.mergeFileset.addFile(jobFile)
            self.bogusFileset.addFile(jobFile)

        self.mergeFileset.commit()
        self.bogusFileset.commit()

        return

    def testMinMergeSize1(self):
        """
        _testMinMergeSize1_

        Set the minimum merge size to be 20,000 bytes which is more than the
        sum of all file sizes in the WMBS instance.  Verify that no merge jobs
        will be produced.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=200000, max_merge_size=2000000000,
                            max_merge_events=200000000)

        assert len(result) == 0, \
            "ERROR: No job groups should be returned."

        return

    def testMinMergeSize1a(self):
        """
        _testMinMergeSize1a_

        Set the minimum merge size to be 20,000 bytes which is more than the
        sum of all file sizes in the WMBS instance and mark the fileset as
        closed.  Verify that one job containing all files is pushed out.
        """
        self.stuffWMBS()
        self.mergeFileset.markOpen(False)

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=200000, max_merge_size=2000000,
                            max_merge_events=2000000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % len(result)

        assert len(result[0].jobs) == 1, \
            "Error: One job should have been returned: %s" % len(result[0].jobs)

        self.assertEqual(result[0].jobs[0]["estimatedDiskUsage"], 10 + 2 * 100)

        self.assertEqual(result[0].jobs[0]["possiblePSN"], set(["T1_US_FNAL"]))

        goldenFiles = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                       "fileC", "fileI", "fileII", "fileIII", "fileIV"]

        jobFiles = result[0].jobs[0].getFiles()

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for jobFile in jobFiles:
            jobFile.loadData()
            self.assertTrue(jobFile["lfn"] in goldenFiles,
                            "Error: Unknown file: %s" % jobFile["lfn"])
            goldenFiles.remove(jobFile["lfn"])

            fileRun = list(jobFile["runs"])[0].run
            fileLumi = min(list(jobFile["runs"])[0])
            fileEvent = jobFile["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                    "ERROR: Files not ordered by lumi"

            if fileLumi == currentLumi:
                assert fileEvent >= currentEvent, \
                    "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        return

    def testMaxMergeSize(self):
        """
        _testMaxMergeSize_

        Set the maximum merge size to be 100000 bytes.  Verify that two merge
        jobs are created, one for the one large file and another for the rest of
        the files.  Verify that each merge job contains the expected files and
        that we merge across runs.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=1, max_merge_size=100000,
                            max_merge_events=200000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 2, \
            "ERROR: Two jobs should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC", "fileI", "fileII", "fileIII"]
        goldenFilesB = ["fileIV"]

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], set(["T1_US_FNAL"]))

            jobFiles = job.getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 11)
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                self.assertEqual(job["estimatedDiskUsage"], 2 * 100)
                goldenFiles = goldenFilesB

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for jobFile in jobFiles:
                self.assertTrue(jobFile["lfn"] in goldenFiles,
                                "Error: Unknown file: %s" % jobFile["lfn"])

                goldenFiles.remove(jobFile["lfn"])

            fileRun = list(jobFile["runs"])[0].run
            fileLumi = min(list(jobFile["runs"])[0])
            fileEvent = jobFile["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                    "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent >= currentEvent, \
                        "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0, \
            "ERROR: Files missing from merge jobs."

        return

    def testMaxEvents(self):
        """
        _testMaxEvents_

        Verify the the max_merge_events parameter works and that we correctly
        merge across runs.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=1, max_merge_size=20000000,
                            max_merge_events=100000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 2, \
            "ERROR: Two jobs should have been returned: %s" % len(result[0].jobs)

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC", "fileI", "fileII", "fileIV"]
        goldenFilesB = ["fileIII"]

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], set(["T1_US_FNAL"]))

            jobFiles = job.getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 9 + 2 * 100)
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                self.assertEqual(job["estimatedDiskUsage"], 2)
                goldenFiles = goldenFilesB

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for jobFile in jobFiles:
                self.assertTrue(jobFile["lfn"] in goldenFiles,
                                "Error: Unknown file: %s" % jobFile["lfn"])

                goldenFiles.remove(jobFile["lfn"])

                fileRun = list(jobFile["runs"])[0].run
                fileLumi = min(list(jobFile["runs"])[0])
                fileEvent = jobFile["first_event"]

                if currentRun == 0:
                    currentRun = fileRun
                    currentLumi = fileLumi
                    currentEvent = fileEvent
                    continue

                assert fileRun >= currentRun, \
                    "ERROR: Files not sorted by run: %s, %s" % (fileRun, currentRun)

                if fileRun == currentRun:
                    assert fileLumi >= currentLumi, \
                        "ERROR: Files not ordered by lumi"

                    if fileLumi == currentLumi:
                        assert fileEvent >= currentEvent, \
                            "ERROR: Files not ordered by first event"

                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0 and \
               "ERROR: Files missing from merge jobs."

        return

    def testMinMergeSize1aNoRunMerge(self):
        """
        _testMinMergeSize1aNoRunMerge_

        Set the minimum merge size to be 20,000 bytes which is more than the
        sum of all file sizes in the WMBS instance and mark the fileset as
        closed.  Verify that two jobs are pushed out and that we don't merge
        accross run boundaries.
        """
        self.stuffWMBS()
        self.mergeFileset.markOpen(False)

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=200000, max_merge_size=2000000,
                            max_merge_events=2000000, merge_across_runs=False)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % len(result)

        assert len(result[0].jobs) == 2, \
            "Error: Two jobs should have been returned: %s" % len(result[0].jobs)

        goldenFilesA = ["file1", "file2", "file3", "file4",
                        "fileA", "fileB", "fileC"]
        goldenFilesB = ["fileI", "fileII", "fileIII", "fileIV"]
        goldenFilesA.sort()
        goldenFilesB.sort()

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], set(["T1_US_FNAL"]))

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            jobLFNs = []

            for jobFile in job.getFiles():
                jobFile.loadData()
                jobLFNs.append(jobFile["lfn"])

                fileRun = list(jobFile["runs"])[0].run
                fileLumi = min(list(jobFile["runs"])[0])
                fileEvent = jobFile["first_event"]

                if currentRun == 0:
                    currentRun = fileRun
                    currentLumi = fileLumi
                    currentEvent = fileEvent
                    continue

                assert fileRun >= currentRun, \
                    "ERROR: Files not sorted by run."

                if fileRun == currentRun:
                    assert fileLumi >= currentLumi, \
                        "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent >= currentEvent, \
                        "ERROR: Files not ordered by first event"

                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent

            jobLFNs.sort()
            if jobLFNs == goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 8)
                goldenFilesA = []
            else:
                self.assertEqual(job["estimatedDiskUsage"], 3 + 2 * 100)
                self.assertEqual(jobLFNs, goldenFilesB,
                                 "Error: LFNs do not match.")
                goldenFilesB = []

        return

    def testMaxMergeSizeNoRunMerge(self):
        """
        _testMaxMergeSizeNoRunMerge_

        Set the maximum merge size to be 100000 bytes.  Verify that two merge
        jobs are created, one for the one large file and another for the rest of
        the files.  Verify that each merge job contains the expected files and
        that we don't merge across run boundaries.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=1, max_merge_size=100000,
                            max_merge_events=200000, merge_across_runs=False)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 3, \
            "ERROR: Three jobs should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC"]
        goldenFilesB = ["fileI", "fileII", "fileIII"]
        goldenFilesC = ["fileIV"]

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], set(["T1_US_FNAL"]))

            jobFiles = job.getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 8)
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                self.assertEqual(job["estimatedDiskUsage"], 4)
                goldenFiles = goldenFilesB
            else:
                self.assertEqual(job["estimatedDiskUsage"], 2 * 100)
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for jobFile in jobFiles:
                self.assertTrue(jobFile["lfn"] in goldenFiles,
                                "Error: Unknown file: %s" % jobFile["lfn"])

                goldenFiles.remove(jobFile["lfn"])

            fileRun = list(jobFile["runs"])[0].run
            fileLumi = min(list(jobFile["runs"])[0])
            fileEvent = jobFile["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            self.assertTrue(fileRun >= currentRun,
                            "ERROR: Files not sorted by run.")
            if fileRun == currentRun:
                self.assertTrue(fileLumi >= currentLumi,
                                "ERROR: Files not ordered by lumi")
                if fileLumi == currentLumi:
                    self.assertTrue(fileEvent >= currentEvent,
                                    "ERROR: Files not ordered by first event")

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        self.assertTrue(len(goldenFilesA) == 0 and len(goldenFilesB) == 0,
                        "ERROR: Files missing from merge jobs.")

        return

    def testMaxEventsNoRunMerge(self):
        """
        _testMaxEventsNoRunMerge_

        Verify that the max events merge parameter works correctly and that we
        don't merge accross run boundaries.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=1, max_merge_size=20000000,
                            max_merge_events=100000, merge_across_runs=False)

        self.assertTrue(len(result) == 1,
                        "ERROR: More than one JobGroup returned: %s" % result)

        self.assertTrue(len(result[0].jobs) == 3,
                        "ERROR: Three jobs should have been returned: %s" % len(result[0].jobs))

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC", ]
        goldenFilesB = ["fileI", "fileII", "fileIV"]
        goldenFilesC = ["fileIII"]

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], set(["T1_US_FNAL"]))

            jobFiles = job.getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 8)
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                self.assertEqual(job["estimatedDiskUsage"], 2 + 2 * 100)
                goldenFiles = goldenFilesB
            else:
                self.assertEqual(job["estimatedDiskUsage"], 2 * 1)
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for jobFile in jobFiles:
                self.assertTrue(jobFile["lfn"] in goldenFiles,
                                "Error: Unknown file: %s" % jobFile["lfn"])

                goldenFiles.remove(jobFile["lfn"])

                fileRun = list(jobFile["runs"])[0].run
                fileLumi = min(list(jobFile["runs"])[0])
                fileEvent = jobFile["first_event"]

                if currentRun == 0:
                    currentRun = fileRun
                    currentLumi = fileLumi
                    currentEvent = fileEvent
                    continue

                self.assertTrue(fileRun >= currentRun,
                                "ERROR: Files not sorted by run: %s, %s" % (fileRun, currentRun))
                if fileRun == currentRun:
                    self.assertTrue(fileLumi >= currentLumi,
                                    "ERROR: Files not ordered by lumi")
                    if fileLumi == currentLumi:
                        self.assertTrue(fileEvent >= currentEvent,
                                        "ERROR: Files not ordered by first event")

                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent

        self.assertTrue(len(goldenFilesA) == 0 and len(goldenFilesB) == 0 and len(goldenFilesC) == 0,
                        "ERROR: Files missing from merge jobs.")

        return

    def testLocationMerging(self):
        """
        _testLocationMerging_

        Verify that files residing on different SEs are not merged together in
        the same job.
        """
        self.stuffWMBS()

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "T1_UK_RAL", pnn = "T1_UK_RAL_Disk")

        fileSite2 = File(lfn = "fileRAL", size = 4098, events = 1024,
                         first_event = 0, locations = set(["T1_UK_RAL_Disk"]))
        fileSite2.addRun(Run(1, *[46]))
        fileSite2.create()

        self.mergeFileset.addFile(fileSite2)
        self.mergeFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=4097, max_merge_size=99999999,
                            max_merge_events=999999999, merge_across_runs=False)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 3, \
            "ERROR: Three jobs should have been returned."

        ralJobs = 0
        fnalJobs = 0
        for job in result[0].jobs:
            if job["possiblePSN"] == set(["T1_UK_RAL"]):
                ralJobs += 1
            elif job["possiblePSN"] == set(["T1_US_FNAL"]):
                fnalJobs += 1

        self.assertEqual(ralJobs, 1)
        self.assertEqual(fnalJobs, 2)

        return

    def testMaxWaitTime(self):
        """
        _testMaxWaitTime_

        Set the max wait times to be negative - this should force all files to merge
        out immediately

        Using the first setup as the first merge test which should normally produce
        no jobGroups
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=200000, max_merge_size=2000000000,
                            max_merge_events=200000000, max_wait_time=-10)

        # Everything should be in one, small jobGroup
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0].jobs), 1)
        job = result[0].jobs[0]
        # All files should be in one job
        self.assertEqual(len(job.getFiles()), 11)

        return

    def testDifferentSubscritionIDs(self):
        """
        _testDifferentSubscriptionIDs_

        Make sure that the merge splitting still runs if the subscription ID
        is not equal to the workflow ID.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        dummyWorkflow = Workflow(name="dummyWorkflow", spec="bunk49",
                                 owner="Steve", task="Test2")
        dummyWorkflow.create()
        dummyFileset = Fileset(name="dummyFileset")
        dummyFileset.create()
        dummySubscription1 = Subscription(fileset=dummyFileset,
                                          workflow=dummyWorkflow,
                                          split_algo="ParentlessMergeBySize")
        dummySubscription2 = Subscription(fileset=dummyFileset,
                                          workflow=dummyWorkflow,
                                          split_algo="ParentlessMergeBySize")
        dummySubscription1.create()
        dummySubscription2.create()
        myThread.transaction.commit()

        self.stuffWMBS()
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)
        result = jobFactory(min_merge_size=4097, max_merge_size=99999999,
                            max_merge_events=999999999, merge_across_runs=False)
        self.assertEqual(len(result), 1)
        jobGroup = result[0]
        self.assertEqual(len(jobGroup.jobs), 2)
        return


if __name__ == '__main__':
    unittest.main()
