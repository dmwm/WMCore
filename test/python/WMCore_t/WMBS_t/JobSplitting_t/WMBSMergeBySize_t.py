#!/usr/bin/env python
"""
_WMBSMergeBySize_t

Unit tests for generic WMBS merging.
"""

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit


class WMBSMergeBySize(unittest.TestCase):
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

    def stuffWMBS(self, injected=True):
        """
        _stuffWMBS_

        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test job creation.  Three completed job groups each
        containing several files are injected.  Another incomplete job group is
        also injected.  Also files are added to the "Mergeable" subscription as
        well as to the output fileset for their jobgroups.
        """
        locationAction = self.daoFactory(classname="Locations.New")
        locationAction.execute(siteName="T2_CH_CERN", pnn="T2_CH_CERN")
        locationAction.execute(siteName="T1_US_FNAL", pnn="T2_CH_CERN")

        changeStateDAO = self.daoFactory(classname="Jobs.ChangeState")

        self.mergeFileset = Fileset(name="mergeFileset")
        self.mergeFileset.create()
        self.bogusFileset = Fileset(name="bogusFileset")
        self.bogusFileset.create()

        self.mergeMergedFileset = Fileset(name="mergeMergedFileset")
        self.mergeMergedFileset.create()
        self.bogusMergedFileset = Fileset(name="bogusMergedFileset")
        self.bogusMergedFileset.create()

        mergeWorkflow = Workflow(name="mergeWorkflow", spec="bunk2",
                                 owner="Steve", task="Test")
        mergeWorkflow.create()
        markWorkflow = self.daoFactory(classname="Workflow.MarkInjectedWorkflows")
        markWorkflow.execute(names=[mergeWorkflow.name], injected=injected)

        self.mergeSubscription = Subscription(fileset=self.mergeFileset,
                                              workflow=mergeWorkflow,
                                              split_algo="WMBSMergeBySize")
        self.mergeSubscription.create()
        self.bogusSubscription = Subscription(fileset=self.bogusFileset,
                                              workflow=mergeWorkflow,
                                              split_algo="WMBSMergeBySize")

        inputFileset = Fileset(name="inputFileset")
        inputFileset.create()

        inputWorkflow = Workflow(name="inputWorkflow", spec="input",
                                 owner="Steve", task="Test")
        inputWorkflow.create()
        inputWorkflow.addOutput("output", self.mergeFileset,
                                self.mergeMergedFileset)
        inputWorkflow.addOutput("output2", self.bogusFileset,
                                self.bogusMergedFileset)
        bogusInputWorkflow = Workflow(name="bogusInputWorkflow", spec="input",
                                      owner="Steve", task="Test")
        bogusInputWorkflow.create()

        inputSubscription = Subscription(fileset=inputFileset,
                                         workflow=inputWorkflow)
        inputSubscription.create()
        bogusInputSubscription = Subscription(fileset=inputFileset,
                                              workflow=bogusInputWorkflow)
        bogusInputSubscription.create()

        parentFile1 = File(lfn="parentFile1")
        parentFile1.create()
        parentFile2 = File(lfn="parentFile2")
        parentFile2.create()
        parentFile3 = File(lfn="parentFile3")
        parentFile3.create()
        parentFile4 = File(lfn="parentFile4")
        parentFile4.create()
        self.parentFileSite2 = File(lfn="parentFileSite2")
        self.parentFileSite2.create()

        jobGroup1 = JobGroup(subscription=inputSubscription)
        jobGroup1.create()
        jobGroup2 = JobGroup(subscription=inputSubscription)
        jobGroup2.create()
        jobGroup3 = JobGroup(subscription=bogusInputSubscription)
        jobGroup3.create()

        testJob1 = Job()
        testJob1.addFile(parentFile1)
        testJob1.create(jobGroup1)
        testJob1["state"] = "cleanout"
        testJob1["oldstate"] = "new"
        testJob1["couch_record"] = "somejive"
        testJob1["retry_count"] = 0
        testJob1["outcome"] = "success"
        testJob1.save()
        changeStateDAO.execute([testJob1])

        testJob1A = Job()
        testJob1A.addFile(parentFile1)
        testJob1A.create(jobGroup3)
        testJob1A["state"] = "cleanout"
        testJob1A["oldstate"] = "new"
        testJob1A["couch_record"] = "somejive"
        testJob1A["retry_count"] = 0
        testJob1A["outcome"] = "failure"
        testJob1A.save()
        changeStateDAO.execute([testJob1A])

        testJob2 = Job()
        testJob2.addFile(parentFile2)
        testJob2.create(jobGroup1)
        testJob2["state"] = "cleanout"
        testJob2["oldstate"] = "new"
        testJob2["couch_record"] = "somejive"
        testJob2["retry_count"] = 0
        testJob2["outcome"] = "success"
        testJob2.save()
        changeStateDAO.execute([testJob2])

        testJob3 = Job()
        testJob3.addFile(parentFile3)
        testJob3.create(jobGroup2)
        testJob3["state"] = "cleanout"
        testJob3["oldstate"] = "new"
        testJob3["couch_record"] = "somejive"
        testJob3["retry_count"] = 0
        testJob3["outcome"] = "success"
        testJob3.save()
        changeStateDAO.execute([testJob3])

        testJob4 = Job()
        testJob4.addFile(parentFile4)
        testJob4.create(jobGroup2)
        testJob4["state"] = "cleanout"
        testJob4["oldstate"] = "new"
        testJob4["couch_record"] = "somejive"
        testJob4["retry_count"] = 0
        testJob4["outcome"] = "failure"
        testJob4.save()
        changeStateDAO.execute([testJob4])

        # We'll simulate a failed split by event job that the merger should
        # ignore.
        parentFile5 = File(lfn="parentFile5")
        parentFile5.create()

        testJob5 = Job()
        testJob5.addFile(parentFile5)
        testJob5.create(jobGroup2)
        testJob5["state"] = "cleanout"
        testJob5["oldstate"] = "new"
        testJob5["couch_record"] = "somejive"
        testJob5["retry_count"] = 0
        testJob5["outcome"] = "success"
        testJob5.save()
        changeStateDAO.execute([testJob5])

        testJob6 = Job()
        testJob6.addFile(parentFile5)
        testJob6.create(jobGroup2)
        testJob6["state"] = "cleanout"
        testJob6["oldstate"] = "new"
        testJob6["couch_record"] = "somejive"
        testJob6["retry_count"] = 0
        testJob6["outcome"] = "failure"
        testJob6.save()
        changeStateDAO.execute([testJob6])

        testJob7 = Job()
        testJob7.addFile(self.parentFileSite2)
        testJob7.create(jobGroup2)
        testJob7["state"] = "cleanout"
        testJob7["oldstate"] = "new"
        testJob7["couch_record"] = "somejive"
        testJob7["retry_count"] = 0
        testJob7["outcome"] = "success"
        testJob7.save()
        changeStateDAO.execute([testJob7])

        badFile1 = File(lfn="badFile1", size=10241024, events=10241024,
                        first_event=0, locations={"T2_CH_CERN"})
        badFile1.addRun(Run(1, *[45]))
        badFile1.create()
        badFile1.addParent(parentFile5["lfn"])

        file1 = File(lfn="file1", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        file1.addRun(Run(1, *[45]))
        file1.create()
        file1.addParent(parentFile1["lfn"])
        file2 = File(lfn="file2", size=1024, events=1024,
                     first_event=1024, locations={"T2_CH_CERN"})
        file2.addRun(Run(1, *[45]))
        file2.create()
        file2.addParent(parentFile1["lfn"])
        file3 = File(lfn="file3", size=1024, events=1024,
                     first_event=2048, locations={"T2_CH_CERN"})
        file3.addRun(Run(1, *[45]))
        file3.create()
        file3.addParent(parentFile1["lfn"])
        file4 = File(lfn="file4", size=1024, events=1024,
                     first_event=3072, locations={"T2_CH_CERN"})
        file4.addRun(Run(1, *[45]))
        file4.create()
        file4.addParent(parentFile1["lfn"])

        fileA = File(lfn="fileA", size=1024, events=1024,
                     first_event=0, locations={"T2_CH_CERN"})
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileA.addParent(parentFile2["lfn"])
        fileB = File(lfn="fileB", size=1024, events=1024,
                     first_event=1024, locations={"T2_CH_CERN"})
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileB.addParent(parentFile2["lfn"])
        fileC = File(lfn="fileC", size=1024, events=1024,
                     first_event=2048, locations={"T2_CH_CERN"})
        fileC.addRun(Run(1, *[46]))
        fileC.create()
        fileC.addParent(parentFile2["lfn"])

        fileI = File(lfn="fileI", size=1024, events=1024,
                     first_event=0, locations={"T2_CH_CERN"})
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileI.addParent(parentFile3["lfn"])
        fileII = File(lfn="fileII", size=1024, events=1024,
                      first_event=1024, locations={"T2_CH_CERN"})
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileII.addParent(parentFile3["lfn"])
        fileIII = File(lfn="fileIII", size=1024, events=1024,
                       first_event=2048, locations={"T2_CH_CERN"})
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIII.addParent(parentFile3["lfn"])
        fileIV = File(lfn="fileIV", size=1024, events=1024,
                      first_event=3072, locations={"T2_CH_CERN"})
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()
        fileIV.addParent(parentFile3["lfn"])

        fileX = File(lfn="badFileA", size=1024, events=1024,
                     first_event=0, locations={"T2_CH_CERN"})
        fileX.addRun(Run(1, *[47]))
        fileX.create()
        fileX.addParent(parentFile4["lfn"])
        fileY = File(lfn="badFileB", size=1024, events=1024,
                     first_event=1024, locations={"T2_CH_CERN"})
        fileY.addRun(Run(1, *[47]))
        fileY.create()
        fileY.addParent(parentFile4["lfn"])
        fileZ = File(lfn="badFileC", size=1024, events=1024,
                     first_event=2048, locations={"T2_CH_CERN"})
        fileZ.addRun(Run(1, *[47]))
        fileZ.create()
        fileZ.addParent(parentFile4["lfn"])

        jobGroup1.output.addFile(file1)
        jobGroup1.output.addFile(file2)
        jobGroup1.output.addFile(file3)
        jobGroup1.output.addFile(file4)
        jobGroup1.output.addFile(fileA)
        jobGroup1.output.addFile(fileB)
        jobGroup1.output.addFile(fileC)
        jobGroup1.output.commit()

        jobGroup2.output.addFile(fileI)
        jobGroup2.output.addFile(fileII)
        jobGroup2.output.addFile(fileIII)
        jobGroup2.output.addFile(fileIV)
        jobGroup2.output.addFile(fileX)
        jobGroup2.output.addFile(fileY)
        jobGroup2.output.addFile(fileZ)
        jobGroup2.output.addFile(badFile1)
        jobGroup2.output.commit()

        for fileObj in [file1, file2, file3, file4, fileA, fileB, fileC, fileI,
                        fileII, fileIII, fileIV, fileX, fileY, fileZ, badFile1]:
            self.mergeFileset.addFile(fileObj)
            self.bogusFileset.addFile(fileObj)

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

        result = jobFactory(min_merge_size=20000, max_merge_size=2000000000,
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

        result = jobFactory(min_merge_size=20000, max_merge_size=200000,
                            max_merge_events=20000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % len(result)

        assert len(result[0].jobs) == 2, \
            "Error: Two jobs should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC"]
        goldenFilesB = ["fileI", "fileII", "fileIII", "fileIV"]

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], {"T1_US_FNAL", "T2_CH_CERN"})

            jobFiles = job.getFiles()

            if len(jobFiles) == len(goldenFilesA):
                self.assertEqual(job["estimatedDiskUsage"], 7)
                goldenFiles = goldenFilesA
            else:
                self.assertEqual(job["estimatedDiskUsage"], 4)
                goldenFiles = goldenFilesB

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for fileObj in jobFiles:
                fileObj.loadData()
                assert fileObj["lfn"] in goldenFiles, \
                    "Error: Unknown file: %s" % fileObj["lfn"]
                goldenFiles.remove(fileObj["lfn"])

                fileRun = list(fileObj["runs"])[0].run
                fileLumi = min(list(fileObj["runs"])[0])
                fileEvent = fileObj["first_event"]

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

    def testMinMergeSize2(self):
        """
        _testMinMergeSize2_

        Set the minimum merge size to be 7,167 bytes which is one byte less
        than the sum of all the file sizes in the largest merge group in the
        WMBS instance.  Verify that one merge job containing all the files in
        the largest merge group is produced.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=7167, max_merge_size=20000,
                            max_merge_events=20000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %d" % len(result)

        assert len(result[0].jobs) == 1, \
            "ERROR: One job should have been returned."

        self.assertEqual(result[0].jobs[0]["estimatedDiskUsage"], 7)

        self.assertEqual(result[0].jobs[0]["possiblePSN"], {"T1_US_FNAL", "T2_CH_CERN"})

        jobFiles = list(result[0].jobs)[0].getFiles()

        goldenFiles = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                       "fileC"]

        assert len(jobFiles) == len(goldenFiles), \
            "ERROR: Merge job should contain %d files." % len(goldenFiles)

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for fileObj in jobFiles:
            assert fileObj["lfn"] in goldenFiles, \
                "Error: Unknown file: %s" % fileObj["lfn"]
            goldenFiles.remove(fileObj["lfn"])

            fileRun = list(fileObj["runs"])[0].run
            fileLumi = min(list(fileObj["runs"])[0])
            fileEvent = fileObj["first_event"]

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

    def testMaxMergeSize1(self):
        """
        _testMaxMergeSize1_

        Set the maximum merge size to be two bytes.  Verify that three merge
        jobs are created, one for each job group that exists inside the WMBS
        instance.  Verify that each merge job contains the expected files.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=1, max_merge_size=2,
                            max_merge_events=20000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 3, \
            "ERROR: Three jobs should have been returned."

        self.assertEqual(result[0].jobs[0]["possiblePSN"], {"T1_US_FNAL", "T2_CH_CERN"})

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        for job in result[0].jobs:
            jobFiles = job.getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 4)
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                self.assertEqual(job["estimatedDiskUsage"], 3)
                goldenFiles = goldenFilesB
            else:
                self.assertEqual(job["estimatedDiskUsage"], 4)
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for fileObj in jobFiles:
                assert fileObj["lfn"] in goldenFiles, \
                    "Error: Unknown file in merge jobs."
                goldenFiles.remove(fileObj["lfn"])

                fileRun = list(fileObj["runs"])[0].run
                fileLumi = min(list(fileObj["runs"])[0])
                fileEvent = fileObj["first_event"]

                if currentRun == 0:
                    continue

                assert fileRun >= currentRun, \
                    "ERROR: Files not sorted by run."

                if fileRun == currentRun:
                    assert fileLumi >= currentLumi, \
                        "ERROR: Files not ordered by lumi"

                    if fileLumi == currentLumi:
                        assert fileEvent >= currentEvent, \
                            "ERROR: Files not ordered by first event"

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0 and \
               len(goldenFilesC) == 0, \
            "ERROR: Files missing from merge jobs."

        return

    def testMaxMergeSize2(self):
        """
        _testMaxMergeSize2_

        Set the minimum merge size to be one byte larger than the largest job
        group in the WMBS instance and the max merge size to be one byte larger
        than the total size of two of the groups.  Verify that one merge job
        is produced with two of the job groups in it.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=4097, max_merge_size=7169,
                            max_merge_events=20000)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 1, \
            "ERROR: One job should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        self.assertEqual(result[0].jobs[0]["estimatedDiskUsage"], 7)

        self.assertEqual(result[0].jobs[0]["possiblePSN"], {"T1_US_FNAL", "T2_CH_CERN"})

        jobFiles = list(result[0].jobs)[0].getFiles()

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for fileObj in jobFiles:

            if fileObj["lfn"] in goldenFilesA:
                goldenFilesA.remove(fileObj["lfn"])
            elif fileObj["lfn"] in goldenFilesB:
                goldenFilesB.remove(fileObj["lfn"])
            elif fileObj["lfn"] in goldenFilesC:
                goldenFilesC.remove(fileObj["lfn"])

            fileRun = list(fileObj["runs"])[0].run
            fileLumi = min(list(fileObj["runs"])[0])
            fileEvent = fileObj["first_event"]

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

        assert len(goldenFilesB) == 0 and \
               (len(goldenFilesA) == 0 or len(goldenFilesC) == 0), \
            "ERROR: Files not allocated to jobs correctly."

        return

    def testMaxEvents1(self):
        """
        _testMaxEvents1_

        Set the maximum number of events per merge job to 1.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=1, max_merge_size=20000,
                            max_merge_events=1)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 3, \
            "ERROR: Three jobs should have been returned: %s" % len(result[0].jobs)

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        for job in result[0].jobs:

            self.assertEqual(job["possiblePSN"], {"T1_US_FNAL", "T2_CH_CERN"})

            jobFiles = job.getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                self.assertEqual(job["estimatedDiskUsage"], 4)
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                self.assertEqual(job["estimatedDiskUsage"], 3)
                goldenFiles = goldenFilesB
            else:
                self.assertEqual(job["estimatedDiskUsage"], 4)
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for fileObj in jobFiles:
                assert fileObj["lfn"] in goldenFiles, \
                    "Error: Unknown file in merge jobs."
                goldenFiles.remove(fileObj["lfn"])

                fileRun = list(fileObj["runs"])[0].run
                fileLumi = min(list(fileObj["runs"])[0])
                fileEvent = fileObj["first_event"]

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
               len(goldenFilesC) == 0, \
            "ERROR: Files missing from merge jobs."

        return

    def testMaxEvents2(self):
        """
        _testMaxEvents2_

        Set the minimum merge size to be one byte larger than the largest job
        group in the WMBS instance and the max events to be one event larger
        than the total events in two of the groups.  Verify that one merge job
        is produced with two of the job groups in it.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=4097, max_merge_size=20000,
                            max_merge_events=7169)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 1, \
            "ERROR: One job should have been returned."

        self.assertEqual(result[0].jobs[0]["estimatedDiskUsage"], 7)

        self.assertEqual(result[0].jobs[0]["possiblePSN"], {"T1_US_FNAL", "T2_CH_CERN"})

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        jobFiles = list(result[0].jobs)[0].getFiles()

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for fileObj in jobFiles:

            if fileObj["lfn"] in goldenFilesA:
                goldenFilesA.remove(fileObj["lfn"])
            elif fileObj["lfn"] in goldenFilesB:
                goldenFilesB.remove(fileObj["lfn"])
            elif fileObj["lfn"] in goldenFilesC:
                goldenFilesC.remove(fileObj["lfn"])

            fileRun = list(fileObj["runs"])[0].run
            fileLumi = min(list(fileObj["runs"])[0])
            fileEvent = fileObj["first_event"]

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

        assert len(goldenFilesB) == 0 and \
               (len(goldenFilesA) == 0 or len(goldenFilesC) == 0), \
            "ERROR: Files not allocated to jobs correctly."

        return

    def testParallelProcessing(self):
        """
        _testParallelProcessing_

        Verify that merging works correctly when multiple processing
        subscriptions are run over the same input files.  The merging algorithm
        should ignore processing jobs that feed into different merge
        subscriptions.
        """
        locationAction = self.daoFactory(classname="Locations.New")
        locationAction.execute(siteName="T2_CH_CERN", pnn="T2_CH_CERN")
        locationAction.execute(siteName="T1_US_FNAL", pnn="T2_CH_CERN")

        mergeFilesetA = Fileset(name="mergeFilesetA")
        mergeFilesetB = Fileset(name="mergeFilesetB")
        mergeFilesetA.create()
        mergeFilesetB.create()

        mergeMergedFilesetA = Fileset(name="mergeMergedFilesetA")
        mergeMergedFilesetB = Fileset(name="mergeMergedFilesetB")
        mergeMergedFilesetA.create()
        mergeMergedFilesetB.create()

        mergeWorkflow = Workflow(name="mergeWorkflow", spec="bogus",
                                 owner="Steve", task="Test")
        mergeWorkflow.create()

        mergeSubscriptionA = Subscription(fileset=mergeFilesetA,
                                          workflow=mergeWorkflow,
                                          split_algo="WMBSMergeBySize")
        mergeSubscriptionB = Subscription(fileset=mergeFilesetB,
                                          workflow=mergeWorkflow,
                                          split_algo="WMBSMergeBySize")
        mergeSubscriptionA.create()
        mergeSubscriptionB.create()

        inputFileset = Fileset(name="inputFileset")
        inputFileset.create()

        inputFileA = File(lfn="inputLFNA")
        inputFileB = File(lfn="inputLFNB")
        inputFileA.create()
        inputFileB.create()

        procWorkflowA = Workflow(name="procWorkflowA", spec="bunk2",
                                 owner="Steve", task="Test")
        procWorkflowA.create()
        procWorkflowA.addOutput("output", mergeFilesetA, mergeMergedFilesetA)
        procWorkflowB = Workflow(name="procWorkflowB", spec="bunk3",
                                 owner="Steve", task="Test2")
        procWorkflowB.create()
        procWorkflowB.addOutput("output", mergeFilesetB, mergeMergedFilesetB)

        procSubscriptionA = Subscription(fileset=inputFileset,
                                         workflow=procWorkflowA,
                                         split_algo="EventBased")
        procSubscriptionA.create()
        procSubscriptionB = Subscription(fileset=inputFileset,
                                         workflow=procWorkflowB,
                                         split_algo="EventBased")
        procSubscriptionB.create()

        jobGroupA = JobGroup(subscription=procSubscriptionA)
        jobGroupA.create()
        jobGroupB = JobGroup(subscription=procSubscriptionB)
        jobGroupB.create()

        changeStateDAO = self.daoFactory(classname="Jobs.ChangeState")

        testJobA = Job()
        testJobA.addFile(inputFileA)
        testJobA.create(jobGroupA)
        testJobA["state"] = "cleanout"
        testJobA["oldstate"] = "new"
        testJobA["couch_record"] = "somejive"
        testJobA["retry_count"] = 0
        testJobA["outcome"] = "success"
        testJobA.save()

        testJobB = Job()
        testJobB.addFile(inputFileB)
        testJobB.create(jobGroupA)
        testJobB["state"] = "cleanout"
        testJobB["oldstate"] = "new"
        testJobB["couch_record"] = "somejive"
        testJobB["retry_count"] = 0
        testJobB["outcome"] = "success"
        testJobB.save()

        testJobC = Job()
        testJobC.addFile(inputFileA)
        testJobC.create(jobGroupB)
        testJobC["state"] = "cleanout"
        testJobC["oldstate"] = "new"
        testJobC["couch_record"] = "somejive"
        testJobC["retry_count"] = 0
        testJobC["outcome"] = "success"
        testJobC.save()

        testJobD = Job()
        testJobD.addFile(inputFileA)
        testJobD.create(jobGroupB)
        testJobD["state"] = "cleanout"
        testJobD["oldstate"] = "new"
        testJobD["couch_record"] = "somejive"
        testJobD["retry_count"] = 0
        testJobD["outcome"] = "failure"
        testJobD.save()

        testJobE = Job()
        testJobE.addFile(inputFileB)
        testJobE.create(jobGroupB)
        testJobE["state"] = "cleanout"
        testJobE["oldstate"] = "new"
        testJobE["couch_record"] = "somejive"
        testJobE["retry_count"] = 0
        testJobE["outcome"] = "success"
        testJobE.save()

        testJobF = Job()
        testJobF.addFile(inputFileB)
        testJobF.create(jobGroupB)
        testJobF["state"] = "cleanout"
        testJobF["oldstate"] = "new"
        testJobF["couch_record"] = "somejive"
        testJobF["retry_count"] = 0
        testJobF["outcome"] = "failure"
        testJobF.save()

        changeStateDAO.execute([testJobA, testJobB, testJobC, testJobD,
                                testJobE, testJobF])

        fileA = File(lfn="fileA", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        fileA.addRun(Run(1, *[45]))
        fileA.create()
        fileA.addParent(inputFileA["lfn"])
        fileB = File(lfn="fileB", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        fileB.addRun(Run(1, *[45]))
        fileB.create()
        fileB.addParent(inputFileB["lfn"])

        jobGroupA.output.addFile(fileA)
        jobGroupA.output.addFile(fileB)
        jobGroupA.output.commit()

        mergeFilesetA.addFile(fileA)
        mergeFilesetA.addFile(fileB)
        mergeFilesetA.commit()

        fileC = File(lfn="fileC", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        fileC.addRun(Run(1, *[45]))
        fileC.create()
        fileC.addParent(inputFileA["lfn"])
        fileD = File(lfn="fileD", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        fileD.addRun(Run(1, *[45]))
        fileD.create()
        fileD.addParent(inputFileB["lfn"])

        jobGroupB.output.addFile(fileC)
        jobGroupB.output.addFile(fileD)

        mergeFilesetB.addFile(fileC)
        mergeFilesetB.addFile(fileD)
        mergeFilesetB.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=mergeSubscriptionB)

        result = jobFactory(min_merge_size=1, max_merge_size=20000,
                            max_merge_events=7169)

        assert len(result) == 0, \
            "Error: No merge jobs should have been created."

        fileE = File(lfn="fileE", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        fileE.addRun(Run(1, *[45]))
        fileE.create()
        fileE.addParent(inputFileA["lfn"])
        fileF = File(lfn="fileF", size=1024, events=1024, first_event=0,
                     locations={"T2_CH_CERN"})
        fileF.addRun(Run(1, *[45]))
        fileF.create()
        fileF.addParent(inputFileB["lfn"])

        jobGroupB.output.addFile(fileE)
        jobGroupB.output.addFile(fileF)

        mergeFilesetB.addFile(fileE)
        mergeFilesetB.addFile(fileF)
        mergeFilesetB.commit()

        testJobD["outcome"] = "success"
        testJobD.save()
        testJobF["outcome"] = "success"
        testJobF.save()

        changeStateDAO.execute([testJobD, testJobF])

        result = jobFactory(min_merge_size=1, max_merge_size=20000,
                            max_merge_events=7169)

        assert len(result) == 1, \
            "Error: One merge job should have been created: %s" % len(result)

        return

    def testLocationMerging(self):
        """
        _testLocationMerging_

        Verify that files residing on different SEs are not merged together in
        the same job.
        """
        self.stuffWMBS()

        locationAction = self.daoFactory(classname="Locations.New")
        locationAction.execute(siteName="T1_UK_RAL", pnn="T1_UK_RAL_Disk")

        fileSite2 = File(lfn="fileSite2", size=4098, events=1024,
                         first_event=0, locations={"T1_UK_RAL_Disk"})
        fileSite2.addRun(Run(1, *[46]))
        fileSite2.create()
        fileSite2.addParent(self.parentFileSite2["lfn"])

        self.mergeFileset.addFile(fileSite2)
        self.mergeFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=4097, max_merge_size=99999999,
                            max_merge_events=999999999)

        assert len(result) == 1, \
            "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 2, \
            "ERROR: Two jobs should have been returned."

        ralJobs = 0
        fnalcernJobs = 0
        for job in result[0].jobs:
            if job["possiblePSN"] == {"T1_UK_RAL"}:
                ralJobs += 1
            elif job["possiblePSN"] == {"T1_US_FNAL", "T2_CH_CERN"}:
                fnalcernJobs += 1

        self.assertEqual(ralJobs, 1)
        self.assertEqual(fnalcernJobs, 1)

        return

    def testFilesetCloseout(self):
        """
        _testFilesetCloseout_

        Verify that the merge algorithm works correctly when it's input fileset
        is closed.  The split algorithm should create merge jobs for all files
        regardless of size and then mark any orphaned files (files that are the
        result of a split by lumi / split by event where one of the parent
        processing jobs has failed while others have succeeded) as failed so
        that the fileset closing works.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        # Get out all the good merge jobs out of the way.
        result = jobFactory(min_merge_size=1, max_merge_size=999999999999,
                            max_merge_events=999999999)

        # Verify that the bad files are the only "available" files
        availableAction = self.daoFactory(classname="Subscriptions.GetAvailableFilesMeta")
        availFiles = availableAction.execute(self.mergeSubscription["id"])

        assert len(availFiles) == 4, \
            "Error: Wrong number of available files."

        goldenFiles = ["badFile1", "badFileA", "badFileB", "badFileC"]
        for availFile in availFiles:
            assert availFile["lfn"] in goldenFiles, \
                "Error: Extra file is available."

            goldenFiles.remove(availFile["lfn"])

        self.mergeFileset.markOpen(False)
        result = jobFactory(min_merge_size=1, max_merge_size=999999999999,
                            max_merge_events=999999999)

        assert len(result) == 0, \
            "Error: Merging should have returned zero jobs."

        self.mergeFileset.markOpen(False)

        availFiles2 = availableAction.execute(self.mergeSubscription["id"])

        assert len(availFiles2) == 0, \
            "Error: There should be no more available files."

        failedAction = self.daoFactory(classname="Subscriptions.GetFailedFiles")
        failedFiles = failedAction.execute(self.mergeSubscription["id"])

        assert len(failedFiles) == 4, \
            "Error: Wrong number of failed files: %s" % failedFiles

        goldenIDs = []
        for availFile in availFiles:
            goldenIDs.append(availFile["id"])

        for failedFile in failedFiles:
            assert failedFile["file"] in goldenIDs, \
                "Error: Extra failed file."

        return

    def testFilesetCloseout2(self):
        """
        _testFilesetCloseout2_

        Verify that the fail orphan file code does not fail files that have
        failed for other workflows.
        """
        self.stuffWMBS()
        self.mergeFileset.markOpen(False)

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        # Get out all the good merge jobs out of the way.
        result = jobFactory(min_merge_size=1, max_merge_size=999999999999,
                            max_merge_events=999999999)

        self.assertEqual(len(result), 1, "Error: Wrong number of job groups.")
        self.assertEqual(len(result[0].jobs), 2, "Error: Wrong number of jobs.")

        failedAction = self.daoFactory(classname="Subscriptions.GetFailedFiles")
        failedFiles = failedAction.execute(self.mergeSubscription["id"])

        self.assertEqual(len(failedFiles), 4,
                         "Error: Wrong number of failed files: %s" % failedFiles)
        return

    def testForcedMerge(self):
        """
        _testForcedMerge_

        Repeat testMinMergeSize1a, but with non-injected files to assert that
        this causes no jobgroups to be created.
        """
        self.stuffWMBS(injected=False)
        self.mergeFileset.markOpen(False)

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.mergeSubscription)

        result = jobFactory(min_merge_size=20000, max_merge_size=200000,
                            max_merge_events=20000)

        self.assertEqual(len(result), 0)

        return


if __name__ == '__main__':
    unittest.main()
