#!/usr/bin/env python
"""
_SplitFileBased_t_

Unit tests for the split file job splitting algorithm.
"""

import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DataStructs.Run import Run

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.TestInit import TestInit

class SplitFileBasedTest(unittest.TestCase):
    """
    _SplitFileBasedTest_

    Unit tests for the split file job splitting algorithm.
    """

    def setUp(self):
        """
        _setUp_

        Create database connection and load up the WMBS schema.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

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
        locationAction = self.daoFactory(classname = "Locations.New")
        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")

        locationAction.execute("site1", pnn = "T2_CH_CERN")

        self.mergeFileset = Fileset(name = "mergeFileset")
        self.mergeFileset.create()
        self.bogusFileset = Fileset(name = "bogusFileset")
        self.bogusFileset.create()

        self.mergeMergedFileset = Fileset(name = "mergeMergedFileset")
        self.mergeMergedFileset.create()
        self.bogusMergedFileset = Fileset(name = "bogusMergedFileset")
        self.bogusMergedFileset.create()

        mergeWorkflow = Workflow(name = "mergeWorkflow", spec = "bunk2",
                                 owner = "Steve", task="Test")
        mergeWorkflow.create()

        self.mergeSubscription = Subscription(fileset = self.mergeFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "SplitFileBased")
        self.mergeSubscription.create()
        self.bogusSubscription = Subscription(fileset = self.bogusFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "SplitFileBased")

        inputFileset = Fileset(name = "inputFileset")
        inputFileset.create()

        inputWorkflow = Workflow(name = "inputWorkflow", spec = "input",
                                owner = "Steve", task = "Test")
        inputWorkflow.create()
        inputWorkflow.addOutput("someOutput", self.mergeFileset,
                                self.mergeMergedFileset)
        inputWorkflow.addOutput("someOutput2", self.bogusFileset,
                                self.bogusMergedFileset)

        inputSubscription = Subscription(fileset = inputFileset,
                                        workflow = inputWorkflow)
        inputSubscription.create()

        parentFile1 = File(lfn = "parentFile1")
        parentFile1.create()
        parentFile2 = File(lfn = "parentFile2")
        parentFile2.create()
        parentFile3 = File(lfn = "parentFile3")
        parentFile3.create()
        parentFile4 = File(lfn = "parentFile4")
        parentFile4.create()

        jobGroup1 = JobGroup(subscription = inputSubscription)
        jobGroup1.create()
        jobGroup2 = JobGroup(subscription = inputSubscription)
        jobGroup2.create()

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
        parentFile5 = File(lfn = "parentFile5")
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

        badFile1 = File(lfn = "badFile1", size = 10241024, events = 10241024, first_event = 0, locations = set(["T2_CH_CERN"]))
        badFile1.addRun(Run(1, *[45]))
        badFile1.create()
        badFile1.addParent(parentFile5["lfn"])

        file1 = File(lfn = "file1", size = 1024, events = 1024, first_event = 0, locations = set(["T2_CH_CERN"]))
        file1.addRun(Run(1, *[45]))
        file1.create()
        file1.addParent(parentFile1["lfn"])
        file2 = File(lfn = "file2", size = 1024, events = 1024, first_event = 1024, locations = set(["T2_CH_CERN"]))
        file2.addRun(Run(1, *[45]))
        file2.create()
        file2.addParent(parentFile1["lfn"])
        file3 = File(lfn = "file3", size = 1024, events = 1024, first_event = 2048, locations = set(["T2_CH_CERN"]))
        file3.addRun(Run(1, *[45]))
        file3.create()
        file3.addParent(parentFile1["lfn"])
        file4 = File(lfn = "file4", size = 1024, events = 1024, first_event = 3072, locations = set(["T2_CH_CERN"]))
        file4.addRun(Run(1, *[45]))
        file4.create()
        file4.addParent(parentFile1["lfn"])

        fileA = File(lfn = "fileA", size = 1024, events = 1024, first_event = 0, locations = set(["T2_CH_CERN"]))
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileA.addParent(parentFile2["lfn"])
        fileB = File(lfn = "fileB", size = 1024, events = 1024, first_event = 1024, locations = set(["T2_CH_CERN"]))
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileB.addParent(parentFile2["lfn"])
        fileC = File(lfn = "fileC", size = 1024, events = 1024, first_event = 2048, locations = set(["T2_CH_CERN"]))
        fileC.addRun(Run(1, *[46]))
        fileC.create()
        fileC.addParent(parentFile2["lfn"])

        fileI = File(lfn = "fileI", size = 1024, events = 1024, first_event = 0, locations = set(["T2_CH_CERN"]))
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileI.addParent(parentFile3["lfn"])
        fileII = File(lfn = "fileII", size = 1024, events = 1024, first_event = 1024, locations = set(["T2_CH_CERN"]))
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileII.addParent(parentFile3["lfn"])
        fileIII = File(lfn = "fileIII", size = 1024, events = 1024, first_event = 2048, locations = set(["T2_CH_CERN"]))
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIII.addParent(parentFile3["lfn"])
        fileIV = File(lfn = "fileIV", size = 1024, events = 1024, first_event = 3072, locations = set(["T2_CH_CERN"]))
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()
        fileIV.addParent(parentFile3["lfn"])

        fileX = File(lfn = "badFileA", size = 1024, events = 1024, first_event = 0, locations = set(["T2_CH_CERN"]))
        fileX.addRun(Run(1, *[47]))
        fileX.create()
        fileX.addParent(parentFile4["lfn"])
        fileY = File(lfn = "badFileB", size = 1024, events = 1024, first_event = 1024, locations = set(["T2_CH_CERN"]))
        fileY.addRun(Run(1, *[47]))
        fileY.create()
        fileY.addParent(parentFile4["lfn"])
        fileZ = File(lfn = "badFileC", size = 1024, events = 1024, first_event = 2048, locations = set(["T2_CH_CERN"]))
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

        for file in [file1, file2, file3, file4, fileA, fileB, fileC, fileI,
                     fileII, fileIII, fileIV, fileX, fileY, fileZ, badFile1]:
            self.mergeFileset.addFile(file)
            self.bogusFileset.addFile(file)

        self.mergeFileset.commit()
        self.bogusFileset.commit()

        return

    def testSplitAlgo(self):
        """
        _testSplitAlgo_

        Run the SplitFileBased splitting algorithm over the data created in
        the merge subscription.  This should produce three job groups each
        containing one job.  The files in the job should be ordered correctly.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory()

        assert len(result) == 3, \
               "ERROR: Wrong number of job groups returned: %s" % len(result)

        for jobGroup in result:
            assert len(jobGroup.jobs) == 1, \
                   "ERROR: One job should be in a job group."

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        for jobGroup in result:
            jobFiles = jobGroup.jobs.pop().getFiles()

            if jobFiles[0]["lfn"] in goldenFilesA:
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                goldenFiles = goldenFilesB
            else:
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for file in jobFiles:
                file.loadData()
                assert file["lfn"] in goldenFiles, \
                       "Error: Unknown file in merge jobs."
                assert len(file["locations"]) == 1, \
                       "Error: Wrong number of file locations."
                assert "T2_CH_CERN" in file["locations"], \
                       "Error: File is missing a location."

                goldenFiles.remove(file["lfn"])

            fileRun = list(file["runs"])[0].run
            fileLumi = min(list(file["runs"])[0])
            fileEvent = file["first_event"]

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
                    assert fileEvent > currentEvent, \
                           "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0 and \
               len(goldenFilesC) == 0, \
               "ERROR: Files missing from merge jobs."

        return

if __name__ == '__main__':
    unittest.main()
