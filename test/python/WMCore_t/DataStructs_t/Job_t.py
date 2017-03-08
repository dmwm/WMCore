#!/usr/bin/env python
"""
_Job_t_

Testcase for the Job class.
"""

from builtins import range
import unittest, logging, random, time

from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Run import Run

from unittest import TestCase

class JobTest(unittest.TestCase):
    """
    _JobTest_

    Testcase for the Job class

    Instantiate a dummy Job object with a dummy Subscription
    and a dummy Fileset full of random files as input
    """

    def setUp(self):
        """
        _setUp_

        Initial Setup for the Job Testcase
        """
        self.inputFiles = []

        for i in range(1,1000):
            lfn = "/store/data/%s/%s/file.root" % (random.randint(1000, 9999),
                                                   random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn = lfn, size = size, events = events, checksums = {"cksum": "1"})
            file.addRun(Run(run, *[lumi]))
            self.inputFiles.append(file)

        self.dummyJob = Job(files = self.inputFiles)
        return

    def tearDown(self):
        """
        No tearDown method for this Testcase
        """
        pass

    def testGetFilesList(self):
        """
        _testGetFilesList_

        Verify that the Job::getFiles(type = "list") method returns the same
        files in the same order that they were passed in.
        """
        assert self.dummyJob.getFiles() == self.inputFiles, \
            "ERROR: Initial fileset does not match Job fileset"

        return

    def testGetFilesSet(self):
        """
        _testGetFilesSet_

        Verify that the Job::getFiles(type = "set") method returns the correct
        input files in the form of a set.
        """
        assert self.dummyJob.getFiles(type = "set") == set(self.inputFiles), \
            "ERROR: getFiles(type = 'set') does not work correctly."

        return

    def testGetFilesLFN(self):
        """
        _testGetFilesLFN_

        Verify that the Job::getFiles(type = "lfn") method returns the same
        files in the same order that they were passed in.
        """
        jobLFNs = self.dummyJob.getFiles(type = "lfn")

        goldenLFNs = []
        for file in self.inputFiles:
            goldenLFNs.append(file["lfn"])

        assert len(goldenLFNs) == len(jobLFNs), \
            "ERROR: Job has different number of files than input"

        for jobLFN in jobLFNs:
            assert jobLFN in goldenLFNs, \
                "ERROR: LFN missing from job."

        return

    def testAddFile(self):
        """
        _testAddFile_

        Verify that the Job::addFile() method works properly.
        """
        dummyFileAddFile = File("/tmp/dummyFileAddFileTest", 1234, 1, 2)
        self.dummyJob.addFile(dummyFileAddFile)

        assert dummyFileAddFile in self.dummyJob.getFiles(), \
            "ERROR: Couldn't add file to Job - addFile method error"

        return

    def testChangeState(self):
        """
        _testChangeState_

        Verify that the Job::changeState() method updates the state and the
        state time.
        """
        currentTime = time.time()
        self.dummyJob.changeState("created")

        assert self.dummyJob["state_time"] > currentTime - 1 and \
            self.dummyJob["state_time"] < currentTime + 1, \
            "ERROR: State time not updated on state change"

        assert self.dummyJob["state"] == "created", \
            "ERROR: Couldn't change Job state - changeState method error"

    def testChangeOutcome(self):
        """
        _testChangeOutcome_

        Verify that the Job::changeOutcome() method changes the final outcome
        of the job.
        """
        self.dummyJob.changeOutcome("success")

        assert self.dummyJob["outcome"] == "success", \
            "ERROR: Job outcome failed to update."

        return

    def testGetBaggage(self):
        """
        test that setting/accessing the Job Baggage ConfigSection works
        """
        self.dummyJob.addBaggageParameter("baggageContents", True)
        self.dummyJob.addBaggageParameter("trustPUSitelists", False)
        self.dummyJob.addBaggageParameter("skipPileupEvents", 20000)

        baggage = self.dummyJob.getBaggage()
        self.assertTrue(getattr(baggage, "baggageContents"))
        self.assertFalse(getattr(baggage, "trustPUSitelists"))
        self.assertEqual(getattr(baggage, "skipPileupEvents"), 20000)
        self.assertFalse(hasattr(baggage, "IDontExist"))


if __name__ == "__main__":
    unittest.main()
