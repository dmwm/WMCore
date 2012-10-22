#!/usr/bin/env python
"""
_JobGroup_t_

Testcase for the JobGroup class.
"""

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.JobGroup import JobGroup
from WMCore.DataStructs.Subscription import Subscription

from WMQuality.TestInit import TestInit

import unittest

class JobGroupTest(unittest.TestCase):
    def setUp(self):
        pass

    def testCreate(self):
        """
        _testCreate_

        Test the JobGroup constructor and passing different job containers
        into it.
        """
        testSubscription = Subscription()
        testJobGroupA = JobGroup(subscription = testSubscription)

        assert testJobGroupA.subscription == testSubscription, \
            "ERROR: Failed to pass subscription in constructor"
        assert len(testJobGroupA.jobs) == 0 and len(testJobGroupA.newjobs) == 0, \
            "ERROR: JobGroup not empty on creation"

        testJobA = Job()
        testJobB = Job()

        testJobGroupB = JobGroup(jobs = [testJobA, testJobB])

        assert testJobGroupB.jobs == [], \
            "ERROR: Jobs committed to jobgroup too soon."

        jobGroupJobs = testJobGroupB.newjobs
        goldenJobs = [testJobA, testJobB]
        for job in jobGroupJobs:
            assert job in goldenJobs, \
                "ERROR: Extra job in job group"

            goldenJobs.remove(job)

        assert len(goldenJobs) == 0, \
            "ERROR: Job missing from job group"

        testJobGroupC = JobGroup(jobs = testJobA)

        assert testJobGroupC.jobs == [], \
            "ERROR: Jobs committed to jobgroup too soon."

        jobGroupJobs = testJobGroupC.newjobs

        assert len(jobGroupJobs) == 1, \
            "ERROR: Wrong number of jobs in jobgroup."
        assert testJobA in jobGroupJobs, \
            "ERROR: Wrong job in jobgroup."

        return

    def testAddCommit(self):
        """
        _testAddCommit_

        Test the add() and commit() methods of the JobGroup class.  Verify that
        jobs are not returned from getJobs() until commit() has been called.
        """
        testJob = Job()
        testJobGroup = JobGroup()

        assert len(testJobGroup.getJobs()) == 0, \
            "ERROR: JobGroup has jobs before jobs have been added."

        testJobGroup.add(testJob)

        assert len(testJobGroup.getJobs()) == 0, \
            "ERROR: JobGroup has jobs commit() was called."

        testJobGroup.commit()

        assert len(testJobGroup.getJobs()) == 1, \
            "ERROR: JobGroup has wrong number of jobs."
        assert testJob in testJobGroup.getJobs(), \
            "ERROR: JobGroup has unknown jobs."

        return

    def testAddOutput(self):
        """
        _testAddOutput_

        Test the JobGroup's addOutput() method.  Verify that files are committed
        to the output fileset immediately and are available from the getOutput()
        method.
        """
        testFile = File()
        testJobGroup = JobGroup()

        assert len(testJobGroup.getOutput()) == 0, \
            "ERROR: Files in the output fileset before anything has been added."

        testJobGroup.addOutput(testFile)

        assert len(testJobGroup.getOutput()) == 1, \
            "ERROR: Unknown number of files in JobGroup output fileset."
        assert testFile in testJobGroup.getOutput(), \
            "ERROR: Unknown file in the JobGroup output fileset."

        return

    def testGetJobs(self):
        """
        _testGetJobs_

        Verify that the getJobs() method of the JobGroup class returns the
        correct output for each output container type it supports.
        """
        testJobA = Job()
        testJobB = Job()
        testJobGroup = JobGroup(jobs = [testJobA, testJobB])
        testJobGroup.commit()

        assert len(testJobGroup.getJobs()) == 2,  \
            "ERROR: Wrong number of jobs in job group"

        goldenJobs = [testJobA, testJobB]
        for job in testJobGroup.getJobs():
            assert job in goldenJobs, \
                "ERROR: Unknown Job in JobGroup."

            goldenJobs.remove(job)

        assert len(goldenJobs) == 0, \
            "ERROR: Jobs are missing from the JobGroup."

        goldenIDs = []
        goldenIDs.append(testJobA["id"])
        goldenIDs.append(testJobB["id"])
        for jobID in testJobGroup.getJobs(type = "id"):
            assert jobID in goldenIDs, \
                "ERROR: Unknown JobID in JobGroup"

            goldenIDs.remove(jobID)

        assert len(goldenIDs) == 0, \
            "ERROR: Job IDs are missing from the JobGroup."

        return

    def testLen(self):
        """
        __testLen__

        Test that the __len__ function will actualy return the correct length.

        """

        #This is simple.  It should just have a length equal to the number of committed
        #And yet to be committed jobs

        testJobA = Job()
        testJobB = Job()
        testJobC = Job()
        testJobD = Job()
        testJobGroup = JobGroup(jobs = [testJobA, testJobB])
        testJobGroup.commit()

        self.assertEqual(len(testJobGroup), 2)

        testJobGroup.add(testJobC)

        self.assertEqual(len(testJobGroup), 3)

        testJobGroup.commit()
        testJobGroup.add(testJobD)

        self.assertEqual(len(testJobGroup), 4)

        return


if __name__ == '__main__':
    unittest.main()
