#!/usr/bin/env python
"""
_JobPackage_

Unittests for JobPackage persistency mechanism
"""

from builtins import range
import os
import unittest

from WMQuality.TestInit import TestInit

from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.DataStructs.Job import Job

class JobPackageTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Create a temporary file to presist the JobPackage to.
        """
        self.testInit = TestInit(__file__)
        self.persistFile = os.path.join(self.testInit.generateWorkDir(),
                                        "JobPackage.pkl")
        return

    def tearDown(self):
        pass

    def testAddingJobs(self):
        """
        _testAddingJobs_

        Verify that adding jobs to the package works as expected.
        """
        package = JobPackage()

        for i in range(100):
            newJob = Job("Job%s" % i)
            newJob["id"] = i
            package[i] = newJob

        # There is an extra key for the directory the package is stored in.
        assert len(package) == 101, \
               "Error: Wrong number of jobs in package."

        for i in range(100):
            job = package[i]
            assert job["id"] == i, \
                   "Error: Jobs has wrong ID."
            assert job["name"] == "Job%d" % i, \
                   "Error: Job has wrong name."

        return

    def testPersist(self):
        """
        _testPersist_

        Verify that we're able to save and load the job package.
        """
        package = JobPackage()

        for i in range(100):
            newJob = Job("Job%s" % i)
            newJob["id"] = i
            package[i] = newJob

        package.save(self.persistFile)

        assert os.path.exists(self.persistFile), \
               "Error: Package file was never created."

        newPackage = JobPackage()
        newPackage.load(self.persistFile)

        # There is an extra key for the directory the package is stored in.
        assert len(newPackage) == 101, \
               "Error: Wrong number of jobs in package."

        for i in range(100):
            job = newPackage[i]
            assert job["id"] == i, \
                   "Error: Jobs has wrong ID."
            assert job["name"] == "Job%d" % i, \
                   "Error: Job has wrong name."

        return

    def testBaggage(self):
        """
        _testBaggage_

        Verify that job baggage is persisted with the package.
        """
        package = JobPackage()

        for i in range(100):
            newJob = Job("Job%s" % i)
            newJob["id"] = i
            baggage = newJob.getBaggage()
            setattr(baggage, "thisJob", newJob["name"])
            setattr(baggage, "seed1", 11111111)
            setattr(baggage, "seed2", 22222222)
            setattr(baggage, "seed3", 33333333)
            setattr(baggage, "seed4", 44444444)
            setattr(baggage, "seed5", 55555555)
            package[i] = newJob

        package.save(self.persistFile)

        assert os.path.exists(self.persistFile), \
               "Error: Package file was never created."

        newPackage = JobPackage()
        newPackage.load(self.persistFile)

        # There is an extra key for the directory the package is stored in.
        assert len(newPackage) == 101, \
               "Error: Wrong number of jobs in package."

        for i in range(100):
            job = newPackage[i]
            assert job["id"] == i, \
                   "Error: Jobs has wrong ID."
            assert job["name"] == "Job%d" % i, \
                   "Error: Job has wrong name."
            jobBaggage = job.getBaggage()

            assert jobBaggage.thisJob == "Job%d" % i, \
                   "Error: Job baggage has wrong name."
            assert jobBaggage.seed1 == 11111111, \
                   "Error: Job baggee has wrong value for seed1."
            assert jobBaggage.seed2 == 22222222, \
                   "Error: Job baggee has wrong value for seed2."
            assert jobBaggage.seed3 == 33333333, \
                   "Error: Job baggee has wrong value for seed3."
            assert jobBaggage.seed4 == 44444444, \
                   "Error: Job baggee has wrong value for seed4."
            assert jobBaggage.seed5 == 55555555, \
                   "Error: Job baggee has wrong value for seed5."


        return

if __name__ == '__main__':
    unittest.main()
