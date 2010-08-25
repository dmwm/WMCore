#!/usr/bin/env python
"""
_JobPackage_

Unittests for JobPackage persistency mechanism

"""


import os
import unittest

from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.DataStructs.Job import Job

class JobPackageTest(unittest.TestCase):
    """
    TestCase for JobPackage object

    """
    def setUp(self):
        """set up"""
        self.persistFile = "/tmp/JobPackage_t.pkl"



    def tearDown(self):
        """cleanup"""
        if os.path.exists(self.persistFile):
            os.remove(self.persistFile)



    def testA(self):
        """instantiation"""

        try:
            package = JobPackage()
        except Exception, ex:
            msg = "Failed to instantiate JobPackage object:\n"
            msg += str(ex)
            self.fail(msg)


    def testB(self):
        """population"""

        package = JobPackage()
        numJobs = 100

        jobs = [ Job("Job%s" % i) for i in range(0, numJobs)]

        try:
            package.extend(jobs)
        except Exception, ex:
            msg = "Error adding list of Jobs to JobPackage:\n"
            msg += str(ex)
            self.fail(msg)


        self.assertEqual(len(jobs), len(package))
        # order should be preserved
        for i, j in zip(jobs, package):
            self.assertEqual(i['name'], j['name'])

    def testC(self):
        """save/load"""

        package = JobPackage()
        package.extend([ Job("Job%s" % i) for i in range(0, 100)])

        try:
            package.save(self.persistFile)
        except Exception, ex:
            msg = "Failed to save JobPackage:\n"
            msg += str(ex)
            self.fail(msg)

        self.failUnless(os.path.exists(self.persistFile))

        newPackage = JobPackage()
        try:
            newPackage.load(self.persistFile)
        except Exception, ex:
            msg = "Failed to load JobPackage:\n"
            msg += str(ex)
            self.fail(msg)

        self.assertEqual(len(newPackage), len(package))

        # order should be preserved
        for i, j in zip(newPackage, package):
            self.assertEqual(i['name'], j['name'])



    def testD(self):
        """save/load with baggage"""
        package = JobPackage()
        jobs = [ Job("Job%s" % i) for i in range(0, 100)]
        for job in jobs:
            baggage = job.getBaggage()
            setattr(baggage, "thisJob", job['name'])
            setattr(baggage, "seed1", 11111111)
            setattr(baggage, "seed2", 22222222)
            setattr(baggage, "seed3", 33333333)
            setattr(baggage, "seed4", 44444444)
            setattr(baggage, "seed5", 55555555)


        package.extend(jobs)




        try:
            package.save(self.persistFile)
        except Exception, ex:
            msg = "Failed to save JobPackage:\n"
            msg += str(ex)
            self.fail(msg)

        self.failUnless(os.path.exists(self.persistFile))



        newPackage = JobPackage()
        try:
            newPackage.load(self.persistFile)
        except Exception, ex:
            msg = "Failed to load JobPackage:\n"
            msg += str(ex)
            self.fail(msg)


        for i, j in zip(newPackage, package):
            self.assertEqual(i.baggage.thisJob, j.baggage.thisJob)






if __name__ == '__main__':
    unittest.main()


