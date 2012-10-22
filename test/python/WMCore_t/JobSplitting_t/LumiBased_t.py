#!/usr/bin/env python
"""
_LumiBased_t_

Lumi based splitting test.
"""

import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID

class LumiBasedTest(unittest.TestCase):
    """
    _LumiBasedTest_

    Test event based job splitting.
    """


    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """

        self.testWorkflow = Workflow()

        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass


    def createSubscription(self, nFiles, lumisPerFile, twoSites = False):
        """
        _createSubscription_

        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name = baseName)
        for i in range(nFiles):
            newFile = File(lfn = '%s_%i' % (baseName, i), size = 1000,
                           events = 100)
            lumis = []
            for lumi in range(lumisPerFile):
                lumis.append((i * 100) + lumi)
            newFile.addRun(Run(i, *lumis))
            newFile.setLocation('blenheim')
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = File(lfn = '%s_%i_2' % (baseName, i), size = 1000,
                               events = 100)
                lumis = []
                for lumi in range(lumisPerFile):
                    lumis.append((i * 100) + lumi)
                newFile.addRun(Run(i, *lumis))
                newFile.setLocation('malpaquet')
                testFileset.addFile(newFile)


        testSubscription  = Subscription(fileset = testFileset,
                                         workflow = self.testWorkflow,
                                         split_algo = "LumiBased",
                                         type = "Processing")

        return testSubscription

    def testA_FileSplitting(self):
        """
        _FileSplitting_

        Test that things work if we split files between jobs
        """
        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles = 10, lumisPerFile = 1)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = oneSetSubscription)


        jobGroups = jobFactory(lumis_per_job = 3,
                               halt_job_on_file_boundaries = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)




        twoLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 1,
                               halt_job_on_file_boundaries = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)



        wholeLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 3)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = wholeLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 2,
                               halt_job_on_file_boundaries = True)
        self.assertEqual(len(jobGroups), 1)
        # 10 because we split on run boundaries
        self.assertEqual(len(jobGroups[0].jobs), 10)
        jobList = jobGroups[0].jobs
        for job in jobList:
            # Have should have one file, half two
            self.assertTrue(len(job['input_files']) in [1,2])


        mask0 = jobList[0]['mask'].getRunAndLumis()
        self.assertEqual(mask0, {0L: [[0L, 1L]]})
        mask1 = jobList[1]['mask'].getRunAndLumis()
        self.assertEqual(mask1, {0L: [[2L, 2L]]})
        mask2 = jobList[2]['mask'].getRunAndLumis()
        self.assertEqual(mask2, {1L: [[100L, 101L]]})
        mask3 = jobList[3]['mask'].getRunAndLumis()
        self.assertEqual(mask3, {1L: [[102L, 102L]]})

        self.assertEqual(jobList[0]['mask'].getRunAndLumis(), {0L: [[0L, 1L]]})

        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 2, twoSites = True)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job = 1,
                               halt_job_on_file_boundaries = True)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)



    def testB_NoRunNoFileSplitting(self):
        """
        _NoRunNoFileSplitting_

        Test the splitting algorithm in the odder fringe
        cases that might be required.
        """
        splitter = SplitterFactory()
        testSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 5, twoSites = False)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = testSubscription)

        jobGroups = jobFactory(lumis_per_job = 3,
                               halt_job_on_file_boundaries = False,
                               splitOnRun = False)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 9)

        # The first job should have three lumis from one run
        # The second three lumis from two different runs
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0L: [[0L, 2L]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0L: [[3L, 4L]], 1L: [[100L, 100L]]})


        # And it should still be the same when you load it out of the database
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0L: [[3L, 4L]], 1L: [[100L, 100L]]})

        # Assert that this works differently with file splitting on and run splitting on
        testSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 5, twoSites = False)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = testSubscription)
        jobGroups = jobFactory(lumis_per_job = 3,
                               halt_job_on_file_boundaries = True,
                               splitOnRun = True)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)

        # In this case it should slice things up so that each job only has one run
        # in it.
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0L: [[0L, 2L]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0L: [[3L, 4L]]})
        return


if __name__ == '__main__':
    unittest.main()
