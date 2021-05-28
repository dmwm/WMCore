#!/usr/bin/env python
"""
_LumiBased_t_

Lumi based splitting tests, using the DataStructs classes.
See WMCore/WMBS/JobSplitting/ for the WMBS (SQL database) version.
"""

from builtins import next, range

import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID

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
        self.performanceParams = {'timePerEvent' : 12,
                                  'memoryRequirement' : 2300,
                                  'sizePerEvent' : 400}

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
                    lumis.append(5 + 10 * (i * 100) + lumi) #lumis should be different
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
                               halt_job_on_file_boundaries = True,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)




        twoLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 1,
                               halt_job_on_file_boundaries = True,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)



        wholeLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 3)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = wholeLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 2,
                               halt_job_on_file_boundaries = True,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        # 10 because we split on run boundaries
        self.assertEqual(len(jobGroups[0].jobs), 10)
        jobList = jobGroups[0].jobs
        for job in jobList:
            # Have should have one file, half two
            self.assertTrue(len(job['input_files']) in [1,2])


        mask0 = jobList[0]['mask'].getRunAndLumis()
        self.assertEqual(mask0, {0: [[0, 1]]})
        mask1 = jobList[1]['mask'].getRunAndLumis()
        self.assertEqual(mask1, {0: [[2, 2]]})
        mask2 = jobList[2]['mask'].getRunAndLumis()
        self.assertEqual(mask2, {1: [[100, 101]]})
        mask3 = jobList[3]['mask'].getRunAndLumis()
        self.assertEqual(mask3, {1: [[102, 102]]})

        self.assertEqual(jobList[0]['mask'].getRunAndLumis(), {0: [[0, 1]]})

        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 2, twoSites = True)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job = 1,
                               halt_job_on_file_boundaries = True,
                               performance = self.performanceParams)
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
                               splitOnRun = False,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 9)

        # The first job should have three lumis from one run
        # The second three lumis from two different runs
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        job1runLumi = jobs[1]['mask'].getRunAndLumis()
        self.assertEqual(job1runLumi[0][0][0] + 1, job1runLumi[0][0][1])  # Run 0, startLumi+1 == endLumi
        self.assertEqual(job1runLumi[1][0][0], job1runLumi[1][0][1])  # Run 1, startLumi == endLumi

        # Assert that this works differently with file splitting on and run splitting on
        testSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 5, twoSites = False)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = testSubscription)
        jobGroups = jobFactory(lumis_per_job = 3,
                               halt_job_on_file_boundaries = True,
                               splitOnRun = True,
                               performance = self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)

        # In this case it should slice things up so that each job only has one run
        # in it.
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0: [[3, 4]]})
        return


    def testC_LumiCorrections(self):
        """
        _LumiCorrections_

        Test the splitting algorithm can handle lumis which
        cross multiple files.
        """
        splitter = SplitterFactory()
        testSubscription = self.createSubscription(nFiles = 2, lumisPerFile = 2, twoSites = False)
        files = testSubscription.getFileset().getFiles()
        self.assertEqual(len(files), 2)
        for runObj in files[0]['runs']:
            if runObj.run != 0:
                continue
            runObj.appendLumi(42)
        for runObj in files[1]['runs']:
            if runObj.run != 1:
                continue
            runObj.run = 0
            runObj.appendLumi(42)
        files[1]['locations'] = set(['blenheim'])

        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = testSubscription)

        jobGroups = jobFactory(lumis_per_job = 3,
                               halt_job_on_file_boundaries = False,
                               splitOnRun = False,
                               performance = self.performanceParams,
                               applyLumiCorrection = True
                              )

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)

        self.assertEqual(len(jobs[0]['input_files']), 2)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 1], [42, 42]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0: [[100, 101]]})

        #Test that we are not removing all the lumis from the jobs anymore
        removedLumi = self.createSubscription(nFiles = 4, lumisPerFile = 1)
        #Setting the lumi of job 0 to value 100, as the one of job one
        runObj = next(iter(removedLumi.getFileset().getFiles()[0]['runs']))
        runObj.run = 1
        runObj[0] = 100
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = removedLumi)
        jobGroups = jobFactory(lumis_per_job = 1,
                               halt_job_on_file_boundaries = True,
                               performance = self.performanceParams,
                               applyLumiCorrection = True)
        # we need to end up with 3 jobs and one job with two input files
        jobs = jobGroups[0].jobs

        self.assertEqual(len(jobs), 3)
        self.assertEqual(len(jobs[0]['input_files']), 2)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(len(jobs[2]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {1: [[100, 100]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {2: [[200, 200]]})
        self.assertEqual(jobs[2]['mask'].getRunAndLumis(), {3: [[300, 300]]})


        #Check that if the last two jobs have the same duplicated lumi you do not get an error
        testSubscription = self.createSubscription(nFiles = 2, lumisPerFile = 2,
                                           twoSites = False)
        files = testSubscription.getFileset().getFiles()
        # Now modifying and adding the same duplicated lumis in the Nth and Nth-1 jobs
        for runObj in files[0]['runs']:
            if runObj.run != 0:
                continue
            runObj.appendLumi(42)
        for runObj in files[1]['runs']:
            runObj.run = 0
            runObj.lumis = [42]
        files[1]['locations'] = set(['blenheim'])
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = testSubscription)
        jobGroups = jobFactory(events_per_job = 50,
                               halt_job_on_file_boundaries = True,
                               performance = self.performanceParams,
                               applyLumiCorrection = True)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)

if __name__ == '__main__':
    unittest.main()
