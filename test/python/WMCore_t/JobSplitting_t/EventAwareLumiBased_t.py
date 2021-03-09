"""
_EventAwareLumiBased_t_

Lumi based splitting test with awareness of events per lumi, using the DataStructs classes.
It must pass the same tests as the LumiBased algorithm, plus
specific ones for this algorithm.
See WMCore/WMBS/JobSplitting/ for the WMBS (SQL database) version.

Created on Sep 25, 2012

@author: dballest
"""
from builtins import range
from future.utils import viewvalues

import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID


class EventAwareLumiBasedTest(unittest.TestCase):
    """
    _EventAwareLumiBasedTest_

    Test event based job splitting.
    """

    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """

        self.testWorkflow = Workflow()
        self.performanceParams = {'timePerEvent': 12,
                                  'memoryRequirement': 2300,
                                  'sizePerEvent': 400}

        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def createSubscription(self, nFiles, lumisPerFile, twoSites=False, nEventsPerFile=100):
        """
        _createSubscription_

        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name=baseName)
        for i in range(nFiles):
            newFile = self.createFile('%s_%i' % (baseName, i), nEventsPerFile,
                                      i, lumisPerFile, 'blenheim')
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = self.createFile('%s_%i_2' % (baseName, i), nEventsPerFile,
                                          i, lumisPerFile, 'malpaquet')
                testFileset.addFile(newFile)

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")

        return testSubscription

    def createFile(self, lfn, events, run, lumis, location):
        """
        _createFile_

        Create a file for testing
        """
        newFile = File(lfn=lfn, size=1000,
                       events=events)
        lumiList = []
        for lumi in range(lumis):
            lumiList.append((run * lumis) + lumi)
        newFile.addRun(Run(run, *lumiList))
        newFile.setLocation(location)
        return newFile

    def testA_FileSplitting(self):
        """
        _FileSplitting_

        Test that things work if we split files between jobs
        """
        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles=10, lumisPerFile=1)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=oneSetSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=100,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)

        twoLumiFiles = self.createSubscription(nFiles=5, lumisPerFile=2)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=twoLumiFiles)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=50,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)

        wholeLumiFiles = self.createSubscription(nFiles=5, lumisPerFile=3)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=wholeLumiFiles)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=67,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        # 10 because we split on run boundaries
        self.assertEqual(len(jobGroups[0].jobs), 10)
        jobList = jobGroups[0].jobs
        for job in jobList:
            # Have should have one file, half two
            self.assertTrue(len(job['input_files']) in [1, 2])

        mask0 = jobList[0]['mask'].getRunAndLumis()
        self.assertEqual(mask0, {0: [[0, 1]]})
        mask1 = jobList[1]['mask'].getRunAndLumis()
        self.assertEqual(mask1, {0: [[2, 2]]})
        mask2 = jobList[2]['mask'].getRunAndLumis()
        self.assertEqual(mask2, {1: [[3, 4]]})
        mask3 = jobList[3]['mask'].getRunAndLumis()
        self.assertEqual(mask3, {1: [[5, 5]]})

        self.assertEqual(jobList[0]['mask'].getRunAndLumis(), {0: [[0, 1]]})

        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles=5, lumisPerFile=2, twoSites=True)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=twoSiteSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=50,
                               performance=self.performanceParams)
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
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=60,
                               performance=self.performanceParams)

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
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=60,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)

        # In this case it should slice things up so that each job only has one run
        # in it.
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0: [[3, 4]]})

        # Test total_events limit. (The algorithm cuts off after the lumi that
        # brings the total average event count over -or equal to- total_events.)
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=60,
                               total_events=10,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 0]]})

        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=60,
                               total_events=179,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        job1runLumi = jobs[1]['mask'].getRunAndLumis()
        self.assertEqual(job1runLumi[0][0][0] + 1, job1runLumi[0][0][1])  # Run 0, startLumi+1 == endLumi
        self.assertEqual(job1runLumi[1][0][0], job1runLumi[1][0][1])  # Run 1, startLumi == endLumi
        self.assertEqual(len(jobs[2]['mask'].getRunAndLumis()), 1)  # All lumis from one run again

        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=60,
                               total_events=180,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        job1runLumi = jobs[1]['mask'].getRunAndLumis()
        self.assertEqual(job1runLumi[0][0][0] + 1, job1runLumi[0][0][1])  # Run 0, startLumi+1 == endLumi
        self.assertEqual(job1runLumi[1][0][0], job1runLumi[1][0][1])  # Run 1, startLumi == endLumi
        self.assertEqual(len(jobs[2]['mask'].getRunAndLumis()), 1)  # All lumis from one run again

        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=60,
                               total_events=181,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 4)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        job1runLumi = jobs[1]['mask'].getRunAndLumis()
        self.assertEqual(job1runLumi[0][0][0] + 1, job1runLumi[0][0][1])  # Run 0, startLumi+1 == endLumi
        self.assertEqual(job1runLumi[1][0][0], job1runLumi[1][0][1])  # Run 1, startLumi == endLumi
        self.assertEqual(len(jobs[2]['mask'].getRunAndLumis()), 1)  # All lumis from one run again
        self.assertEqual(len(jobs[3]['mask'].getRunAndLumis()), 1)  # All lumis from one run again

        return

    def testC_FileSplitNoHardLimit(self):
        """
        _testC_FileSplitNoHardLimit_

        Simplest use case, there is only a self limit of events per job which
        the algorithm must adapt to on a file by file basis. At most
        one file per job so we don't have to pass information between files.
        """
        splitter = SplitterFactory()

        # Create 5 files with 7 lumi per file and 100 events per lumi on average.
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=7, twoSites=False,
                                                   nEventsPerFile=700)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        # First test, the optimal settings are 360 events per job
        # As we have files with 100 events per lumi, this will configure the splitting to
        # 3.6 lumis per job, which rounds to 3, the algorithm always approximates to the lower integer.
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=360,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 15, "There should be 15 jobs")

        # Now set the average to 200 events per job
        # This results in the algorithm reducing the lumis per job to 2
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=200,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 20, "There should be 20 jobs")

        # Check extremes, process a zero event files with lumis. It must be processed in one job
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=100, twoSites=False,
                                                   nEventsPerFile=0)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5, "There should be 5 jobs")

        # Process files with 10k events per lumi, fallback to one lumi per job. We can't do better
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False,
                                                   nEventsPerFile=50000)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 25, "There should be 5 jobs")

        # Test total_events limit. (The algorithm cuts off after the lumi that
        # brings the total average event count over -or equal to- total_events.)
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=3, twoSites=False,
                                                   nEventsPerFile=300)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=250,
                               total_events=750,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5, "There should be 5 jobs")
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 1]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0: [[2, 2]]})
        self.assertEqual(jobs[2]['mask'].getRunAndLumis(), {1: [[3, 4]]})
        self.assertEqual(jobs[3]['mask'].getRunAndLumis(), {1: [[5, 5]]})
        self.assertEqual(len(jobs[4]['mask'].getRunAndLumis()), 1)  # All lumis from one run

        return

    def testD_NoFileSplitNoHardLimit(self):
        """
        _testD_NoFileSplitNoHardLimit_

        In this case we don't split on file boundaries, check different combination of files
        make sure we make the most of the splitting, e.g. include many zero event files in
        a single job.
        """
        splitter = SplitterFactory()

        # Create 100 files with 7 lumi per file and 0 events per lumi on average.
        testSubscription = self.createSubscription(nFiles=100, lumisPerFile=7, twoSites=False,
                                                   nEventsPerFile=0)
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        # First test, the optimal settings are 360 events per job
        # As we have files with 0 events per lumi, this will configure the splitting to
        # a single job containing all files
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=360,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 1, "There should be 1 job")
        self.assertEqual(len(jobs[0]['input_files']), 100, "All 100 files must be in the job")

        # Create 7 files, each one with different lumi/event distributions
        testFileset = Fileset(name="FilesetA")
        testFileA = self.createFile("/this/is/file1", 250, 0, 5, "blenheim")  # job1, job2
        testFileB = self.createFile("/this/is/file2", 600, 1, 1, "blenheim")  # job3
        testFileC = self.createFile("/this/is/file3", 1200, 2, 2, "blenheim")  # job4, job5
        testFileD = self.createFile("/this/is/file4", 100, 3, 1, "blenheim")  # job6
        testFileE = self.createFile("/this/is/file5", 30, 4, 1, "blenheim")  # job6
        testFileF = self.createFile("/this/is/file6", 10, 5, 1, "blenheim")  # job6
        testFileG = self.createFile("/this/is/file7", 151, 6, 3, "blenheim")  # job7
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.addFile(testFileG)

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")

        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        # Optimal settings are: jobs with 150 events per job
        # This means, the first file must be splitted in 3 lumis per job which would leave room
        # for another lumi in the second job, but the second file has a lumi too big for that
        # The 3rd job only contains the second file, the fourth and fifth job split the third file
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=150,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 7, "Seven jobs must be in the jobgroup")
        self.assertEqual(jobs[0]["mask"].getRunAndLumis(), {0: [[0, 2]]}, "Wrong mask for the first job")
        self.assertEqual(jobs[1]["mask"].getRunAndLumis(), {0: [[3, 4]]}, "Wrong mask for the second job")
        self.assertEqual(jobs[2]["mask"].getRunAndLumis(), {1: [[1, 1]]}, "Wrong mask for the third job")
        self.assertEqual(jobs[3]["mask"].getRunAndLumis(), {2: [[4, 4]]}, "Wrong mask for the fourth job")
        self.assertEqual(jobs[4]["mask"].getRunAndLumis(), {2: [[5, 5]]}, "Wrong mask for the fifth job")
        self.assertEqual(jobs[5]["mask"].getRunAndLumis(),
                         {3: [[3, 3]], 4: [[4, 4]], 5: [[5, 5]]}, "Wrong mask for the sixth job")
        self.assertEqual(jobs[6]["mask"].getRunAndLumis(), {6: [[18, 20]]}, "Wrong mask for the seventh job")
        # Test interactions of this algorithm with splitOnRun = True
        # Make 2 files, one with 3 runs and a second one with the last run of the first
        fileA = File(lfn="/this/is/file1", size=1000,
                     events=2400)
        lumiListA = []
        lumiListB = []
        lumiListC = []
        for lumi in range(8):
            lumiListA.append(1 + lumi)
            lumiListB.append(1 + lumi)
            lumiListC.append(1 + lumi)
        fileA.addRun(Run(1, *lumiListA))
        fileA.addRun(Run(2, *lumiListA))
        fileA.addRun(Run(3, *lumiListA))
        fileA.setLocation("malpaquet")

        fileB = self.createFile('/this/is/file2', 200, 3, 5, "malpaquet")

        testFileset = Fileset(name='FilesetB')
        testFileset.addFile(fileA)
        testFileset.addFile(fileB)
        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")

        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        # The settings for this splitting are 700 events per job
        jobGroups = jobFactory(splitOnRun=True,
                               halt_job_on_file_boundaries=False,
                               events_per_job=700,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 6, "Six jobs must be in the jobgroup")

    def testE_HardLimitSpltting(self):
        """
        _testE_HardLimitSplitting_

        Test that we can specify a event limit, the
        algorithm shall take single lumi files with more events than the limit
        and mark them for failure
        """
        splitter = SplitterFactory()

        # Create 3 files, the one in the middle is a "bad" file
        testFileset = Fileset(name="FilesetA")
        testFileA = self.createFile("/this/is/file1", 1000, 0, 5, "blenheim")
        testFileB = self.createFile("/this/is/file2", 1000, 1, 1, "blenheim")
        testFileC = self.createFile("/this/is/file3", 1000, 2, 2, "blenheim")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        # Settings are to split on job boundaries, to fail sing lumis with more than 800 events
        # and to put 550 events per job
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=550,
                               job_time_limit=9600,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 6, "Six jobs must be in the jobgroup")
        failedJobs = [job for job in jobs if job.get('failedOnCreation', False)]
        self.assertEqual(len(failedJobs), 1)
        self.assertEqual(failedJobs[0]['failedReason'],
                         'File /this/is/file2 has a single lumi 1, in run 1 with too many events 1000 and it woud take 12000 sec to run')

        return

    def testF_HardLimitSplittingOnly(self):
        """
        _testF_HardLimitSplittingOnly_

        Checks that we can split a set of files where every file has a single
        lumi too big to fit in a runnable job
        """
        splitter = SplitterFactory()

        # Create 3 single-big-lumi files
        testFileset = Fileset(name="FilesetA")
        testFileA = self.createFile("/this/is/file1", 1000, 0, 1, "somese.cern.ch")
        testFileB = self.createFile("/this/is/file2", 1000, 1, 1, "somese.cern.ch")
        testFileC = self.createFile("/this/is/file3", 1000, 2, 1, "somese.cern.ch")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        # Settings are to split on job boundaries, to fail sing lumis with more than 800 events
        # and to put 550 events per job
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=550,
                               job_time_limit=9600,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3, "Three jobs must be in the jobgroup")
        for i in range(0, 3):
            self.assertTrue(jobs[i]['failedOnCreation'], "It should have been marked as failed")

            runNums = list(jobs[i]['mask']['runAndLumis'])
            self.assertEqual(len(runNums), 1)

            lumiNums = next(iter(viewvalues(jobs[i]['mask']['runAndLumis'])))
            self.assertEqual(len(lumiNums), 1)

            finalLumi = []
            for pair in lumiNums:
                finalLumi.extend(list(range(pair[0], pair[1] + 1)))
            self.assertEqual(len(finalLumi), 1)

            self.assertEqual(jobs[i]['failedReason'],
                             "File /this/is/file%d has a single lumi %s, in run %s with too many events 1000 and it woud take 12000 sec to run" % (
                             i + 1, finalLumi[0], runNums[0]))

        return

    def testG_LumiMask(self):
        """
        _testG_LumiMask_

        Test that we can use a lumi-mask to filter good runs/lumis.
        """
        splitter = SplitterFactory()

        # Create 3 files with 100 events per lumi:
        # - file1 with 1 run  of 8 lumis
        # - file2 with 2 runs of 2 lumis each
        # - file3 with 1 run  of 5 lumis
        fileA = File(lfn="/this/is/file1", size=1000, events=800)
        fileB = File(lfn="/this/is/file2", size=1000, events=400)
        fileC = File(lfn="/this/is/file3", size=1000, events=500)

        lumiListA = []
        for lumi in range(8):
            lumiListA.append(10 + lumi)
        fileA.addRun(Run(1, *lumiListA))
        fileA.setLocation("somese.cern.ch")
        lumiListB1 = []
        lumiListB2 = []
        for lumi in range(2):
            lumiListB1.append(20 + lumi)
            lumiListB2.append(30 + lumi)
        fileB.addRun(Run(2, *lumiListB1))
        fileB.addRun(Run(3, *lumiListB2))
        fileB.setLocation("somese.cern.ch")
        lumiListC = []
        for lumi in range(5):
            lumiListC.append(40 + lumi)
        fileC.addRun(Run(4, *lumiListC))
        fileC.setLocation("somese.cern.ch")

        testFileset = Fileset(name='Fileset')
        testFileset.addFile(fileA)
        testFileset.addFile(fileB)
        testFileset.addFile(fileC)

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        # Use a lumi-mask = {1: [[10,14]], 2: [[20,21]], 4: [[40,41]]}
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=850,
                               runs=['1', '2', '4'],
                               lumis=['10,14', '20,21', '40,41'],
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2, "Two jobs must be in the jobgroup")
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {1: [[10, 14]], 2: [[20, 21]], 4: [[40, 40]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {4: [[41, 41]]})

    def testH_LumiCorrections(self):
        """
        _LumiCorrections_

        Test the splitting algorithm can handle lumis which
        cross multiple files.
        """
        splitter = SplitterFactory()
        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2,
                                                   twoSites=False, nEventsPerFile=150)
        files = testSubscription.getFileset().getFiles()
        self.assertEqual(len(files), 2)
        # at the moment we have two files with two lumis each:
        #  file0 has run0 and lumis 0,1. 150 events
        #  file1 has run1 and lumis 2,3. 150 evens
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        jobGroups = jobFactory(events_per_job=50,
                               halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               performance=self.performanceParams,
                               applyLumiCorrection=False
                               )

        # The splitting algorithm will assume 75 events per lumi.
        # We will have one job per lumi
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 4)

        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2,
                                                   twoSites=False, nEventsPerFile=150)
        files = testSubscription.getFileset().getFiles()
        # Now modifyng and adding duplicated lumis.
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
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=50,
                               halt_job_on_file_boundaries=True,
                               performance=self.performanceParams,
                               applyLumiCorrection=True)

        # Now we will have:
        #   file0: Run0 and lumis [0, 1, 42]
        #   file1: Run0 and lumis [2, 3, 42]
        # Splitting algorithm is assuming 50 events per lumi
        # Three jobs (one per lumu) for the first file
        # Two jobs for the second file (42 is duplicated)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5)
        self.assertEqual(len(jobs[0]['input_files']), 1)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(len(jobs[2]['input_files']), 2)
        self.assertEqual(len(jobs[3]['input_files']), 1)
        self.assertEqual(len(jobs[4]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 0]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0: [[1, 1]]})
        self.assertEqual(jobs[2]['mask'].getRunAndLumis(), {0: [[42, 42]]})
        self.assertEqual(jobs[3]['mask'].getRunAndLumis(), {0: [[2, 2]]})
        self.assertEqual(jobs[4]['mask'].getRunAndLumis(), {0: [[3, 3]]})

        # Check that if the last two jobs have the same duplicated lumi you do not get an error
        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2,
                                                   twoSites=False, nEventsPerFile=150)
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
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=50,
                               halt_job_on_file_boundaries=True,
                               performance=self.performanceParams,
                               applyLumiCorrection=True)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)

    def testI_DisableHardLimitSplitting(self):
        """
        _testI_DisableHardLimitSplitting_

        Test that we can bypass the job time limit when allowCreationFailure is
        set to False. The algorithm shall take single lumi files with time per
        lumi greater than the job time limit but not mark them for failure
        """
        splitter = SplitterFactory()

        # Create 3 files, the one in the middle is a "bad" file
        testFileset = Fileset(name="FilesetA")
        testFileA = self.createFile("/this/is/file1", 1000, 0, 5, "blenheim")
        testFileB = self.createFile("/this/is/file2", 1000, 1, 1, "blenheim")
        testFileC = self.createFile("/this/is/file3", 1000, 2, 2, "blenheim")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)
        # Settings are to split on job boundaries, to fail sing lumis with more than 800 events
        # and to put 550 events per job
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=550,
                               job_time_limit=9600,
                               allowCreationFailure=False,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 6, "Six jobs must be in the jobgroup")
        failedJobs = [job for job in jobs if job.get('failedOnCreation', False)]
        self.assertEqual(len(failedJobs), 0, "There should be no failed jobs")

        return

if __name__ == '__main__':
    unittest.main()
