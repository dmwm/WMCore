"""
_EventAwareLumiByWork_t_

Lumi based splitting test with awareness of events per lumi, using the DataStructs classes.
It must pass the same tests as the LumiBased algorithm, plus
specific ones for this algorithm.
See WMCore/WMBS/JobSplitting/ for the WMBS (SQL database) version.
"""

from __future__ import division, print_function

from builtins import range
import logging
import unittest
from collections import Counter

from Utils.PythonVersion import PY3

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.LumiList import LumiList
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID


class EventAwareLumiByWorkTest(unittest.TestCase):
    """
    _EventAwareLumiByWorkTest_

    Test event based job splitting.
    """

    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """

        self.testWorkflow = Workflow()
        self.performanceParams = {'timePerEvent': 12, 'memoryRequirement': 2300, 'sizePerEvent': 400}

        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

        return

    def createSubscription(self, nFiles, lumisPerFile, twoSites=False, nEventsPerFile=100):
        """
        _createSubscription_

        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name=baseName)
        for i in range(nFiles):
            newFile = self.createFile('%s_%i' % (baseName, i), nEventsPerFile, i, lumisPerFile, 'blenheim')
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = self.createFile('%s_%i_2' % (baseName, i), nEventsPerFile, nFiles+i, lumisPerFile, 'malpaquet')
                testFileset.addFile(newFile)

        testSubscription = Subscription(fileset=testFileset, workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiByWork", type="Processing")

        return testSubscription

    @staticmethod
    def createFile(lfn, events, run, lumis, location):
        """
        _createFile_

        Create a file for testing
        """
        newFile = File(lfn=lfn, size=1000, events=events)
        lumiList = []
        for lumi in range(lumis):
            lumiList.append((run * lumis) + lumi)
        newFile.addRun(Run(run, *lumiList))
        newFile.setLocation(location)
        return newFile

    def testFileSplitting(self):
        """
        _testFileSplitting_

        Test that things work if we split files between jobs
        """
        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles=10, lumisPerFile=1)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=oneSetSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=True, events_per_job=100, performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)

        twoLumiFiles = self.createSubscription(nFiles=5, lumisPerFile=2)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=twoLumiFiles)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, events_per_job=50, performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)

        wholeLumiFiles = self.createSubscription(nFiles=5, lumisPerFile=3)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=wholeLumiFiles)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, events_per_job=67, performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        # 10 because we split on run boundaries
        self.assertEqual(len(jobGroups[0].jobs), 10)
        jobList = jobGroups[0].jobs
        for job in jobList:
            # Have should have one file, half two
            self.assertTrue(len(job['input_files']) in [1, 2])

        jobLumiList = [jobList[i]['mask'].getRunAndLumis() for i in range(0, 10)]
        correctJobLumiList = [{0: [[0, 1]]}, {0: [[2, 2]]},
                              {1: [[3, 4]]}, {1: [[5, 5]]},
                              {4: [[12, 13]]}, {4: [[14, 14]]}
                             ]

        for lumiList in correctJobLumiList:
            self.assertIn(lumiList, jobLumiList)

        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles=5, lumisPerFile=2, twoSites=True)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=twoSiteSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, events_per_job=50, performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)

    def testNoRunNoFileSplitting(self):
        """
        _testNoRunNoFileSplitting_

        Test the splitting algorithm in the odder fringe
        cases that might be required.
        """
        splitter = SplitterFactory()
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=60,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 9)
        self.enforceLimits(filesPerJob=2, jobsPerFile=3)

        # Assert that this works differently with file splitting on and run splitting on
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=60,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)
        self.enforceLimits(filesPerJob=1, jobsPerFile=2, runsPerJob=1)

        # Test total_events limit. (The algorithm cuts off after the lumi that
        # brings the total average event count over -or equal to- total_events.)
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=60, total_events=10,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 1)
        self.assertTrue(jobs[0]['mask'].getRunAndLumis())  # Make sure it has a lumi to process

        # Test the total event limit again
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=60, total_events=179,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)

        # Test the total event limit on the boundary
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=60, total_events=180,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)

        # Test the total event limit just past the boundary
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=60, total_events=181,
                               sperformance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 4)

        return

    def testFileSplitNoHardLimit(self):
        """
        _testFileSplitNoHardLimit_

        Simplest use case, there is only a self limit of events per job which
        the algorithm must adapt to on a file by file basis. At most
        one file per job so we don't have to pass information between files.
        """
        splitter = SplitterFactory()

        # Create 5 files with 7 lumi per file and 100 events per lumi on average.
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=7, twoSites=False, nEventsPerFile=700)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        # First test, the optimal settings are 360 events per job
        # As we have files with 100 events per lumi, this will configure the splitting to
        # 3.6 lumis per job, which rounds to 3, the algorithm always approximates to the lower integer.
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=360,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)

        # Now set the average to 200 events per job
        # This results in the algorithm reducing the lumis per job to 2
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=200,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 20)

        # Check extremes, process a zero event files with lumis. It must be processed in one job
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=100, twoSites=False,
                                                   nEventsPerFile=0)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5)

        # Process files with 10k events per lumi, fallback to one lumi per job. We can't do better
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False,
                                                   nEventsPerFile=50000)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 25)

        # Test total_events limit. (The algorithm cuts off after the lumi that
        # brings the total average event count over -or equal to- total_events.)
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=3, twoSites=False,
                                                   nEventsPerFile=300)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=250, total_events=750,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5)
        self.enforceLimits(filesPerJob=1, jobsPerFile=2, runsPerJob=1)

        return

    def testNoFileSplitNoHardLimit(self):
        """
        _testNoFileSplitNoHardLimit_

        In this case we don't split on file boundaries, check different combination of files
        make sure we make the most of the splitting, e.g. include many zero event files in
        a single job.
        """
        splitter = SplitterFactory()

        # Create 100 files with 7 lumi per file and 0 events per lumi on average.
        testSubscription = self.createSubscription(nFiles=100, lumisPerFile=7, twoSites=False, nEventsPerFile=0)
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        # First test, the optimal settings are 360 events per job. As we have files with 0 events per lumi, this will
        # configure the splitting to a single job containing all files
        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=360,
                               performance=self.performanceParams)

        # One job in one job group with 100 files
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0]['input_files']), 100)

        # Create 7 files, each one with different lumi/event distributions
        testFileset = Fileset(name="FilesetA")
        testFileA = self.createFile("/this/is/file1", 250, 0, 5, "blenheim")
        testFileB = self.createFile("/this/is/file2", 600, 1, 1, "blenheim")
        testFileC = self.createFile("/this/is/file3", 1200, 2, 2, "blenheim")
        testFileD = self.createFile("/this/is/file4", 100, 3, 1, "blenheim")
        testFileE = self.createFile("/this/is/file5", 30, 4, 1, "blenheim")
        testFileF = self.createFile("/this/is/file6", 10, 5, 1, "blenheim")
        testFileG = self.createFile("/this/is/file7", 153, 6, 3, "blenheim")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.addFile(testFileG)

        testSubscription = Subscription(fileset=testFileset, workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiByWork", type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        # Split the work targeting 150 events per job
        jobGroups = jobFactory(halt_job_on_file_boundaries=False, splitOnRun=False, events_per_job=150,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 7)

        # Test interactions of this algorithm with splitOnRun = True
        # Make 2 files, one with 3 runs and a second one with the last run of the first
        fileA = File(lfn="/this/is/file1", size=1000, events=2400)
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
        testSubscription = Subscription(fileset=testFileset, workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiByWork", type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        # The settings for this splitting are 700 events per job
        jobGroups = jobFactory(splitOnRun=True, halt_job_on_file_boundaries=False, events_per_job=700,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 6)
        # Make sure each job has one run
        for job in jobs:
            self.assertEqual(len(job['mask'].getRunAndLumis()), 1)

    def testHardLimitSplitting(self):
        """
        _testHardLimitSplitting_

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

        testSubscription = Subscription(fileset=testFileset, workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiByWork", type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        # Settings are to split on job boundaries, to fail single lumis with more than 800 events
        # and to put 550 events per job
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=550,
                               job_time_limit=9600, performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5)

        # One job should be failed, the rest should be fine
        for jobNum in (0, 1, 3, 4):
            self.assertFalse(jobs[jobNum].get('failedOnCreation'))
        self.assertTrue(jobs[2]['failedOnCreation'])
        self.assertEqual(jobs[2]['failedReason'], 'File /this/is/file2 has a single lumi 1, in run 1 with too many events 1000 and it would take 12000 sec to run')

        return

    def testHardLimitSplittingOnly(self):
        """
        _testHardLimitSplittingOnly_

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

        testSubscription = Subscription(fileset=testFileset, workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiByWork", type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        # Fail single lumis with more than 800 events and put 550 events per job
        jobGroups = jobFactory(halt_job_on_file_boundaries=True, splitOnRun=True, events_per_job=550,
                               job_time_limit=9600, performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)
        for job in jobs:
            self.assertTrue(job['failedOnCreation'])
            self.assertIn(' with too many events 1000 and it would take 12000 sec to run', job['failedReason'])

        return

    def testLumiMask(self):
        """
        _testLumiMask_

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
                                        split_algo="EventAwareLumiByWork",
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
        processedLumis = LumiList()
        for job in jobs:
            processedLumis += LumiList(compactList=job['mask'].getRunAndLumis())
        correctLumis = LumiList(compactList={1: [[10, 14]], 2: [[20, 21]], 4: [[40, 41]]})
        self.assertEqual(processedLumis.getCMSSWString(), correctLumis.getCMSSWString())

    def testRunWhiteList(self):
        """
        _testRunWhiteList_

        Test that we can use a run white list to filter good runs/lumis.
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
                                        split_algo="EventAwareLumiByWork",
                                        type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        # Split with no breaks
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=725,
                               runWhitelist=[1, 4],
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)
        for job in jobs:
            for run in job['mask'].getRunAndLumis():
                self.assertIn(run, [1, 4])

        # Re-split with a break on runs
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=True,
                               events_per_job=595,
                               runWhitelist=[1, 3, 4],
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 4)
        self.enforceLimits(jobs=jobs, runsPerJob=1)
        for job in jobs:
            for run in job['mask'].getRunAndLumis():
                self.assertIn(run, [1, 3, 4])

        # Re-split with a break on files
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=False,
                               events_per_job=595,
                               runWhitelist=[1, 2, 3],
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 3)
        self.enforceLimits(jobs=jobs, filesPerJob=1)
        for job in jobs:
            for run in job['mask'].getRunAndLumis():
                self.assertIn(run, [1, 2, 3])

    def testLumiMaskAndWhitelist(self):
        """
        _testLumiMaskAndWhitelist_

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
                                        split_algo="EventAwareLumiByWork",
                                        type="Processing")
        jobFactory = splitter(package="WMCore.DataStructs",
                              subscription=testSubscription)

        # Use a lumi-mask = {1: [[10,14]], 2: [[20,21]], 4: [[40,41]]}
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=850,
                               runs=['1', '2', '4'],
                               lumis=['10,14', '20,21', '40,41'],
                               runWhitelist=[1, 4],
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {1: [[10, 14]], 4: [[40, 41]]})

    def testLumiCorrections(self):
        """
        _testLumiCorrections_

        Test the splitting algorithm can handle lumis which cross multiple files.
        No need for applyLumiCorrection=True
        """

        splitter = SplitterFactory()
        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2, twoSites=False, nEventsPerFile=150)
        files = testSubscription.getFileset().getFiles()
        self.assertEqual(len(files), 2)

        # Two files with 2 lumis each: file0 has run0 and lumis 0,1 - file1 has run1 and lumis 2,3 - each 150 events
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)

        jobGroups = jobFactory(events_per_job=50, halt_job_on_file_boundaries=False, splitOnRun=False,
                               performance=self.performanceParams)

        # The splitting algorithm will assume 75 events per lumi so we will have one job per lumi
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 4)

        # Recreate the same subscription as before
        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2, twoSites=False, nEventsPerFile=150)
        files = testSubscription.getFileset().getFiles()
        # Now modifyng and adding duplicated lumis.
        for runObj in files[0]['runs']:
            if runObj.run == 0:
                # continue
                runObj.appendLumi(42)
        for runObj in files[1]['runs']:
            if runObj.run == 1:
                # continue
                runObj.run = 0
                runObj.appendLumi(42)
        files[1]['locations'] = {'blenheim'}
        jobFactory = splitter(package="WMCore.DataStructs", subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=50, halt_job_on_file_boundaries=True, performance=self.performanceParams)

        # Now we will have: file0: Run0 and lumis [0, 1, 42] file1: Run0 and lumis [2, 3, 42]
        # With 50 events per lumi, one job per lumi, one will have two files on lumi 42
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5)

        n1files = 0
        n2files = 0
        lumi1files = []
        lumi2files = []

        for job in jobs:
            runLumis = job['mask'].getRunAndLumis()
            lumis = runLumis[0]
            self.assertEqual(len(runLumis), 1)
            self.assertEqual(len(lumis), 1)
            self.assertEqual(lumis[0][0], lumis[0][1])  # Make sure only one lumi per job
            if len(job['input_files']) == 1:
                n1files += 1
                lumi1files.append(lumis[0][0])
            elif len(job['input_files']) == 2:
                n2files += 1
                lumi2files.append(lumis[0][0])
            else:
                self.fail("At least one job has nFiles =! 1, 2")

        self.assertEqual(n1files, 4)
        self.assertEqual(n2files, 1)
        self.assertItemsEqual(lumi1files, [0, 1, 2, 3])
        self.assertItemsEqual(lumi2files, [42])

    def enforceLimits(self, jobs=None, filesPerJob=None, jobsPerFile=None, runsPerJob=None):
        """
        Args:
            jobs: the list of jobs to check
            filesPerJob: The maximum number of files that can be in one job
            jobsPerFile: The maximum number of jobs that a particular file can appear in
            runsPerJob: The maximum number of runs that can be in a job

        Returns: nothing

        Check the various limits on job and file distribution, raise assertion errors if they are violated
        """

        jobs = jobs or []
        jobsPerFileMap = Counter()
        for job in jobs:
            if filesPerJob:
                self.assertLessEqual(len(job['input_files']), filesPerJob)
            if runsPerJob:
                self.assertLessEqual(len(job['mask'].getRunAndLumis()), runsPerJob)
            for f in job['input_files']:
                jobsPerFileMap[f['lfn']] += 1
        for lfn in jobsPerFileMap:
            if jobsPerFile:
                self.assertLessEqual(jobsPerFileMap[lfn], jobsPerFile)

        return


if __name__ == '__main__':
    unittest.main()
