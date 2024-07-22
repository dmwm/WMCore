"""
WMBS version of the EventAwareLumiBased splitting
algorithm unittest

Created on Oct 2, 2012

@author: dballest
"""

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit


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

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationAction = daofactory(classname="Locations.New")
        locationAction.execute(siteName="T1_US_FNAL", pnn="T1_US_FNAL_Disk")
        locationAction.execute(siteName="T2_CH_CERN", pnn="T2_CH_CERN")

        self.testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                     name="wf001", task="Test")
        self.testWorkflow.create()

        self.performanceParams = {'timePerEvent': 12,
                                  'memoryRequirement': 2300,
                                  'sizePerEvent': 400}

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def createSubscription(self, nFiles, lumisPerFile, twoSites=False, nEventsPerFile=100):
        """
        _createSubscription_

        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name=baseName)
        testFileset.create()
        for i in range(nFiles):
            newFile = self.createFile('%s_%i' % (baseName, i), nEventsPerFile,
                                      i, lumisPerFile, 'T1_US_FNAL_Disk')
            newFile.create()
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = self.createFile('%s_%i_2' % (baseName, i), nEventsPerFile,
                                          i, lumisPerFile, 'T2_CH_CERN')
                newFile.create()
                testFileset.addFile(newFile)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()

        return testSubscription

    def createFile(self, lfn, events, run, lumis, location, lumiMultiplier=None):
        """
        _createFile_

        Create a file for testing
        """
        if lumiMultiplier is None:
            lumiMultiplier = run

        newFile = File(lfn=lfn, size=1000, events=events)
        lumiList = []
        for lumi in range(lumis):
            lumiList.append((lumiMultiplier * lumis) + lumi)
        newFile.addRun(Run(run, *lumiList))
        newFile.setLocation(location)
        return newFile

    def testA_FileSplitNoHardLimit(self):
        """
        _testA_FileSplitNoHardLimit_

        Simplest use case, there is only a self limit of events per job which
        the algorithm must adapt to on a file by file basis. At most
        one file per job so we don't have to pass information between files.
        """
        splitter = SplitterFactory()

        # Create 5 files with 7 lumi per file and 100 events per lumi on average.
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=7, twoSites=False,
                                                   nEventsPerFile=700)
        jobFactory = splitter(package="WMCore.WMBS",
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
        for idx, job in enumerate(jobs, start=1):
            # Jobs may have 1 lumi or 2 check performance figures accordingly
            self.assertEqual(job['estimatedMemoryUsage'], 2300)
            if idx % 3 == 0:
                self.assertEqual(job['estimatedDiskUsage'], 100 * 400)
                self.assertEqual(job['estimatedJobTime'], 100 * 12)
            else:
                self.assertEqual(job['estimatedDiskUsage'], 3 * 100 * 400)
                self.assertEqual(job['estimatedJobTime'], 3 * 100 * 12)

        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=7, twoSites=False,
                                                   nEventsPerFile=700)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        # Now set the average to 200 events per job
        # This results in the algorithm reducing the lumis per job to 2
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=200,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 20, "There should be 20 jobs")
        for idx, job in enumerate(jobs, start=1):
            # Jobs may have 1 lumi or 2 check performance figures accordingly
            self.assertEqual(job['estimatedMemoryUsage'], 2300)
            if idx % 4 == 0:
                self.assertEqual(job['estimatedDiskUsage'], 100 * 400)
                self.assertEqual(job['estimatedJobTime'], 100 * 12)
            else:
                self.assertEqual(job['estimatedDiskUsage'], 2 * 100 * 400)
                self.assertEqual(job['estimatedJobTime'], 2 * 100 * 12)

        # Check extremes, process a zero event files with lumis. It must be processed in one job
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=100, twoSites=False,
                                                   nEventsPerFile=0)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 0, "There are not enough events, so it should be 0 instead")

        # we close this fileset to get it moving
        fileset = testSubscription.getFileset()
        fileset.markOpen(False)

        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 5, "There should be 5 jobs")
        for job in jobs:
            self.assertEqual(job['estimatedMemoryUsage'], 2300)
            self.assertEqual(job['estimatedDiskUsage'], 0)
            self.assertEqual(job['estimatedJobTime'], 0)

        # Process files with 10k events per lumi, fallback to one lumi per job. We can't do better
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False,
                                                   nEventsPerFile=50000)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               events_per_job=5000,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 25, "There should be 5 jobs")
        for job in jobs:
            self.assertEqual(job['estimatedMemoryUsage'], 2300)
            self.assertEqual(job['estimatedDiskUsage'], 10000 * 400)
            self.assertEqual(job['estimatedJobTime'], 10000 * 12)

    def testB_NoFileSplitNoHardLimit(self):
        """
        _testB_NoFileSplitNoHardLimit_

        In this case we don't split on file boundaries, check different combination of files
        make sure we make the most of the splitting, e.g. include many zero event files in
        a single job.
        """
        splitter = SplitterFactory()

        # Create 100 files with 7 lumi per file and 0 events per lumi on average.
        testSubscription = self.createSubscription(nFiles=100, lumisPerFile=7, twoSites=False,
                                                   nEventsPerFile=0)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)

        # First test, the optimal settings are 360 events per job
        # As we have files with 0 events per lumi, this will configure the splitting to
        # a single job containing all files
        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=360,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 0, "There aren't enough events, so it should have 0 job groups")

        # we close this fileset to get it moving
        fileset = testSubscription.getFileset()
        fileset.markOpen(False)

        jobGroups = jobFactory(halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               events_per_job=360,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 1, "There should be 1 job")
        self.assertEqual(len(jobs[0]['input_files']), 100, "All 100 files must be in the job")
        self.assertEqual(jobs[0]['estimatedMemoryUsage'], 2300)
        self.assertEqual(jobs[0]['estimatedDiskUsage'], 0)
        self.assertEqual(jobs[0]['estimatedJobTime'], 0)

        # Create 7 files, each one with different lumi/event distributions
        testFileset = Fileset(name="FilesetA")
        testFileset.create()
        testFileA = self.createFile("/this/is/file1", 250, 0, 5, "T2_CH_CERN")
        testFileB = self.createFile("/this/is/file2", 600, 1, 1, "T2_CH_CERN")
        testFileC = self.createFile("/this/is/file3", 1200, 2, 2, "T2_CH_CERN")
        testFileD = self.createFile("/this/is/file4", 100, 3, 1, "T2_CH_CERN")
        testFileE = self.createFile("/this/is/file5", 30, 4, 1, "T2_CH_CERN")
        testFileF = self.createFile("/this/is/file6", 10, 5, 1, "T2_CH_CERN")
        testFileG = self.createFile("/this/is/file7", 151, 6, 3, "T2_CH_CERN")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.addFile(testFileG)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()

        jobFactory = splitter(package="WMCore.WMBS",
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
        self.assertEqual(len(jobs), 7, "7 jobs must be in the jobgroup")
        self.assertEqual(jobs[0]["mask"].getRunAndLumis(), {0: [[0, 2]]}, "Wrong mask for the first job")
        self.assertEqual(jobs[0]["estimatedJobTime"], 150 * 12)
        self.assertEqual(jobs[0]["estimatedDiskUsage"], 150 * 400)
        self.assertEqual(jobs[1]["mask"].getRunAndLumis(), {0: [[3, 4]]}, "Wrong mask for the second job")
        self.assertEqual(jobs[1]["estimatedJobTime"], 100 * 12)
        self.assertEqual(jobs[1]["estimatedDiskUsage"], 100 * 400)
        self.assertEqual(jobs[2]["mask"].getRunAndLumis(), {1: [[1, 1]]}, "Wrong mask for the third job")
        self.assertEqual(jobs[2]["estimatedJobTime"], 600 * 12)
        self.assertEqual(jobs[2]["estimatedDiskUsage"], 600 * 400)
        self.assertEqual(jobs[3]["mask"].getRunAndLumis(), {2: [[4, 4]]}, "Wrong mask for the fourth job")
        self.assertEqual(jobs[3]["estimatedJobTime"], 600 * 12)
        self.assertEqual(jobs[3]["estimatedDiskUsage"], 600 * 400)
        self.assertEqual(jobs[4]["mask"].getRunAndLumis(), {2: [[5, 5]]}, "Wrong mask for the fifth job")
        self.assertEqual(jobs[4]["estimatedJobTime"], 600 * 12)
        self.assertEqual(jobs[4]["estimatedDiskUsage"], 600 * 400)
        self.assertEqual(jobs[5]["mask"].getRunAndLumis(),
                         {3: [[3, 3]], 4: [[4, 4]], 5: [[5, 5]]}, "Wrong mask for the sixth job")
        self.assertEqual(jobs[5]["estimatedJobTime"], 140 * 12)
        self.assertEqual(jobs[5]["estimatedDiskUsage"], 140 * 400)
        self.assertEqual(jobs[6]["mask"].getRunAndLumis(), {6: [[18, 20]]}, "Wrong mask for the seventh job")
        self.assertEqual(jobs[6]["estimatedJobTime"], 150 * 12)
        self.assertEqual(jobs[6]["estimatedDiskUsage"], 150 * 400)

        for job in jobs:
            self.assertEqual(job["estimatedMemoryUsage"], 2300)
        # Test interactions of this algorithm with splitOnRun = True
        # Make 2 files, one with 3 runs and a second one with the last run of the first
        fileA = File(lfn="/this/is/file1a", size=1000,
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
        fileA.setLocation("T1_US_FNAL_Disk")

        fileB = self.createFile('/this/is/file2a', 200, 3, 5, "T1_US_FNAL_Disk")

        testFileset = Fileset(name='FilesetB')
        testFileset.create()
        testFileset.addFile(fileA)
        testFileset.addFile(fileB)
        testFileset.commit()
        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        # The settings for this splitting are 700 events per job
        jobGroups = jobFactory(splitOnRun=True,
                               halt_job_on_file_boundaries=False,
                               events_per_job=700,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1, "There should be only one job group")
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 6, "Six jobs must be in the jobgroup")
        self.assertEqual(jobs[0]["estimatedJobTime"], 700 * 12)
        self.assertEqual(jobs[0]["estimatedDiskUsage"], 700 * 400)
        self.assertEqual(jobs[1]["estimatedJobTime"], 100 * 12)
        self.assertEqual(jobs[1]["estimatedDiskUsage"], 100 * 400)
        self.assertEqual(jobs[2]["estimatedJobTime"], 700 * 12)
        self.assertEqual(jobs[2]["estimatedDiskUsage"], 700 * 400)
        self.assertEqual(jobs[3]["estimatedJobTime"], 100 * 12)
        self.assertEqual(jobs[3]["estimatedDiskUsage"], 100 * 400)
        self.assertEqual(jobs[4]["estimatedJobTime"], 700 * 12)
        self.assertEqual(jobs[4]["estimatedDiskUsage"], 700 * 400)
        self.assertEqual(jobs[5]["estimatedJobTime"], 300 * 12)
        self.assertEqual(jobs[5]["estimatedDiskUsage"], 300 * 400)

    def testC_HardLimitSplitting(self):
        """
        _testC_HardLimitSplitting_

        Test that we can specify a event limit, the
        algorithm shall take single lumi files with more events than the limit
        and mark them for failure
        """
        splitter = SplitterFactory()

        # Create 3 files, the one in the middle is a "bad" file
        testFileset = Fileset(name="FilesetA")
        testFileset.create()
        testFileA = self.createFile("/this/is/file1", 1000, 0, 5, "T1_US_FNAL_Disk")
        testFileB = self.createFile("/this/is/file2", 1000, 1, 1, "T1_US_FNAL_Disk")
        testFileC = self.createFile("/this/is/file3", 1000, 2, 2, "T1_US_FNAL_Disk")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()
        jobFactory = splitter(package="WMCore.WMBS",
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
        self.assertTrue(jobs[3]['failedOnCreation'], "The job processing the second file should me marked for failure")
        self.assertEqual(jobs[3]['failedReason'],
                         'File /this/is/file2 has a single lumi 1, in run 1 with too many events 1000 and it would take 12000 sec to run')

    def testD_HardLimitSplittingOnly(self):
        """
        _testD_HardLimitSplittingOnly_

        Checks that we can split a set of files where every file has a single
        lumi too big to fit in a runnable job
        """
        splitter = SplitterFactory()

        # Create 3 single-big-lumi files
        testFileset = Fileset(name="FilesetA")
        testFileset.create()
        testFileA = self.createFile("/this/is/file1", 1000, 0, 1, "T1_US_FNAL_Disk")
        testFileB = self.createFile("/this/is/file2", 1000, 1, 1, "T1_US_FNAL_Disk")
        testFileC = self.createFile("/this/is/file3", 1000, 2, 1, "T1_US_FNAL_Disk")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()
        jobFactory = splitter(package="WMCore.WMBS",
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
        for i in range(1, 4):
            self.assertTrue(jobs[i - 1]['failedOnCreation'],
                            "The job processing the second file should me marked for failure")
            error = 'File /this/is/file%s has a single lumi %d, in run %s' % (i, i - 1, i - 1)
            error += ' with too many events 1000 and it would take 12000 sec to run'
            self.assertEqual(jobs[i - 1]['failedReason'], error)

        return
    def testE_DisableHardLimitSplitting(self):
        """
        _testC_DisableHardLimitSplitting_
        Test that we can bypass and event limit when allowCreationFailure is
        set to False. The algorithm shall take single lumi files with more events
        than the limit but not mark them for failure
        """
        splitter = SplitterFactory()

        # Create 3 files, the one in the middle is a "bad" file
        testFileset = Fileset(name="FilesetA")
        testFileset.create()
        testFileA = self.createFile("/this/is/file1", 1000, 0, 5, "T1_US_FNAL_Disk")
        testFileB = self.createFile("/this/is/file2", 1000, 1, 1, "T1_US_FNAL_Disk")
        testFileC = self.createFile("/this/is/file3", 1000, 2, 2, "T1_US_FNAL_Disk")
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()
        jobFactory = splitter(package="WMCore.WMBS",
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

    def test_NotEnoughEvents(self):
        """
        _test_NotEnoughEvents_

        Checks whether jobs are not created when there are not enough files (actually, events)
        according to the events_per_job requested to the splitter algorithm
        """
        splitter = SplitterFactory()

        # Very small fileset (single file) without enough events
        testSubscription = self.createSubscription(nFiles=1, lumisPerFile=2, nEventsPerFile=200)

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=500,
                               performance=self.performanceParams,
                               splitOnRun=False)

        self.assertEqual(len(jobGroups), 0)

        # Still a small fileset (two files) without enough events
        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2, nEventsPerFile=200)

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=500,
                               performance=self.performanceParams,
                               splitOnRun=False)

        self.assertEqual(len(jobGroups), 0)

        # Finally an acceptable fileset size (three files) with enough events
        testSubscription = self.createSubscription(nFiles=3, lumisPerFile=2, nEventsPerFile=200)

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=500,
                               performance=self.performanceParams,
                               splitOnRun=False)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)
        self.assertEqual(len(jobs[0]['input_files']), 3)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 1]], 1: [[2, 3]], 2: [[4, 4]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {2: [[5, 5]]})

        # Test fileset with a single run and splitOnRun=True
        testFileset = Fileset(name="FilesetA")
        testFileA = self.createFile("/this/is/file1", 200, 1, 2, "T1_US_FNAL_Disk", lumiMultiplier=0)
        testFileB = self.createFile("/this/is/file2", 200, 1, 2, "T1_US_FNAL_Disk", lumiMultiplier=1)
        testFileC = self.createFile("/this/is/file3", 200, 1, 2, "T1_US_FNAL_Disk", lumiMultiplier=2)
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="EventAwareLumiBased",
                                        type="Processing")
        testSubscription.create()

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(events_per_job=500,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)
        self.assertEqual(len(jobs[0]['input_files']), 3)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {1: [[0, 1], [2, 3], [4, 4]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {1: [[5, 5]]})

        return


if __name__ == '__main__':
    unittest.main()
