#!/usr/bin/env python
"""
_FileBased_t_

File based splitting test.
"""


import unittest
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.TestInit import TestInit
#from nose.plugins.attrib import attr

class FileBasedTest(unittest.TestCase):
    """
    _FileBasedTest_

    Test file based job splitting.
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
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "T1_US_FNAL", pnn = "T1_US_FNAL_Disk")
        locationAction.execute(siteName = "T2_CH_CERN", pnn = "T2_CH_CERN")

        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        parentFile = File('/parent/lfn/', size = 1000, events = 100,
                          locations = set(["T1_US_FNAL_Disk"]))
        parentFile.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["T1_US_FNAL_Disk"]))
            newFile.addRun(Run(i, *[45]))
            newFile.create()
            newFile.addParent(lfn = parentFile['lfn'])
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = set(["T1_US_FNAL_Disk"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()


        self.multipleSiteFileset = Fileset(name = "TestFileset3")
        self.multipleSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["T1_US_FNAL_Disk"]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["T2_CH_CERN", "T1_US_FNAL_Disk"]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test" )
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "FileBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "FileBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()

        self.multipleSiteSubscription = Subscription(fileset = self.multipleSiteFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "FileBased",
                                                     type = "Processing")
        self.multipleSiteSubscription.create()

        self.performanceParams = {'timePerEvent' : 12,
                                  'memoryRequirement' : 2300,
                                  'sizePerEvent' : 400}
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def createLargeFileBlock(self):
        """
        _createLargeFileBlock_

        Creates a large group of files for testing
        """
        testFileset = Fileset(name = "TestFilesetX")
        testFileset.create()
        for _ in range(5000):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["T1_US_FNAL_Disk"]))
            newFile.create()
            testFileset.addFile(newFile)
        testFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "mnorman",
                                name = "wf003", task="Test" )
        testWorkflow.create()

        largeSubscription = Subscription(fileset = testFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "FileBased",
                                                   type = "Processing")
        largeSubscription.create()

        return largeSubscription

    def testExactFiles(self):
        """
        _testExactFiles_

        Test file based job splitting when the number of files per job is
        exactly the same as the number of files in the input fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 1,
                               performance = self.performanceParams)


        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])
        self.assertEqual(job["estimatedMemoryUsage"], 2300)
        self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
        self.assertEqual(job["estimatedJobTime"], 12 * 100)

        return

    def testMoreFiles(self):
        """
        _testMoreFiles_

        Test file based job splitting when the number of files per job is
        greater than the number of files in the input fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])
        self.assertEqual(job["estimatedMemoryUsage"], 2300)
        self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
        self.assertEqual(job["estimatedJobTime"], 12 * 100)

        return

    def test2FileSplit(self):
        """
        _test2FileSplit_

        Test file based job splitting when the number of files per job is
        2, this should result in five jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 2,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)

        fileList = []
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)
            for lfn in job.getFiles(type = "lfn"):
                fileList.append(lfn)
            self.assertEqual(job["estimatedMemoryUsage"], 2300)
            self.assertEqual(job["estimatedDiskUsage"], 400 * 100 * 2)
            self.assertEqual(job["estimatedJobTime"], 12 * 100 * 2)

        self.assertEqual(len(fileList), 10)

        return

    def test3FileSplit(self):
        """
        _test3FileSplit_

        Test file based job splitting when the number of files per job is
        3, this should result in four jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 3,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 4)

        fileList = []
        for job in jobGroups[0].jobs:
            assert len(job.getFiles()) in [3, 1], "ERROR: Job contains incorrect number of files"
            for lfn in job.getFiles(type = "lfn"):
                fileList.append(lfn)
            if len(job.getFiles()) == 3:
                self.assertEqual(job["estimatedMemoryUsage"], 2300)
                self.assertEqual(job["estimatedDiskUsage"], 400 * 100 * 3)
                self.assertEqual(job["estimatedJobTime"], 12 * 100 * 3)
            elif len(job.getFiles()) == 1:
                self.assertEqual(job["estimatedMemoryUsage"], 2300)
                self.assertEqual(job["estimatedDiskUsage"], 400 * 100)
                self.assertEqual(job["estimatedJobTime"], 12 * 100)
            else:
                self.fail("Unexpected splitting reached")
        self.assertEqual(len(fileList), 10)

        return


    def testLocationSplit(self):

        """

        _testLocationSplit_

        This should test whether or not the FileBased algorithm understands that files at
        seperate sites cannot be in the same jobGroup (this is the current standard).

        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["possiblePSN"], set(["T2_CH_CERN", "T1_US_FNAL"]))
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["possiblePSN"], set(["T1_US_FNAL"]))

        return

    def testSiteWhitelist(self):
        """
        _testSiteWhitelist_

        Same as testLocationSplit, but with a siteWhitelist for T1_US_FNAL.
        (only allow jobs to run at T1_US_FNAL)

        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               siteWhitelist = ["T1_US_FNAL"],
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["possiblePSN"], set(["T1_US_FNAL"]))
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["possiblePSN"], set(["T1_US_FNAL"]))

        return

    def testSiteBlacklist(self):
        """
        _testSiteBlacklist_

        Same as testLocationSplit, but with a siteBlacklist for T2_CH_CERN.
        (do not allow jobs to run at T2_CH_CERN)

        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               siteBlacklist = ["T2_CH_CERN"],
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["possiblePSN"], set(["T1_US_FNAL"]))
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["possiblePSN"], set(["T1_US_FNAL"]))

        return

    def testSiteWhiteBlacklist(self):
        """
        _testSiteBlacklist_

        Same as testLocationSplit, but with a siteWhitelist for T2_CH_CERN and
        T1_US_FNAL and a siteBlacklist for T1_US_FNAL.

        SiteBlacklist should take preference over siteWhitelist, jobs should
        only be allowed to run at T2_CH_CERN.

        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               siteWhitelist = ["T2_CH_CERN", "T1_US_FNAL"],
                               siteBlacklist = ["T1_US_FNAL"],
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[0].jobs[0]["possiblePSN"], set(["T2_CH_CERN"]))
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedDiskUsage"], 100 * 400 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["estimatedJobTime"], 100 * 12 * 5)
        self.assertEqual(jobGroups[1].jobs[0]["possiblePSN"], set([]))

        return

    def testTrustSiteLists(self):
        """
        _testTrustSiteLists_

        Test trustSitelists splitting parameter to ignore job input file
        location and use siteWhitelist and siteBlacklist instead.

        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               siteWhitelist = ["T2_AA_AAA", "T2_BB_BBB", "T2_CC_CCC", "T2_DD_DDD"],
                               siteBlacklist = ["T2_BB_BBB", "T2_DD_DDD", "T2_EE_EEE"],
                               trustSitelists = True,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(len(jobGroups[0].jobs[0].getFiles()), 10)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedMemoryUsage"], 2300)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedDiskUsage"], 100 * 400 * 10)
        self.assertEqual(jobGroups[0].jobs[0]["estimatedJobTime"], 100 * 12 * 10)
        self.assertEqual(jobGroups[0].jobs[0]["possiblePSN"], set(["T2_AA_AAA", "T2_CC_CCC"]))

        return

    def testLimit(self):
        """
        _testLimit_

        Test what happens when you limit the number of files.
        This should run each separate file in a separate loop,
        creating one jobGroups with one job with one file
        (The limit argument tells it what to do)
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)


        jobGroups = jobFactory(files_per_job = 10, limit_file_loading = True,
                               file_load_limit = 1,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)

        return

    def testRespectRunBoundaries(self):
        """
        _testRespectRunBoundaries_

        Test whether or not this thing will respect run boundaries
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 10, respect_run_boundaries = True,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)

        return

    def test_getParents(self):
        """
        _getParents_

        Check that we can do the same as the TwoFileBased
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 2,
                               include_parents  = True,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)

        fileList = []
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)
            for lfn in job.getFiles(type = "lfn"):
                fileList.append(lfn)
        self.assertEqual(len(fileList), 10)

        for j in jobGroups[0].jobs:
            for f in j['input_files']:
                self.assertEqual(len(f['parents']), 1)
                self.assertEqual(list(f['parents'])[0]['lfn'], '/parent/lfn/')

        return

    def testRandomCrap1(self):
        """
        First part of what used to be testRandomCrapForGenerators
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=self.multipleSiteSubscription)

        jobFactory.open()
        jobGroups = []

        a = self.crazyAssFunction(jobFactory=jobFactory, file_load_limit=2)

        for _ in range(7):
            try:
                res = next(a)
                jobGroups.extend(res)
            except StopIteration:
                continue

        jobFactory.close()

        self.assertEqual(len(jobGroups), 6)
        for group in jobGroups:
            self.assertTrue(len(group.jobs) in [1, 2])
            for job in group.jobs:
                self.assertTrue(job["possiblePSN"] in [{"T1_US_FNAL"}, {'T2_CH_CERN', 'T1_US_FNAL'}])
                self.assertTrue(len(job['input_files']) in (1, 2))

    def testRandomCrapForGenerators2(self):
        """
        Either this works, and all other tests are obsolete, or it doesn't and they aren't.
        Either way, don't screw around with this.
        """

        def runCode(self, jobFactory):

            func = self.crazyAssFunction(jobFactory=jobFactory, file_load_limit=500)

            goFlag = True
            while goFlag:
                try:
                    res = next(func)
                    self.jobGroups.extend(res)
                except StopIteration:
                    goFlag = False

            return self.jobGroups

        self.jobGroups = []
        subscript = self.createLargeFileBlock()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS", subscription=subscript)

        jobFactory.open()

        runCode(self, jobFactory)

        jobGroups = self.jobGroups

        self.assertEqual(len(jobGroups), 10)
        for group in jobGroups:
            self.assertEqual(len(group.jobs), 500)
            self.assertTrue(group.exists() > 0)

        jobFactory.close()
        return

    def crazyAssFunction(self, jobFactory, file_load_limit=1):
        groups = ['test']
        while groups:
            groups = jobFactory(files_per_job=1, file_load_limit=file_load_limit,
                                performance=self.performanceParams)
            yield groups


if __name__ == '__main__':
    unittest.main()
