#!/usr/bin/env python
"""
_FileBased_t_

File based splitting test.
"""




from builtins import range
import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID

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
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation('blenheim')
            newFile.setLocation('malpaquet')
            lumis = []
            for lumi in range(20):
                lumis.append((i * 100) + lumi)
                newFile.addRun(Run(i, *lumis))
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.setLocation('blenheim')
        lumis = list(range(50,60)) + list(range(70,80))
        newFile.addRun(Run(13, *lumis))
        self.singleFileFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "FileBased",
                                                     type = "Processing")
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "FileBased",
                                                   type = "Processing")

        #self.multipleFileSubscription.create()
        #self.singleFileSubscription.create()

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

    def testExactFiles(self):
        """
        _testExactFiles_

        Test file based job splitting when the number of files per job is
        exactly the same as the number of files in the input fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 1,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        return

    def testMoreFiles(self):
        """
        _testMoreFiles_

        Test file based job splitting when the number of files per job is
        greater than the number of files in the input fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 10,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        return

    def test2FileSplit(self):
        """
        _test2FileSplit_

        Test file based job splitting when the number of files per job is
        2, this should result in five jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 2,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 5, \
               "ERROR: JobFactory didn't create two jobs."

        fileSet = set()
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "set")) == 2, \
                   "ERROR: Job contains incorrect number of files."

            for file in job.getFiles(type = "lfn"):
                fileSet.add(file)

        assert len(fileSet) == 10, \
               "ERROR: Not all files assinged to job."

        return

    def test3FileSplit(self):
        """
        _test3FileSplit_

        Test file based job splitting when the number of files per job is
        3, this should result in four jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 3,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 4)

        fileList = []
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "list")) in [3, 1], \
                   "ERROR: Job contains incorrect number of files."

            for file in job.getFiles(type = "lfn"):
                assert file not in fileList, \
                       "ERROR: File duplicated!"
                fileList.append(file)

        self.assertEqual(len(fileList), 10)

        return

    def test4WithLumiMask(self):
        """
        _test4WithLumiMask_

        Test file based job splitting when
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(files_per_job = 2,
                               total_files = 3,
                               runs = ['1', '2', '4', '5'],
                               lumis = ['100,130', '203,204,207,221', '401,405', '500, 520'],
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1)

        self.assertEqual(len(jobGroups[0].jobs), 2)

        fileList = []
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "list")) in [2, 1], \
                   "ERROR: Job contains incorrect number of files."

            for file in job.getFiles(type = "lfn"):
                assert file not in fileList, \
                       "ERROR: File duplicated!"
                fileList.append(file)

        self.assertEqual(len(fileList), 3)

        return

if __name__ == '__main__':
    unittest.main()
