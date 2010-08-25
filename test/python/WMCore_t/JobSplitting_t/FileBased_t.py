#!/usr/bin/env python
"""
_FileBased_t_

File based splitting test.
"""

__revision__ = "$Id: FileBased_t.py,v 1.7 2009/10/13 22:42:55 meloam Exp $"
__version__ = "$Revision: 1.7 $"

from sets import Set
import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID

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
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.setLocation('blenheim')
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

        jobGroups = jobFactory(files_per_job = 1)

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
        
        jobGroups = jobFactory(files_per_job = 10)
        
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
        
        jobGroups = jobFactory(files_per_job = 2)
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 5, \
               "ERROR: JobFactory didn't create two jobs."

        fileSet = Set()
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
        
        jobGroups = jobFactory(files_per_job = 3)
        
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

if __name__ == '__main__':
    unittest.main()
