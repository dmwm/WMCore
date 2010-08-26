#!/usr/bin/env python
"""
_TwoFileBased_t_

File based splitting test with parentage.
"""

__revision__ = "$Id: TwoFileBased_t.py,v 1.3 2010/06/28 18:55:38 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMQuality.TestInit import TestInit

class TwoFileBasedTest(unittest.TestCase):
    """
    _TwoFileBasedTest_

    Test file based job splitting with parentage.
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
        locationAction.execute("site1", seName = "somese.cern.ch")
        locationAction.execute("site2", seName = "otherse.cern.ch")
        
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = set(["somese.cern.ch"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()


        self.multipleSiteFileset = Fileset(name = "TestFileset3")
        self.multipleSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["otherse.cern.ch", "somese.cern.ch"]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test" )
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "TwoFileBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "TwoFileBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()

        self.multipleSiteSubscription = Subscription(fileset = self.multipleSiteFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "TwoFileBased",
                                                     type = "Processing")
        self.multipleSiteSubscription.create()
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
        for i in range(2000):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
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

        jobGroups = jobFactory(files_per_job = 1)


        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

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
        
        jobGroups = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

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
        
        jobGroups = jobFactory(files_per_job = 2)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)

        fileList = []
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)
            for file in job.getFiles(type = "lfn"):
                fileList.append(file)
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
        
        jobGroups = jobFactory(files_per_job = 3)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 4)

        fileList = []
        for job in jobGroups[0].jobs:
            assert len(job.getFiles()) in [3, 1], "ERROR: Job contains incorrect number of files"
            for file in job.getFiles(type = "lfn"):
                fileList.append(file)
        self.assertEqual(len(fileList), 10)
        
        return


    def testLocationSplit(self):

        """

        _testLocationSplit_

        This should test whether or not the FileBased algorithm understands that files at seperate sites
        cannot be in the same jobGroup (this is the current standard).
        
        """
        myThread = threading.currentThread()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 1)

        fileList = []
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 5)

        
        return

    def testFileParentage(self):
        """
        _testFileParentage_

        Test whether we get file parentage correctly
        """
        # Create base name
        baseName = makeUUID()

        # Create files
        testFileset1 = Fileset(name = "TestParents1")
        testFileset1.create()
        for i in range(2):
            newFile = File(lfn = '%s_first_%i' % (baseName, i), size = 1000,
                           events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset1.addFile(newFile)
        testFileset1.commit()

        testFileset2 = Fileset(name = "TestParents2")
        testFileset2.create()
        for i in range(2):
            newFile = File(lfn = '%s_second_%i' % (baseName, i), size = 1000,
                           events = 100,
                           locations = set(["somese.cern.ch"]),
                           merged = False)
            newFile.create()
            newFile.addParent('%s_first_0' % (baseName))
            testFileset2.addFile(newFile)
        testFileset2.commit()

        testFileset3 = Fileset(name = "TestParents3")
        testFileset3.create()
        for i in range(2):
            newFile = File(lfn = '%s_third_%i' % (baseName, i), size = 1000,
                           events = 100,
                           locations = set(["somese.cern.ch"]),
                           merged = False)
            newFile.create()
            newFile.addParent('%s_second_1' % (baseName))
            testFileset3.addFile(newFile)
        testFileset3.commit()

        testFileset4 = Fileset(name = "TestParents4")
        testFileset4.create()
        for i in range(10):
            newFile = File(lfn = '%s_child_%i' % (baseName, i), size = 1000,
                           events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            newFile.addParent('%s_third_1' % (baseName))
            testFileset4.addFile(newFile)
        testFileset4.commit()

        # Create workflow
        testWorkflow = Workflow(spec = "spec2.xml", owner = "Steve",
                                name = "wf002", task="Test" )
        testWorkflow.create()

        testSubscription = Subscription(fileset = testFileset4,
                                        workflow = testWorkflow,
                                        split_algo = "TwoFileBased",
                                        type = "Processing")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        
        jobGroups = jobFactory(files_per_job = 2)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)

        fileList = []
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)
            for file in job.getFiles(type = "list"):
                parents = file.getParentLFNs()
                self.assertEqual(parents, ['%s_first_0' % (baseName)])

                fileList.append(file)
        self.assertEqual(len(fileList), 10)
        return

if __name__ == '__main__':
    unittest.main()
