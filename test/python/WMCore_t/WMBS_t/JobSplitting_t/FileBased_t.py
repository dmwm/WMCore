#!/usr/bin/env python
"""
_FileBased_t_

File based splitting test.
"""

__revision__ = "$Id: FileBased_t.py,v 1.1 2009/02/19 19:50:05 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from sets import Set
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

class FileBasedTest(unittest.TestCase):
    """
    _FileBasedTest_

    Test file based job splitting.
    """
    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(sename = "somese.cern.ch")
        
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = Set(["somese.cern.ch"]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = Set(["somese.cern.ch"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001")
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
        return
    
    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        myThread = threading.currentThread()

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
            
        myThread.transaction.begin()
            
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
            
        myThread.transaction.commit()
        return            

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
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)
        
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
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)
        
        jobGroups = jobFactory(files_per_job = 2)
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 5, \
               "ERROR: JobFactory didn't create five jobs."

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
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)
        
        jobGroups = jobFactory(files_per_job = 3)
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 4, \
               "ERROR: JobFactory didn't create four jobs."

        fileSet = Set()
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "set")) in [3, 1], \
                   "ERROR: Job contains incorrect number of files."

            for file in job.getFiles(type = "lfn"):
                fileSet.add(file)

        assert len(fileSet) == 10, \
               "ERROR: Not all files assinged to job."

        return    

if __name__ == '__main__':
    unittest.main()
