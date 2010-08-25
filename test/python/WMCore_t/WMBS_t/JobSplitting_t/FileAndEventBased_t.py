#!/usr/bin/env python
"""
_FileAndEventBased_t_

Event based splitting test.
"""

__revision__ = "$Id: FileAndEventBased_t.py,v 1.3 2009/04/05 21:47:00 gowdy Exp $"
__version__ = "$Revision: 1.3 $"

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

class FileAndEventBasedTest(unittest.TestCase):
    """
    _FileAndEventBasedTest_

    Test event based job splitting.
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
            newFile = File(makeUUID(), size = 1000, events = 150,
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

        self.zeroEventFileset = Fileset(name = "TestFileset3")
        self.zeroEventFileset.create()
        zeroEventFile = File("/some/file/name", size = 1000, events = 0,
                             locations = Set(["somese.cern.ch"]))
        zeroEventFile.create()
        self.zeroEventFileset.addFile(zeroEventFile)
        self.zeroEventFileset.commit()        

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001")
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "FileAndEventBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "FileAndEventBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()
        self.zeroEventSubscription = Subscription(fileset = self.zeroEventFileset,
                                                  workflow = testWorkflow,
                                                  split_algo = "FileAndEventBased",
                                                  type = "Processing")
        self.zeroEventSubscription.create()        
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

    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 100)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."
        
        assert job.mask.getMaxEvents() == 100, \
               "ERROR: Job's max events is incorrect."
        
        assert job.mask["FirstEvent"] == 0, \
               "ERROR: Job's first event is incorrect."

        return

    def testZeroEvents(self):
        """
        _testZeroEvents_

        Test how the job splitting code works with 0 event files.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.zeroEventSubscription)

        jobGroups = jobFactory(events_per_job = 100)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."
        
        assert job.mask.getMaxEvents() == 100, \
               "ERROR: Job's max events is incorrect."
        
        assert job.mask["FirstEvent"] == 0, \
               "ERROR: Job's first event is incorrect."

        return    

    def testMoreEvents(self):
        """
        _testMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        
        jobGroups = jobFactory(events_per_job = 1000)
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."
        
        assert job.mask.getMaxEvents() == 1000, \
               "ERROR: Job's max events is incorrect."
        
        assert job.mask["FirstEvent"] == 0, \
               "ERROR: Job's first event is incorrect."

        return

    def test50EventSplit(self):
        """
        _test50EventSplit_

        Test event based job splitting when the number of events per job is
        50, this should result in two jobs.        
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        
        jobGroups = jobFactory(events_per_job = 50)
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 2, \
               "ERROR: JobFactory didn't create two jobs."

        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."
        
            assert job.mask.getMaxEvents() == 50, \
                   "ERROR: Job's max events is incorrect."
        
            assert job.mask["FirstEvent"] in [0, 50], \
                   "ERROR: Job's first event is incorrect."

        return

    def test99EventSplit(self):
        """
        _test99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs.        
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        
        jobGroups = jobFactory(events_per_job = 99)
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 2, \
               "ERROR: JobFactory didn't create two jobs."

        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."
        
            assert job.mask.getMaxEvents() == 99, \
                   "ERROR: Job's max events is incorrect."
        
            assert job.mask["FirstEvent"] in [0, 99], \
                   "ERROR: Job's first event is incorrect."

        return

    def test100EventMultipleFileSplit(self):
        """
        _test100EventMultipleFileSplit_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 150)

        assert len(jobGroups) == 10, \
               "ERROR: JobFactory didn't return one JobGroup."

        for jobGroup in jobGroups:
            assert len(jobGroup.jobs) == 1, \
                   "ERROR: JobFactory didn't create one job."
        
            for job in jobGroup.jobs:
                assert len(job.getFiles(type = "lfn")) == 1, \
                       "ERROR: Job contains too many files."
        
                assert job.mask.getMaxEvents() == 150, \
                       "ERROR: Job's max events is incorrect."
        
                assert job.mask["FirstEvent"] == 0, \
                       "ERROR: Job's first event is incorrect."

        return
    
    def test50EventMultipleFileSplit(self):
        """
        _test50EventMultipleFileSplit_

        Test job splitting into 50 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)        

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50)

        assert len(jobGroups) == 10, \
               "ERROR: JobFactory didn't return ten JobGroups."

        for jobGroup in jobGroups:
            assert len(jobGroups[0].jobs) == 3, \
                   "ERROR: JobFactory didn't create three jobs."
        
            for job in jobGroup.jobs:
                assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        
                assert job.mask.getMaxEvents() == 50, \
                       "ERROR: Job's max events is incorrect."
        
                assert job.mask["FirstEvent"] in [0, 50, 100], \
                       "ERROR: Job's first event is incorrect."

        return

    def test150EventMultipleFileSplit(self):
        """
        _test150EventMultipleFileSplit_

        Test job splitting into 150 event jobs when the input subscription has
        more than one file available.  This test verifies that the job splitting
        code will put at most one file in a job.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)        

        jobGroups = jobFactory(events_per_job = 100)

        assert len(jobGroups) == 10, \
               "ERROR: JobFactory didn't return ten JobGroups."

        for jobGroup in jobGroups:
            assert len(jobGroup.jobs) == 2, \
               "ERROR: JobFactory didn't create 2 jobs."
            for job in jobGroup.jobs:
                assert len(job.getFiles(type = "lfn")) == 1, \
                       "ERROR: Job contains too many files."
        
                assert job.mask.getMaxEvents() == 100, \
                       "ERROR: Job's max events is incorrect."
        
                assert job.mask["FirstEvent"] in [0, 100], \
                       "ERROR: Job's first event is incorrect."

        return    

    def selectionAlgorithm( self, wmbsFile ):
        """
        Needed to test selective splitting
        """
        if wmbsFile['id'] == 1:
            return False
        return True

    def testSelectiveFileSplit(self):
        """
        _testSelectiveFileSplit_

        Test job splitting event jobs when the input subscription has
        more than one file available and we select only one of them.
        Should only get nine job groups back.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)        

        jobGroups = jobFactory(events_per_job = 100,
                               selection_algorithm = self.selectionAlgorithm )

        assert len(jobGroups) == 9, \
               "ERROR: JobFactory didn't return nine JobGroups."

        for jobGroup in jobGroups:
            assert len(jobGroup.jobs) == 2, \
               "ERROR: JobFactory didn't create 2 jobs."
            for job in jobGroup.jobs:
                assert len(job.getFiles(type = "lfn")) == 1, \
                       "ERROR: Job contains too many files."
        
                assert job.mask.getMaxEvents() == 100, \
                       "ERROR: Job's max events is incorrect."
        
                assert job.mask["FirstEvent"] in [0, 100], \
                       "ERROR: Job's first event is incorrect."

        return    

if __name__ == '__main__':
    unittest.main()
