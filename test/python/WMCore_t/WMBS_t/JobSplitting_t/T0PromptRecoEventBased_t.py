#!/usr/bin/env python
"""
_T0PromptRecoEventBased_t_

T0 Prompt Reco event based splitting test.
"""

__revision__ = "$Id: T0PromptRecoEventBased_t.py,v 1.2 2009/10/27 09:03:44 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

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
from WMCore.DataStructs.Run import Run

class EventBasedTest(unittest.TestCase):
    """
    _EventBasedTest_

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
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        # The run table is T0 specific, we'll re-create the portion of it that
        # is used by this splitting algorithm here.
        create = "CREATE TABLE run (run_id INT, reco_started INT)"
        insertA = "INSERT INTO run (run_id, reco_started) VALUES (1, 1)"
        insertB = "INSERT INTO run (run_id, reco_started) VALUES (2, 0)"
        insertC = "INSERT INTO run (run_id, reco_started) VALUES (3, 0)"        
        
        myThread = threading.currentThread()

        myThread.dbi.processData(create)
        myThread.dbi.processData(insertA)
        myThread.dbi.processData(insertB)
        myThread.dbi.processData(insertC)

        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "somese.cern.ch")
        
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = Set(["somese.cern.ch"]))
            newFile.addRun(Run(1, *[45]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)

        newFile = File(makeUUID(), size = 1000, events = 100,
                       locations = Set(["somese.cern.ch"]))
        newFile.addRun(Run(2, *[45]))
        newFile.create()
        self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = Set(["somese.cern.ch"]))
        newFile.addRun(Run(1, *[45]))        
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        newFile = File("/some/file/name2", size = 1000, events = 100,
                       locations = Set(["somese.cern.ch"]))
        newFile.addRun(Run(2, *[45]))        
        newFile.create()
        self.singleFileFileset.addFile(newFile)        
        self.singleFileFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Bogus")
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "T0PromptRecoEventBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "T0PromptRecoEventBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()

        myThread = threading.currentThread()

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
            
        myThread.transaction.begin()

        delete = "DROP TABLE run"
        myThread.dbi.processData(delete)
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
        
        assert job["mask"].getMaxEvents() == 100, \
               "ERROR: Job's max events is incorrect."
        
        assert job["mask"]["FirstEvent"] == 0, \
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
        
        assert job["mask"].getMaxEvents() == 1000, \
               "ERROR: Job's max events is incorrect."
        
        assert job["mask"]["FirstEvent"] == 0, \
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
        
            assert job["mask"].getMaxEvents() == 50, \
                   "ERROR: Job's max events is incorrect."
        
            assert job["mask"]["FirstEvent"] in [0, 50], \
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
        
            assert job["mask"].getMaxEvents() == 99, \
                   "ERROR: Job's max events is incorrect."
        
            assert job["mask"]["FirstEvent"] in [0, 99], \
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

        jobGroups = jobFactory(events_per_job = 100)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 10, \
               "ERROR: JobFactory didn't create 10 jobs: %s" % len(jobGroups[0].jobs)
        
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        
            assert job["mask"].getMaxEvents() == 100, \
                   "ERROR: Job's max events is incorrect."
        
            assert job["mask"]["FirstEvent"] == 0, \
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

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory didn't create 20 jobs."
        
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        
            assert job["mask"].getMaxEvents() == 50, \
                   "ERROR: Job's max events is incorrect."
        
            assert job["mask"]["FirstEvent"] in [0, 50], \
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

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 150)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 10, \
               "ERROR: JobFactory didn't create 10 jobs."
        
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        
            assert job["mask"].getMaxEvents() == 150, \
                   "ERROR: Job's max events is incorrect."
        
            assert job["mask"]["FirstEvent"] == 0, \
                   "ERROR: Job's first event is incorrect."

        return    

if __name__ == '__main__':
    unittest.main()
