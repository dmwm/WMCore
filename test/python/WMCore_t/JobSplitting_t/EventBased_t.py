#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

__revision__ = "$Id: EventBased_t.py,v 1.4 2009/02/19 19:51:17 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from sets import Set
import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID

class EventBasedTest(unittest.TestCase):
    """
    _EventBasedTest_

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
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        self.singleFileFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "EventBased",
                                                     type = "Processing")
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "EventBased",
                                                   type = "Processing")
        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

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

        jobGroups = jobFactory(events_per_job = 100)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 10, \
               "ERROR: JobFactory didn't create 10 jobs."
        
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        
            assert job.mask.getMaxEvents() == 100, \
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

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory didn't create 20 jobs."
        
        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        
            assert job.mask.getMaxEvents() == 50, \
                   "ERROR: Job's max events is incorrect."
        
            assert job.mask["FirstEvent"] in [0, 50], \
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
        
            assert job.mask.getMaxEvents() == 150, \
                   "ERROR: Job's max events is incorrect."
        
            assert job.mask["FirstEvent"] == 0, \
                   "ERROR: Job's first event is incorrect."

        return    

if __name__ == '__main__':
    unittest.main()
