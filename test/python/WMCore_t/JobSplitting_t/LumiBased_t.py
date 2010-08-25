#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

__revision__ = "$Id: LumiBased_t.py,v 1.1 2009/06/05 16:31:36 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from sets import Set
import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

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
            newFile.addRun(Run(i, *[45+i]))
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.addRun(Run(1, *[45]))
        self.singleFileFileset.addFile(newFile)

        self.multipleFileLumiset = Fileset(name = "TestFileset3")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45+i/3]))
            self.multipleFileLumiset.addFile(newFile)

        self.singleLumiFileset = Fileset(name = "TestFileset4")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45]))
            self.singleLumiFileset.addFile(newFile)
            

        testWorkflow = Workflow()
        self.multipleFileSubscription  = Subscription(fileset = self.multipleFileFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "LumiBased",
                                                      type = "Processing")
        self.singleFileSubscription    = Subscription(fileset = self.singleFileFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "LumiBased",
                                                      type = "Processing")
        self.multipleLumiSubscription  = Subscription(fileset = self.multipleFileLumiset,
                                                      workflow = testWorkflow,
                                                      split_algo = "LumiBased",
                                                      type = "Processing")
        self.singleLumiSubscription    = Subscription(fileset = self.singleLumiFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "LumiBased",
                                                      type = "Processing")


        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def testExactLumi(self):
        """
        _testExactLumi_

        Test lumi based job splitting when the lumi per file is
        exactly the same as the lumi in the input file.
        """

        print "testExactLumi"

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(lumis_per_job = 1)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."
        
        return


    def testMoreLumi(self):
        """
        _testMoreLumi_

        Test lumi based job splitting when the lumi per job is
        more than the lumis in the input file.
        """

        print "testMoreLumi"

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(lumis_per_job = 2)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(jobGroups[0].jobs[0].getFiles(type = "lfn"), ["/some/file/name"])
        
        return




    def testFileBasedSplitting(self):
        """
        _testFileBasedSplitting_

        Test lumi based job splitting with multiple files from the
        same lumi
        """

        print "testFileBasedSplitting"
        
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleLumiSubscription)

        jobGroups = jobFactory(files_per_job = 1)


        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 10)

        jobGroup2 = jobFactory(files_per_job = 2)

        self.assertEqual(len(jobGroup2),         1)
        self.assertEqual(len(jobGroup2[0].jobs), 5)

        jobGroup3 = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroup3),                       1)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 8)
        self.assertEqual(len(jobGroup3[0].jobs[1].getFiles()), 2)
        
        return


    def testLumiBasedSplitting(self):
        """
        _testLumiBasedSplitting_

        Test lumi based job splitting with multiple files from multiple
        lumis
        """

        print "testLumiBasedSplitting"
        
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleLumiSubscription)

        jobGroups = jobFactory(lumis_per_job = 1)


        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 4)

        jobGroup2 = jobFactory(lumis_per_job = 2)

        self.assertEqual(len(jobGroup2),                       1)
        self.assertEqual(len(jobGroup2[0].jobs),               2)
        self.assertEqual(len(jobGroup2[0].jobs[0].getFiles()), 4)
        self.assertEqual(len(jobGroup2[0].jobs[1].getFiles()), 6)

        jobGroup3 = jobFactory(lumis_per_job = 4)

        self.assertEqual(len(jobGroup3),                       1)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 10)
        
        return



    def testEventBasedSplitting(self):
        """
        _testEventBasedSplitting_

        Test event based job splitting with multiple files from
        a single lumi
        """

        print "testEventBasedSplitting"
        
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleLumiSubscription)

        jobGroups = jobFactory(events_per_job = 100)


        self.assertEqual(len(jobGroups),         1)
        self.assertEqual(len(jobGroups[0].jobs), 10)

        jobGroup2 = jobFactory(events_per_job = 220)

        self.assertEqual(len(jobGroup2),                       1)
        self.assertEqual(len(jobGroup2[0].jobs),               5)
        self.assertEqual(len(jobGroup2[0].jobs[0].getFiles()), 2)
        self.assertEqual(len(jobGroup2[0].jobs[1].getFiles()), 2)

        jobGroup3 = jobFactory(events_per_job = 800)

        self.assertEqual(len(jobGroup3),                       1)
        self.assertEqual(len(jobGroup3[0].jobs),               2)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 8)
        
        return



    def testMultipleLumi(self):
        """
        _testMultipleLumi_

        Test lumi based job splitting with 10 files, each with different lumis
        
        """

        print "testMultipleLumi"

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleLumiSubscription)

        jobGroups = jobFactory(files_per_job = 1)


        self.assertEqual(len(jobGroups),         4)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(len(jobGroups[1].jobs), 3)
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 1)

        jobGroup2 = jobFactory(files_per_job = 2)

        self.assertEqual(len(jobGroup2),         4)
        self.assertEqual(len(jobGroup2[0].jobs), 1)
        self.assertEqual(len(jobGroup2[1].jobs), 2)
        self.assertEqual(len(jobGroup2[1].jobs[0].getFiles()), 2)

        jobGroup3 = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroup3),                       4)
        self.assertEqual(len(jobGroup3[1].jobs),               1)
        self.assertEqual(len(jobGroup3[1].jobs[0].getFiles()), 3)

        
        return
        







    
#
#    def testMoreEvents(self):
#        """
#        _testMoreEvents_
#
#        Test event based job splitting when the number of events per job is
#        greater than the number of events in the input file.
#        """
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.singleFileSubscription)
#        
#        jobGroups = jobFactory(events_per_job = 1000)
#        
#        assert len(jobGroups) == 1, \
#               "ERROR: JobFactory didn't return one JobGroup."
#
#        assert len(jobGroups[0].jobs) == 1, \
#               "ERROR: JobFactory didn't create a single job."
#
#        job = jobGroups[0].jobs.pop()
#
#        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
#               "ERROR: Job contains unknown files."
#        
#        assert job.mask.getMaxEvents() == 1000, \
#               "ERROR: Job's max events is incorrect."
#        
#        assert job.mask["FirstEvent"] == 0, \
#               "ERROR: Job's first event is incorrect."
#
#        return
#
#    def test50EventSplit(self):
#        """
#        _test50EventSplit_
#
#        Test event based job splitting when the number of events per job is
#        50, this should result in two jobs.        
#        """
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.singleFileSubscription)
#        
#        jobGroups = jobFactory(events_per_job = 50)
#        
#        assert len(jobGroups) == 1, \
#               "ERROR: JobFactory didn't return one JobGroup."
#
#        assert len(jobGroups[0].jobs) == 2, \
#               "ERROR: JobFactory didn't create two jobs."
#
#        firstEvents = []
#        for job in jobGroups[0].jobs:
#            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
#                   "ERROR: Job contains unknown files."
#        
#            assert job.mask.getMaxEvents() == 50, \
#                   "ERROR: Job's max events is incorrect."
#        
#            assert job.mask["FirstEvent"] in [0, 50], \
#                   "ERROR: Job's first event is incorrect."
#
#            assert job.mask["FirstEvent"] not in firstEvents, \
#                   "ERROR: Job's first event is repeated."
#            firstEvents.append(job.mask["FirstEvent"])
#
#        return
#
#    def test99EventSplit(self):
#        """
#        _test99EventSplit_
#
#        Test event based job splitting when the number of events per job is
#        99, this should result in two jobs.        
#        """
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.singleFileSubscription)
#        
#        jobGroups = jobFactory(events_per_job = 99)
#        
#        assert len(jobGroups) == 1, \
#               "ERROR: JobFactory didn't return one JobGroup."
#
#        assert len(jobGroups[0].jobs) == 2, \
#               "ERROR: JobFactory didn't create two jobs."
#
#        firstEvents = []
#        for job in jobGroups[0].jobs:
#            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
#                   "ERROR: Job contains unknown files."
#        
#            assert job.mask.getMaxEvents() == 99, \
#                   "ERROR: Job's max events is incorrect."
#        
#            assert job.mask["FirstEvent"] in [0, 99], \
#                   "ERROR: Job's first event is incorrect."
#
#            assert job.mask["FirstEvent"] not in firstEvents, \
#                   "ERROR: Job's first event is repeated."
#            firstEvents.append(job.mask["FirstEvent"])            
#
#        return
#
#    def test100EventMultipleFileSplit(self):
#        """
#        _test100EventMultipleFileSplit_
#
#        Test job splitting into 100 event jobs when the input subscription has
#        more than one file available.
#        """
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.multipleFileSubscription)
#
#        jobGroups = jobFactory(events_per_job = 100)
#
#        assert len(jobGroups) == 1, \
#               "ERROR: JobFactory didn't return one JobGroup."
#
#        assert len(jobGroups[0].jobs) == 10, \
#               "ERROR: JobFactory didn't create 10 jobs."
#        
#        for job in jobGroups[0].jobs:
#            assert len(job.getFiles(type = "lfn")) == 1, \
#                   "ERROR: Job contains too many files."
#        
#            assert job.mask.getMaxEvents() == 100, \
#                   "ERROR: Job's max events is incorrect."
#        
#            assert job.mask["FirstEvent"] == 0, \
#                   "ERROR: Job's first event is incorrect."
#
#        return
#    
#    def test50EventMultipleFileSplit(self):
#        """
#        _test50EventMultipleFileSplit_
#
#        Test job splitting into 50 event jobs when the input subscription has
#        more than one file available.
#        """
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.multipleFileSubscription)        
#
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.multipleFileSubscription)
#
#        jobGroups = jobFactory(events_per_job = 50)
#
#        assert len(jobGroups) == 1, \
#               "ERROR: JobFactory didn't return one JobGroup."
#
#        assert len(jobGroups[0].jobs) == 20, \
#               "ERROR: JobFactory didn't create 20 jobs."
#        
#        for job in jobGroups[0].jobs:
#            assert len(job.getFiles(type = "lfn")) == 1, \
#                   "ERROR: Job contains too many files."
#        
#            assert job.mask.getMaxEvents() == 50, \
#                   "ERROR: Job's max events is incorrect."
#        
#            assert job.mask["FirstEvent"] in [0, 50], \
#                   "ERROR: Job's first event is incorrect."
#
#        return
#
#    def test150EventMultipleFileSplit(self):
#        """
#        _test150EventMultipleFileSplit_
#
#        Test job splitting into 150 event jobs when the input subscription has
#        more than one file available.  This test verifies that the job splitting
#        code will put at most one file in a job.
#        """
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.multipleFileSubscription)        
#
#        splitter = SplitterFactory()
#        jobFactory = splitter(self.multipleFileSubscription)
#
#        jobGroups = jobFactory(events_per_job = 150)
#
#        assert len(jobGroups) == 1, \
#               "ERROR: JobFactory didn't return one JobGroup."
#
#        assert len(jobGroups[0].jobs) == 10, \
#               "ERROR: JobFactory didn't create 10 jobs."
#        
#        for job in jobGroups[0].jobs:
#            assert len(job.getFiles(type = "lfn")) == 1, \
#                   "ERROR: Job contains too many files."
#        
#            assert job.mask.getMaxEvents() == 150, \
#                   "ERROR: Job's max events is incorrect."
#        
#            assert job.mask["FirstEvent"] == 0, \
#                   "ERROR: Job's first event is incorrect."
#
#        return    

if __name__ == '__main__':
    unittest.main()
