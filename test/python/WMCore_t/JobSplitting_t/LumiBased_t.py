#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

__revision__ = "$Id: LumiBased_t.py,v 1.6 2009/11/19 21:00:02 mnorman Exp $"
__version__ = "$Revision: 1.6 $"

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
            newFile.setLocation('blenheim')
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.addRun(Run(1, *[45]))
        newFile.setLocation('blenheim')
        self.singleFileFileset.addFile(newFile)

        self.multipleFileLumiset = Fileset(name = "TestFileset3")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45+i/3]))
            newFile.setLocation('blenheim')
            self.multipleFileLumiset.addFile(newFile)

        self.singleLumiFileset = Fileset(name = "TestFileset4")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45]))
            newFile.setLocation('blenheim')
            self.singleLumiFileset.addFile(newFile)
        self.multipleSiteLumiset = Fileset(name = "TestFileset5")
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45+i/3]))
            newFile.setLocation('blenheim')
            self.multipleSiteLumiset.addFile(newFile)
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.addRun(Run(1, *[45+i/3]))
            newFile.setLocation('malpaquet')
            self.multipleSiteLumiset.addFile(newFile)

        self.multiRunFileset = Fileset(name = "TestFileset5")
        for j in range(4):
            newFile = File(makeUUID(), size = 1000, events = 100)
            for i in range(5):
                # Eric's test which splits each run across two files, lumis 45-54 for each run
                #lumiNum = 45+i*j
                #if lumiNum > 64:
                #    lumiNum -= 20
                #newFile.addRun(Run(1+j/2, *[lumiNum]))
                run = Run(1+j, 45 + i)
                newFile.addRun(run)
            newFile.setLocation('blenheim')
            self.multiRunFileset.addFile(newFile)



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
        self.multipleSiteSubscription  = Subscription(fileset = self.multipleSiteLumiset,
                                                      workflow = testWorkflow,
                                                      split_algo = "LumiBased",
                                                      type = "Processing")
        self.multiRunSubscription      = Subscription(fileset = self.multiRunFileset,
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

#        jobGroups = jobFactory(files_per_job = 1)
#
#
#        self.assertEqual(len(jobGroups),         1)
#        self.assertEqual(len(jobGroups[0].jobs), 10)
#
#        jobGroup2 = jobFactory(files_per_job = 2)
#
#        self.assertEqual(len(jobGroup2),         1)
#        self.assertEqual(len(jobGroup2[0].jobs), 5)
#
        jobGroup3 = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroup3),                       1)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 8)
        self.assertEqual(len(jobGroup3[0].jobs[1].getFiles()), 2)

        return


    def testLumiBasedSplitting(self):
        """
        _testLumiBasedSplitting_

        Test lumi based job splitting with multiple files from four different
        lumis
        """

        print "testLumiBasedSplitting"

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleLumiSubscription)

#        jobGroups = jobFactory(lumis_per_job = 1)
#
#
#        self.assertEqual(len(jobGroups),         1)
#        self.assertEqual(len(jobGroups[0].jobs), 4)
#
#        jobGroup2 = jobFactory(lumis_per_job = 2)
#
#        self.assertEqual(len(jobGroup2),                       1)
#        self.assertEqual(len(jobGroup2[0].jobs),               2)
#        self.assertEqual(len(jobGroup2[0].jobs[0].getFiles()), 6)
#        self.assertEqual(len(jobGroup2[0].jobs[1].getFiles()), 4)

        jobGroup3 = jobFactory(lumis_per_job = 4)

        self.assertEqual(len(jobGroup3),                       1)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 10)

        return



    def testEventBasedSplitting(self):
        """
        _testEventBasedSplitting_

        Test event based job splitting with multiple files belonging
        to a single lumi section
        """

        print "testEventBasedSplitting"

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleLumiSubscription)

#        jobGroups = jobFactory(events_per_job = 100)
#
#
#        self.assertEqual(len(jobGroups),         1)
#        self.assertEqual(len(jobGroups[0].jobs), 10)
#
#        jobGroup2 = jobFactory(events_per_job = 220)
#
#        self.assertEqual(len(jobGroup2),                       1)
#        self.assertEqual(len(jobGroup2[0].jobs),               5)
#        self.assertEqual(len(jobGroup2[0].jobs[0].getFiles()), 2)
#        self.assertEqual(len(jobGroup2[0].jobs[1].getFiles()), 2)

        jobGroup3 = jobFactory(events_per_job = 800)

        self.assertEqual(len(jobGroup3),                       1)
        self.assertEqual(len(jobGroup3[0].jobs),               2)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 8)

        return



    def testMultipleLumi(self):
        """
        _testMultipleLumi_


        Test lumi based job splitting with 10 files, split between four different lumi sections
        but with jobs limited by number of files


        """

        print "testMultipleLumi"

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleLumiSubscription)

#        jobGroups = jobFactory(files_per_job = 1)
#
#
#        self.assertEqual(len(jobGroups),         4)
#        self.assertEqual(len(jobGroups[0].jobs), 3)
#        self.assertEqual(len(jobGroups[3].jobs), 1)
#        self.assertEqual(len(jobGroups[1].jobs), 3)
#        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 1)
#
#        jobGroup2 = jobFactory(files_per_job = 2)
#
#        self.assertEqual(len(jobGroup2),         4)
#        self.assertEqual(len(jobGroup2[3].jobs), 1)
#        self.assertEqual(len(jobGroup2[1].jobs), 2)
#        self.assertEqual(len(jobGroup2[1].jobs[0].getFiles()), 2)

        jobGroup3 = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroup3),                       4)
        self.assertEqual(len(jobGroup3[3].jobs),               1)
        self.assertEqual(len(jobGroup3[1].jobs[0].getFiles()), 3)


        return


    def testMultipleRun(self):
        """
        _testMultipleRun_

        Test lumi based job splitting with 4 files with different run #'s but the same lumi numbers

        """

        print "testMultipleRun"

        fileset = self.multiRunSubscription.availableFiles()

        splitter = SplitterFactory()
        jobFactory = splitter(self.multiRunSubscription)

#        jobGroups = jobFactory(lumis_per_job = 5)
#
#        self.assertEqual(len(jobGroups),         1)
#        self.assertEqual(len(jobGroups[0].jobs), 4)
#        self.assertEqual(len(jobGroups[0].jobs[0].getFiles()), 1)

        jobGroup2 = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroup2),         4)
        self.assertEqual(len(jobGroup2[1].jobs), 1)
        self.assertEqual(len(jobGroup2[1].jobs[0].getFiles()), 1)

        return



    def testMultipleSites(self):
        """
        _testMultipleLumi_

        Test lumi based job splitting with 20 files, with 10 each in two different sites

        """

        print "testMultipleSites"

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleSiteSubscription)

#        jobGroups = jobFactory(lumis_per_job = 1)
#
#
#        self.assertEqual(len(jobGroups),         2)
#        self.assertEqual(len(jobGroups[0].jobs), 4)
#        self.assertEqual(len(jobGroups[1].jobs), 4)
#        self.assertEqual(len(jobGroups[0].jobs[0].getFiles()), 3)
#        self.assertEqual(len(jobGroups[0].jobs[3].getFiles()), 1)
#
#        jobGroup2 = jobFactory(files_per_job = 2)
#
#        self.assertEqual(len(jobGroup2),         8)
#        self.assertEqual(len(jobGroup2[3].jobs), 1)
#        self.assertEqual(len(jobGroup2[1].jobs), 2)
#        self.assertEqual(len(jobGroup2[1].jobs[0].getFiles()), 2)

        jobGroup3 = jobFactory(events_per_job = 100)

        self.assertEqual(len(jobGroup3),                       8)
        self.assertEqual(len(jobGroup3[1].jobs),               3)
        self.assertEqual(len(jobGroup3[0].jobs[0].getFiles()), 1)


        return


if __name__ == '__main__':
    unittest.main()
