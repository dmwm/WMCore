#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""

__revision__ = "$Id: SizeBased_t.py,v 1.4 2009/12/16 19:02:24 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

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

        self.multipleSiteFileset = Fileset(name = "TestFileset3")
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation("somese.cern.ch")
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100)
            newFile.setLocation(["somese.cern.ch","otherse.cern.ch"])
            self.multipleSiteFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "SizeBased",
                                                     type = "Processing")
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "SizeBased",
                                                   type = "Processing")
        self.multipleSiteSubscription = Subscription(fileset = self.multipleSiteFileset,
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

        jobGroups = jobFactory(size_per_job = 1000)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."
        

        return


    def testMultipleFiles(self):
        """
        _testMultipleFiles_
        
        Tests the mechanism for splitting up multiple files into jobs with
        a variety of different arguments.
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups  = jobFactory(size_per_job = 1000)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 1)


        #Test it with two files per job
        jobGroups  = jobFactory(size_per_job = 2000)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)


        #Now test it with a size that can't be broken up evenly
        jobGroups  = jobFactory(size_per_job = 2500)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)


        #Test it with something too small to handle; should return no group
        jobGroups  = jobFactory(size_per_job = 500)
        self.assertEqual(len(jobGroups), 0)

        return


    def testMultipleSites(self):
        """
        _testMultipleSites_

        Tests how to break up files at different locations
        """

        splitter   = SplitterFactory()
        jobFactory = splitter(self.multipleSiteSubscription)

        jobGroups  = jobFactory(size_per_job = 1000)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 1)


if __name__ == '__main__':
    unittest.main()
