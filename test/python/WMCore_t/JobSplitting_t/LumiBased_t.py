#!/usr/bin/env python
"""
_EventBased_t_

Event based splitting test.
"""




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

        self.testWorkflow = Workflow()

        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass


    def createSubscription(self, nFiles, lumisPerFile, twoSites = False):
        """
        _createSubscription_
        
        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name = baseName)
        for i in range(nFiles):
            newFile = File(lfn = '%s_%i' % (baseName, i), size = 1000,
                           events = 100)
            lumis = []
            for lumi in range(lumisPerFile):
                lumis.append((i * 100) + lumi)
            newFile.addRun(Run(i, *lumis))
            newFile.setLocation('blenheim')
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = File(lfn = '%s_%i_2' % (baseName, i), size = 1000,
                               events = 100)
                lumis = []
                for lumi in range(lumisPerFile):
                    lumis.append((i * 100) + lumi)
                newFile.addRun(Run(i, *lumis))
                newFile.setLocation('malpaquet')
                testFileset.addFile(newFile)


        testSubscription  = Subscription(fileset = testFileset,
                                         workflow = self.testWorkflow,
                                         split_algo = "LumiBased",
                                         type = "Processing")

        return testSubscription



    def testA_NoFileSplitting(self):
        """
        _NoFileSplitting_

        Test that things work if we do no file splitting
        """

        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles = 10, lumisPerFile = 1)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = oneSetSubscription)
        jobGroups = jobFactory(lumis_per_job = 3)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 4)


        # Do some fairly extensive checking
        self.assertEqual(len(jobGroups[0].jobs[0]['input_files']), 3)
        self.assertEqual(jobGroups[0].jobs[0]['mask'],
                         {'LastRun': 2L, 'FirstRun': 0L, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 200L, 'FirstLumi': 0L})
        self.assertEqual(len(jobGroups[0].jobs[1]['input_files']), 3)
        self.assertEqual(jobGroups[0].jobs[1]['mask'],
                         {'LastRun': 5L, 'FirstRun': 3L, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 500L, 'FirstLumi': 300L})
        self.assertEqual(len(jobGroups[0].jobs[2]['input_files']), 3)
        self.assertEqual(jobGroups[0].jobs[2]['mask'],
                         {'LastRun': 8L, 'FirstRun': 6L, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 800L, 'FirstLumi': 600L})
        self.assertEqual(len(jobGroups[0].jobs[3]['input_files']), 1)
        self.assertEqual(jobGroups[0].jobs[3]['mask'],
                         {'LastRun': 9L, 'FirstRun': 9L, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 900L, 'FirstLumi': 900L})





        # Now do five files with two lumis per file
        twoLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 3)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 3)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']) in [1, 2])


        # Now do five files with two lumis per file
        tooBigFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = tooBigFiles)
        jobGroups = jobFactory(lumis_per_job = 1)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)


        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 1, twoSites = True)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job = 1)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)


        return



    def testB_FileSplitting(self):
        """
        _FileSplitting_

        Test that things work if we split files between jobs
        """

        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles = 10, lumisPerFile = 1)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = oneSetSubscription)


        jobGroups = jobFactory(lumis_per_job = 3,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']) in [1, 3])




        twoLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 1,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)



        wholeLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 3)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = wholeLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 2,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        jobList = jobGroups[0].jobs
        for job in jobList:
            self.assertEqual(len(job['input_files']), 1)

        self.assertEqual(jobList[0]['mask'],
                         {'LastRun': 0, 'FirstRun': 0, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 1, 'FirstLumi': 0})
        self.assertEqual(jobList[1]['mask'],
                         {'LastRun': 0, 'FirstRun': 0, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 2, 'FirstLumi': 2})
        self.assertEqual(jobList[2]['mask'],
                         {'LastRun': 1, 'FirstRun': 1, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 101, 'FirstLumi': 100})
        self.assertEqual(jobList[3]['mask'],
                         {'LastRun': 1, 'FirstRun': 1, 'LastEvent': None,
                          'FirstEvent': None, 'LastLumi': 102, 'FirstLumi': 102})



        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 2, twoSites = True)
        jobFactory = splitter(package = "WMCore.DataStructs",
                              subscription = twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job = 1,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)

        return



if __name__ == '__main__':
    unittest.main()
