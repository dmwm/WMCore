#!/usr/bin/env python
"""
_LumiBased_t

Test lumi based splitting.
"""




import os
import threading
import logging
import unittest
import random

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMQuality.TestInit import TestInit

class LumiBasedTest(unittest.TestCase):
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
        #self.testInit.clearDatabase(modules = ['WMCore.WMBS'])
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = 's1', seName = "somese.cern.ch")
        locationAction.execute(siteName = 's2', seName = "otherse.cern.ch")

        self.testWorkflow = Workflow(spec = "spec.xml", owner = "mnorman",
                                     name = "wf001", task="Test")
        self.testWorkflow.create()


        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """

        self.testInit.clearDatabase()

        
        return


    def createSubscription(self, nFiles, lumisPerFile, twoSites = False, rand = False):
        """
        _createSubscription_
        
        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name = baseName)
        testFileset.create()
        for i in range(nFiles):
            newFile = File(lfn = '%s_%i' % (baseName, i), size = 1000,
                           events = 100, locations = "somese.cern.ch")
            lumis = []
            for lumi in range(lumisPerFile):
                if rand:
                    lumis.append(random.randint(1000 * i, 1000 * (i + 1)))
                else:
                    lumis.append((100 * i) + lumi)
            newFile.addRun(Run(i, *lumis))
            newFile.create()
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = File(lfn = '%s_%i_2' % (baseName, i), size = 1000,
                               events = 100, locations = "otherse.cern.ch")
                lumis = []
                for lumi in range(lumisPerFile):
                    if rand:
                        lumis.append(random.randint(1000 * i, 1000 * (i + 1)))
                    else:
                        lumis.append((100 * i) + lumi)
                newFile.addRun(Run(i, *lumis))
                newFile.create()
                testFileset.addFile(newFile)
        testFileset.commit()


        testSubscription  = Subscription(fileset = testFileset,
                                         workflow = self.testWorkflow,
                                         split_algo = "LumiBased",
                                         type = "Processing")
        testSubscription.create()

        return testSubscription



    def testA_NoFileSplitting(self):
        """
        _NoFileSplitting_

        Test that things work if we do no file splitting
        """

        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles = 10, lumisPerFile = 1)
        jobFactory = splitter(package = "WMCore.WMBS",
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
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 3)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 3)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']) in [1, 2])


        # Now do five files with two lumis per file
        tooBigFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = tooBigFiles)
        jobGroups = jobFactory(lumis_per_job = 1)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)


        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles = 5, lumisPerFile = 1, twoSites = True)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job = 1)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 5)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)



        # Lumi test?
        lumiSubscription = self.createSubscription(nFiles = 3, lumisPerFile = 20,
                                                   twoSites = False, rand = True)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = lumiSubscription)
        jobGroups = jobFactory(lumis_per_job = 100)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs[0]
        for f in job['input_files']:
            for run in f['runs']:
                self.assertEqual(run.lumis, sorted(run.lumis))


        #myThread = threading.currentThread()
        #print myThread.dbi.processData("SELECT site_name FROM wmbs_location")[0].fetchall()

        return

            


        



    def testB_FileSplitting(self):
        """
        _FileSplitting_

        Test that things work if we split files between jobs
        """

        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles = 10, lumisPerFile = 1)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = oneSetSubscription)


        jobGroups = jobFactory(lumis_per_job = 3,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)




        twoLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 2)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job = 1,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)



        wholeLumiFiles = self.createSubscription(nFiles = 5, lumisPerFile = 3)
        jobFactory = splitter(package = "WMCore.WMBS",
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
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job = 1,
                               split_files_between_job = True)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)

            
        
        


if __name__ == '__main__':
    unittest.main()
