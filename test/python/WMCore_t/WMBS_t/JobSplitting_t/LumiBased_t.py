#!/usr/bin/env python
"""
_LumiBased_t

Test lumi based splitting.
"""

__revision__ = "$Id: LumiBased_t.py,v 1.9 2010/04/13 20:53:40 mnorman Exp $"
__version__ = "$Revision: 1.9 $"

import os
import threading
import logging
import unittest

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

        
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "somese.cern.ch")
            newFile.addRun(Run(i, *[45+i]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100, locations = "somese.cern.ch")
        newFile.addRun(Run(1, *[45]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()

        self.multipleFileLumiset = Fileset(name = "TestFileset3")
        self.multipleFileLumiset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "somese.cern.ch")
            newFile.addRun(Run(1, *[45+i/3]))
            newFile.create()
            self.multipleFileLumiset.addFile(newFile)
        self.multipleFileLumiset.commit()

        self.singleLumiFileset = Fileset(name = "TestFileset4")
        self.singleLumiFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "somese.cern.ch")
            newFile.addRun(Run(1, *[45]))
            newFile.create()
            self.singleLumiFileset.addFile(newFile)
        self.singleLumiFileset.commit()

        self.multiSiteFileset = Fileset(name = "TestFileset5")
        self.multiSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "somese.cern.ch")
            newFile.addRun(Run(1, *[45]))
            newFile.create()
            self.multiSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "otherse.cern.ch")
            newFile.addRun(Run(1, *[45]))
            newFile.create()
            self.multiSiteFileset.addFile(newFile)
        self.multiSiteFileset.commit()
            

        testWorkflow = Workflow(spec = "spec.xml", owner = "mnorman", name = "wf001", task="Test")
        testWorkflow.create()
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
        self.multiSiteSubscription     = Subscription(fileset = self.multiSiteFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "LumiBased",
                                                      type = "Processing")


        self.multipleFileSubscription.create()
        self.singleFileSubscription.create()
        self.multipleLumiSubscription.create()
        self.singleLumiSubscription.create()
        self.multiSiteSubscription.create()


        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
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

    def testExactLumi(self):
        """
        _testExactLumi_

        Test lumi based job splitting when the lumi per file is
        exactly the same as the lumi in the input file.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

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

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleFileSubscription)

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

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleLumiSubscription)

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

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleLumiSubscription)

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

        myThread = threading.currentThread()
        
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.singleLumiSubscription)

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

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multipleLumiSubscription)

        jobGroup3 = jobFactory(files_per_job = 8)

        self.assertEqual(len(jobGroup3),                       4)
        self.assertEqual(len(jobGroup3[1].jobs),               1)
        self.assertEqual(len(jobGroup3[1].jobs[0].getFiles()), 3)

        
        return



    def testMultiSite(self):
        """
        _testMultiSite_

        Test lumiBased when there are files from two seperate groups of sites
        """

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = self.multiSiteSubscription)

        jobGroupList = jobFactory(lumis_per_job = 2)

        self.assertEqual(len(jobGroupList), 2)

        jobGroup1 = jobGroupList[0]
        jobGroup2 = jobGroupList[1]
        self.assertEqual(len(jobGroup1.jobs[0]['input_files'].files), 5)
        self.assertEqual(len(jobGroup2.jobs[0]['input_files'].files), 5)
        for file in jobGroup1.jobs[0]['input_files'].files:
            self.assertEqual(file['locations'], set(['somese.cern.ch']))
        for file in jobGroup2.jobs[0]['input_files'].files:
            self.assertEqual(file['locations'], set(['otherse.cern.ch']))


        return





    
if __name__ == '__main__':
    unittest.main()
