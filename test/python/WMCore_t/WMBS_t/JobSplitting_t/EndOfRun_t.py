#!/usr/bin/env python
"""
_EndOfRun_t_

End of run splitting test
"""

__revision__ = "$Id: EndOfRun_t.py,v 1.6 2010/05/07 14:36:45 mnorman Exp $"
__version__ = "$Revision: 1.6 $"

import unittest
import os
import threading

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

class EndOfRunTest(unittest.TestCase):
    """
    _EndOfRun_t_
    
    End of run splitting test
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


        self.multipleSiteFileset = Fileset(name = "TestFileset5")
        self.multipleSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "somese.cern.ch")
            newFile.addRun(Run(i, *[45+i]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100, locations = "otherse.cern.ch")
            newFile.addRun(Run(i, *[45+i]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()
            

        testWorkflow = Workflow(spec = "spec.xml", owner = "mnorman", name = "wf001", task="Test")
        testWorkflow.create()
        self.multipleFileSubscription  = Subscription(fileset = self.multipleFileFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "EndOfRun",
                                                      type = "Processing")
        self.singleFileSubscription    = Subscription(fileset = self.singleFileFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "EndOfRun",
                                                      type = "Processing")
        self.multipleLumiSubscription  = Subscription(fileset = self.multipleFileLumiset,
                                                      workflow = testWorkflow,
                                                      split_algo = "EndOfRun",
                                                      type = "Processing")
        self.singleLumiSubscription    = Subscription(fileset = self.singleLumiFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "EndOfRun",
                                                      type = "Processing")
        self.multipleSiteSubscription  = Subscription(fileset = self.multipleSiteFileset,
                                                      workflow = testWorkflow,
                                                      split_algo = "EndOfRun",
                                                      type = "Processing")

        self.multipleFileSubscription.create()
        self.singleFileSubscription.create()
        self.multipleLumiSubscription.create()
        self.singleLumiSubscription.create()
        self.multipleSiteSubscription.create()


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
    
    def testNone(self):
        """
        _testNone_
        since the subscriptions are open, we shouldn't get any jobs back
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")

        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
        return
    
    def testClosed(self):
        """
        _testClosed_
        since the subscriptions are closed and none of the files have been
        acquired, all of the files should show up
        """
        splitter = SplitterFactory()
        self.singleFileSubscription.getFileset().markOpen(False)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)
        jobGroups = jobFactory()
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."
        
        self.multipleFileSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory()

        self.assertEquals(len(jobGroups), 1)
        self.assertEquals(len(jobGroups[0].jobs),1)
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEquals(len(myfiles), 10)
        
        self.multipleLumiSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(len(jobGroups), 1)
        self.assertEquals(len(jobGroups[0].jobs),1)
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEquals(len(myfiles), 10)
        #self.assertEquals(jobGroups, [], "Should have returned a null set")

        self.singleLumiSubscription.getFileset().markOpen(False)
        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory()
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEquals(len(myfiles), 10)
        
        
    def testAllAcquired(self):
        """
        _testAllAcquired_
        should all return no job groups
        """
        splitter = SplitterFactory()
        self.singleFileSubscription.acquireFiles(
                           self.singleFileSubscription.availableFiles())
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
        self.multipleFileSubscription.acquireFiles(
                           self.multipleFileSubscription.availableFiles())
        jobFactory = splitter(self.multipleFileSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
        self.multipleLumiSubscription.acquireFiles(
                           self.multipleLumiSubscription.availableFiles())
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")

        self.singleLumiSubscription.acquireFiles(
                           self.singleLumiSubscription.availableFiles())
        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
    def testClosedSomeAcquired(self):
        """
        _testClosedSomeAcquired_
        since the subscriptions are closed and none of the files ahve been
        acquired, all of the files should show up
        """
        splitter = SplitterFactory()
        self.multipleFileSubscription.getFileset().markOpen(False)
        self.singleFileSubscription.acquireFiles(
                           [self.singleFileSubscription.availableFiles().pop()])
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)
        jobGroups = jobFactory()
        self.assertEquals(jobGroups, [], "Should have returned a null set")
        
        
        
        self.multipleFileSubscription.getFileset().markOpen(False)
        self.multipleFileSubscription.acquireFiles(
                           [self.multipleFileSubscription.availableFiles().pop()])
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription =self.multipleFileSubscription)
        jobGroups = jobFactory()
        self.assertEquals(len(jobGroups),1, "Should have gotten one jobGroup")
        self.assertEquals(len(jobGroups[0].jobs), 1, \
               "JobFactory should have made one job")
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEquals(len(myfiles), 9, \
                "JobFactory should have provides us with 9 files")
        
        self.multipleLumiSubscription.getFileset().markOpen(False)
        self.multipleLumiSubscription.acquireFiles(
                           [self.multipleLumiSubscription.availableFiles().pop()])
        jobFactory = splitter(self.multipleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(len(jobGroups),1, "Should have gotten one jobGroup")
        self.assertEquals(len(jobGroups[0].jobs), 1, \
               "JobFactory should have made one job")
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEquals(len(myfiles), 9, \
                "JobFactory should have provides us with 9 files")
        
        self.singleLumiSubscription.getFileset().markOpen(False)
        self.singleLumiSubscription.acquireFiles(
                           [self.singleLumiSubscription.availableFiles().pop()])
        jobFactory = splitter(self.singleLumiSubscription)
        jobGroups = jobFactory()
        self.assertEquals(len(jobGroups),1, "Should have gotten one jobGroup")
        self.assertEquals(len(jobGroups[0].jobs), 1, \
               "JobFactory should have made one job")
        myfiles = jobGroups[0].jobs[0].getFiles()
        self.assertEquals(len(myfiles), 9, \
                "JobFactory should have provides us with 9 files")
        
        self.assertEquals(len(myfiles), 9)


    def testClosed_MultipleJobs(self):
        """
        _testClosed_MultipleJobs_
        
        Should break subscription into jobs based on number of files
        """

        splitter = SplitterFactory()
        self.multipleFileSubscription.getFileset().markOpen(False)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)
        jobGroups = jobFactory(files_per_job = 1)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)

        job = jobGroups[0].jobs[0]

        # We should have one file per job
        self.assertEqual(len(job['input_files']), 1)

        return


    def testClosed_MultipleSites(self):
        """
        _testClosed_MultipleSites_

        Test whether we can handle multiple sites
        """

        splitter = SplitterFactory()
        self.multipleSiteSubscription.getFileset().markOpen(False)
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)
        jobGroups = jobFactory(files_per_job = 10)


        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 2)

        for job in jobGroups[0].jobs:
            self.assertEqual(len(jobGroups[0].jobs[0]['input_files']), 5)
        
        

        

if __name__ == '__main__':
    unittest.main()
