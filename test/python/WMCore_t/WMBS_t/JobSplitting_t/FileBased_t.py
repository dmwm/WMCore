#!/usr/bin/env python
"""
_FileBased_t_

File based splitting test.
"""




import unittest
import os
import threading
import logging
import time
import hotshot, hotshot.stats
import cProfile, pstats

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
#from nose.plugins.attrib import attr

class FileBasedTest(unittest.TestCase):
    """
    _FileBasedTest_

    Test file based job splitting.
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
        locationAction.execute(siteName = "site1", seName = "somese.cern.ch")
        locationAction.execute(siteName = "site2", seName = "otherse.cern.ch")
        
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        self.multipleFileFileset.create()
        for i in range(10):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            self.multipleFileFileset.addFile(newFile)
        self.multipleFileFileset.commit()

        self.singleFileFileset = Fileset(name = "TestFileset2")
        self.singleFileFileset.create()
        newFile = File("/some/file/name", size = 1000, events = 100,
                       locations = set(["somese.cern.ch"]))
        newFile.create()
        self.singleFileFileset.addFile(newFile)
        self.singleFileFileset.commit()


        self.multipleSiteFileset = Fileset(name = "TestFileset3")
        self.multipleSiteFileset.create()
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        for i in range(5):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["otherse.cern.ch", "somese.cern.ch"]))
            newFile.create()
            self.multipleSiteFileset.addFile(newFile)
        self.multipleSiteFileset.commit()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task="Test" )
        testWorkflow.create()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "FileBased",
                                                     type = "Processing")
        self.multipleFileSubscription.create()
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "FileBased",
                                                   type = "Processing")
        self.singleFileSubscription.create()

        self.multipleSiteSubscription = Subscription(fileset = self.multipleSiteFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "FileBased",
                                                     type = "Processing")
        self.multipleSiteSubscription.create()
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

    def createLargeFileBlock(self):
        """
        _createLargeFileBlock_
        
        Creates a large group of files for testing
        """
        testFileset = Fileset(name = "TestFilesetX")
        testFileset.create()
        for i in range(5000):
            newFile = File(makeUUID(), size = 1000, events = 100,
                           locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset.addFile(newFile)
        testFileset.commit()
            
        testWorkflow = Workflow(spec = "spec.xml", owner = "mnorman",
                                name = "wf003", task="Test" )
        testWorkflow.create()

        largeSubscription = Subscription(fileset = testFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "FileBased",
                                                   type = "Processing")
        largeSubscription.create()

        return largeSubscription

    def testExactFiles(self):
        """
        _testExactFiles_

        Test file based job splitting when the number of files per job is
        exactly the same as the number of files in the input fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)

        jobGroups = jobFactory(files_per_job = 1)


        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

        return

    def testMoreFiles(self):
        """
        _testMoreFiles_

        Test file based job splitting when the number of files per job is
        greater than the number of files in the input fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.singleFileSubscription)
        
        jobGroups = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        job = jobGroups[0].jobs.pop()
        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"])

        return
        


    def test2FileSplit(self):
        """
        _test2FileSplit_

        Test file based job splitting when the number of files per job is
        2, this should result in five jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)
        
        jobGroups = jobFactory(files_per_job = 2)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 5)

        fileList = []
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job.getFiles()), 2)
            for file in job.getFiles(type = "lfn"):
                fileList.append(file)
        self.assertEqual(len(fileList), 10)
        
        return

    def test3FileSplit(self):
        """
        _test3FileSplit_

        Test file based job splitting when the number of files per job is
        3, this should result in four jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)
        
        jobGroups = jobFactory(files_per_job = 3)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 4)

        fileList = []
        for job in jobGroups[0].jobs:
            assert len(job.getFiles()) in [3, 1], "ERROR: Job contains incorrect number of files"
            for file in job.getFiles(type = "lfn"):
                fileList.append(file)
        self.assertEqual(len(fileList), 10)
        
        return


    def testLocationSplit(self):

        """

        _testLocationSplit_

        This should test whether or not the FileBased algorithm understands that files at seperate sites
        cannot be in the same jobGroup (this is the current standard).
        
        """
        myThread = threading.currentThread()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleSiteSubscription)

        jobGroups = jobFactory(files_per_job = 10)

        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 1)

        fileList = []
        self.assertEqual(len(jobGroups[1].jobs[0].getFiles()), 5)

        
        return
    
    #@attr('performance')
    def testTiming(self):
        """
        _testTiming_

        This is based off of test2FileSplit, but built for timing
        """

        #return
        
        myThread = threading.currentThread()

        #profiler = hotshot.Profile('hotshot.stats')

        subscript = self.createLargeFileBlock()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = subscript)

        #jobGroups = profiler.runcall(jobFactory, files_per_job = 2, jobs_per_group = 1)

        #profiler.close()

        jobGroups = cProfile.runctx("jobFactory(files_per_job = 1)", globals(), locals(), "cProfile.stats")

        #self.assertEqual(len(jobGroups), 1)
        #self.assertEqual(len(jobGroups[0].jobs), 2000)

        p = pstats.Stats('cProfile.stats')
        p.strip_dirs().sort_stats('cumulative').print_stats(.1)
        p.strip_dirs().sort_stats('time').print_stats(.1)

        #print myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()


        return


    def testLimit(self):
        """
        _testLimit_
        
        Test what happens when you limit the number of files.
        This should run each separate file in a separate loop,
        creating one jobGroups with one job with one file
        (The limit argument tells it what to do)
        """

        return


        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)
                              
        
        jobGroups = jobFactory(files_per_job = 10, limit_file_loading = True,
                               file_load_limit = 1)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)

        return



    def testZ_randomCrapForGenerators(self):
        """
        Either this works, and all other tests are obsolete, or it doesn't and they aren't.
        Either way, don't screw around with this.
        """


        def runCode(self, jobFactory):

            func = self.crazyAssFunction(jobFactory = jobFactory, file_load_limit = 500)

            

            startTime = time.time()
            goFlag    = True
            while goFlag:
                try:
                    res = func.next()
                    self.jobGroups.extend(res)
                    logging.error("Going through function")
                except StopIteration:
                    logging.error("Received StopIteration")
                    logging.error("Halting iteration")
                    goFlag = False
                    
            stopTime  = time.time()

            logging.error("Job took %f seconds" % (stopTime - startTime))

            return jobGroups

        

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.multipleFileSubscription)

        jobFactory.open()
        jobGroups = []



        a = self.crazyAssFunction(jobFactory = jobFactory, file_load_limit = 2)

        for x in range(7):
            try:
                res = a.next()
                jobGroups.extend(res)
            except StopIteration:
                logging.error("Received StopIteration")
                logging.error("Halting iteration")

        jobFactory.close()


        self.assertEqual(len(jobGroups), 5)
        for group in jobGroups:
            self.assertEqual(len(group.jobs), 2)



        self.jobGroups = []

        subscript = self.createLargeFileBlock()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS", subscription = subscript)

        jobFactory.open()

        runCode(self, jobFactory)
        #cProfile.runctx("runCode(self, jobFactory)", globals(), locals(), "coroutine.stats")

        jobGroups = self.jobGroups
        



        self.assertEqual(len(jobGroups), 10)
        for group in jobGroups:
            self.assertEqual(len(group.jobs), 500)
            self.assertTrue(group.exists() > 0)


        jobFactory.close()


        p = pstats.Stats('coroutine.stats')
        p.strip_dirs().sort_stats('cumulative').print_stats(.2)
        #p.strip_dirs().sort_stats('time').print_stats(.1)


    def crazyAssFunction(self, jobFactory, file_load_limit = 1):
        groups = ['test']
        while groups != []:
            groups = jobFactory(files_per_job = 1, file_load_limit = file_load_limit)
            yield groups



        

if __name__ == '__main__':
    unittest.main()
