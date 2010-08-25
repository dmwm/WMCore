#!/usr/bin/env python
"""
_JobMaker Test_

Unittest for JobMaker class

"""


__revision__ = "$Id: JobMaker_t.py,v 1.4 2009/10/13 22:20:07 meloam Exp $"
__version__ = "$Revision: 1.4 $"

import os
import os.path
import unittest
import threading
import tempfile
import logging
import time

import hotshot

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.File         import File
from WMCore.WMBS.Job          import Job
from WMCore.DataStructs.Run   import Run
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Workflow     import Workflow
from WMQuality.TestInit       import TestInit
from WMCore.WMFactory         import WMFactory

from WMCore.WMSpec.Makers.JobMaker import JobMaker
from WMCore.WMSpec.Makers.Interface.CreateWorkArea import CreateWorkArea

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
#from WMCore.WMBS.MySQL.Jobs.SetBulkCache import SetBulkCache

class JobMakerTest(unittest.TestCase):
    """
    JobMaker test class

    """

    lock = threading.Condition()

    def setUp(self):

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = 
                                ["WMCore.WMBS",
                                 "WMCore.MsgService",
                                 "WMCore.ThreadPool"],
                                useDefault = False)
        self.cwd = os.getcwd()




    def tearDown(self):
        self.testInit.clearDatabase()

    def getConfig(self, configPath):
        config = self.testInit.getConfiguration(configPath)
        self.testInit.generateWorkDir(config)
        return config
    
    def createSingleJobGroup(self, nameStr = ''):

        myThread = threading.currentThread()

        myThread.transaction.begin()

        testWorkflow = Workflow(spec = os.path.join(self.cwd, "basicWorkload.pcl"), owner = "Simon",
                                name = "wf001"+nameStr, task="Test")
        testWorkflow.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.create()
        testFileB.create()

        testJobA = Job(name = "TestJobA"+nameStr)
        testJobA.addFile(testFileA)
        
        testJobB = Job(name = "TestJobB"+nameStr)
        testJobB.addFile(testFileB)
        
        testJobGroup.add(testJobA)
        testJobGroup.add(testJobB)

        testJobGroup.commit()

        myThread.transaction.commit()

        return testJobGroup


    def createHugeJobGroup(self, nameStr = ''):
        
        myThread = threading.currentThread()

        myThread.transaction.begin()

        testWorkflow = Workflow(spec = os.path.join(self.cwd, "basicWorkload.pcl"), owner = "mnorman",
                                name = "wf001"+nameStr, task="Test")
        testWorkflow.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        for i in range(0,5000):
            testFile = File(lfn = "/this/is/a/lfn"+nameStr, size = 1024, events = 10)
            testFile.create()
            testJob  = Job(name = 'testJob_%s_%i' %(nameStr, i))
            testJob.addFile(testFile)
            testJobGroup.add(testJob)


        testJobGroup.commit()

        myThread.transaction.commit()

        
        return testJobGroup   

    
    def testASingleJobGroup(self):
        """
        Test for creation of single job groups and threading

        """
        config = self.getConfig()
        os.chdir(config.General.workDir)

        testJobGroupList = [] 

        testJobMaker = JobMaker(config)
        testJobMaker.prepareToStart()


        for i in range(0,100):
            testJobGroup = self.createSingleJobGroup(str(i))
            testJobGroupList.append(testJobGroup)

        for job in testJobGroupList:
            if job.exists():
                testJobMaker.handleMessage('MakeJob', {'jobGroupID': job.exists(), 'startDir': os.getcwd()})


        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)


        self.assertEqual(os.path.isdir('TestWorkload'), True)
        self.assertEqual(os.path.isdir('TestWorkload/TestTask'), True)
        self.assertEqual(os.path.isdir('TestWorkload/TestTask/JobCollection_1_0'), True)


        os.chdir(self.cwd)

        #os.popen3('rm -r test/TestWorkload/TestTask')

        #print myThread.dbi.processData("SELECT cache_dir FROM wmbs_job")[0].fetchall()
        
        return



    def testMultipleJobGroups(self):
        """
        Test for creation of huge job groups and threading

        """
        config = self.getConfig()
        os.chdir(config.General.workDir)
        currDir = os.getcwd()
        testJobGroupList = [] 

        testJobMaker = JobMaker(config)
        testJobMaker.prepareToStart()

        for i in range(0,5):
            testJobGroup = self.createHugeJobGroup(str(i))
            testJobGroupList.append(testJobGroup)

        starttime = time.time()

        for job in testJobGroupList:
            if job.exists():
                testJobMaker.handleMessage('MakeJob', {'jobGroupID': job.exists(), 'startDir': currDir})

        print "The number of threads is %i" %(threading.activeCount())

        #time.sleep(600)

        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)

        #for job in testJobGroupList:
        #    if job.exists():
        #        self.assertEqual(os.path.isdir(job['uid']), True)

        endtime = time.time()

        print "This process took %i seconds to run" %(int(endtime - starttime))

        self.assertEqual(os.path.isdir('TestHugeWorkload'), True)
        self.assertEqual(os.path.isdir('TestHugeWorkload/TestHugeTask'), True)
        listOfCollections = ['JobCollection_1_0', 'JobCollection_1_1', 'JobCollection_2_0', 'JobCollection_2_1', 'JobCollection_4_0', \
                             'JobCollection_3_0', 'JobCollection_5_0', 'JobCollection_3_1', 'JobCollection_5_1', 'JobCollection_4_1', \
                             'JobCollection_4_2', 'JobCollection_4_3', 'JobCollection_4_4']
        for coll in listOfCollections:
            assert coll in os.listdir('TestHugeWorkload/TestHugeTask'), "Failed to create TestHugeWorkload/TestHugeTask/%s" %(coll)
            self.assertEqual(len(os.listdir('TestHugeWorkload/TestHugeTask/%s' %(coll))), 1000)
        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT cache_dir FROM wmbs_job WHERE id = 1")[0].fetchall()[0].values()[0]

        self.assertEqual(result, os.path.join(os.getcwd(),'TestHugeWorkload/TestHugeTask/JobCollection_1_0/job_1'))
        
        #Get rid of all this crap
        #os.popen3('rm -r TestHugeWorkload')

        os.chdir(self.cwd)
        
        return


    def testUnthreadedHuge(self):
        """
        Test for creation of huge job groups and threading

        """
        config = self.getConfig()
        os.chdir(config.General.workDir)

        testJobGroupList = [] 

        testJobMaker = JobMaker(config)
        testJobMaker.prepareToStart()

        for i in range(0,5):
            testJobGroup = self.createHugeJobGroup(str(i))
            testJobGroupList.append(testJobGroup)

        starttime = time.time()
        
        for job in testJobGroupList:
            if job.exists():
                testJobMaker.processJobs(job.exists(), os.getcwd())

        print "The number of threads is %i" %(threading.activeCount())

        #time.sleep(600)

        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)

        endtime = time.time()

        print "This process took %i seconds to run" %(int(endtime - starttime))

        self.assertEqual(os.path.isdir('BasicProduction'), True)
        self.assertEqual(os.path.isdir('BasicProduction/Test'), True)
        listOfCollections = ['JobCollection_1_0', 'JobCollection_1_1', 'JobCollection_2_0', 'JobCollection_2_1', 'JobCollection_4_0', \
                             'JobCollection_3_0', 'JobCollection_5_0', 'JobCollection_3_1', 'JobCollection_5_1', 'JobCollection_4_1', \
                             'JobCollection_4_2', 'JobCollection_4_3', 'JobCollection_4_4']
        for coll in listOfCollections:
            assert coll in os.listdir('BasicProduction/Test'), "Failed to create TestHugeWorkload/TestHugeTask/%s" %(coll)
            self.assertEqual(len(os.listdir('BasicProduction/Test/%s' %(coll))), 1000)
        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT cache_dir FROM wmbs_job WHERE id = 1")[0].fetchall()[0].values()[0]

        self.assertEqual(result, os.path.join(os.getcwd(),'BasicProduction/Test/JobCollection_1_0/job_1'))
        
        #Get rid of all this crap
        #os.popen3('rm -r TestHugeWorkload')

        os.chdir(self.cwd)
        
        return
    



if __name__ == "__main__":

    prof = hotshot.Profile("hotshot_stats")
    prof.runcall(unittest.main())
    prof.close()
    #unittest.main() 
