#!/bin/env python



import unittest
import random
import threading
import time
import os

from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory


from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription

from WMComponent.JobCreator.JobCreator import JobCreator

from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper

class JobCreatorTest(unittest.TestCase):
    """
    Test case for the JobCreator

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']


    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        #Stolen from Subscription_t.py
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ['WMCore.WMBS', 
                                                 'WMCore.MsgService',
                                                 'WMCore.ThreadPool'], useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")

        for site in self.sites:
            locationAction.execute(siteName = site)

        self.testDir = self.testInit.generateWorkDir()
        self.cwd = os.getcwd()

        return





    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()




    def createBigJobCollection(self, instance, nSubs):
        """

        Creates a giant block of jobs


        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec = "TestHugeWorkload/TestHugeTask", owner = "mnorman",
                                name = "wf001", task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        
            for j in range(0,100):
                #pick a random site
                site = random.choice(self.sites)
                testFile = File(lfn = "/this/is/a/lfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()
        return



    def createSingleSiteCollection(self, instance, nSubs, workloadSpec = None):
        """
        Creates a giant block of jobs at one site
        """



        myThread = threading.currentThread()

        if not workloadSpec:
            workloadSpec = "TestSingleWorkload/TestHugeTask"


        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                name = "wf001", task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,100):
                #pick the first site
                site = self.sites[0]
                testFile = File(lfn = "/singleLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()

        return

    def createMutlipleSiteCollection(self, instance, nSubs, workloadSpec = None):
        """

        Creates a giant block of jobs at multiple sites


        """



        myThread = threading.currentThread()

        nameStr = str(instance)


        if not workloadSpec:
            workloadSpec = "TestSingleWorkload%s/TestHugeTask" %(nameStr)


        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman2",
                                name = "wf001"+nameStr, task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,100):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/multLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(self.sites)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()


        return


    def getAbsolutelyMassiveJobGroup(self, instance, nSubs, workloadSpec = None):
        """

        Creates a giant block of jobs at multiple sites


        """



        myThread = threading.currentThread()

        if not workloadSpec:
            workloadSpec = "TestSingleWorkload/TestReallyHugeTask" 

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman3",
                                name = "afmwf001", task="Merge")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()
        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()

            for j in range(0,2000):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/multLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(self.sites)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            myThread.transaction.commit()


        return


    def getConfig(self):
        """
        _getConfig_

        Creates a common config.
        """



        return self.testInit.getConfiguration(
                    os.path.join(os.getenv('WMCOREBASE'), 'src/python/WMComponent/JobCreator/DefaultConfig.py'))



    def testA(self):
        """
        Test for whether or not the job will actually run

        """

        #return

        myThread = threading.currentThread()

        nSubs = 5

        self.createBigJobCollection("first", nSubs)

        
        config = self.getConfig()

        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()


        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), len(self.sites) * nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        assert len(result[0].fetchall()) > nSubs * 20, "Not enough jobs!"

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)

        #self.teardown = True


        return


    def testB(self):
        """
        This one actually tests something

        """

        #return
        
        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs)

        config = self.getConfig()


        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(10)
        

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 20)

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)

        #os.chdir(self.cwd)

        return


    def testC(self):
        """
        This one actually tests whether we can read the WMSpec

        """

        #return

        if not os.path.exists("basicWorkload.pcl"):
            print "Could not find local WMWorkload file"
            print "Aborting!"
            raise Exception

        if os.path.exists("basicWorkloadUpdated.pcl"):
            os.remove("basicWorkloadUpdated.pcl")


        wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
        wmWorkload.load("basicWorkload.pcl")
        wmTask     = wmWorkload.getTask("Merge")
        #print wmTask.data

        wmTask.data.section_("seeders")
        wmTask.data.seeders.section_("RandomSeeder")
        wmTask.data.seeders.section_("RunAndLumiSeeder")
        wmTask.data.seeders.RandomSeeder.simMuonRPCDigis            = None
        wmTask.data.seeders.RandomSeeder.simEcalUnsuppressedDigis   = None
        wmTask.data.seeders.RandomSeeder.simCastorDigis             = None
        wmTask.data.seeders.RandomSeeder.generator                  = None 
        wmTask.data.seeders.RandomSeeder.simSiStripDigis            = None
        wmTask.data.seeders.RandomSeeder.LHCTransport               = None

        wmWorkload.save("basicWorkloadUpdated.pcl")
        


        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs, os.getcwd() + "/basicWorkloadUpdated.pcl")

        config = self.getConfig()


        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(20)


        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 10)

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)

        return


    def testD(self):
        """
        This one tests whether or not we can choose a site from a list in the file
        This is not well tested, since I don't know which location it will end up in.

        """

        print "Starting testD"
        print os.getcwd()

        if not os.path.exists("basicWorkload.pcl"):
            print "Could not find local WMWorkload file"
            print "Aborting!"
            raise Exception


        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs, os.getcwd() + "/basicWorkload.pcl")
        

        config = self.getConfig()

        print "Should have jobs in: "
        print config.JobCreator.jobCacheDir


        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(20)


        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT id FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 10)

        self.assertEqual(os.listdir('test/BasicProduction/Merge/JobCollection_1_0/job_1'), ['baggage.pcl'])

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)


        print myThread.dbi.processData("SELECT * FROM wmbs_location")[0].fetchall()
        return


    def testAbsoFuckingLoutelyHugeJob(self):
        """
        This one takes a long time to run, but it runs
        """

        #return

        print "This should take about twelve to fifteen minutes"

        
        if not os.path.exists("basicWorkload.pcl"):
            print "Could not find local WMWorkload file"
            print "Aborting!"
            raise Exception


        myThread = threading.currentThread()

        nSubs = 5

        self.getAbsolutelyMassiveJobGroup("first", nSubs, os.getcwd() + "/basicWorkload.pcl")

        config = self.getConfig()

        startTime = time.clock()
        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()
        stopTime = time.clock()

        print "Time taken: "
        print stopTime - startTime

        result = myThread.dbi.processData('SELECT id FROM wmbs_job')

        self.assertEqual(len(result[0].fetchall()), nSubs * 200)

        return


if __name__ == "__main__":

    unittest.main() 
