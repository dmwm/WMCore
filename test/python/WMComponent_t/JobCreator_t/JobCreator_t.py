#!/bin/env python



import unittest
import random
import os
import os.path
import logging
import threading
import time


from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job

from WMCore.Agent.Configuration import loadConfigurationFile

from WMComponent.JobCreator.JobCreator import JobCreator

class JobCreatorTest(unittest.TestCase):
    """
    Test case for the JobCreator

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']
    _setup    = False
    _teardown = False


    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        #Stolen from Subscription_t.py
        
        if self._setup:
            return
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setSchema(customModules = ["WMCore.Services.BossLite"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        #locationAction.execute(siteName = "goodse.cern.ch")
        #locationAction.execute(siteName = "badse.cern.ch")
        for site in self.sites:
            locationAction.execute(siteName = site)


        testWorkflow = Workflow(spec = "faketest", owner = "mnorman",
                                name = "whatever", task="Test")
        testWorkflow.create()
        
        testFileset = Fileset(name = "NullFileset")
        testFileset.create()
        
        testFileset.commit()
        #testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
        #testSubscription.create()
        
        self._setup = True


        return





    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        #Likewise
        
        myThread = threading.currentThread()
        
        if self._teardown:
            return
        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()

        factory2 = WMFactory("WMBS", "WMCore.Services.BossLite")
        destroy2 = factory2.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy2.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        
        self._teardown = True



        return




    def createBigJobCollection(self, instance, nSubs):
        """

        Creates a giant block of jobs


        """

        myThread = threading.currentThread()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            myThread.transaction.begin()

            testWorkflow = Workflow(spec = "TestHugeWorkload%s/TestHugeTask" %(nameStr), owner = "mnorman",
                                    name = "wf001"+nameStr, task="Test")
            testWorkflow.create()
        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        


            #testJobGroup = JobGroup(subscription = testSubscription)
            #testJobGroup.create()

            for j in range(0,100):
                #pick a random site
                site = random.choice(self.sites)
                testFile = File(lfn = "/this/is/a/lfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                #testFile.setLocation(site)
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()
            #testSubscription.commit()
            #print testSubscription.filesOfStatus('Available')
            print 'Created subscription %s' %(testSubscription['id'])


            myThread.transaction.commit()

    

        #myThread.transaction.commit()

        return



    def createSingleSiteCollection(self, instance, nSubs, workloadSpec = None):
        """

        Creates a giant block of jobs at one site


        """



        myThread = threading.currentThread()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            if not workloadSpec:
                workloadSpec = "TestSingleWorkload%s/TestHugeTask" %(nameStr)

            myThread.transaction.begin()

            testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                    name = "wf001"+nameStr, task="Test"+nameStr)
            testWorkflow.create()
        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,100):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/singleLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()
            print 'Created subscription %s' %(testSubscription['id'])


            myThread.transaction.commit()


        return






    def testA(self):
        """
        Test for whether we can handle jobs at random sites

        """

        myThread = threading.currentThread()

        self.createBigJobCollection("first", 5)

        
        result = myThread.dbi.processData('SELECT * FROM wmbs_subscription',{})

        #print "location data:"
        #print result[0].fetchall()


        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), 'src/python/WMCore/JobStateMachine/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        if not os.getenv("DBSOCK") == None:
            config.CoreDatabase.dbsock = os.getenv("DBSOCK")
        else:
            config.CoreDatabase.dbsock = None            



        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        #time.sleep(40)

        #self.createBigJobCollection("second", 5)

        #print myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()

        #time.sleep(20)

        

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

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

        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs)


        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), 'src/python/WMCore/JobStateMachine/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        if not os.getenv("DBSOCK") == None:
            config.CoreDatabase.dbsock = os.getenv("DBSOCK")
        else:
            config.CoreDatabase.dbsock = None            



        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        time.sleep(10)


        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        #print result[0].fetchall()

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        #print result[0].fetchall()

        self.assertEqual(len(result[0].fetchall()), nSubs * 20)

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)


        return


    def testC(self):
        """
        This one actually tests whether we can read the WMSpec

        """

        if not os.path.exists("basicWorkload.pcl"):
            print "Could not find local WMWorkload file"
            print "Aborting!"
            raise Exception


        myThread = threading.currentThread()

        nSubs = 5

        self.createSingleSiteCollection("first", nSubs, os.getcwd() + "/basicWorkload.pcl")


        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), 'src/python/WMComponent/JobCreator/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        if not os.getenv("DBSOCK") == None:
            config.CoreDatabase.dbsock = os.getenv("DBSOCK")
        else:
            config.CoreDatabase.dbsock = None



        testJobCreator = JobCreator(config)
        testJobCreator.prepareToStart()

        time.sleep(60)


        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')

        #print result[0].fetchall()

        self.assertEqual(len(result[0].fetchall()), nSubs*100)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_jobgroup')

        #print result[0].fetchall()

        self.assertEqual(len(result[0].fetchall()), nSubs)


        result = myThread.dbi.processData('SELECT ID FROM wmbs_job')

        #print result[0].fetchall()

        self.assertEqual(len(result[0].fetchall()), nSubs * 10)

        while (threading.activeCount() > 1):
            #We should never trigger this, but something weird is going on
            print "Waiting for threads to finish"
            time.sleep(1)



        return






if __name__ == "__main__":

    unittest.main() 
