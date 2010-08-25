#!/bin/env python



import unittest
import random
import os
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

from WMComponent.JobSubmitter.JobSubmitter import JobSubmitter

class JobSubmitterTest(unittest.TestCase):
    """
    Test class for the JobSubmitter

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']
    _setup    = False
    _teardown = False


    def setUp(self):
        """
        Standard setup


        """

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

        for site in self.sites:
            locationAction.execute(siteName = site)


        self._setup = True


        return


        return

    def tearDown(self):
        """
        Standard tearDown

        """

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



    def createJobGroup(self, nSubs, instance = '', workloadSpec = None):
        """
        This function acts to create a number of test job group with jobs that we can submit

        """

        myThread = threading.currentThread()

        jobGroupList = []

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

            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()

            for file in testFileset.getFiles():
                testJob = Job(name = "test-%s" %(file["lfn"]))
                testJob.addFile(file)
                testJob["location"] = file.getLocations()[0]
                testJob.create(testJobGroup)
                testJobGroup.add(testJob)

            testJobGroup.commit()
            jobGroupList.append(testJobGroup)




                
            print 'Created subscription %s' %(testSubscription['id'])


            myThread.transaction.commit()

            return jobGroupList
        


    def testBasicSubmission(self):
        """
        This is the simplest of tests

        """

        #This submits a job without a scheduler, and hence to fake
        #Because of this, this test is basically nonfunctional - there are no results
        #Other then it runs without BossLite committing suicide.

        myThread = threading.currentThread()

        jobGroupList = self.createJobGroup(5, 'first')


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
            myThread.database = os.getenv("DATABASE")
        if not os.getenv("DBSOCK") == None:
            config.CoreDatabase.dbsock = os.getenv("DBSOCK")
        else:
            config.CoreDatabase.dbsock = None

        cfg_params = {}
        jobGroupConfig  = {'globalSandbox': os.getcwd() + '/tmp/',
                           'jobType'      : 'test',
                           'jobCacheDir'  : os.getcwd() + '/tmp/'}
            

        jobSubmitter = JobSubmitter(config)
        jobSubmitter.configure(cfg_params)

        for jobGroup in jobGroupList:
            #print "I have a jobGroup"
            jobSubmitter.submitJobs(jobGroup, jobGroupConfig)

        return



if __name__ == "__main__":

    unittest.main() 
