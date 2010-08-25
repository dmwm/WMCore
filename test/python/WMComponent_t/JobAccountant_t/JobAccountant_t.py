#!/usr/bin/env python

"""

DBSUpload test TestDBSUpload module and the harness

"""

__revision__ = "$Id $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import logging
import os
import os.path
import threading
import unittest
import time

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.Agent.Configuration import Configuration

from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.File         import File
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Fileset      import Fileset

from WMComponent.JobAccountant.JobAccountant import JobAccountant

from WMCore.DataStructs.Run import Run

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer



class JobAccountantTest(unittest.TestCase):
    """
    TestCase for DBSUpload module 
    """

    _teardown = False

    def setUp(self):
        """
        _setUp_
        
        setUp function for unittest

        """

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        #self.tearDown()

        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
                                useDefault = True)
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        #self.testInit.setSchema(customModules = ["WMCore.ThreadPool"],
        #                        useDefault = False)
        #self.testInit.setSchema(customModules = ["WMCore.MsgService"],
        #                        useDefault = False)
        #self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
        #                        useDefault = False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        locationAction = self.daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "malpaquet") 

        self._teardown = False

        self.nJobs = 10


        return


    def tearDown(self):
        """
        Standard tearDown function

        """

        myThread = threading.currentThread()
        
        if self._teardown:
            return

        factory2 = WMFactory("WMBS", "WMCore.WMBS")
        destroy2 = factory2.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy2.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        factory = WMFactory("Threadpool", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()

        factory = WMFactory("Trigger", "WMCore.Trigger")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete Trigger tear down.")
        myThread.transaction.commit()

        factory2 = WMFactory("MsgService", "WMCore.MsgService")
        destroy2 = factory2.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy2.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        myThread.transaction.commit()


        return



    def getConfig(self, configPath=os.path.join(os.getenv('WMCOREBASE'), \
                                                'src/python/WMComponent/JobAccountant/DefaultConfig.py')):


        if os.path.isfile(configPath):
            # read the default config first.
            config = loadConfigurationFile(configPath)
        else:
            config = Configuration()
            config.component_("JobAccountant")
            #The log level of the component. 
            config.JobAccountant.logLevel = 'INFO'
            config.JobAccountant.pollInterval = 10

        myThread = threading.currentThread()

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
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



        return config


    def createTestJobGroup(self, FJR_Path = None, commitFlag = True):
        """
        _createTestJobGroup_
        
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        for i in range(0,self.nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['test'] = 'help%i' %(i) 
            testJob['FJR_Path'] = FJR_Path
            testJobGroup.add(testJob)
        
        if commitFlag:
            testJobGroup.commit()



        #action = self.daofactory(classname = "Jobs.ChangeState")
        #action.execute(jobs = testJobGroup.jobs)


        return testJobGroup



    def testGoodFJR(self):
        """
        _testA_
        
        Basic functionality test
        """

        myThread = threading.currentThread()




        

        config       = self.getConfig()
        testJobGroup = self.createTestJobGroup(FJR_Path = "/home/mnorman/WMCORE/test/python/WMComponent_t/JobAccountant_t/testFJR.xml")

        uniqueCouchDbName = 'jsm_test'
        changer = ChangeState(config, uniqueCouchDbName)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')

        print "About to start JobAccountant"

        testJobAccountant = JobAccountant(config)
        testJobAccountant.prepareToStart()

        time.sleep(20)

        while threading.activeCount() > 2:

            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        time.sleep(20)

        getJobs = self.daofactory(classname = "Jobs.GetAllJobs")
        idList = getJobs.execute(state = 'complete')
        self.assertEqual(len(idList), 0)

        idList = getJobs.execute(state = 'closeout')
        self.assertEqual(len(idList), self.nJobs)

        files = myThread.dbi.processData("SELECT * FROM dbsbuffer_file")[0].fetchall()
        self.assertEqual(len(files), 2)

        datasets = myThread.dbi.processData("SELECT * FROM dbsbuffer_dataset")[0].fetchall()
        self.assertEqual(len(datasets), 2)

        self.assertEqual(datasets[0][1].split('/')[2], 'Commissioning09-PromptReco-v6')

        return


    def testBadFJR(self):
        """
        _testBadFJR_
        
        Basic functionality test for simple failed FJR
        """

        myThread = threading.currentThread()

        config       = self.getConfig()
        testJobGroup = self.createTestJobGroup(FJR_Path = os.path.join(os.getenv("WMCOREBASE"), "test/python/WMComponent_t/JobAccountant_t/failFJR.xml"))

        uniqueCouchDbName = 'jsm_test'
        changer = ChangeState(config, uniqueCouchDbName)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')

        print "About to start JobAccountant"

        testJobAccountant = JobAccountant(config)
        testJobAccountant.prepareToStart()

        time.sleep(20)

        while threading.activeCount() > 2:

            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()


        getJobs = self.daofactory(classname = "Jobs.GetAllJobs")
        idList = getJobs.execute(state = 'complete')
        self.assertEqual(len(idList), 0)

        idList = getJobs.execute(state = 'jobfailed')
        self.assertEqual(len(idList), self.nJobs)

        return



if __name__ == '__main__':
    unittest.main()
