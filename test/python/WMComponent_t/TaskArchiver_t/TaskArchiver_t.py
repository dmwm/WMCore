#!/usr/bin/env python

"""
JobArchiver test 
"""




import os
import logging
import threading
import unittest
import time
import shutil

from WMCore.Agent.Configuration import loadConfigurationFile


from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.WMFactory     import WMFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

from WMCore.DataStructs.Run   import Run

from WMComponent.TaskArchiver.TaskArchiver       import TaskArchiver
from WMComponent.TaskArchiver.TaskArchiverPoller import TaskArchiverPoller

from WMCore.JobStateMachine.ChangeState import ChangeState


class TaskArchiverTest(unittest.TestCase):
    """
    TestCase for TestTaskArchiver module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", 
                                                 "WMCore.ThreadPool", 'WMCore.WorkQueue.Database'],

                                useDefault = False)

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")


        self.nJobs = 10

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()

        self.testInit.clearDatabase()

        return


    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()
        #self.testInit.generateWorkDir(config)

        config.section_("General")
        config.General.workDir = "."

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl     = os.getenv("COUCHURL", "cmssrv52.fnal.gov:5984")
        config.JobStateMachine.couchDBName  = "job_accountant_t"

        config.component_("TaskArchiver")
        config.TaskArchiver.componentDir    = self.testInit.generateWorkDir()
        config.TaskArchiver.WorkQueueParams = {}
        config.TaskArchiver.pollInterval    = 60
        config.TaskArchiver.logLevel        = 'SQLDEBUG'
        config.TaskArchiver.timeOut         = 0


        return config
        
        

    def createTestJobGroup(self, config, name = "TestWorkthrough"):
        """
        Creates a group of several jobs

        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = name, task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = name)
        testWMBSFileset.create()

        testFileA = File(lfn = "/this/is/a/lfnA" , size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()
        testWMBSFileset.markOpen(0)
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        for i in range(0,self.nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJobGroup.add(testJob)
        
        testJobGroup.commit()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')
        changer.propagate(testJobGroup.jobs, 'cleanout', 'success')

        testSubscription.completeFiles([testFileA, testFileB])

        return testJobGroup


    def createGiantJobSet(self, name, config, nSubs = 10, nJobs = 10, nFiles = 1):
        """
        Creates a massive set of jobs

        """


        jobList = []

        

        for i in range(0, nSubs):
            # Make a bunch of subscriptions
            localName = '%s-%i' % (name, i)
            testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                    name = localName, task="Test")
            testWorkflow.create()
            
            testWMBSFileset = Fileset(name = localName)
            testWMBSFileset.create()    


            testSubscription = Subscription(fileset = testWMBSFileset,
                                            workflow = testWorkflow)
            testSubscription.create()
            
            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()

            filesToComplete = []

            for j in range(0, nJobs):
                # Create jobs for each subscription
                testFileA = File(lfn = "%s-%i-lfnA" % (localName, j) , size = 1024, events = 10)
                testFileA.addRun(Run(10, *[11,12,13,14,15,16,17,18,19,20,
                                           21,22,23,24,25,26,27,28,29,30,
                                           31,32,33,34,35,36,37,38,39,40]))
                testFileA.setLocation('malpaquet')
                testFileA.create()

                testWMBSFileset.addFile(testFileA)
                testWMBSFileset.commit()

                filesToComplete.append(testFileA)
                
                testJob = Job(name = '%s-%i' % (localName, j))
                testJob.addFile(testFileA)
                testJob['retry_count'] = 1
                testJob['retry_max'] = 10
                testJobGroup.add(testJob)
                jobList.append(testJob)

                for k in range(0, nFiles):
                    # Create output files
                    testFile = File(lfn = "%s-%i-output" % (localName, k) , size = 1024, events = 10)
                    testFile.addRun(Run(10, *[12312]))
                    testFile.setLocation('malpaquet')
                    testFile.create()

                    testJobGroup.output.addFile(testFile)

                testJobGroup.output.commit()


            testJobGroup.commit()

            changer = ChangeState(config)

            changer.propagate(testJobGroup.jobs, 'created', 'new')
            changer.propagate(testJobGroup.jobs, 'executing', 'created')
            changer.propagate(testJobGroup.jobs, 'complete', 'executing')
            changer.propagate(testJobGroup.jobs, 'success', 'complete')
            changer.propagate(testJobGroup.jobs, 'cleanout', 'success')

            testWMBSFileset.markOpen(0)

            testSubscription.completeFiles(filesToComplete)


        return jobList
            

    def testA_BasicFunctionTest(self):
        """
        _BasicFunctionTest_
        
        Tests the components, by seeing if they can process a simple set of closeouts
        """

        myThread = threading.currentThread()

        config = self.getConfig()

        testJobGroup = self.createTestJobGroup(config = config)

        

        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 1)

        testTaskArchiver = TaskArchiverPoller(config = config)
        testTaskArchiver.algorithm()

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_fileset")[0].fetchall()
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)

        testWMBSFileset = Fileset(id = 1)
        self.assertEqual(testWMBSFileset.exists(), False)
        
        return


    def testB_Profile(self):
        """
        _Profile_
        
        DON'T RUN THIS!
        """

        import cProfile, pstats

        return

        myThread = threading.currentThread()

        name    = makeUUID()

        config = self.getConfig()

        jobList = self.createGiantJobSet(name = name, config = config,
                                         nSubs = 10, nJobs = 1000, nFiles = 10)

        testTaskArchiver = TaskArchiverPoller(config = config)

        
        cProfile.runctx("testTaskArchiver.algorithm()", globals(), locals(), filename = "testStats.stat")

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()



        return



    def testC_Timing(self):
        """
        _Timing_

        This is to see how fast things go.
        """

        return

        myThread = threading.currentThread()

        name    = makeUUID()

        config  = self.getConfig()
        jobList = self.createGiantJobSet(name = name, config = config, nSubs = 10,
                                         nJobs = 1000, nFiles = 10)


        testTaskArchiver = TaskArchiverPoller(config = config)

        startTime = time.time()
        testTaskArchiver.algorithm()
        stopTime  = time.time()


        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)
        testWMBSFileset = Fileset(id = 1)
        self.assertEqual(testWMBSFileset.exists(), False)


        print "TaskArchiver took %f seconds" % (stopTime - startTime)


        

        



if __name__ == '__main__':
    unittest.main()

