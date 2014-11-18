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
import cProfile, pstats
import inspect

from subprocess import Popen, PIPE

from WMCore.Agent.Configuration import loadConfigurationFile, Configuration


from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMQuality.Emulators import EmulatorSetup
#from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

from WMCore.DataStructs.Run   import Run

from WMComponent.JobArchiver.JobArchiver       import JobArchiver
from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPoller
from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPollerException

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMComponent_t.AlertGenerator_t.Pollers_t import utils

from nose.plugins.attrib import attr

from WMCore.Services.EmulatorSwitch import EmulatorHelper


class JobArchiverTest(unittest.TestCase):
    """
    TestCase for TestJobArchiver module
    """


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
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setupCouch("jobarchiver_t_0/jobs", "JobDump")
        self.testInit.setupCouch("jobarchiver_t_0/fwjrs", "FWJRDump")

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction = False)

        self.nJobs = 10

        self.alertsReceiver = None

        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                                    siteDB = True, requestMgr = False)
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        return

    def tearDown(self):
        """
        Database deletion
        """
        EmulatorHelper.resetEmulators()
        self.testInit.clearDatabase(modules = ["WMCore.WMBS"])
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        EmulatorSetup.deleteConfig(self.configFile)
        if self.alertsReceiver:
            self.alertsReceiver.shutdown()

        return


    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())
        config.General.WorkDir = os.getenv("TESTDIR", os.getcwd())

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl    = os.getenv("COUCHURL", "cmssrv48.fnal.gov:5984")
        config.JobStateMachine.couchDBName = "jobarchiver_t_0"

        config.component_("JobArchiver")
        config.JobArchiver.pollInterval          = 60
        config.JobArchiver.logLevel              = 'INFO'
        #config.JobArchiver.logDir                = os.path.join(self.testDir, 'logs')
        config.JobArchiver.componentDir          = self.testDir
        config.JobArchiver.numberOfJobsToCluster = 1000

        config.component_('WorkQueueManager')
        config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
        config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
        config.WorkQueueManager.level = 'LocalQueue'
        config.WorkQueueManager.logLevel = 'DEBUG'
        config.WorkQueueManager.couchurl = 'https://None'
        config.WorkQueueManager.dbname = 'whatever'
        config.WorkQueueManager.inboxDatabase = 'whatever2'
        config.WorkQueueManager.queueParams = {}
        config.WorkQueueManager.queueParams["ParentQueueCouchUrl"] = "https://cmsweb.cern.ch/couchdb/workqueue"

        # addition for Alerts messaging framework, work (alerts) and control
        # channel addresses to which the component will be sending alerts
        # these are destination addresses where AlertProcessor:Receiver listens
        config.section_("Alert")
        config.Alert.address = "tcp://127.0.0.1:5557"
        config.Alert.controlAddr = "tcp://127.0.0.1:5559"

        return config


    def createTestJobGroup(self):
        """
        Creates a group of several jobs

        """

        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
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

        return testJobGroup



    def testA_BasicFunctionTest(self):
        """
        _BasicFunctionTest_

        Tests the components, by seeing if they can process a simple set of closeouts
        """

        myThread = threading.currentThread()

        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        cacheDir = os.path.join(self.testDir, 'test')

        if not os.path.isdir(cacheDir):
            os.mkdir(cacheDir)

        #if os.path.isdir(config.JobArchiver.logDir):
        #    shutil.rmtree(config.JobArchiver.logDir)

        for job in testJobGroup.jobs:
            myThread.transaction.begin()
            job["outcome"] = "success"
            job.save()
            myThread.transaction.commit()
            path = os.path.join(cacheDir, job['name'])
            os.makedirs(path)
            f = open('%s/%s.out' %(path, job['name']),'w')
            f.write(job['name'])
            f.close()
            job.setCache(path)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')

        testJobArchiver = JobArchiverPoller(config = config)
        testJobArchiver.algorithm()


        result = myThread.dbi.processData("SELECT wmbs_job_state.name FROM wmbs_job_state INNER JOIN wmbs_job ON wmbs_job.state = wmbs_job_state.id")[0].fetchall()

        for val in result:
            self.assertEqual(val.values(), ['cleanout'])


        dirList = os.listdir(cacheDir)
        for job in testJobGroup.jobs:
            self.assertEqual(job["name"] in dirList, False)

        logPath = os.path.join(config.JobArchiver.componentDir, 'logDir', 'w', 'wf001', 'JobCluster_0')
        logList = os.listdir(logPath)
        for job in testJobGroup.jobs:
            self.assertEqual('Job_%i.tar.bz2' %(job['id']) in logList, True, 'Could not find transferred tarball for job %i' %(job['id']))
            pipe = Popen(['tar', '-jxvf', os.path.join(logPath, 'Job_%i.tar.bz2' % (job['id']))],
                         stdout = PIPE, stderr = PIPE, shell = False)
            pipe.wait()
            #filename = '%s/%s/%s.out' %(cacheDir[1:], job['name'], job['name'])
            filename = 'Job_%i/%s.out' %(job['id'], job['name'])
            self.assertEqual(os.path.isfile(filename), True, 'Could not find file %s' %(filename))
            f = open(filename, 'r')
            fileContents = f.readlines()
            f.close()
            self.assertEqual(fileContents[0].find(job['name']) > -1, True)
            shutil.rmtree('Job_%i' %(job['id']))
            if os.path.isfile('Job_%i.tar.bz2' %(job['id'])):
                os.remove('Job_%i.tar.bz2' %(job['id']))

        return

    @attr('integration')
    def testB_SpeedTest(self):
        """
        _SpeedTest_

        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        return
        myThread = threading.currentThread()

        config = self.getConfig()

        self.nJobs = 2000

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        cacheDir = os.path.join(self.testDir, 'test')

        for job in testJobGroup.jobs:
            job["outcome"] = "success"
            job.save()
            path = os.path.join(cacheDir, job['name'])
            os.makedirs(path)
            f = open('%s/%s.out' %(path, job['name']),'w')
            f.write(job['name'])
            f.close()
            job.setCache(path)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')




        testJobArchiver = JobArchiverPoller(config = config)
        cProfile.runctx("testJobArchiver.algorithm()", globals(), locals(), filename = "testStats.stat")


        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(.2)

        return


    def testJobArchiverPollerAlertsSending_constructor(self):
        """
        Cause exception (alert-worthy situation) in
        the JobArchiverPoller constructor.

        """
        myThread = threading.currentThread()
        config = self.getConfig()

        handler, self.alertsReceiver = \
            utils.setUpReceiver(config.Alert.address, config.Alert.controlAddr)

        config.JobArchiver.logDir = ""
        config.JobArchiver.componentDir = ""
        # invoke exception and thus Alert message
        self.assertRaises(JobArchiverPollerException, JobArchiverPoller, config = config)
        # wait for the generated alert to arrive
        while len(handler.queue) == 0:
            time.sleep(0.3)
            print "%s waiting for alert to arrive ..." % inspect.stack()[0][3]

        self.alertsReceiver.shutdown()
        self.alertsReceiver = None
        # now check if the alert was properly sent
        self.assertEqual(len(handler.queue), 1)
        alert = handler.queue[0]
        self.assertEqual(alert["Source"], "JobArchiverPoller")


    def testJobArchiverPollerAlertsSending_cleanJobCache(self):
        """
        Cause exception (alert-worthy situation) in
        the cleanJobCache method.

        """
        myThread = threading.currentThread()
        config = self.getConfig()

        handler, self.alertsReceiver = \
            utils.setUpReceiver(config.Alert.address, config.Alert.controlAddr)

        testJobArchiver = JobArchiverPoller(config = config)

        # invoke the problem and thus Alert message
        job = dict(cache_dir = None)
        testJobArchiver.cleanJobCache(job)
        # wait for the generated alert to arrive
        while len(handler.queue) == 0:
            time.sleep(0.3)
            print "%s waiting for alert to arrive ..." % inspect.stack()[0][3]

        self.alertsReceiver.shutdown()
        self.alertsReceiver = None
        # now check if the alert was properly sent
        self.assertEqual(len(handler.queue), 1)
        alert = handler.queue[0]
        self.assertEqual(alert["Source"], testJobArchiver.__class__.__name__)



if __name__ == '__main__':
    unittest.main()
