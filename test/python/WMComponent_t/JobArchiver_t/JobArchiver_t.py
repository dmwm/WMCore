#!/usr/bin/env python

"""
JobArchiver test
"""

from builtins import range
from future.utils import listvalues

import os
import shutil
import threading
import unittest
from subprocess import PIPE, Popen

from nose.plugins.attrib import attr

from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPoller
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class JobArchiverTest(EmulatedUnitTestCase):
    """
    TestCase for TestJobArchiver module
    """

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """
        super(JobArchiverTest, self).setUp()

        myThread = threading.currentThread()
        super(JobArchiverTest, self).setUp()
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        # self.tearDown()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testInit.setupCouch("jobarchiver_t_0/jobs", "JobDump")
        self.testInit.setupCouch("jobarchiver_t_0/fwjrs", "FWJRDump")

        self.daofactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.getJobs = self.daofactory(classname="Jobs.GetAllJobs")

        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction=False)

        self.nJobs = 10

        self.configFile = EmulatorSetup.setupWMAgentConfig()

        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase(modules=["WMCore.WMBS"])
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        EmulatorSetup.deleteConfig(self.configFile)
        super(JobArchiverTest, self).tearDown()

        return

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())
        config.General.WorkDir = os.getenv("TESTDIR", os.getcwd())

        # Now the CoreDatabase information
        # This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = os.getenv("COUCHURL", "cmssrv48.fnal.gov:5984")
        config.JobStateMachine.couchDBName = "jobarchiver_t_0"

        config.component_("JobArchiver")
        config.JobArchiver.pollInterval = 60
        config.JobArchiver.logLevel = 'INFO'
        # config.JobArchiver.logDir                = os.path.join(self.testDir, 'logs')
        config.JobArchiver.componentDir = self.testDir
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

        return config

    def createTestJobGroup(self):
        """
        Creates a group of several jobs

        """

        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFileset.addFile(testFileB)
        testWMBSFileset.commit()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        for _ in range(0, self.nJobs):
            testJob = Job(name=makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJobGroup.add(testJob)

        testJobGroup.commit()

        return testJobGroup

    def testBasicFunctionTest(self):
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

        # if os.path.isdir(config.JobArchiver.logDir):
        #    shutil.rmtree(config.JobArchiver.logDir)

        for job in testJobGroup.jobs:
            myThread.transaction.begin()
            job["outcome"] = "success"
            job.save()
            myThread.transaction.commit()
            path = os.path.join(cacheDir, job['name'])
            os.makedirs(path)
            f = open('%s/%s.out' % (path, job['name']), 'w')
            f.write(job['name'])
            f.close()
            job.setCache(path)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')

        testJobArchiver = JobArchiverPoller(config=config)
        testJobArchiver.algorithm()

        result = myThread.dbi.processData(
            "SELECT wmbs_job_state.name FROM wmbs_job_state INNER JOIN wmbs_job ON wmbs_job.state = wmbs_job_state.id")[
            0].fetchall()

        for val in result:
            self.assertEqual(listvalues(val), ['cleanout'])

        dirList = os.listdir(cacheDir)
        for job in testJobGroup.jobs:
            self.assertEqual(job["name"] in dirList, False)

        logPath = os.path.join(config.JobArchiver.componentDir, 'logDir', 'w', 'wf001', 'JobCluster_0')
        logList = os.listdir(logPath)
        for job in testJobGroup.jobs:
            self.assertEqual('Job_%i.tar.bz2' % (job['id']) in logList, True,
                             'Could not find transferred tarball for job %i' % (job['id']))
            pipe = Popen(['tar', '-jxvf', os.path.join(logPath, 'Job_%i.tar.bz2' % (job['id']))],
                         stdout=PIPE, stderr=PIPE, shell=False)
            pipe.wait()
            # filename = '%s/%s/%s.out' %(cacheDir[1:], job['name'], job['name'])
            filename = 'Job_%i/%s.out' % (job['id'], job['name'])
            self.assertEqual(os.path.isfile(filename), True, 'Could not find file %s' % (filename))
            f = open(filename, 'r')
            fileContents = f.readlines()
            f.close()
            self.assertEqual(fileContents[0].find(job['name']) > -1, True)
            shutil.rmtree('Job_%i' % (job['id']))
            if os.path.isfile('Job_%i.tar.bz2' % (job['id'])):
                os.remove('Job_%i.tar.bz2' % (job['id']))

        return

    @attr('integration')
    def testSpeedTest(self):
        """
        _SpeedTest_

        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        return
        # What used to be here - stop pylint from complaining about unreachable code
        # myThread = threading.currentThread()
        #
        # config = self.getConfig()
        #
        # self.nJobs = 2000
        #
        # testJobGroup = self.createTestJobGroup()
        #
        # changer = ChangeState(config)
        #
        # cacheDir = os.path.join(self.testDir, 'test')
        #
        # for job in testJobGroup.jobs:
        #     job["outcome"] = "success"
        #     job.save()
        #     path = os.path.join(cacheDir, job['name'])
        #     os.makedirs(path)
        #     f = open('%s/%s.out' % (path, job['name']), 'w')
        #     f.write(job['name'])
        #     f.close()
        #     job.setCache(path)
        #
        # changer.propagate(testJobGroup.jobs, 'created', 'new')
        # changer.propagate(testJobGroup.jobs, 'executing', 'created')
        # changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        # changer.propagate(testJobGroup.jobs, 'success', 'complete')
        #
        # testJobArchiver = JobArchiverPoller(config=config)
        # cProfile.runctx("testJobArchiver.algorithm()", globals(), locals(), filename="testStats.stat")
        #
        # p = pstats.Stats('testStats.stat')
        # p.sort_stats('cumulative')
        # p.print_stats(.2)
        #
        # return


if __name__ == '__main__':
    unittest.main()
