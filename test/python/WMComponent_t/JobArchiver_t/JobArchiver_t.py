#!/usr/bin/env python

"""
JobArchiver test 
"""

__revision__ = "$Id: JobArchiver_t.py,v 1.5 2010/02/04 15:52:06 mnorman Exp $"
__version__ = "$Revision: 1.5 $"

import os
import logging
import threading
import unittest
import time
import shutil

from subprocess import Popen, PIPE

from WMCore.Agent.Configuration import loadConfigurationFile



from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

from WMCore.DataStructs.Run   import Run

from WMComponent.JobArchiver.JobArchiver import JobArchiver

from WMCore.JobStateMachine.ChangeState import ChangeState


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
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction = False)

        self.nJobs = 10

        return

    def tearDown(self):
        """
        Database deletion
        """

        self.testInit.clearDatabase(modules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"])

        self.testInit.delWorkDir()

        return


    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()

        config.section_("General")
        config.General.workDir = "."

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl    = os.getenv("COUCHURL", "cmssrv48.fnal.gov:5984")
        #config.JobStateMachine.couchDBName = "job_accountant_t"

        config.component_("JobArchiver")
        config.JobArchiver.pollInterval  = 60
        config.JobArchiver.logLevel      = 'INFO'
        config.JobArchiver.logDir        = os.path.join(self.testDir, 'logs')

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

    def testA_ComponentTest(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """

        return

        myThread = threading.currentThread()

        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        for job in testJobGroup.jobs:
            job["outcome"] = "success"
            job.save()

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')




        testJobArchiver = JobArchiver(config)
        testJobArchiver.prepareToStart()

        logging.debug("Killing")
        myThread.workerThreadManager.terminateWorkers()

        return


    def testB_BasicFunctionTest(self):
        """
        Tests the components, by seeing if they can process a simple set of closeouts
        """

        myThread = threading.currentThread()

        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        cacheDir = os.path.join(self.testDir, 'test')

        if not os.path.isdir(cacheDir):
            os.mkdir(cacheDir)

        if not os.path.isdir(config.JobArchiver.logDir):
            os.mkdir(config.JobArchiver.logDir)

        print "Should be about to make job caches"

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

        testJobArchiver = JobArchiver(config)
        testJobArchiver.prepareToStart()

        logging.debug("Killing")
        myThread.workerThreadManager.terminateWorkers()
        
        
        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()
        
        for val in result:
            self.assertEqual(val.values()[0], 11, "Job did not end in cleanout state, instead in state %i" %(val.values()[0]))
        
        
        dirList = os.listdir(cacheDir)
        for job in testJobGroup.jobs:
            self.assertEqual(job["name"] in dirList, False)

        logList = os.listdir(config.JobArchiver.logDir)
        for job in testJobGroup.jobs:
            self.assertEqual('Job_%s.tar' %(job['name']) in logList, True, 'Could not find transferred tarball for job %s' %(job['name']))
            pipe = Popen(['tar', '-xvf', '%s/Job_%s.tar' %(config.JobArchiver.logDir, job['name'])], stdout = PIPE, stderr = PIPE, shell = False)
            pipe.wait()
            filename = '%s/%s/%s.out' %(cacheDir[1:], job['name'], job['name'])
            self.assertEqual(os.path.isfile(filename), True, 'Could not find file %s' %(filename))
            f = open(filename, 'r')
            fileContents = f.readlines()
            f.close()
            self.assertEqual(fileContents[0].find(job['name']) > -1, True)
            shutil.rmtree(os.path.join(os.getcwd(), 'tmp'))  # We unpack the tarball locally, so we have to clean up

        return


if __name__ == '__main__':
    unittest.main()

