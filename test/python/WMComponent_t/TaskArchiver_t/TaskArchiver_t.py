#!/usr/bin/env python

"""
JobArchiver test 
"""

__revision__ = "$Id: TaskArchiver_t.py,v 1.1 2009/10/30 13:45:16 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import os
import logging
import threading
import unittest
import time

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

from WMComponent.TaskArchiver.TaskArchiver import TaskArchiver

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
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
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
        config.JobStateMachine.couchurl    = os.getenv("COUCHURL", "cmssrv48.fnal.gov:5984")
        config.JobStateMachine.couchDBName = "job_accountant_t"

        config.component_("TaskArchiver")
        config.TaskArchiver.pollInterval  = 60
        config.TaskArchiver.logLevel      = 'INFO'

        return config
        
        

    def createTestJobGroup(self):
        """
        Creates a group of several jobs

        """

        myThread = threading.currentThread()

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

        myThread = threading.currentThread()

        config = self.getConfig()

        testJobGroup = self.createTestJobGroup()

        changer = ChangeState(config)

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')
        changer.propagate(testJobGroup.jobs, 'cleanout', 'success')



        testTaskArchiver = TaskArchiver(config)
        testTaskArchiver.prepareToStart()

        print "Killing"
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

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'success', 'complete')
        changer.propagate(testJobGroup.jobs, 'cleanout', 'success')

        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 1)

        testTaskArchiver = TaskArchiver(config)
        testTaskArchiver.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_file_details")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_subscription")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        testWMBSFileset = Fileset(id = 1)
        self.assertEqual(testWMBSFileset.exists(), False)
        
        return


if __name__ == '__main__':
    unittest.main()

