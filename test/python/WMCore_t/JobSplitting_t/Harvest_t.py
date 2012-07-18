#!/usr/bin/env python
"""
_Harvest_t_

Harvest job splitting test

"""

import unittest
import threading
import logging
import time
import os

from WMCore.Agent.Configuration import Configuration
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMQuality.TestInit import TestInit


class HarvestTest(unittest.TestCase):
    """
    _HarvestTest_

    Test for EndOfRun job splitter
    """

    def setUp(self):
        """
        _setUp_

        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testInit.setSchema(customModules = ["WMCore.WMBS"])

        self.splitterFactory = SplitterFactory(package = "WMCore.JobSplitting")

        myThread = threading.currentThread()
        self.myThread = myThread
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = logging,
                                dbinterface = myThread.dbi)
        self.WMBSFactory = daoFactory

        config = self.getConfig()
        self.changer = ChangeState(config)

        myThread.dbi.processData("""INSERT INTO wmbs_location
                                    (id, site_name, se_name)
                                    VALUES (wmbs_location_SEQ.nextval, 'SomeSite', 'SomeSE')
                                    """, transaction = False)

        myThread.dbi.processData("""INSERT INTO wmbs_location
                                    (id, site_name, se_name)
                                    VALUES (wmbs_location_SEQ.nextval, 'SomeSite2', 'SomeSE2')
                                    """, transaction = False)

        self.fileset1 = Fileset(name = "TestFileset1")
        for file in range(11):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(1,*[1]))
            newFile.setLocation('SomeSE')
            self.fileset1.addFile(newFile)

        self.fileset1.create()

        workflow1 = Workflow(spec = "spec.xml", owner = "hufnagel", name = "TestWorkflow1", task="Test")
        workflow1.create()

        self.subscription1  = Subscription(fileset = self.fileset1,
                                           workflow = workflow1,
                                           split_algo = "Harvest",
                                           type = "Harvest")

        self.subscription1.create()

        return

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.clearDatabase()

        return

    def getConfig(self):
        """
        _getConfig_

        """
        config = Configuration()

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', None)
        config.JobStateMachine.couchDBName     = 'wmagent_jobdump'

        return config

    def finishJobs(self, jobGroups):
        """
        _finishJobs_

        """
        for f in self.subscription1.acquiredFiles():
            self.subscription1.completeFiles(f)

        for jobGroup in jobGroups:
            self.changer.propagate(jobGroup.jobs, 'executing', 'created')
            self.changer.propagate(jobGroup.jobs, 'complete', 'executing')
            self.changer.propagate(jobGroup.jobs, 'success', 'complete')
            self.changer.propagate(jobGroup.jobs, 'cleanout', 'success')

        return

    def testHarvestEndOfRunTrigger(self):
        """
        _testDQMHarvestEndOfRunTrigger_

        Make sure that the basic splitting algo works, which is only, ExpressMerge is ALL done, fire a job against that fileset

        """
        self.assertEqual(self.fileset1.open, True, "Fileset is closed. Shouldn't")

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)

        jobGroups = jobFactory()

        self.assertEqual(len(jobGroups), 0 , "We got 1 or more jobGroups with an open fileset and no periodic configuration")

        self.fileset1.markOpen(False)
        self.assertEqual(self.fileset1.open, False, "Fileset is opened, why?")

        # We should also check if there are aqcuired files, if there are, there are jobs,
        # we don't want to fire another jobs while previous are running (output is integrating whatever  input)
        # TODO : The above one we can do when all is done. Not priority

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory()

        self.assertEqual(len(jobGroups), 1 , "Harvest jobsplitter didn't create a single jobGroup after the fileset was closed")

        return

    def testPeriodicTrigger(self):
        """
        _testPeriodicTrigger_

        """
        self.assertEqual(self.fileset1.open, True, "Fileset is not open, not testing periodic here")
        # Test timeout (5s for this first test)
        # there should be no acquired files, if there are, shouldn't be a job
        #self.subscription1.acquireFiles(self.subscription1.availableFiles().pop())

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 3)

        self.assertEqual(len(jobGroups), 1 , "Didn't created the first periodic job when there were acquired files")

        # For the whole thing to work, faking the first job finishing, and putting the files as complete
        self.finishJobs(jobGroups)

        # Adding more of files, so we have new stuff to process
        for file in range(12,24):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(1,*[1]))
            newFile.setLocation('SomeSE')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        # Testing that it doesn't create a job unless the delay is past
        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 2)

        self.assertEqual(len(jobGroups), 0 , "Created one or more job, when there were non-acquired file and the period is not passed by")

        time.sleep(2)

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 2)

        self.assertEqual(len(jobGroups), 1 , "Didn't created one or more job, and there weren't and the period is passed by")

        # Finishing out previous jobs
        self.finishJobs(jobGroups)

        # Adding more of files, so we have new stuff to process
        for file in range(26,36):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(1,*[1]))
            newFile.setLocation('SomeSE')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        # Trying to create another job just afterwards, it shouldn't, because it should respect the configured delay
        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 2)

        self.assertEqual(len(jobGroups), 0 , "Created one or more job, there are new files, but the delay is not past")

        time.sleep(2)

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 2)

        self.assertEqual(len(jobGroups), 1 , "Didn't created one or more job, there are new files and the delay is past")

        # Last check is whether the job gets all the files or not

        numFilesJob = jobGroups[0].jobs[0].getFiles()
        numFilesFileset = self.fileset1.getFiles()
        self.assertEqual(numFilesJob, numFilesFileset, "Job didn't got all the files")

        # Finishing out previous jobs
        self.finishJobs(jobGroups)

        # Adding files for the first location
        for file in range(38,48):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(1,*[1]))
            newFile.setLocation('SomeSE')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()
        # Then another location
        for file in range(50,56):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(1,*[1]))
            newFile.setLocation('SomeSE2')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        # We should have jobs in both locations
        time.sleep(2)

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 2)

        self.assertEqual(len(jobGroups[0].getJobs()), 2 , "We didn't get 2 jobs for 2 locations")

        firstJobLocation = jobGroups[0].getJobs()[0].getFileLocations()[0]
        secondJobLocation = jobGroups[0].getJobs()[1].getFileLocations()[0]

        self.assertEqual(firstJobLocation, 'SomeSite2', "First job location is not SomeSite")
        self.assertEqual(secondJobLocation, 'SomeSite', "Second job location is not SomeSite2")

        self.finishJobs(jobGroups)

        for file in range(60,65):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(2,*[2]))
            newFile.setLocation('SomeSE2')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        for file in range(70,75):
            newFile = File("/some/file/name%d" % file, size = 1000, events = 100)
            newFile.addRun(Run(3,*[3]))
            newFile.setLocation('SomeSE2')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        time.sleep(2)

        jobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval = 2)

        # This is one of the most "complicated" tests so worth to comment, 4 jobs should be created
        # 1 - all previous files from SomeSE and run = 1 (a lot, like ~45)
        # 2 - Few files from SomeSE2, Run = 1
        # 3 - Few files from SomeSE2, Run = 2
        # 4 - Few files from SomeSE2, Run = 3
        self.assertEqual(len(jobGroups[0].getJobs()), 4 , "We didn't get 4 jobs for adding 2 different runs to SomeSE2")

        return

if __name__ == '__main__':
    unittest.main()
