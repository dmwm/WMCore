#!/usr/bin/env python
"""
_Harvest_t_

Harvest job splitting test

"""

from builtins import str, range

import unittest
import threading
import logging
import time
import os

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.LumiList import LumiList
from WMCore.DataStructs.Run import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMQuality.TestInit import TestInit
from WMQuality.Emulators import EmulatorSetup


def createCommonFileset():
    """
    Create a simple fileset with 2 files at the same location
    """
    multipleFilesFileset = Fileset(name="TestFileset")

    newFile = File("/some/file/test1", size=1000, events=100)
    newFile.addRun(Run(1, *[1, 3, 4, 5, 6, 7]))
    newFile.addRun(Run(2, *[1, 2, 4, 5, 6, 7]))
    newFile.setLocation('T2_CH_CERN')
    multipleFilesFileset.addFile(newFile)

    newFile = File("/some/file/test2", size=2000, events=200)
    newFile.addRun(Run(3, *[2, 8]))
    newFile.addRun(Run(4, *[3, 8]))
    newFile.setLocation('T2_CH_CERN')
    multipleFilesFileset.addFile(newFile)

    newFile = File("/some/file/test3", size=3000, events=300)
    newFile.addRun(Run(5, *[10, 11, 12]))
    newFile.addRun(Run(6, *[10, 11, 12]))
    newFile.setLocation('T2_CH_CERN')
    multipleFilesFileset.addFile(newFile)

    newFile = File("/some/file/test4", size=4000, events=400)
    newFile.addRun(Run(2, *[3, 8, 9]))
    newFile.addRun(Run(3, *[3, 4, 5, 6]))
    newFile.setLocation('T2_CH_CERN')
    multipleFilesFileset.addFile(newFile)

    multipleFilesFileset.create()
    return multipleFilesFileset


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

        self.testInit.setSchema(customModules=["WMCore.WMBS"])

        self.splitterFactory = SplitterFactory(package="WMCore.JobSplitting")

        myThread = threading.currentThread()
        self.myThread = myThread
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger=logging,
                                dbinterface=myThread.dbi)
        self.WMBSFactory = daoFactory

        config = self.getConfig()
        self.changer = ChangeState(config)

        myResourceControl = ResourceControl()
        myResourceControl.insertSite("T1_US_FNAL", 10, 20, "T1_US_FNAL_Disk", "T1_US_FNAL")
        myResourceControl.insertSite("T1_US_FNAL", 10, 20, "T3_US_FNALLPC", "T1_US_FNAL")
        myResourceControl.insertSite("T2_CH_CERN", 10, 20, "T2_CH_CERN", "T2_CH_CERN")

        self.fileset1 = Fileset(name="TestFileset1")
        for fileNum in range(11):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(1, *[1]))
            newFile.setLocation('T1_US_FNAL_Disk')
            self.fileset1.addFile(newFile)

        self.fileset1.create()

        workflow1 = Workflow(spec="spec.xml", owner="hufnagel", name="TestWorkflow1", task="Test")
        workflow1.create()

        self.subscription1 = Subscription(fileset=self.fileset1,
                                          workflow=workflow1,
                                          split_algo="Harvest",
                                          type="Harvesting")

        self.subscription1.create()
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        return

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.clearDatabase()
        EmulatorSetup.deleteConfig(self.configFile)

        return

    def getConfig(self):
        """
        _getConfig_

        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL', None)
        config.JobStateMachine.couchDBName = 'wmagent_jobdump'

        return config

    def finishJobs(self, jobGroups, subscription=None):
        """
        _finishJobs_

        """
        if not subscription:
            subscription = self.subscription1
        for f in subscription.acquiredFiles():
            subscription.completeFiles(f)

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

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)

        jobGroups = jobFactory()

        self.assertEqual(len(jobGroups), 0,
                         "We got 1 or more jobGroups with an open fileset and no periodic configuration")

        self.fileset1.markOpen(False)
        self.assertEqual(self.fileset1.open, False, "Fileset is opened, why?")

        # We should also check if there are aqcuired files, if there are, there are jobs,
        # we don't want to fire another jobs while previous are running (output is integrating whatever  input)
        # TODO : The above one we can do when all is done. Not priority

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory()

        self.assertEqual(len(jobGroups), 1,
                         "Harvest jobsplitter didn't create a single jobGroup after the fileset was closed")

        return

    def testPeriodicTrigger(self):
        """
        _testPeriodicTrigger_

        """
        self.assertEqual(self.fileset1.open, True, "Fileset is not open, not testing periodic here")
        # Test timeout (5s for this first test)
        # there should be no acquired files, if there are, shouldn't be a job
        # self.subscription1.acquireFiles(self.subscription1.availableFiles().pop())

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=3)

        self.assertEqual(len(jobGroups), 1, "Didn't created the first periodic job when there were acquired files")

        # For the whole thing to work, faking the first job finishing, and putting the files as complete
        self.finishJobs(jobGroups)

        # Adding more of files, so we have new stuff to process
        for fileNum in range(12, 24):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(1, *[1]))
            newFile.setLocation('T1_US_FNAL_Disk')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        # Testing that it doesn't create a job unless the delay is past
        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=2)

        self.assertEqual(len(jobGroups), 0,
                         "Created one or more job, when there were non-acquired file and the period is not passed by")

        time.sleep(2)

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=2)

        self.assertEqual(len(jobGroups), 1,
                         "Didn't created one or more job, and there weren't and the period is passed by")

        # Finishing out previous jobs
        self.finishJobs(jobGroups)

        # Adding more of files, so we have new stuff to process
        for fileNum in range(26, 36):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(1, *[1]))
            newFile.setLocation('T1_US_FNAL_Disk')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        # Trying to create another job just afterwards, it shouldn't, because it should respect the configured delay
        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=2)

        self.assertEqual(len(jobGroups), 0, "Created one or more job, there are new files, but the delay is not past")

        time.sleep(2)

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=2)

        self.assertEqual(len(jobGroups), 1, "Didn't created one or more job, there are new files and the delay is past")

        # Last check is whether the job gets all the files or not

        numFilesJob = jobGroups[0].jobs[0].getFiles()
        numFilesFileset = self.fileset1.getFiles()
        self.assertEqual(numFilesJob, numFilesFileset, "Job didn't got all the files")

        # Finishing out previous jobs
        self.finishJobs(jobGroups)

        # Adding files for the first location
        for fileNum in range(38, 48):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(1, *[1]))
            newFile.setLocation('T1_US_FNAL_Disk')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()
        # Then another location
        for fileNum in range(50, 56):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(1, *[1]))
            newFile.setLocation('T2_CH_CERN')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        # We should have jobs in both locations
        time.sleep(2)

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=2)

        self.assertEqual(len(jobGroups[0].getJobs()), 2, "We didn't get 2 jobs for 2 locations")

        firstJobLocation = jobGroups[0].getJobs()[0].getFileLocations()[0]
        secondJobLocation = jobGroups[0].getJobs()[1].getFileLocations()[0]

        self.assertEqual(firstJobLocation, 'T2_CH_CERN')
        self.assertEqual(secondJobLocation, 'T1_US_FNAL')

        self.finishJobs(jobGroups)

        for fileNum in range(60, 65):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(2, *[2]))
            newFile.setLocation('T2_CH_CERN')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        for fileNum in range(70, 75):
            newFile = File("/some/file/name%d" % fileNum, size=1000, events=100)
            newFile.addRun(Run(3, *[3]))
            newFile.setLocation('T2_CH_CERN')
            self.fileset1.addFile(newFile)
        self.fileset1.commit()

        time.sleep(2)

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=self.subscription1)
        jobGroups = jobFactory(periodic_harvest_interval=2)

        # This is one of the most "complicated" tests so worth to comment, 4 jobs should be created
        # 1 - all previous files from SomeSE and run = 1 (a lot, like ~45)
        # 2 - Few files from SomeSE3, Run = 1
        # 3 - Few files from SomeSE3, Run = 2
        # 4 - Few files from SomeSE3, Run = 3
        self.assertEqual(len(jobGroups[0].getJobs()), 4, "We didn't get 4 jobs for adding 2 different runs to SomeSE3")

        return

    def testMultipleRunHarvesting(self):
        """
        _testMultipleRunHarvesting_

        Add some files with multiple runs in each, make sure the jobs
        are created by location and run. Verify each job mask afterwards.
        Note that in this test run are splitted between sites,
        in real life that MUST NOT happen we still don't support that.
        """
        multipleFilesFileset = Fileset(name="TestFileset")

        newFile = File("/some/file/test1", size=1000, events=100)
        newFile.addRun(Run(1, *[1, 3, 4, 5, 6, 7]))
        newFile.addRun(Run(2, *[1, 2, 4, 5, 6, 7]))
        newFile.setLocation('T1_US_FNAL_Disk')
        multipleFilesFileset.addFile(newFile)
        newFile = File("/some/file/test2", size=1000, events=100)
        newFile.addRun(Run(1, *[2, 8]))
        newFile.addRun(Run(2, *[3, 8]))
        newFile.setLocation('T2_CH_CERN')
        multipleFilesFileset.addFile(newFile)
        multipleFilesFileset.create()

        harvestingWorkflow = Workflow(spec="spec.xml",
                                      owner="hufnagel",
                                      name="TestWorkflow",
                                      task="Test")
        harvestingWorkflow.create()

        harvestSub = Subscription(fileset=multipleFilesFileset,
                                  workflow=harvestingWorkflow,
                                  split_algo="Harvest",
                                  type="Harvesting")
        harvestSub.create()

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=harvestSub)
        jobGroups = jobFactory(periodic_harvest_interval=2)
        self.assertEqual(len(jobGroups), 1, "A single job group was not created")
        self.assertEqual(len(jobGroups[0].getJobs()), 4,
                         "Four jobs were not created")

        for job in jobGroups[0].getJobs():
            runs = job['mask'].getRunAndLumis()
            self.assertEqual(len(runs), 1, "Job has more than one run configured")
            ll = LumiList(compactList={1: [[1, 1], [3, 7], [2, 2], [8, 8]],
                                       2: [[1, 2], [4, 7], [3, 3], [8, 8]]})
            run = next(iter(runs))
            for lumiPair in runs[run]:
                for lumi in range(lumiPair[0], lumiPair[1] + 1):
                    self.assertTrue((str(run), lumi) in ll, "All of %s not in %s" % (lumiPair, ll))

        self.finishJobs(jobGroups, harvestSub)

        newFile = File("/some/file/test3", size=1000, events=100)
        newFile.addRun(Run(1, *list(range(9, 15))))
        newFile.setLocation('T2_CH_CERN')
        multipleFilesFileset.addFile(newFile)
        multipleFilesFileset.commit()

        time.sleep(2)

        jobGroups = jobFactory(periodic_harvest_interval=2)
        self.assertEqual(len(jobGroups), 1, "A single job group was not created")
        self.assertEqual(len(jobGroups[0].getJobs()), 4, "Four jobs were not created")

        for job in jobGroups[0].getJobs():
            runs = job['mask'].getRunAndLumis()
            self.assertEqual(len(runs), 1, "Job has more than one run configured")
            ll = LumiList(compactList={1: [[1, 1], [3, 7], [2, 2], [8, 8], [9, 14]],
                                       2: [[1, 2], [4, 7], [3, 3], [8, 8]]})
            run = next(iter(runs))
            for lumiPair in runs[run]:
                for lumi in range(lumiPair[0], lumiPair[1] + 1):
                    self.assertTrue((run, lumi) in ll, "All of %s not in %s" % (lumiPair, ll))

        harvestingWorkflowSib = Workflow(spec="spec.xml",
                                         owner="hufnagel",
                                         name="TestWorkflowSib",
                                         task="TestSib")
        harvestingWorkflowSib.create()

        harvestSubSib = Subscription(fileset=multipleFilesFileset,
                                     workflow=harvestingWorkflowSib,
                                     split_algo="Harvest",
                                     type="Harvesting")
        harvestSubSib.create()

        jobFactorySib = self.splitterFactory(package="WMCore.WMBS", subscription=harvestSubSib)

        multipleFilesFileset.markOpen(False)

        jobGroups = jobFactorySib(periodic_harvest_sibling=True)
        self.assertEqual(len(jobGroups), 0, "A single job group was created")

        self.finishJobs(jobGroups, harvestSub)

        jobGroups = jobFactorySib(periodic_harvest_sibling=True)
        self.assertEqual(len(jobGroups), 1, "A single job group was not created")
        self.assertEqual(len(jobGroups[0].getJobs()), 4, "Four jobs were not created")

        for job in jobGroups[0].getJobs():
            runs = job['mask'].getRunAndLumis()
            self.assertEqual(len(runs), 1, "Job has more than one run configured")
            ll = LumiList(compactList={1: [[1, 1], [3, 7], [2, 2], [8, 8], [9, 14]],
                                       2: [[1, 2], [4, 7], [3, 3], [8, 8]]})
            run = next(iter(runs))
            for lumiPair in runs[run]:
                for lumi in range(lumiPair[0], lumiPair[1] + 1):
                    self.assertTrue((run, lumi) in ll, "All of %s not in %s" % (lumiPair, ll))

    def testMultiRunHarvesting(self):
        """
        _testMultiRunHarvesting_

        Provided a fileset with a couple of files and different runs, create a
        single job for all the runs at a specific location, which also adds a
        baggage to the job (True) which is later on looked up by SetupCMSSWPSet.
        """
        multipleFilesFileset = createCommonFileset()
        self.assertEqual(multipleFilesFileset.open, True)

        harvestingWorkflow = Workflow(spec="spec.xml",
                                      owner="amaltaro",
                                      name="TestWorkflow",
                                      task="Test")
        harvestingWorkflow.create()

        harvestSub = Subscription(fileset=multipleFilesFileset,
                                  workflow=harvestingWorkflow,
                                  split_algo="Harvest",
                                  type="Harvesting")
        harvestSub.create()

        multipleFilesFileset.markOpen(False)
        self.assertEqual(multipleFilesFileset.open, False, "Fileset should now be closed")

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=harvestSub)
        jobGroups = jobFactory(dqmHarvestUnit="multiRun")
        self.assertEqual(len(jobGroups), 1)

        for jobGroup in jobGroups:
            self.assertEqual(len(jobGroup.jobs), 1)
            for job in jobGroup.jobs:
                baggage = job.getBaggage()
                self.assertTrue(getattr(baggage, "multiRun", False), "It's supposed to be a multiRun job")
                self.assertEqual(getattr(baggage, "runLimits", ""), "-1-6")

    def testByRunHarvesting(self):
        """
        _testByRunHarvesting_
        Provided a fileset with a couple of files and 4 different runs, create
        one single job per run and location.
        The multiRun baggage should be false in this case.
        """
        multipleFilesFileset = createCommonFileset()
        self.assertEqual(multipleFilesFileset.open, True, "Fileset should be open!")

        harvestingWorkflow = Workflow(spec="spec.xml",
                                      owner="amaltaro",
                                      name="TestWorkflow",
                                      task="Test")
        harvestingWorkflow.create()

        harvestSub = Subscription(fileset=multipleFilesFileset,
                                  workflow=harvestingWorkflow,
                                  split_algo="Harvest",
                                  type="Harvesting")
        harvestSub.create()

        multipleFilesFileset.markOpen(False)
        self.assertEqual(multipleFilesFileset.open, False, "Fileset should now be closed")

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=harvestSub)
        jobGroups = jobFactory()
        self.assertEqual(len(jobGroups), 1, "Should have created 1 job group")

        for jobGroup in jobGroups:
            self.assertEqual(len(jobGroup.jobs), 6, "Should have created 6 jobs")
            for job in jobGroup.jobs:
                baggage = job.getBaggage()
                self.assertFalse(getattr(baggage, "multiRun", False), "It's supposed to be a byRun job")

    def testByRunAndRunWhitelist(self):
        """
        _testByRunAndRunWhitelist_

        Create harvesting jobs by run for the runs provided in the RunWhitelist
        """
        multipleFilesFileset = createCommonFileset()
        self.assertEqual(multipleFilesFileset.open, True)

        harvestingWorkflow = Workflow(spec="spec.xml", owner="amaltaro",
                                      name="TestWorkflow", task="Test")
        harvestingWorkflow.create()

        harvestSub = Subscription(fileset=multipleFilesFileset, workflow=harvestingWorkflow,
                                  split_algo="Harvest", type="Harvesting")
        harvestSub.create()

        multipleFilesFileset.markOpen(False)
        self.assertEqual(multipleFilesFileset.open, False, "Fileset should now be closed")

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=harvestSub)
        jobGroups = jobFactory(runWhitelist=[1, 3])
        self.assertEqual(len(jobGroups), 1, "One jobgroup per location")

        for jobGroup in jobGroups:
            self.assertEqual(len(jobGroup.jobs), 2)

    def testByRunAndRunBlacklist(self):
        """
        _testByRunAndRunWhitelist_

        Create harvesting jobs by run for the runs provided in the RunWhitelist
        """
        multipleFilesFileset = createCommonFileset()
        self.assertEqual(multipleFilesFileset.open, True)

        harvestingWorkflow = Workflow(spec="spec.xml", owner="amaltaro",
                                      name="TestWorkflow", task="Test")
        harvestingWorkflow.create()

        harvestSub = Subscription(fileset=multipleFilesFileset, workflow=harvestingWorkflow,
                                  split_algo="Harvest", type="Harvesting")
        harvestSub.create()

        multipleFilesFileset.markOpen(False)
        self.assertEqual(multipleFilesFileset.open, False, "Fileset should now be closed")

        jobFactory = self.splitterFactory(package="WMCore.WMBS", subscription=harvestSub)
        jobGroups = jobFactory(runWhitelist=[1, 2, 3, 4, 5], runBlacklist=[1, 3])
        self.assertEqual(len(jobGroups), 1, "One jobgroup per location")

        for jobGroup in jobGroups:
            self.assertEqual(len(jobGroup.jobs), 3)


if __name__ == '__main__':
    unittest.main()
