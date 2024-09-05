#!/usr/bin/env python
"""
_JobAccountant_t_

Unit tests for the WMAgent JobAccountant component.
"""
from __future__ import print_function, division
from builtins import range

import copy
import os.path
import threading
import time
import unittest
import logging

from nose.plugins.attrib import attr

import WMCore.WMBase
from WMComponent.DBS3Buffer.DBSBufferDataset import DBSBufferDataset
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMComponent.JobAccountant.AccountantWorker import AccountantWorker
from WMComponent.JobAccountant.JobAccountantPoller import JobAccountantPoller
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.FwkJobReport.Report import Report
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.WMWorkload import newWorkload
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class JobAccountantTest(EmulatedUnitTestCase):
    """
    _JobAccountantTest_

    Unit tests for the WMAgent JobAccountant.
    """

    def setUp(self):
        """
        _setUp_

        Create the database connections, install the schemas and create the
        DAO objects.
        """
        super(JobAccountantTest, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("jobaccountant_t", "JobDump", "FWJRDump")
        self.testInit.setupCouch("jobaccountant_acdc_t", "ACDC", "GroupUser")
        self.testInit.setupCouch("jobaccountant_wmstats_t", "WMStats")
        self.testInit.setSchema(customModules=["WMComponent.DBS3Buffer",
                                               "WMCore.WMBS"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        locationAction = self.daofactory(classname="Locations.New")
        locationAction.execute(siteName="site1", pnn="T1_US_FNAL_Disk")
        locationAction.execute(siteName="site2", pnn="T2_CH_CERN")
        locationAction.execute(siteName="site3", pnn="T2_CH_CERN")

        self.stateChangeAction = self.daofactory(classname="Jobs.ChangeState")
        self.setFWJRAction = self.daofactory(classname="Jobs.SetFWJRPath")
        self.getJobTypeAction = self.daofactory(classname="Jobs.GetType")
        self.getOutputMapAction = self.daofactory(classname="Jobs.GetOutputMap")

        self.dbsbufferFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                           logger=myThread.logger,
                                           dbinterface=myThread.dbi)
        self.countDBSFilesAction = self.dbsbufferFactory(classname="CountFiles")
        self.insertWorkflow = self.dbsbufferFactory(classname="InsertWorkflow")

        dbsLocationAction = self.dbsbufferFactory(classname="DBSBufferFiles.AddLocation")
        dbsLocationAction.execute(siteName="T1_US_FNAL_Disk")
        dbsLocationAction.execute(siteName="T2_CH_CERN")
        dbsLocationAction.execute(siteName="T2_CH_CERN")

        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the WMBS and DBSBuffer database schemas.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        return

    def createConfig(self):
        """
        _createConfig_

        Create a config for the JobAccountant.  This config needs to include
        information for connecting to the database as the component will create
        it's own database connections.  These parameters are still pulled from
        the environment.
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        config.section_("ACDC")
        config.ACDC.couchurl = os.getenv("COUCHURL")
        config.ACDC.database = "jobaccountant_acdc_t"

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = os.getenv("COUCHURL")
        config.JobStateMachine.couchDBName = "jobaccountant_t"
        config.JobStateMachine.jobSummaryDBName = "jobaccountant_wmstats_t"

        config.component_("JobAccountant")
        config.JobAccountant.pollInterval = 60
        config.JobAccountant.componentDir = os.getcwd()
        config.JobAccountant.logLevel = 'SQLDEBUG'
        config.JobAccountant.specDir = self.testDir

        config.component_("TaskArchiver")
        config.TaskArchiver.localWMStatsURL = "%s/%s" % (
            config.JobStateMachine.couchurl, config.JobStateMachine.jobSummaryDBName)

        return config

    def setupDBForJobFailure(self, jobName, fwjrName):
        """
        _setupDBForJobFailure_

        Create the appropriate workflows, filesets, subscriptions, files,
        jobgroups and jobs in the database so that the accountant's handling of
        failed jobs can be tested.  Move the job to the complete state and set
        the path to the job report.
        """
        testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                name="TestWF", task="None")
        testWorkflow.create()

        inputFile = File(lfn="/path/to/some/lfn", size=10, events=10,
                         locations="T1_US_FNAL_Disk")
        inputFile.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()

        self.testSubscription = Subscription(fileset=testFileset,
                                             workflow=testWorkflow,
                                             split_algo="FileBased",
                                             type="Processing")
        self.testSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=self.testSubscription)
        testJobGroup.create()

        testJob = Job(name=jobName, files=[inputFile])
        testJob.create(group=testJobGroup)
        testJob["state"] = "complete"
        self.stateChangeAction.execute(jobs=[testJob])

        fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t/fwjrs", fwjrName)
        self.setFWJRAction.execute(jobID=testJob["id"], fwjrPath=fwjrPath)
        return

    def verifyJobFailure(self, jobName):
        """
        _verifyJobFailure_

        Verify that the accountant handled a job failure correctly.  The state
        should be executing, the outcome should be "fail", the retry count
        should be 1 and all the input files should be marked as failed.
        """
        testJob = Job(name=jobName)
        testJob.load()

        assert testJob["state"] == "jobfailed", \
            "Error: test job in wrong state: %s" % testJob["state"]
        assert testJob["outcome"] == "failure", \
            "Error: test job has wrong outcome: %s" % testJob["outcome"]

        # We no longer mark files as failed in the Accountant
        self.assertEqual(len(self.testSubscription.filesOfStatus("Acquired")),
                         1, "Error: Wrong number of acquired files.")
        self.assertEqual(len(self.testSubscription.filesOfStatus("Failed")), 0,
                         "Error: Wrong number of failed files: %s" \
                         % len(self.testSubscription.filesOfStatus("Failed")))
        return

    def testFailedJob(self):
        """
        _testFailedJob_

        Run a failed job that has a vaid job report through the accountant.
        Verify that it functions correctly.
        """
        self.setupDBForJobFailure(jobName="T0Skim-Run2-Skim2-Jet-631",
                                  fwjrName="badBackfillJobReport.pkl")

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        self.verifyJobFailure("T0Skim-Run2-Skim2-Jet-631")

        f = File(id=2)
        f.load()
        self.assertEqual(f['lfn'],
                         '/store/unmerged/logs/prod/2011/3/16/cmsdataops_Backfill_110311_01_T1_IT_CNAF_Mu_110311_171929/DataProcessing/DataProcessingMergeRECOoutput/MuSkim/0000/0/4c32fa40-4da3-11e0-b58a-00221959e72f-0-0-logArchive.tar.gz')
        return

    def testEmptyFWJR(self):
        """
        _testEmptyFWJR_

        Run an empty framework job report through the accountant.  Verify that
        it functions correctly.
        """
        self.setupDBForJobFailure(jobName="T0Skim-Run2-Skim2-Jet-631",
                                  fwjrName="EmptyJobReport.pkl")

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        self.verifyJobFailure("T0Skim-Run2-Skim2-Jet-631")
        return

    def testBadFWJR(self):
        """
        _testBadFWJR_

        Run a framework job report that has invalid XML through the accountant.
        Verify that it functions correctly.
        """
        self.setupDBForJobFailure(jobName="T0Merge-Run1-Mu-AOD-722",
                                  fwjrName="MergeSuccessBadPKL.pkl")

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        self.verifyJobFailure("T0Merge-Run1-Mu-AOD-722")
        return

    def setupDBForSplitJobSuccess(self):
        """
        _setupDBForSplitJobSuccess_

        Create the appropriate workflows, filesets, subscriptions, files,
        jobgroups and jobs in the database so that the accountant's handling of
        split jobs can be tested.  Move the one job to the complete state and
        the rest to the executing state.  Set the paths to the job reports in
        all of the jobs.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        self.recoOutputFileset = Fileset(name="RECO")
        self.recoOutputFileset.create()
        self.alcaOutputFileset = Fileset(name="ALCA")
        self.alcaOutputFileset.create()
        self.cleanupFileset = Fileset(name="Cleanup")
        self.cleanupFileset.create()

        self.testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                     name="TestWF", task="None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("FEVTRECO", self.recoOutputFileset,
                                    self.recoOutputFileset)
        self.testWorkflow.addOutput("FEVTRECO", self.cleanupFileset, None)
        self.testWorkflow.addOutput("ALCARECOStreamCombinedALCARECO", self.alcaOutputFileset,
                                    self.alcaOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombinedALCARECO", self.cleanupFileset, None)

        inputFile = File(lfn="/path/to/some/lfn", size=600000, events=60000,
                         locations="T1_US_FNAL_Disk")
        inputFile.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()

        self.testSubscription = Subscription(fileset=testFileset,
                                             workflow=self.testWorkflow,
                                             split_algo="EventBased",
                                             type="Processing")
        self.testSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=self.testSubscription)
        testJobGroup.create()

        self.testJobA = Job(name="SplitJobA", files=[inputFile])
        self.testJobA.create(group=testJobGroup)
        self.testJobA["state"] = "complete"
        self.testJobA["mask"].setMaxAndSkipEvents(20000, 0)
        self.testJobA.save()
        self.stateChangeAction.execute(jobs=[self.testJobA],
                                       conn=myThread.transaction.conn,
                                       transaction=True)

        self.testJobB = Job(name="SplitJobB", files=[inputFile])
        self.testJobB.create(group=testJobGroup)
        self.testJobB["mask"].setMaxAndSkipEvents(20000, 20000)
        self.testJobB["state"] = "executing"
        self.testJobB.save()
        self.stateChangeAction.execute(jobs=[self.testJobB],
                                       conn=myThread.transaction.conn,
                                       transaction=True)

        self.testJobC = Job(name="SplitJobC", files=[inputFile])
        self.testJobC.create(group=testJobGroup)
        self.testJobC["state"] = "executing"
        self.testJobC["mask"].setMaxAndSkipEvents(20000, 40000)
        self.testJobC.save()
        self.stateChangeAction.execute(jobs=[self.testJobC],
                                       conn=myThread.transaction.conn,
                                       transaction=True)

        fwjrBasePath = os.path.join(WMCore.WMBase.getTestBase(),
                                    "WMComponent_t/JobAccountant_t/fwjrs/")
        self.setFWJRAction.execute(jobID=self.testJobA["id"],
                                   fwjrPath=os.path.join(fwjrBasePath, "SplitSuccessA.pkl"),
                                   conn=myThread.transaction.conn,
                                   transaction=True)
        self.setFWJRAction.execute(jobID=self.testJobB["id"],
                                   fwjrPath=fwjrBasePath + "SplitSuccessB.pkl",
                                   conn=myThread.transaction.conn,
                                   transaction=True)
        self.setFWJRAction.execute(jobID=self.testJobC["id"],
                                   fwjrPath=fwjrBasePath + "SplitSuccessC.pkl",
                                   conn=myThread.transaction.conn,
                                   transaction=True)
        myThread.transaction.commit()
        return

    def verifyJobSuccess(self, jobID):
        """
        _verifyJobSuccess_

        Verify that the metadata for a successful job is correct.  This will
        check the outcome, retry count and state.
        """
        testJob = Job(id=jobID)
        testJob.load()

        assert testJob["state"] == "success", \
            "Error: test job in wrong state: %s" % testJob["state"]
        assert testJob["retry_count"] == 0, \
            "Error: test job has wrong retry count: %s" % testJob["retry_count"]
        assert testJob["outcome"] == "success", \
            "Error: test job has wrong outcome: %s" % testJob["outcome"]

        return

    def verifyFileMetaData(self, jobID, fwkJobReportFiles, site="T1_US_FNAL_Disk",
                           skippedJobReportFiles=0):
        """
        _verifyFileMetaData_

        Verify that all the files that were output by a job made it into WMBS
        correctly.  Compare the contents of WMBS to the files in the frameworks
        job report.

        Note that fwkJobReportFiles is a list of DataStructs File objects.
        """
        testJob = Job(id=jobID)
        testJob.loadData()

        inputLFNs = []
        for inputFile in testJob["input_files"]:
            inputLFNs.append(inputFile["lfn"])

        for fwkJobReportFile in fwkJobReportFiles:
            outputFile = File(lfn=fwkJobReportFile["lfn"])
            outputFile.load()
            outputFile.loadData(parentage=1)

            # Output file should be at the same location as the input files
            self.assertEqual(outputFile['locations'], set([site]))

            assert outputFile["events"] == int(fwkJobReportFile["events"]), \
                "Error: Output file has wrong events: %s, %s" % \
                (outputFile["events"], fwkJobReportFile["events"])
            assert outputFile["size"] == int(fwkJobReportFile["size"]), \
                "Error: Output file has wrong size: %s, %s" % \
                (outputFile["size"], fwkJobReportFile["size"])

            for ckType in fwkJobReportFile["checksums"]:
                assert ckType in outputFile["checksums"], \
                    "Error: Output file is missing checksums: %s" % ckType
                assert outputFile["checksums"][ckType] == fwkJobReportFile["checksums"][ckType], \
                    "Error: Checksums don't match."

            assert len(fwkJobReportFile["checksums"]) == \
                   len(outputFile["checksums"]), \
                "Error: Wrong number of checksums."

            jobType = self.getJobTypeAction.execute(jobID=jobID)
            if jobType == "Merge":
                assert str(outputFile["merged"]) == "True", \
                    "Error: Merge jobs should output merged files."
            else:
                assert outputFile["merged"] == fwkJobReportFile["merged"], \
                    "Error: Output file merged output is wrong: %s, %s" % \
                    (outputFile["merged"], fwkJobReportFile["merged"])

            assert len(outputFile["locations"]) == 1, \
                "Error: outputfile should have one location: %s" % outputFile["locations"]
            assert list(outputFile["locations"])[0] == list(fwkJobReportFile["locations"])[0], \
                "Error: wrong location for file."

            assert len(outputFile["parents"]) == (len(inputLFNs) - skippedJobReportFiles), \
                "Error: Output file has wrong number of parents."
            for outputParent in outputFile["parents"]:
                assert outputParent["lfn"] in inputLFNs, \
                    "Error: Unknown parent file: %s" % outputParent["lfn"]

            fwjrRuns = {}
            for run in fwkJobReportFile["runs"]:
                fwjrRuns[run.run] = run.lumis

            for run in outputFile["runs"]:
                assert run.run in fwjrRuns, \
                    "Error: Extra run in output: %s" % run.run

                for lumi in run:
                    assert lumi in fwjrRuns[run.run], \
                        "Error: Extra lumi: %s" % lumi

                    fwjrRuns[run.run].remove(lumi)

                if len(fwjrRuns[run.run]) == 0:
                    del fwjrRuns[run.run]

            assert len(fwjrRuns) == 0, \
                "Error: Missing runs, lumis: %s" % fwjrRuns

            testJobGroup = JobGroup(id=testJob["jobgroup"])
            testJobGroup.loadData()
            jobGroupFileset = testJobGroup.output
            jobGroupFileset.loadData()

            assert outputFile["id"] in jobGroupFileset.getFiles(type="id"), \
                "Error: output file not in jobgroup fileset."

            if testJob["mask"]["FirstEvent"] == None:
                assert outputFile["first_event"] == 0, \
                    "Error: first event not set correctly: 0, %s" % \
                    outputFile["first_event"]
            else:
                assert testJob["mask"]["FirstEvent"] == outputFile["first_event"], \
                    "Error: last event not set correctly: %s, %s" % \
                    (testJob["mask"]["FirstEvent"], outputFile["first_event"])

        return

    def verifyDBSBufferContents(self, subType, parentFileLFNs, fwkJobReportFiles):
        """
        _verifyDBSBufferContents_

        Verify that merged files in the framework job report made it into
        the DBS buffer correctly.  Compare the metadata in the DBS buffer to
        the files in the framework job report.  Also verify file parentage.

        Note that parentFileLFNs can be a list of LFNs or a dictionary.  The
        dictionary would contain keys for each of the files produced by the
        job.  Each value would be a list of the parent LFNs.
        """

        for fwkJobReportFile in fwkJobReportFiles:
            if fwkJobReportFile["merged"] != True and subType != "Merge":
                continue

            dbsFile = DBSBufferFile(lfn=fwkJobReportFile["lfn"])

            assert dbsFile.exists() != False, \
                "Error: File is not in DBSBuffer: %s" % fwkJobReportFile["lfn"]

            dbsFile.load(parentage=1)

            assert dbsFile["events"] == fwkJobReportFile["events"], \
                "Error: DBS file has wrong events: %s, %s" % \
                (dbsFile["events"], fwkJobReportFile["events"])
            assert dbsFile["size"] == fwkJobReportFile["size"], \
                "Error: DBS file has wrong size: %s, %s" % \
                (dbsFile["size"], fwkJobReportFile["size"])

            for ckType in fwkJobReportFile["checksums"]:
                assert ckType in dbsFile["checksums"], \
                    "Error: DBS file is missing checksums: %s" % ckType
                assert dbsFile["checksums"][ckType] == fwkJobReportFile["checksums"][ckType], \
                    "Error: Checksums don't match."

            assert len(fwkJobReportFile["checksums"]) == \
                   len(dbsFile["checksums"]), \
                "Error: Wrong number of checksums."

            assert len(dbsFile["locations"]) == 1, \
                "Error: DBS file should have one location"
            assert list(dbsFile["locations"])[0] == list(fwkJobReportFile["locations"])[0], \
                "Error: wrong location for DBS file."

            fwjrRuns = {}
            for run in fwkJobReportFile["runs"]:
                fwjrRuns[run.run] = run.lumis

            for run in dbsFile["runs"]:
                assert run.run in fwjrRuns, \
                    "Error: Extra run in output: %s" % run.run

                for lumi in run:
                    assert lumi in fwjrRuns[run.run], \
                        "Error: Extra lumi: %s" % lumi

                    fwjrRuns[run.run].remove(lumi)

                if len(fwjrRuns[run.run]) == 0:
                    del fwjrRuns[run.run]

            assert len(fwjrRuns) == 0, \
                "Error: Missing runs, lumis: %s" % fwjrRuns

            # PSetHash and ConfigContent are not currently used.
            datasetInfo = fwkJobReportFile["dataset"]
            assert dbsFile["appName"] == datasetInfo["applicationName"], \
                "Error: app name is wrong in DBS buffer."
            assert dbsFile["appVer"] == datasetInfo["applicationVersion"], \
                "Error: app ver is wrong in DBS buffer."
            assert dbsFile["appFam"] == fwkJobReportFile["module_label"], \
                "Error: app fam is wrong in DBS buffer."

            datasetPath = "/%s/%s/%s" % (datasetInfo["primaryDataset"],
                                         datasetInfo["processedDataset"],
                                         datasetInfo["dataTier"])
            assert dbsFile["datasetPath"] == datasetPath, \
                "Error: dataset path in buffer is wrong."

            if isinstance(parentFileLFNs, dict):
                parentFileLFNsCopy = copy.deepcopy(parentFileLFNs[fwkJobReportFile["lfn"]])
            else:
                parentFileLFNsCopy = copy.deepcopy(parentFileLFNs)

            for dbsParent in dbsFile["parents"]:
                assert dbsParent["lfn"] in parentFileLFNsCopy, \
                    "Error: unknown parents: %s" % dbsParent["lfn"]

                parentFileLFNsCopy.remove(dbsParent["lfn"])

            assert len(parentFileLFNsCopy) == 0, \
                "Error: missing parents %s." % parentFileLFNsCopy

        return

    def testSplitJobs(self):
        """
        _testSplitJobs_

        Verify that split processing jobs are accounted correctly.  This is
        mainly to verify that the input for a series of split jobs is not marked
        as complete until all the split jobs are complete.
        """
        self.setupDBForSplitJobSuccess()
        config = self.createConfig()

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        fwjrBasePath = os.path.join(WMCore.WMBase.getTestBase(),
                                    "WMComponent_t/JobAccountant_t/fwjrs/")
        jobReport = Report()
        jobReport.unpersist(fwjrBasePath + "SplitSuccessA.pkl")
        self.verifyFileMetaData(self.testJobA["id"], jobReport.getAllFilesFromStep("cmsRun1"), site="T2_CH_CERN")
        self.verifyJobSuccess(self.testJobA["id"])

        self.recoOutputFileset.loadData()
        self.alcaOutputFileset.loadData()
        self.cleanupFileset.loadData()

        for fwjrFile in jobReport.getAllFilesFromStep("cmsRun1"):
            assert fwjrFile["lfn"] in self.cleanupFileset.getFiles(type="lfn"), \
                "Error: file is missing from cleanup output fileset."
            if fwjrFile["dataset"]["dataTier"] == "RECO":
                assert fwjrFile["lfn"] in self.recoOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["lfn"] in self.alcaOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from alca output fileset."

        assert len(self.recoOutputFileset.getFiles(type="list")) == 1, \
            "Error: Wrong number of files in reco output fileset."
        assert len(self.alcaOutputFileset.getFiles(type="list")) == 1, \
            "Error: Wrong number of files in alca output fileset."

        assert len(self.testSubscription.filesOfStatus("Acquired")) == 1, \
            "Error: The input file should be acquired."

        self.testJobB["state"] = "complete"
        self.testJobC["state"] = "complete"
        self.stateChangeAction.execute(jobs=[self.testJobB, self.testJobC])

        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(fwjrBasePath + "SplitSuccessB.pkl")
        self.verifyFileMetaData(self.testJobB["id"], jobReport.getAllFilesFromStep("cmsRun1"), site="T2_CH_CERN")
        self.verifyJobSuccess(self.testJobB["id"])

        self.recoOutputFileset.loadData()
        self.alcaOutputFileset.loadData()

        for fwjrFile in jobReport.getAllFilesFromStep("cmsRun1"):
            if fwjrFile["dataset"]["dataTier"] == "RECO":
                assert fwjrFile["lfn"] in self.recoOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["lfn"] in self.alcaOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from alca output fileset."

        jobReport = Report()
        jobReport.unpersist(fwjrBasePath + "SplitSuccessC.pkl")
        self.verifyFileMetaData(self.testJobC["id"], jobReport.getAllFilesFromStep("cmsRun1"), site="T2_CH_CERN")
        self.verifyJobSuccess(self.testJobC["id"])

        for fwjrFile in jobReport.getAllFilesFromStep("cmsRun1"):
            if fwjrFile["dataset"]["dataTier"] == "RECO":
                assert fwjrFile["lfn"] in self.recoOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["lfn"] in self.alcaOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from alca output fileset."

        assert len(self.testSubscription.filesOfStatus("Completed")) == 1, \
            "Error: The input file should be complete."

        assert self.countDBSFilesAction.execute() == 0, \
            "Error: there shouldn't be any files in DBSBuffer."

        assert len(self.recoOutputFileset.getFiles(type="list")) == 3, \
            "Error: Wrong number of files in reco output fileset."
        assert len(self.alcaOutputFileset.getFiles(type="list")) == 3, \
            "Error: Wrong number of files in alca output fileset."

        return

    def setupDBForMergedSkimSuccess(self):
        """
        _setupDBForMergedSkimSuccess_

        Initialize the database so that we can test the result of a skim job
        that produced merged output.  This needs to setup merge workflows and
        filesset to hold the "merged" files.
        Also set a "spec" file and tasks so the jobAccountant can copy this
        information to the dbsbuffer workflow table.
        """
        self.recoOutputFileset = Fileset(name="RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name="MergedRECO")
        self.mergedRecoOutputFileset.create()
        self.alcaOutputFileset = Fileset(name="ALCA")
        self.alcaOutputFileset.create()
        self.mergedAlcaOutputFileset = Fileset(name="MergedALCA")
        self.mergedAlcaOutputFileset.create()
        self.cleanupFileset = Fileset(name="Cleanup")
        self.cleanupFileset.create()
        self.insertWorkflow.execute("Steves", "/Steves/TestAlcaTask", 0, 0, 0, 0)
        dummyWorkload = newWorkload("Steves")
        dummyWorkload.save(os.path.join(self.testDir, 'initialSpecPath.pkl'))
        falseSpec = os.path.join(self.testDir, 'initialSpecPath.pkl')

        self.testWorkflow = Workflow(spec=falseSpec, owner="Steve",
                                     name="TestWF", task="TestTopTask")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("outputRECO", self.recoOutputFileset,
                                    self.mergedRecoOutputFileset)
        self.testWorkflow.addOutput("outputRECO", self.cleanupFileset, None)
        self.testWorkflow.addOutput("ALCARECOStreamCombinedALCARECO", self.alcaOutputFileset,
                                    self.mergedAlcaOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombinedALCARECO", self.cleanupFileset, None)

        self.testRecoMergeWorkflow = Workflow(spec=falseSpec, owner="Steve",
                                              name="TestRecoMergeWF", task="TestRecoMergeTask")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("MergedRECO", self.mergedRecoOutputFileset)

        self.testAlcaMergeWorkflow = Workflow(spec=falseSpec, owner="Steve",
                                              name="TestAlcaMergeWF", task="TestAlcaTask")
        self.testAlcaMergeWorkflow.create()
        self.testAlcaMergeWorkflow.addOutput("MergedALCARECO", self.mergedAlcaOutputFileset)

        inputFile = File(lfn="/path/to/some/lfn", size=600000, events=60000,
                         locations="T1_US_FNAL_Disk")
        inputFile.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()

        # Insert parent
        pFile = DBSBufferFile(lfn="/path/to/some/lfn", size=600000, events=60000)
        pFile.setAlgorithm(appName="cmsRun", appVer="UNKNOWN",
                           appFam="RECO", psetHash="GIBBERISH",
                           configContent="MOREGIBBERISH")
        pFile.setDatasetPath("/bogus/dataset/path")
        pFile.addRun(Run(1, *[45]))
        pFile.create()

        self.testSubscription = Subscription(fileset=testFileset,
                                             workflow=self.testWorkflow,
                                             split_algo="EventBased",
                                             type="Processing")

        self.testMergeRecoSubscription = Subscription(fileset=self.recoOutputFileset,
                                                      workflow=self.testRecoMergeWorkflow,
                                                      split_algo="WMBSMergeBySize",
                                                      type="Merge")

        self.testMergeAlcaSubscription = Subscription(fileset=self.alcaOutputFileset,
                                                      workflow=self.testAlcaMergeWorkflow,
                                                      split_algo="WMBSMergeBySize",
                                                      type="Merge")
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAlcaSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=self.testSubscription)
        testJobGroup.create()

        self.testJob = Job(name="SplitJobA", files=[inputFile])
        self.testJob.create(group=testJobGroup)
        self.testJob["state"] = "complete"
        self.testJob.save()
        self.stateChangeAction.execute(jobs=[self.testJob])

        self.setFWJRAction.execute(self.testJob["id"],
                                   os.path.join(WMCore.WMBase.getTestBase(),
                                                "WMComponent_t/JobAccountant_t/fwjrs",
                                                "MergedSkimSuccess.pkl"))
        return

    def testMergedSkim(self):
        """
        _testMergedSkim_

        Test how the accounant handles a skim that produces merged out.  Verify
        that merged files are inserted into the correct output filesets.
        """
        self.setupDBForMergedSkimSuccess()

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergedSkimSuccess.pkl"))
        self.verifyFileMetaData(self.testJob["id"], jobReport.getAllFilesFromStep("cmsRun1"), site="T2_CH_CERN")
        self.verifyJobSuccess(self.testJob["id"])

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.alcaOutputFileset.loadData()
        self.mergedAlcaOutputFileset.loadData()
        self.cleanupFileset.loadData()

        assert len(self.recoOutputFileset.getFiles(type="list")) == 0, \
            "Error: files should go straight to the merged reco fileset."
        assert len(self.alcaOutputFileset.getFiles(type="list")) == 0, \
            "Error: files should go straight to the merged alca fileset."
        assert len(self.cleanupFileset.getFiles(type="list")) == 0, \
            "Error: There should be no files in the cleanup fileset."

        assert len(self.mergedRecoOutputFileset.getFiles(type="list")) == 1, \
            "Error: Should be only one file in the merged reco fileset."
        assert len(self.mergedAlcaOutputFileset.getFiles(type="list")) == 1, \
            "Error: Should be only one file in the merged alca fileset."

        for fwjrFile in jobReport.getAllFilesFromStep("cmsRun1"):
            if fwjrFile["dataset"]["dataTier"] == "RECO":
                assert fwjrFile["lfn"] in self.mergedRecoOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["lfn"] in self.mergedAlcaOutputFileset.getFiles(type="lfn"), \
                    "Error: file is missing from alca output fileset."

        self.verifyDBSBufferContents("Processing", ["/path/to/some/lfn"],
                                     jobReport.getAllFilesFromStep("cmsRun1"))
        return

    def setupDBForMergeSuccess(self, createDBSParents=True, noLumi=False,
                               skipMode=False):
        """
        _setupDBForMergeSuccess_

        Setup the database to verify the behavior of the accountant when dealing
        with the result of a merge job.  Optionally create the DBS parents so we
        can test the result of MonteCarlo merge jobs that won't have parents in
        DBS.
        """
        self.recoOutputFileset = Fileset(name="RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name="MergedRECO")
        self.mergedRecoOutputFileset.create()
        self.aodOutputFileset = Fileset(name="AOD")
        self.aodOutputFileset.create()
        self.mergedAodOutputFileset = Fileset(name="MergedAOD")
        self.mergedAodOutputFileset.create()

        self.insertWorkflow.execute("Steves", "/Steves/Stupid/Task", 0, 0, 0, 0)
        dummyDataset = DBSBufferDataset(
            path="/Mu/IansMagicMushroomSoup-T0Test-AnalyzeThisAndGetAFreePhD-PreScaleThingy10-v9_29_pre14replaythingy_v5/AOD")
        dummyDataset.create()
        dummyWorkload = newWorkload("Steves")
        dummyWorkload.save(os.path.join(self.testDir, 'initialSpecPath.pkl'))
        falseSpec = os.path.join(self.testDir, 'initialSpecPath.pkl')

        self.testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                     name="Steves", task="/Steves/Stupid/ProcessingTask")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("outputRECO", self.recoOutputFileset,
                                    self.mergedRecoOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombinedALCARECO", self.aodOutputFileset,
                                    self.mergedAodOutputFileset)

        self.testRecoMergeWorkflow = Workflow(spec="wf002.xml", owner="Steve",
                                              name="Steves", task="/Steves/Stupid/RecoMergeTask")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("MergedRECO", self.mergedRecoOutputFileset,
                                             self.mergedRecoOutputFileset)

        self.testAodMergeWorkflow = Workflow(spec=falseSpec, owner="Steve",
                                             name="Steves", task="/Steves/Stupid/Task")
        self.testAodMergeWorkflow.create()
        self.testAodMergeWorkflow.addOutput("MergedAOD", self.mergedAodOutputFileset,
                                            self.mergedAodOutputFileset)

        inputFileA = File(lfn="/path/to/some/lfnA", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=createDBSParents)
        inputFileA.create()
        inputFileB = File(lfn="/path/to/some/lfnB", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=createDBSParents)
        inputFileB.create()
        inputFileC = File(lfn="/path/to/some/lfnC", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=createDBSParents)
        inputFileC.create()

        unmergedFileA = File(lfn="/path/to/some/unmerged/lfnA", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileA.create()
        unmergedFileB = File(lfn="/path/to/some/unmerged/lfnB", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileB.create()
        unmergedFileC = File(lfn="/path/to/some/unmerged/lfnC", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileC.create()

        inputFileA.addChild(unmergedFileA["lfn"])
        inputFileB.addChild(unmergedFileB["lfn"])
        inputFileC.addChild(unmergedFileC["lfn"])

        if createDBSParents:
            for plfn in ["/path/to/some/lfnA", "/path/to/some/lfnB", "/path/to/some/lfnC"]:
                pFile = DBSBufferFile(lfn=plfn, size=600000, events=60000)
                pFile.setAlgorithm(appName="cmsRun", appVer="UNKNOWN",
                                   appFam="RECO", psetHash="GIBBERISH",
                                   configContent="MOREGIBBERISH")
                pFile.setDatasetPath("/bogus/dataset/path")
                pFile.addRun(Run(1, *[45]))
                pFile.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(inputFileA)
        testFileset.addFile(inputFileB)
        testFileset.addFile(inputFileC)
        testFileset.commit()

        self.aodOutputFileset.addFile(unmergedFileA)
        self.aodOutputFileset.addFile(unmergedFileB)
        self.aodOutputFileset.addFile(unmergedFileC)
        self.aodOutputFileset.commit()

        self.testSubscription = Subscription(fileset=testFileset,
                                             workflow=self.testWorkflow,
                                             split_algo="EventBased",
                                             type="Processing")

        self.testMergeRecoSubscription = Subscription(fileset=self.recoOutputFileset,
                                                      workflow=self.testRecoMergeWorkflow,
                                                      split_algo="WMBSMergeBySize",
                                                      type="Merge")

        self.testMergeAodSubscription = Subscription(fileset=self.aodOutputFileset,
                                                     workflow=self.testAodMergeWorkflow,
                                                     split_algo="WMBSMergeBySize",
                                                     type="Merge")
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAodSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=self.testMergeAodSubscription)
        testJobGroup.create()

        self.testJob = Job(name="MergeJob", files=[unmergedFileA,
                                                   unmergedFileB,
                                                   unmergedFileC])
        self.testJob.create(group=testJobGroup)
        self.testJob["state"] = "complete"
        self.testJob.save()
        self.stateChangeAction.execute(jobs=[self.testJob])

        if noLumi:
            self.setFWJRAction.execute(jobID=self.testJob["id"],
                                       fwjrPath=os.path.join(WMCore.WMBase.getTestBase(),
                                                             "WMComponent_t/JobAccountant_t/fwjrs",
                                                             "MergeSuccessNoLumi.pkl"))
        elif skipMode:
            self.setFWJRAction.execute(jobID=self.testJob["id"],
                                       fwjrPath=os.path.join(WMCore.WMBase.getTestBase(),
                                                             "WMComponent_t/JobAccountant_t/fwjrs",
                                                             "MergeSuccessSkipFile.pkl"))
        else:
            self.setFWJRAction.execute(jobID=self.testJob["id"],
                                       fwjrPath=os.path.join(WMCore.WMBase.getTestBase(),
                                                             "WMComponent_t/JobAccountant_t/fwjrs",
                                                             "MergeSuccess.pkl"))
        return

    def testMergeSuccess(self):
        """
        _testMergeSuccess_

        Test the accountant's handling of a merge job.
        """
        self.setupDBForMergeSuccess()

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergeSuccess.pkl"))

        self.verifyFileMetaData(self.testJob["id"], jobReport.getAllFilesFromStep("cmsRun1"))
        self.verifyJobSuccess(self.testJob["id"])

        dbsParents = ["/path/to/some/lfnA", "/path/to/some/lfnB",
                      "/path/to/some/lfnC"]
        self.verifyDBSBufferContents("Merge", dbsParents, jobReport.getAllFilesFromStep("cmsRun1"))

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()

        assert len(self.mergedRecoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the merged reco fileset."
        assert len(self.recoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the reco fileset."

        assert len(self.mergedAodOutputFileset.getFiles(type="list")) == 1, \
            "Error: One file should be in the merged aod fileset."
        assert len(self.aodOutputFileset.getFiles(type="list")) == 3, \
            "Error: Three files should be in the aod fileset."

        fwjrFile = jobReport.getAllFilesFromStep("cmsRun1")[0]
        assert fwjrFile["lfn"] in self.mergedAodOutputFileset.getFiles(type="lfn"), \
            "Error: file is missing from merged aod output fileset."

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_workflow")[0].fetchall()[0]
        self.assertEqual(result[1], 'Steves')
        self.assertEqual(result[2], '/Steves/Stupid/Task')

        result = myThread.dbi.processData("SELECT workflow FROM dbsbuffer_file WHERE id = 4")[0].fetchall()[0][0]
        self.assertEqual(result, 1)

        datasetInfo = myThread.dbi.processData("SELECT * FROM dbsbuffer_dataset")[0].fetchall()[0]
        self.assertEqual(datasetInfo[1],
                         "/Mu/IansMagicMushroomSoup-T0Test-AnalyzeThisAndGetAFreePhD-PreScaleThingy10-v9_29_pre14replaythingy_v5/AOD")
        self.assertEqual(datasetInfo[2], 9) # processing version, should be integer
        self.assertEqual(datasetInfo[3], "IansMagicMushroomSoup")
        self.assertEqual(datasetInfo[4], "Production")
        self.assertEqual(datasetInfo[5], "GT:Super")

        return

    def testMergeSuccessSkippedFiles(self):
        """
        _testMergeSuccessSkippedFiles_

        Test the accountant's handling of a merge job where
        not all inputs where found but it still succeeded.
        """
        self.setupDBForMergeSuccess(skipMode=True)

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergeSuccessSkipFile.pkl"))
        self.verifyFileMetaData(self.testJob["id"], jobReport.getAllFilesFromStep("cmsRun1"),
                                skippedJobReportFiles=1)
        self.verifyJobSuccess(self.testJob["id"])

        dbsParents = ["/path/to/some/lfnA", "/path/to/some/lfnB"]
        self.verifyDBSBufferContents("Merge", dbsParents, jobReport.getAllFilesFromStep("cmsRun1"))

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()

        assert len(self.mergedRecoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the merged reco fileset."
        assert len(self.recoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the reco fileset."

        assert len(self.mergedAodOutputFileset.getFiles(type="list")) == 1, \
            "Error: One file should be in the merged aod fileset."
        assert len(self.aodOutputFileset.getFiles(type="list")) == 3, \
            "Error: Three files should be in the aod fileset."

        fwjrFile = jobReport.getAllFilesFromStep("cmsRun1")[0]
        assert fwjrFile["lfn"] in self.mergedAodOutputFileset.getFiles(type="lfn"), \
            "Error: file is missing from merged aod output fileset."

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_workflow")[0].fetchall()[0]
        self.assertEqual(result[1], 'Steves')
        self.assertEqual(result[2], '/Steves/Stupid/Task')

        result = myThread.dbi.processData("SELECT workflow FROM dbsbuffer_file WHERE id = 4")[0].fetchall()[0][0]
        self.assertEqual(result, 1)

        result = myThread.dbi.processData("SELECT fileid FROM wmbs_sub_files_failed")[0].fetchall()[0][0]
        self.assertEqual(result, 6)

        dataCollectionService = DataCollectionService(os.environ["COUCHURL"], 'jobaccountant_acdc_t')
        x = dataCollectionService.getDataCollection('Steves')
        self.assertEqual(len(x["filesets"]), 1)
        self.assertEqual(len(x["filesets"][0]["files"]), 1)
        return

    def testMergeSuccessNoLumi(self):
        """
        _testMergeSuccessNoLumi_

        Verify that the accountant works correctly with jobs that produce files
        with no run and lumi information.
        """
        self.setupDBForMergeSuccess(noLumi=True)

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergeSuccessNoLumi.pkl"))
        self.verifyFileMetaData(self.testJob["id"], jobReport.getAllFilesFromStep("cmsRun1"))
        self.verifyJobSuccess(self.testJob["id"])

        dbsParents = ["/path/to/some/lfnA", "/path/to/some/lfnB",
                      "/path/to/some/lfnC"]
        self.verifyDBSBufferContents("Merge", dbsParents, jobReport.getAllFilesFromStep("cmsRun1"))

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()

        assert len(self.mergedRecoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the merged reco fileset."
        assert len(self.recoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the reco fileset."

        assert len(self.mergedAodOutputFileset.getFiles(type="list")) == 1, \
            "Error: One file should be in the merged aod fileset."
        assert len(self.aodOutputFileset.getFiles(type="list")) == 3, \
            "Error: Three files should be in the aod fileset."

        fwjrFile = jobReport.getAllFilesFromStep("cmsRun1")[0]
        assert fwjrFile["lfn"] in self.mergedAodOutputFileset.getFiles(type="lfn"), \
            "Error: file is missing from merged aod output fileset."

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_workflow")[0].fetchall()[0]
        self.assertEqual(result[1], 'Steves')
        self.assertEqual(result[2], '/Steves/Stupid/Task')

        result = myThread.dbi.processData("SELECT workflow FROM dbsbuffer_file WHERE id = 4")[0].fetchall()[0][0]
        self.assertEqual(result, 1)

        return

    def testParentlessMergeSuccess(self):
        """
        _testParentlessMergeSuccess_

        Test the accountant's handling of a merge job that would result from
        a montecarlo workflow.
        """
        self.setupDBForMergeSuccess(createDBSParents=False)

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergeSuccess.pkl"))
        self.verifyFileMetaData(self.testJob["id"], jobReport.getAllFilesFromStep("cmsRun1"))
        self.verifyJobSuccess(self.testJob["id"])

        dbsParents = []
        self.verifyDBSBufferContents("Merge", dbsParents, jobReport.getAllFilesFromStep("cmsRun1"))

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()

        assert len(self.mergedRecoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the merged reco fileset."
        assert len(self.recoOutputFileset.getFiles(type="list")) == 0, \
            "Error: No files should be in the reco fileset."

        assert len(self.mergedAodOutputFileset.getFiles(type="list")) == 1, \
            "Error: One file should be in the merged aod fileset."
        assert len(self.aodOutputFileset.getFiles(type="list")) == 3, \
            "Error: Three files should be in the aod fileset."

        fwjrFile = jobReport.getAllFilesFromStep("cmsRun1")[0]
        assert fwjrFile["lfn"] in self.mergedAodOutputFileset.getFiles(type="lfn"), \
            "Error: file is missing from merged aod output fileset."

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_workflow")[0].fetchall()[0]
        self.assertEqual(result[1], 'Steves')
        self.assertEqual(result[2], '/Steves/Stupid/Task')

        result = myThread.dbi.processData("SELECT workflow FROM dbsbuffer_file WHERE id = 1")[0].fetchall()[0][0]
        self.assertEqual(result, 1)

        return

    def testNoFileReport(self):
        """
        _testNoFileReport_

        See that the Accountant does not crash if there are no files.
        """
        self.setupDBForMergeSuccess()

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergeSuccessNoFiles.pkl"))
        self.verifyJobSuccess(self.testJob["id"])
        return

    def setupDBForLoadTest(self, maxJobs=100):
        """
        _setupDBForLoadTest_

        Setup the database for a load test using the framework job reports in
        the DBSBuffer directory.  The job reports are from repack jobs that have
        several outputs, so configure the workflows accordingly.
        """
        inputFileset = Fileset(name="TestFileset")
        inputFileset.create()

        caloFileset = Fileset(name="Calo")
        caloFileset.create()
        cosmicsFileset = Fileset(name="Cosmics")
        cosmicsFileset.create()
        hcalFileset = Fileset(name="HCal")
        hcalFileset.create()
        minbiasFileset = Fileset(name="MinBias")
        minbiasFileset.create()
        ranFileset = Fileset(name="Random")
        ranFileset.create()
        calFileset = Fileset(name="Cal")
        calFileset.create()
        hltFileset = Fileset(name="HLT")
        hltFileset.create()

        testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                name="TestWF", task="None")
        testWorkflow.create()
        testWorkflow.addOutput("write_A_Calo_RAW", caloFileset, caloFileset)
        testWorkflow.addOutput("write_A_Cosmics_RAW", cosmicsFileset, cosmicsFileset)
        testWorkflow.addOutput("write_A_HcalHPDNoise_RAW", hcalFileset, hcalFileset)
        testWorkflow.addOutput("write_A_MinimumBias_RAW", minbiasFileset, minbiasFileset)
        testWorkflow.addOutput("write_A_RandomTriggers_RAW", ranFileset, ranFileset)
        testWorkflow.addOutput("write_A_Calibration_TestEnables_RAW", calFileset, calFileset)
        testWorkflow.addOutput("write_HLTDEBUG_Monitor_RAW", hltFileset, hltFileset)

        self.testSubscription = Subscription(fileset=inputFileset,
                                             workflow=testWorkflow,
                                             split_algo="FileBased",
                                             type="Processing")
        self.testSubscription.create()

        self.jobs = []
        for i in range(maxJobs):
            testJobGroup = JobGroup(subscription=self.testSubscription)
            testJobGroup.create()

            testJob = Job(name=makeUUID())
            testJob.create(group=testJobGroup)
            testJob["state"] = "complete"
            self.stateChangeAction.execute(jobs=[testJob])

            newFile = File(lfn="/some/lfn/for/job/%s" % testJob["id"], size=600000, events=60000,
                           locations="T1_US_FNAL_Disk", merged=True)
            newFile.create()

            pFile = DBSBufferFile(lfn="/some/lfn/for/job/%s" % testJob["id"], size=600000, events=60000)
            pFile.setAlgorithm(appName="cmsRun", appVer="UNKNOWN",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
            pFile.setDatasetPath("/bogus/dataset/path")
            pFile.addRun(Run(1, *[45]))
            pFile.create()

            inputFileset.addFile(newFile)
            testJob.addFile(newFile)
            testJob.associateFiles()

            fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                    "WMComponent_t/JobAccountant_t/fwjrs",
                                    "LoadTest%02d.pkl" % i)

            self.jobs.append((testJob["id"], fwjrPath))
            self.setFWJRAction.execute(jobID=testJob["id"], fwjrPath=fwjrPath)

        inputFileset.commit()
        return

    @attr('performance', 'integration')
    def testOneProcessLoadTest(self):
        """
        _testOneProcessLoadTest_

        Run the load test using one worker process.
        """
        print("  Filling DB...")
        self.setupDBForLoadTest()

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()

        print("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        endTime = time.time()
        print("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            print("  Validating %s, %s" % (jobID, fwjrPath))
            jobReport = Report()
            jobReport.unpersist(fwjrPath)

            self.verifyFileMetaData(jobID, jobReport.getAllFilesFromStep("cmsRun1"))
            self.verifyJobSuccess(jobID)
            self.verifyDBSBufferContents("Processing",
                                         ["/some/lfn/for/job/%s" % jobID],
                                         jobReport.getAllFilesFromStep("cmsRun1"))

        return

    def testDBRollback(self):
        """
        _testDBRollback_

        Verify that if we encounter an error in the accountant the database
        will be rolled back properly.
        """
        self.setupDBForLoadTest(maxJobs=25)

        # We just need to make two jobs process the same report so that we get a
        # duplicate LFN error.
        fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t/fwjrs",
                                "LoadTest07.pkl")
        self.setFWJRAction.execute(jobID=10, fwjrPath=fwjrPath)

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()

        try:
            accountant.algorithm()
        except Exception:
            pass

        sql = "SELECT COUNT(*) FROM wmbs_file_details"
        myThread = threading.currentThread()
        numFiles = myThread.dbi.processData(sql)[0].fetchall()[0][0]

        assert numFiles == 25, \
            "Error: There should only be 25 files in the database: %s" % numFiles

        return

    def setupDBFor4GMerge(self):
        """
        _setupDBFor4GMerge_

        Setup for the MergeSuccess with added generations of parentage
        """
        self.recoOutputFileset = Fileset(name="RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name="MergedRECO")
        self.mergedRecoOutputFileset.create()
        self.aodOutputFileset = Fileset(name="AOD")
        self.aodOutputFileset.create()
        self.mergedAodOutputFileset = Fileset(name="MergedAOD")
        self.mergedAodOutputFileset.create()
        self.insertWorkflow.execute("Steves", "/Steves/Stupid/Task", 0, 0, 0, 0)
        self.testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                     name="TestWF", task="None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("output", self.recoOutputFileset,
                                    self.mergedRecoOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombined", self.aodOutputFileset,
                                    self.mergedAodOutputFileset)

        self.testRecoMergeWorkflow = Workflow(spec="wf002.xml", owner="Steve",
                                              name="TestRecoMergeWF", task="None")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("MergedRECO", self.mergedRecoOutputFileset,
                                             self.mergedRecoOutputFileset)

        self.testAodMergeWorkflow = Workflow(spec="wf003.xml", owner="Steve",
                                             name="TestAodMergeWF", task="None")
        self.testAodMergeWorkflow.create()
        self.testAodMergeWorkflow.addOutput("MergedAOD", self.mergedAodOutputFileset,
                                            self.mergedAodOutputFileset)

        masterFile1 = File(lfn="/path/to/some/lfn1", size=600000, events=60000,
                           locations="T1_US_FNAL_Disk", merged=True)

        masterFile1.create()

        masterFile2 = File(lfn="/path/to/some/lfn2", size=600000, events=60000,
                           locations="T1_US_FNAL_Disk", merged=False)

        masterFile2.create()
        masterFile1.addChild(masterFile2['lfn'])

        inputFileA = File(lfn="/path/to/some/lfnA", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=False)
        inputFileA.create()
        inputFileB = File(lfn="/path/to/some/lfnB", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=False)
        inputFileB.create()
        inputFileC = File(lfn="/path/to/some/lfnC", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=False)
        inputFileC.create()

        unmergedFileA = File(lfn="/path/to/some/unmerged/lfnA", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileA.create()
        unmergedFileB = File(lfn="/path/to/some/unmerged/lfnB", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileB.create()
        unmergedFileC = File(lfn="/path/to/some/unmerged/lfnC", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileC.create()

        masterFile2.addChild(inputFileA['lfn'])
        masterFile2.addChild(inputFileB['lfn'])
        masterFile2.addChild(inputFileC['lfn'])

        inputFileA.addChild(unmergedFileA["lfn"])
        inputFileB.addChild(unmergedFileB["lfn"])
        inputFileC.addChild(unmergedFileC["lfn"])

        # Now you have to create files in DBS for parents
        # NOTE: There should only be one parent
        for plfn in ["/path/to/some/lfn1"]:
            pFile = DBSBufferFile(lfn=plfn, size=600000, events=60000)
            pFile.setAlgorithm(appName="cmsRun", appVer="UNKNOWN",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
            pFile.setDatasetPath("/bogus/dataset/path")
            pFile.addRun(Run(1, *[45]))
            pFile.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(masterFile1)
        testFileset.addFile(masterFile2)
        testFileset.addFile(inputFileA)
        testFileset.addFile(inputFileB)
        testFileset.addFile(inputFileC)
        testFileset.commit()

        self.aodOutputFileset.addFile(unmergedFileA)
        self.aodOutputFileset.addFile(unmergedFileB)
        self.aodOutputFileset.addFile(unmergedFileC)
        self.aodOutputFileset.commit()

        self.testSubscription = Subscription(fileset=testFileset,
                                             workflow=self.testWorkflow,
                                             split_algo="EventBased",
                                             type="Processing")

        self.testMergeRecoSubscription = Subscription(fileset=self.recoOutputFileset,
                                                      workflow=self.testRecoMergeWorkflow,
                                                      split_algo="WMBSMergeBySize",
                                                      type="Merge")

        self.testMergeAodSubscription = Subscription(fileset=self.aodOutputFileset,
                                                     workflow=self.testAodMergeWorkflow,
                                                     split_algo="WMBSMergeBySize",
                                                     type="Merge")
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAodSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=self.testMergeAodSubscription)
        testJobGroup.create()

        self.testJob = Job(name="MergeJob", files=[unmergedFileA,
                                                   unmergedFileB,
                                                   unmergedFileC])
        self.testJob.create(group=testJobGroup)
        self.testJob["state"] = "complete"
        self.testJob.save()
        self.stateChangeAction.execute(jobs=[self.testJob])

        self.setFWJRAction.execute(jobID=self.testJob["id"],
                                   fwjrPath=os.path.join(WMCore.WMBase.getTestBase(),
                                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                                         "MergeSuccess.pkl"))
        return

    def test4GMerge(self):
        """
        _test4GMerge_

        Test the accountant's handling of a merge job.
        """
        self.setupDBFor4GMerge()

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        myThread = threading.currentThread()

        jobReport = Report()
        jobReport.unpersist(os.path.join(WMCore.WMBase.getTestBase(),
                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                         "MergeSuccess.pkl"))

        self.verifyFileMetaData(self.testJob["id"], jobReport.getAllFilesFromStep("cmsRun1"))
        self.verifyJobSuccess(self.testJob["id"])

        dbsParents = ["/path/to/some/lfnA", "/path/to/some/lfnB",
                      "/path/to/some/lfnC"]
        self.verifyDBSBufferContents("Merge", ["/path/to/some/lfn1"], jobReport.getAllFilesFromStep("cmsRun1"))

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()

        self.assertEqual(self.mergedRecoOutputFileset.getFiles(type="list"), [],
                         "Error: No files should be in the merged reco fileset.")
        self.assertEqual(self.recoOutputFileset.getFiles(type="list"), [],
                         "Error: No files should be in the reco fileset.")

        self.assertEqual(len(self.mergedAodOutputFileset.getFiles(type="list")), 1,
                         "Error: One file should be in the merged aod fileset.")
        self.assertEqual(len(self.aodOutputFileset.getFiles(type="list")), 3,
                         "Error: Three files should be in the aod fileset.")

        fwjrFile = jobReport.getAllFilesFromStep("cmsRun1")[0]
        self.assertTrue(fwjrFile["lfn"] in self.mergedAodOutputFileset.getFiles(type="lfn"),
                        "Error: file is missing from merged aod output fileset.")

        # this line below is not functional because it fails to find the spec file on disk
        # fwjrCouchDoc = self.testInit.couch.couchServer.connectDatabase("jobaccountant_t/fwjrs").document('1-0')
        # self.assertTrue(fwjrCouchDoc['fwjr']['steps']['cmsRun1']['output']['Merged'][0]['location'],
        #                 'T1_US_FNAL_Buffer')

        return

    def setupDBForNoLocation(self):
        """
        _setupDBForNoLocation_

        Setup the database for a multi-cmsRun job that misses an output
        file location in one of the output files.
        NOTE: basically a copy/pasting of setupDBFor4GMerge
        """
        self.aodsimOutputFileset = Fileset(name="AODSIM")
        self.aodsimOutputFileset.create()

        self.miniaodOutputFileset = Fileset(name="MINIAODSIM")
        self.miniaodOutputFileset.create()

        self.logArchiveOutputFileset = Fileset(name="logArchive")
        self.logArchiveOutputFileset.create()

        self.insertWorkflow.execute("Steves", "/Steves/Stupid/Task", 0, 0, 0, 0)
        self.testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                     name="TestWF", task="None")
        self.testWorkflow.create()
        # Output modules are suffixed with the expected datatier
        self.testWorkflow.addOutput("AODSIMoutputAODSIM", self.aodsimOutputFileset)
        self.testWorkflow.addOutput("MINIAODSIMoutputMINIAODSIM", self.miniaodOutputFileset)

        unmergedFileA = File(lfn="/path/to/some/unmerged/aodsimlfn", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileA.create()
        unmergedFileB = File(lfn="/path/to/some/unmerged/miniaodsimlfn", size=600000, events=60000,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileB.create()
        unmergedFileC = File(lfn="/path/to/some/unmerged/logarchivelfn", size=0, events=0,
                             locations="T1_US_FNAL_Disk", merged=False)
        unmergedFileC.create()

        # create fileset to be processed
        inputFile1 = File(lfn="/path/to/some/fake/lfn1", size=600000, events=60000,
                          locations="T1_US_FNAL_Disk", merged=True)
        inputFile1.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile1)
        testFileset.commit()

        self.aodsimOutputFileset.addFile(unmergedFileA)
        self.miniaodOutputFileset.addFile(unmergedFileB)
        self.logArchiveOutputFileset.addFile(unmergedFileC)
        self.miniaodOutputFileset.commit()

        self.testSubscription = Subscription(fileset=testFileset,
                                             workflow=self.testWorkflow,
                                             split_algo="EventBased",
                                             type="Production")

        self.testSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=self.testSubscription)
        testJobGroup.create()

        self.testJob = Job(name="Production", files=[unmergedFileA,
                                                     unmergedFileB,
                                                     unmergedFileC])
        self.testJob.create(group=testJobGroup)
        self.testJob["state"] = "complete"
        self.testJob.save()
        self.stateChangeAction.execute(jobs=[self.testJob])

        self.setFWJRAction.execute(jobID=self.testJob["id"],
                                   fwjrPath=os.path.join(WMCore.WMBase.getTestBase(),
                                                         "WMComponent_t/JobAccountant_t/fwjrs",
                                                         "NoLocation.pkl"))
        return

    def testNoLocation(self):
        """
        _testNoLocation_

        Test the accountant's handling of a StepChain report with one
        missing location.
        """
        self.setupDBForNoLocation()

        config = self.createConfig()
        accountantWorker = AccountantWorker(config=config)

        reportPath = os.path.join(WMCore.WMBase.getTestBase(),
                                  "WMComponent_t/JobAccountant_t/fwjrs/NoLocation.pkl")

        # execute the same logic executed by AccountantWorker
        jobReport = accountantWorker.loadJobReport(reportPath)
        jobReport.setJobID(1)
        jobSuccess = accountantWorker.handleJob(jobID=1, fwkJobReport=jobReport)

        self.assertFalse(jobSuccess, "Job should have failed because a file has no location")

        # now verify the WMBS information to be inserted into the database
        self.assertEqual(accountantWorker.parentageBinds, [],
                         "Job report has no parentage relationship to be defined")
        self.assertEqual(len(accountantWorker.wmbsFilesToBuild), 1,
                         "Should have 3 files if all of them had valid location")
        self.assertEqual(accountantWorker.wmbsMergeFilesToBuild, [],
                         "Job report has no merge files to register")
        self.assertEqual(accountantWorker.parentageBindsForMerge, [],
                         "Job report has no parentage files to register")
        self.assertEqual(accountantWorker.mergedOutputFiles, [],
                         "Job report should have no merged output files")
        self.assertEqual(len(accountantWorker.filesetAssoc), 1,
                         "Job report should have no file association")
        self.assertTrue(accountantWorker.filesetAssoc[0]['lfn'].endswith("logArchive.tar.gz"))

        self.assertEqual(len(jobReport.listSteps()), 8)
        # steps that do not have any output files
        for stepName in ['cmsRun1', 'cmsRun2', 'cmsRun3', 'cmsRun4', 'stageOut1']:
            filesForStep = jobReport.getAllFilesFromStep(stepName)
            self.assertEqual(filesForStep, [])

        # steps that produced output files not have any output files
        filesForStep = jobReport.getAllFilesFromStep('cmsRun5')
        self.assertEqual(len(filesForStep), 1)
        self.assertTrue('83BC5087-21BD-6140-9118-51204C0B64B9.root' in filesForStep[0]['lfn'])
        self.assertEqual({'T2_CH_CSCS'}, filesForStep[0]['locations'])

        filesForStep = jobReport.getAllFilesFromStep('cmsRun6')
        self.assertEqual(len(filesForStep), 1)
        self.assertTrue('CFC2B499-098E-0143-8A7D-BED766ED7D87.root' in filesForStep[0]['lfn'])
        self.assertEqual(set(), filesForStep[0]['locations'])

        filesForStep = jobReport.getAllFilesFromStep('logArch1')
        self.assertEqual(len(filesForStep), 1)
        self.assertTrue('e3230232-09ed-40c0-ac47-ddf926edcd57-64-3-logArchive.tar.gz' in filesForStep[0]['lfn'])
        self.assertEqual({'T2_CH_CSCS'}, filesForStep[0]['locations'])


    def setupDBForHeritageTest(self):
        """
        _setupDBForHeritageTest_

        Inject jobs that have a large amount of input files into WMBS.
        """
        totalJobs = 10
        inputFilesPerJob = 50

        inputFileset = Fileset(name="TestFileset")
        inputFileset.create()

        outputModules = ["outputModule1", "outputModule2", "outputModule3",
                         "outputModule4", "outputModule5", "outputModule6",
                         "outputModule7", "outputModule8", "outputModule9",
                         "outputModule10"]

        testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                name="TestWF", task="None")
        testWorkflow.create()

        for outputModuleName in outputModules:
            outputFileset = Fileset(name=outputModuleName)
            outputFileset.create()
            testWorkflow.addOutput(outputModuleName, outputFileset,
                                   outputFileset)

        self.testSubscription = Subscription(fileset=inputFileset,
                                             workflow=testWorkflow,
                                             split_algo="FileBased",
                                             type="Processing")
        self.testSubscription.create()
        testJobGroup = JobGroup(subscription=self.testSubscription)
        testJobGroup.create()

        self.jobs = []
        inputFileCounter = 0
        parentCounter = 0
        for i in range(totalJobs):
            testJob = Job(name=makeUUID())
            testJob.create(group=testJobGroup)
            testJob["state"] = "complete"
            self.stateChangeAction.execute(jobs=[testJob])

            for j in range(inputFilesPerJob):
                newFile = File(lfn="input%i" % inputFileCounter, size=600000, events=60000,
                               locations="T1_US_FNAL_Disk", merged=False)
                newFile.create()

                for k in range(3):
                    lfn = makeUUID()
                    parentFile = File(lfn=lfn, size=600000, events=60000,
                                      locations="T1_US_FNAL_Disk", merged=True)
                    parentFile.create()
                    newFile.addParent(parentFile["lfn"])

                    pFile = DBSBufferFile(lfn=lfn, size=600000, events=60000)
                    pFile.setAlgorithm(appName="cmsRun", appVer="UNKNOWN",
                                       appFam="RECO", psetHash="GIBBERISH",
                                       configContent="MOREGIBBERISH")
                    pFile.setDatasetPath("/bogus/dataset/path")
                    pFile.addRun(Run(1, *[45]))
                    pFile.create()
                    parentCounter += 1
                inputFileset.addFile(newFile)
                testJob.addFile(newFile)

            testJob.associateFiles()
            fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                    "WMComponent_t/JobAccountant_t/fwjrs",
                                    "HeritageTest%02d.pkl" % i)

            self.jobs.append((testJob["id"], fwjrPath))
            self.setFWJRAction.execute(jobID=testJob["id"], fwjrPath=fwjrPath)

        inputFileset.commit()
        return

    def testZ1_BigHeritage(self):
        """
        _testBigHeritage_

        Run the big heritage test.
        """
        return
        print("Starting Heritage Test")

        print("  Filling DB...")

        self.setupDBForHeritageTest()

        config = self.createConfig()
        accountant = JobAccountantPoller(config)
        accountant.setup()

        print("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        # cProfile.runctx("accountant.algorithm()", globals(), locals(), filename = "testStats.stat")

        endTime = time.time()
        print("  Time: %f" % (endTime - startTime))
        print("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            print("  Validating %s, %s" % (jobID, fwjrPath))
            jobReport = Report()
            jobReport.unpersist(fwjrPath)

            self.verifyFileMetaData(jobID, jobReport.getAllFilesFromStep("cmsRun1"))
            self.verifyJobSuccess(jobID)
            # self.verifyDBSBufferContents("Processing",
            #                             ["/some/lfn/for/job/%s" % jobID],
            #                             jobReport.getAllFilesFromStep("cmsRun1"))

        # p = pstats.Stats('testStats.stat')
        # p.sort_stats('cumulative')
        # p.print_stats()

        return

    def testReportReturn(self):
        """
        _testReportReturn_

        Test whether the report returns
        """
        self.setupDBForMergeSuccess()

        config = self.createConfig()
        config.JobAccountant.returnReportFromWorker = True
        accountant = AccountantWorker(config=config)

        # First job should be ready
        job = Job(id=1)
        job.loadData()

        result = accountant.__call__(parameters=[job])

        self.assertTrue('jobReport' in result[0])
        report = result[0]['jobReport']

        self.assertEqual(report.getJobID(), 1)

        return


if __name__ == '__main__':
    unittest.main()
