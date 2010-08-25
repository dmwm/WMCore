#!/usr/bin/env python
"""
_JobAccountant_t_

Unit tests for the WMAgent JobAccountant component.
"""

__revision__ = "$Id: JobAccountant_t.py,v 1.14 2010/02/01 21:42:53 sfoulkes Exp $"
__version__ = "$Revision: 1.14 $"

import logging
import os.path
import threading
import unittest
import time
import copy

from WMCore.FwkJobReport.ReportParser import readJobReport

from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.File         import File
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Fileset      import Fileset

from WMComponent.JobAccountant.JobAccountantPoller import JobAccountantPoller
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

class JobAccountantTest(unittest.TestCase):
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
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.testInit.clearDatabase(modules = ["WMComponent.DBSBuffer.Database", "WMCore.WMBS"]) 
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database",
                                                "WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        locationAction = self.daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "cmssrm.fnal.gov")
        locationAction.execute(siteName = "srm.cern.ch")        

        self.stateChangeAction = self.daofactory(classname = "Jobs.ChangeState")
        self.setFWJRAction = self.daofactory(classname = "Jobs.SetFWJRPath")
        self.getJobTypeAction = self.daofactory(classname = "Jobs.GetType")
        self.getOutputMapAction = self.daofactory(classname = "Jobs.GetOutputMap")
        
        self.dbsbufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                           logger = myThread.logger,
                                           dbinterface = myThread.dbi)
        self.countDBSFilesAction = self.dbsbufferFactory(classname = "CountFiles")
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the WMBS and DBSBuffer database schemas.
        """
        self.testInit.clearDatabase()
        return

    def createConfig(self, workerThreads):
        """
        _createConfig_

        Create a config for the JobAccountant with the given number of worker
        threads.  This config needs to include information for connecting to the
        database as the component will create it's own database connections.
        These parameters are still pulled from the environment.
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        #config.section_("General")
        #config.General.workDir = "."

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = os.getenv("COUCHURL")
        config.JobStateMachine.couchDBName = "job_accountant_t"

        config.component_("JobAccountant")
        config.JobAccountant.pollInterval = 60
        config.JobAccountant.workerThreads = workerThreads
        config.JobAccountant.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobAccountant.logLevel = 'SQLDEBUG'

        return config

    def setupDBForJobFailure(self, jobName, fwjrName):
        """
        _setupDBForJobFailure_

        Create the appropriate workflows, filesets, subscriptions, files,
        jobgroups and jobs in the database so that the accountant's handling of
        failed jobs can be tested.  Move the job to the complete state and set
        the path to the job report.
        """
        testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                name = "TestWF", task = "None")
        testWorkflow.create()

        inputFile = File(lfn = "/path/to/some/lfn", size = 10, events = 10,
                         locations = "cmssrm.fnal.gov")
        inputFile.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()
        
        self.testSubscription = Subscription(fileset = testFileset,
                                             workflow = testWorkflow,
                                             split_algo = "FileBased",
                                             type = "Processing")
        self.testSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = self.testSubscription)
        testJobGroup.create()
        
        testJob = Job(name = jobName, files = [inputFile])
        testJob.create(group = testJobGroup)
        testJob["state"] = "complete"
        self.stateChangeAction.execute(jobs = [testJob])

        fwjrPath = os.path.join(os.getenv("WMCOREBASE"),
                                "test/python/WMComponent_t/JobAccountant_t/", fwjrName)
        self.setFWJRAction.execute(jobID = testJob["id"], fwjrPath = fwjrPath)
        return

    def verifyJobFailure(self, jobName):
        """
        _verifyJobFailure_

        Verify that the accountant handled a job failure correctly.  The state
        should be executing, the outcome should be "fail", the retry count
        should be 1 and all the input files should be marked as failed.
        """
        testJob = Job(name = jobName)
        testJob.load()

        assert testJob["state"] == "jobfailed", \
               "Error: test job in wrong state: %s" % testJob["state"]
        assert testJob["retry_count"] == 1, \
               "Error: test job has wrong retry count: %s" % testJob["retry_count"]
        assert testJob["outcome"] == "failure", \
               "Error: test job has wrong outcome: %s" % testJob["outcome"]

        assert len(self.testSubscription.filesOfStatus("Acquired")) == 0, \
               "Error: There not be any acquired files."
        assert len(self.testSubscription.filesOfStatus("Failed")) == 1, \
               "Error: Wrong number of failed files: %s" % \
               len(self.testSubscription.filesOfStatus("Failed"))
        return
 
    def testFailedJob(self):
        """
        _testFailedJob_

        Run a failed job that has a vaid job report through the accountant.
        Verify that it functions correctly.
        """
        self.setupDBForJobFailure(jobName = "T0Skim-Run2-Skim2-Jet-631",
                                  fwjrName = "SkimFailure.xml")
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        self.verifyJobFailure("T0Skim-Run2-Skim2-Jet-631")
        return

    def testEmptyFWJR(self):
        """
        _testEmptyFWJR_

        Run an empty framework job report through the accountant.  Verify that
        it functions correctly.
        """
        self.setupDBForJobFailure(jobName = "T0Skim-Run2-Skim2-Jet-631",
                                  fwjrName = "EmptyJobReport.xml")
        config = self.createConfig(workerThreads = 1)

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
        self.setupDBForJobFailure(jobName = "T0Merge-Run1-Mu-AOD-722",
                                  fwjrName = "MergeSuccessBadXML.xml")
        config = self.createConfig(workerThreads = 1)

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
        
        self.recoOutputFileset = Fileset(name = "RECO")
        self.recoOutputFileset.create()
        self.alcaOutputFileset = Fileset(name = "ALCA")
        self.alcaOutputFileset.create()

        self.testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                     name = "TestWF", task = "None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("FEVT", self.recoOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombined", self.alcaOutputFileset)

        inputFile = File(lfn = "/path/to/some/lfn", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFile.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()
        
        self.testSubscription = Subscription(fileset = testFileset,
                                             workflow = self.testWorkflow,
                                             split_algo = "EventBased",
                                             type = "Processing")
        self.testSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = self.testSubscription)
        testJobGroup.create()
        
        self.testJobA = Job(name = "SplitJobA", files = [inputFile])
        self.testJobA.create(group = testJobGroup)
        self.testJobA["state"] = "complete"
        self.testJobA["mask"].setMaxAndSkipEvents(20000, 0)
        self.testJobA.save()
        self.stateChangeAction.execute(jobs = [self.testJobA],
                                       conn = myThread.transaction.conn,
                                       transaction = True)                                       

        self.testJobB = Job(name = "SplitJobB", files = [inputFile])
        self.testJobB.create(group = testJobGroup)
        self.testJobB["mask"].setMaxAndSkipEvents(20000, 20000)
        self.testJobB["state"] = "executing"
        self.testJobB.save()
        self.stateChangeAction.execute(jobs = [self.testJobB],
                                       conn = myThread.transaction.conn,
                                       transaction = True)                                       

        self.testJobC = Job(name = "SplitJobC", files = [inputFile])
        self.testJobC.create(group = testJobGroup)
        self.testJobC["state"] = "executing"
        self.testJobC["mask"].setMaxAndSkipEvents(20000, 40000)
        self.testJobC.save()
        self.stateChangeAction.execute(jobs = [self.testJobC],
                                       conn = myThread.transaction.conn,
                                       transaction = True)

        fwjrBasePath = os.getenv("WMCOREBASE") + "/test/python/WMComponent_t/JobAccountant_t/"
        self.setFWJRAction.execute(jobID = self.testJobA["id"],
                                   fwjrPath = fwjrBasePath + "SplitSuccessA.xml",
                                   conn = myThread.transaction.conn,
                                   transaction = True)
        self.setFWJRAction.execute(jobID = self.testJobB["id"],
                                   fwjrPath = fwjrBasePath + "SplitSuccessB.xml",
                                   conn = myThread.transaction.conn,
                                   transaction = True)                                   
        self.setFWJRAction.execute(jobID = self.testJobC["id"],
                                   fwjrPath = fwjrBasePath + "SplitSuccessC.xml",
                                   conn = myThread.transaction.conn,
                                   transaction = True)                                   
        myThread.transaction.commit()
        return

    def verifyJobSuccess(self, jobID):
        """
        _verifyJobSuccess_

        Verify that the metadata for a successful job is correct.  This will
        check the outcome, retry count and state.
        """
        testJob = Job(id = jobID)
        testJob.load()

        assert testJob["state"] == "success", \
               "Error: test job in wrong state: %s" % testJob["state"]
        assert testJob["retry_count"] == 0, \
               "Error: test job has wrong retry count: %s" % testJob["retry_count"]
        assert testJob["outcome"] == "success", \
               "Error: test job has wrong outcome: %s" % testJob["outcome"]

        return

    def verifyFileMetaData(self, jobID, fwkJobReportFiles):
        """
        _verifyFileMetaData_

        Verify that all the files that were output by a job made it into WMBS
        correctly.  Compare the contents of WMBS to the files in the frameworks
        job report.
        """
        testJob = Job(id = jobID)
        testJob.loadData()

        inputLFNs = []
        for inputFile in testJob["input_files"]:
            inputLFNs.append(inputFile["lfn"])

        for fwkJobReportFile in fwkJobReportFiles:
            outputFile = File(lfn = fwkJobReportFile["LFN"])
            outputFile.loadData(parentage = 1)

            assert outputFile["events"] == int(fwkJobReportFile["TotalEvents"]), \
                   "Error: Output file has wrong events: %s, %s" % \
                   (outputFile["events"], fwkJobReportFile["TotalEvents"])
            assert outputFile["size"] == int(fwkJobReportFile["Size"]), \
                   "Error: Output file has wrong size: %s, %s" % \
                   (outputFile["size"], fwkJobReportFile["Size"])

            for ckType in fwkJobReportFile.checksums.keys():
                assert ckType in outputFile["checksums"].keys(), \
                       "Error: Output file is missing checksums: %s" % ckType
                assert outputFile["checksums"][ckType] == fwkJobReportFile.checksums[ckType], \
                       "Error: Checksums don't match."
                       
            assert len(fwkJobReportFile.checksums.keys()) == \
                   len(outputFile["checksums"].keys()), \
                   "Error: Wrong number of checksums."

            jobType = self.getJobTypeAction.execute(jobID = jobID)
            if jobType == "Merge":
                assert str(outputFile["merged"]) == "True", \
                       "Error: Merge jobs should output merged files."
            else:
                assert str(outputFile["merged"]) == fwkJobReportFile["MergedBySize"], \
                       "Error: Output file merged output is wrong: %s, %s" % \
                       (outputFile["merged"], fwkJobReportFile["MergedBySize"])            

            assert len(outputFile["locations"]) == 1, \
                   "Error: outputfile should have one location"
            assert list(outputFile["locations"])[0] == fwkJobReportFile["SEName"], \
                   "Error: wrong location for file."

            assert len(outputFile["parents"]) == len(inputLFNs), \
                   "Error: Output file has wrong number of parents."
            for outputParent in outputFile["parents"]:
                assert outputParent["lfn"] in inputLFNs, \
                       "Error: Unknown parent file: %s" % outputParent["lfn"]

            fwjrRuns = copy.deepcopy(fwkJobReportFile.runs)
            for run in outputFile["runs"]:
                assert fwjrRuns.has_key(run.run), \
                       "Error: Extra run in output: %s" % run.run

                for lumi in run:
                    assert lumi in fwjrRuns[run.run], \
                           "Error: Extra lumi: %s" % lumi

                    fwjrRuns[run.run].remove(lumi)
                    if len(fwjrRuns[run.run]) == 0:
                        del fwjrRuns[run.run]

            assert len(fwjrRuns.keys()) == 0, \
                   "Error: Missing runs, lumis: %s" % fwjrRuns

            testJobGroup = JobGroup(id = testJob["jobgroup"])
            testJobGroup.loadData()
            jobGroupFileset = testJobGroup.output
            jobGroupFileset.loadData()

            assert outputFile["id"] in jobGroupFileset.getFiles(type = "id"), \
                   "Error: output file not in jobgroup fileset."

            if testJob["mask"]["FirstEvent"] == None:
                assert outputFile["first_event"] == 0, \
                       "Error: first event not set correctly: 0, %s" % \
                       outputFile["first_event"]
            else:
                assert testJob["mask"]["FirstEvent"] == outputFile["first_event"], \
                       "Error: last event not set correctly: %s, %s" % \
                       (testJob["mask"]["FirstEvent"], outputFile["first_event"])

            if testJob["mask"]["LastEvent"] == None:
                assert outputFile["last_event"] == int(fwkJobReportFile["TotalEvents"]), \
                       "Error: last event not set correctly: %s, %s" % \
                       (fwkJobReportFile["TotalEvents"], outputFile["last_event"])
            else:
                assert testJob["mask"]["LastEvent"] == outputFile["last_event"], \
                       "Error: last event not set correctly: %s, %s" % \
                       (testJob["mask"]["LastEvent"], outputFile["last_event"])                   

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
            if fwkJobReportFile["MergedBySize"] != "True" and subType != "Merge":
                continue
            
            dbsFile = DBSBufferFile(lfn = fwkJobReportFile["LFN"])

            assert dbsFile.exists() != False, \
                   "Error: File is not in DBSBuffer: %s" % fwkJobReportFile["LFN"]

            dbsFile.load(parentage = 1)

            assert dbsFile["events"] == int(fwkJobReportFile["TotalEvents"]), \
                   "Error: DBS file has wrong events: %s, %s" % \
                   (dbsFile["events"], fwkJobReportFile["TotalEvents"])
            assert dbsFile["size"] == int(fwkJobReportFile["Size"]), \
                   "Error: DBS file has wrong size: %s, %s" % \
                   (dbsFile["size"], fwkJobReportFile["Size"])            

            for ckType in fwkJobReportFile.checksums.keys():
                assert ckType in dbsFile["checksums"].keys(), \
                       "Error: DBS file is missing checksums: %s" % ckType
                assert dbsFile["checksums"][ckType] == fwkJobReportFile.checksums[ckType], \
                       "Error: Checksums don't match."
                       
            assert len(fwkJobReportFile.checksums.keys()) == \
                   len(dbsFile["checksums"].keys()), \
                   "Error: Wrong number of checksums."

            assert len(dbsFile["locations"]) == 1, \
                   "Error: DBS file should have one location"
            assert list(dbsFile["locations"])[0] == fwkJobReportFile["SEName"], \
                   "Error: wrong location for DBS file."

            fwjrRuns = copy.deepcopy(fwkJobReportFile.runs)
            for run in dbsFile["runs"]:
                assert fwjrRuns.has_key(run.run), \
                       "Error: Extra run in output: %s" % run.run

                for lumi in run:
                    assert lumi in fwjrRuns[run.run], \
                           "Error: Extra lumi: %s" % lumi

                    fwjrRuns[run.run].remove(lumi)
                    if len(fwjrRuns[run.run]) == 0:
                        del fwjrRuns[run.run]

            assert len(fwjrRuns.keys()) == 0, \
                   "Error: Missing runs, lumis: %s" % fwjrRuns

            # PSetHash and ConfigContent are not currently used.
            datasetInfo = fwkJobReportFile.dataset[0]
            assert dbsFile["appName"] == datasetInfo["ApplicationName"], \
                   "Error: app name is wrong in DBS buffer."
            assert dbsFile["appVer"] == datasetInfo["ApplicationVersion"], \
                   "Error: app ver is wrong in DBS buffer."            
            assert dbsFile["appFam"] == datasetInfo["OutputModuleName"], \
                   "Error: app fam is wrong in DBS buffer."

            datasetPath = "/%s/%s/%s" % (datasetInfo["PrimaryDataset"],
                                         datasetInfo["ProcessedDataset"],
                                         datasetInfo["DataTier"])
            assert dbsFile["datasetPath"] == datasetPath, \
                   "Error: dataset path in buffer is wrong."

            if type(parentFileLFNs) == dict:
                parentFileLFNsCopy = copy.deepcopy(parentFileLFNs[fwkJobReportFile["LFN"]])
            else:
                parentFileLFNsCopy = copy.deepcopy(parentFileLFNs)
                
            for dbsParent in dbsFile["parents"]:
                assert dbsParent["lfn"] in parentFileLFNsCopy, \
                       "Error: unknown parents: %s" % dbsParent["lfn"]

                parentFileLFNsCopy.remove(dbsParent["lfn"])

            assert len(parentFileLFNsCopy) == 0, \
                   "Error: missing parents."
            
        return
        
    def testSplitJobs(self):
        """
        _testSplitJobs_

        Verify that split processing jobs are accounted correctly.  This is
        mainly to verify that the input for a series of split jobs is not marked
        as complete until all the split jobs are complete.
        """
        self.setupDBForSplitJobSuccess()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        fwjrBasePath = os.getenv("WMCOREBASE") + "/test/python/WMComponent_t/JobAccountant_t/"
        jobReports = readJobReport(fwjrBasePath + "SplitSuccessA.xml")
        self.verifyFileMetaData(self.testJobA["id"], jobReports[0].files)
        self.verifyJobSuccess(self.testJobA["id"])

        self.recoOutputFileset.loadData()
        self.alcaOutputFileset.loadData()

        for fwjrFile in jobReports[0].files:
            if fwjrFile.dataset[0]["DataTier"] == "RECO":
                assert fwjrFile["LFN"] in self.recoOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["LFN"] in self.alcaOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from alca output fileset."

        assert len(self.recoOutputFileset.getFiles(type = "list")) == 1, \
               "Error: Wrong number of files in reco output fileset."
        assert len(self.alcaOutputFileset.getFiles(type = "list")) == 1, \
               "Error: Wrong number of files in alca output fileset."
        
        assert len(self.testSubscription.filesOfStatus("Acquired")) == 1, \
               "Error: The input file should be acquired."

        self.testJobB["state"] = "complete"
        self.testJobC["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.testJobB, self.testJobC])

        accountant.algorithm()
        
        jobReports = readJobReport(fwjrBasePath + "SplitSuccessB.xml")
        self.verifyFileMetaData(self.testJobB["id"], jobReports[0].files)
        self.verifyJobSuccess(self.testJobB["id"])

        self.recoOutputFileset.loadData()
        self.alcaOutputFileset.loadData()

        for fwjrFile in jobReports[0].files:
            if fwjrFile.dataset[0]["DataTier"] == "RECO":
                assert fwjrFile["LFN"] in self.recoOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["LFN"] in self.alcaOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from alca output fileset."

        jobReports = readJobReport(fwjrBasePath + "SplitSuccessC.xml")
        self.verifyFileMetaData(self.testJobC["id"], jobReports[0].files)        
        self.verifyJobSuccess(self.testJobC["id"])

        for fwjrFile in jobReports[0].files:
            if fwjrFile.dataset[0]["DataTier"] == "RECO":
                assert fwjrFile["LFN"] in self.recoOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["LFN"] in self.alcaOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from alca output fileset."

        assert len(self.testSubscription.filesOfStatus("Completed")) == 1, \
               "Error: The input file should be complete."

        assert self.countDBSFilesAction.execute() == 0, \
               "Error: there shouldn't be any files in DBSBuffer."

        assert len(self.recoOutputFileset.getFiles(type = "list")) == 3, \
               "Error: Wrong number of files in reco output fileset."
        assert len(self.alcaOutputFileset.getFiles(type = "list")) == 3, \
               "Error: Wrong number of files in alca output fileset."

        return

    def setupDBForMergedSkimSuccess(self):
        """
        _setupDBForMergedSkimSuccess_

        Initialize the database so that we can test the result of a skim job
        that produced merged output.  This needs to setup merge workflows and
        filesset to hold the "merged" files.
        """
        self.recoOutputFileset = Fileset(name = "RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name = "MergedRECO")
        self.mergedRecoOutputFileset.create()        
        self.alcaOutputFileset = Fileset(name = "ALCA")
        self.alcaOutputFileset.create()
        self.mergedAlcaOutputFileset = Fileset(name = "MergedALCA")
        self.mergedAlcaOutputFileset.create()        

        self.testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                     name = "TestWF", task = "None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("output", self.recoOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombined", self.alcaOutputFileset)

        self.testRecoMergeWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                              name = "TestRecoMergeWF", task = "None")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("anything", self.mergedRecoOutputFileset)

        self.testAlcaMergeWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                              name = "TestAlcaMergeWF", task = "None")
        self.testAlcaMergeWorkflow.create()
        self.testAlcaMergeWorkflow.addOutput("anything", self.mergedAlcaOutputFileset)        

        inputFile = File(lfn = "/path/to/some/lfn", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFile.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()
        
        self.testSubscription = Subscription(fileset = testFileset,
                                             workflow = self.testWorkflow,
                                             split_algo = "EventBased",
                                             type = "Processing")

        self.testMergeRecoSubscription = Subscription(fileset = self.recoOutputFileset,
                                                      workflow = self.testRecoMergeWorkflow,
                                                      split_algo = "WMBSMergeBySize",
                                                      type = "Merge")

        self.testMergeAlcaSubscription = Subscription(fileset = self.alcaOutputFileset,
                                                      workflow = self.testAlcaMergeWorkflow,
                                                      split_algo = "WMBSMergeBySize",
                                                      type = "Merge")
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAlcaSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = self.testSubscription)
        testJobGroup.create()
        
        self.testJob = Job(name = "SplitJobA", files = [inputFile])
        self.testJob.create(group = testJobGroup)
        self.testJob["state"] = "complete"
        self.testJob.save()
        self.stateChangeAction.execute(jobs = [self.testJob])

        self.setFWJRAction.execute(self.testJob["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedSkimSuccess.xml"))
        return

    def testMergedSkim(self):
        """
        _testMergedSkim_

        Test how the accounant handles a skim that produces merged out.  Verify
        that merged files are inserted into the correct output filesets.
        """
        self.setupDBForMergedSkimSuccess()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReports = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedSkimSuccess.xml"))
        self.verifyFileMetaData(self.testJob["id"], jobReports[0].files)
        self.verifyJobSuccess(self.testJob["id"])

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.alcaOutputFileset.loadData()
        self.mergedAlcaOutputFileset.loadData()

        assert len(self.recoOutputFileset.getFiles(type = "list")) == 0, \
               "Error: files should go straight to the merged reco fileset."
        assert len(self.alcaOutputFileset.getFiles(type = "list")) == 0, \
               "Error: files should go straight to the merged alca fileset."

        assert len(self.mergedRecoOutputFileset.getFiles(type = "list")) == 1, \
               "Error: Should be only one file in the merged reco fileset."
        assert len(self.mergedAlcaOutputFileset.getFiles(type = "list")) == 1, \
               "Error: Should be only one file in the merged alca fileset."        

        for fwjrFile in jobReports[0].files:
            if fwjrFile.dataset[0]["DataTier"] == "RECO":
                assert fwjrFile["LFN"] in self.mergedRecoOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from reco output fileset."
            else:
                assert fwjrFile["LFN"] in self.mergedAlcaOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from alca output fileset."

        self.verifyDBSBufferContents("Processing", ["/path/to/some/lfn"], jobReports[0].files)

        return

    def setupDBForMergeSuccess(self):
        """
        _setupDBForMergeSuccess_

        Setup the database to verify the behavior of the accountant when dealing
        with the result of a merge job.
        """
        self.recoOutputFileset = Fileset(name = "RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name = "MergedRECO")
        self.mergedRecoOutputFileset.create()        
        self.aodOutputFileset = Fileset(name = "AOD")
        self.aodOutputFileset.create()
        self.mergedAodOutputFileset = Fileset(name = "MergedAOD")
        self.mergedAodOutputFileset.create()        

        self.testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                     name = "TestWF", task = "None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("output", self.recoOutputFileset)
        self.testWorkflow.addOutput("ALCARECOStreamCombined", self.aodOutputFileset)

        self.testRecoMergeWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                              name = "TestRecoMergeWF", task = "None")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("anything", self.mergedRecoOutputFileset)

        self.testAodMergeWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                             name = "TestAodMergeWF", task = "None")
        self.testAodMergeWorkflow.create()
        self.testAodMergeWorkflow.addOutput("anything", self.mergedRecoOutputFileset)
        self.testAodMergeWorkflow.addOutput("Merged", self.mergedAodOutputFileset)        

        inputFileA = File(lfn = "/path/to/some/lfnA", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov", merged = True)
        inputFileA.create()
        inputFileB = File(lfn = "/path/to/some/lfnB", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov", merged = True)
        inputFileB.create()
        inputFileC = File(lfn = "/path/to/some/lfnC", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov", merged = True)
        inputFileC.create()        

        unmergedFileA = File(lfn = "/path/to/some/unmerged/lfnA", size = 600000, events = 60000,
                             locations = "cmssrm.fnal.gov", merged = False)
        unmergedFileA.create()
        unmergedFileB = File(lfn = "/path/to/some/unmerged/lfnB", size = 600000, events = 60000,
                             locations = "cmssrm.fnal.gov", merged = False)
        unmergedFileB.create()
        unmergedFileC = File(lfn = "/path/to/some/unmerged/lfnC", size = 600000, events = 60000,
                             locations = "cmssrm.fnal.gov", merged = False)
        unmergedFileC.create()        

        inputFileA.addChild(unmergedFileA["lfn"])
        inputFileB.addChild(unmergedFileB["lfn"])
        inputFileC.addChild(unmergedFileC["lfn"])

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFileA)
        testFileset.addFile(inputFileB)
        testFileset.addFile(inputFileC)        
        testFileset.commit()

        self.aodOutputFileset.addFile(unmergedFileA)
        self.aodOutputFileset.addFile(unmergedFileB)
        self.aodOutputFileset.addFile(unmergedFileC)
        self.aodOutputFileset.commit()
        
        self.testSubscription = Subscription(fileset = testFileset,
                                             workflow = self.testWorkflow,
                                             split_algo = "EventBased",
                                             type = "Processing")

        self.testMergeRecoSubscription = Subscription(fileset = self.recoOutputFileset,
                                                      workflow = self.testRecoMergeWorkflow,
                                                      split_algo = "WMBSMergeBySize",
                                                      type = "Merge")

        self.testMergeAodSubscription = Subscription(fileset = self.aodOutputFileset,
                                                     workflow = self.testAodMergeWorkflow,
                                                     split_algo = "WMBSMergeBySize",
                                                     type = "Merge")
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAodSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = self.testMergeAodSubscription)
        testJobGroup.create()
        
        self.testJob = Job(name = "MergeJob", files = [unmergedFileA,
                                                       unmergedFileB,
                                                       unmergedFileC])
        self.testJob.create(group = testJobGroup)
        self.testJob["state"] = "complete"
        self.testJob.save()
        self.stateChangeAction.execute(jobs = [self.testJob])

        self.setFWJRAction.execute(jobID = self.testJob["id"],
                                   fwjrPath = os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergeSuccess.xml"))
        return

    def testMergeSuccess(self):
        """
        _testMergeSuccess_

        Test the accountant's handling of a merge job.
        """
        self.setupDBForMergeSuccess()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReports = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergeSuccess.xml"))
        self.verifyFileMetaData(self.testJob["id"], jobReports[0].files)
        self.verifyJobSuccess(self.testJob["id"])

        dbsParents = ["/path/to/some/lfnA", "/path/to/some/lfnB",
                      "/path/to/some/lfnC"]
        self.verifyDBSBufferContents("Merge", dbsParents, jobReports[0].files)

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()

        assert len(self.mergedRecoOutputFileset.getFiles(type = "list")) == 0, \
               "Error: No files should be in the merged reco fileset."
        assert len(self.recoOutputFileset.getFiles(type = "list")) == 0, \
               "Error: No files should be in the reco fileset."

        assert len(self.mergedAodOutputFileset.getFiles(type = "list")) == 1, \
               "Error: One file should be in the merged aod fileset."
        assert len(self.aodOutputFileset.getFiles(type = "list")) == 3, \
               "Error: Three files should be in the aod fileset."

        fwjrFile = jobReports[0].files[0]
        assert fwjrFile["LFN"] in self.mergedAodOutputFileset.getFiles(type = "lfn"), \
                       "Error: file is missing from merged aod output fileset."

        return

    def setupDBForUnmergedRedneckReco(self):
        """
        _setupDBForUnmergedRedneckReco_

        Setup the database for the unmerged redneck reco test.  We'll create a
        processing workflow that has three output modules: RECO, AOD and Skim.
        The RECO module will be the redneck parent of the AOD module and the
        AOD module will be the redneck parent of the Skim module.  Each file
        produced will go through a merge step.

        Here we'll install the processing workflow into WMBS, creating the
        workflows, input fileset, output filesets, input files, subscriptions
        and five processing jobs.  We'll associate FWJRs with each job and mark
        them as complete.
        """
        self.recoOutputFileset = Fileset(name = "RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name = "MergedRECO")
        self.mergedRecoOutputFileset.create()        
        self.aodOutputFileset = Fileset(name = "AOD")
        self.aodOutputFileset.create()
        self.mergedAodOutputFileset = Fileset(name = "MergedAOD")
        self.mergedAodOutputFileset.create()
        self.skimOutputFileset = Fileset(name = "Skim")
        self.skimOutputFileset.create()
        self.mergedSkimOutputFileset = Fileset(name = "MergedSkim")
        self.mergedSkimOutputFileset.create()        

        self.testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                     name = "TestWF", task = "None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("recoOutputModule", self.recoOutputFileset)
        self.testWorkflow.addOutput("aodOutputModule", self.aodOutputFileset,
                                    "recoOutputModule")
        self.testWorkflow.addOutput("skimOutputModule", self.skimOutputFileset,
                                    "aodOutputModule")

        self.testRecoMergeWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                              name = "TestRecoMergeWF", task = "None")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("merged", self.mergedRecoOutputFileset)

        self.testAodMergeWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                             name = "TestAodMergeWF", task = "None")
        self.testAodMergeWorkflow.create()
        self.testAodMergeWorkflow.addOutput("merged", self.mergedAodOutputFileset)        

        self.testSkimMergeWorkflow = Workflow(spec = "wf004.xml", owner = "Steve",
                                             name = "TestSkimMergeWF", task = "None")
        self.testSkimMergeWorkflow.create()
        self.testSkimMergeWorkflow.addOutput("merged", self.mergedSkimOutputFileset)        

        inputFileA = File(lfn = "/path/to/some/lfnA", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileB = File(lfn = "/path/to/some/lfnB", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileC = File(lfn = "/path/to/some/lfnC", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileD = File(lfn = "/path/to/some/lfnD", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileE = File(lfn = "/path/to/some/lfnE", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")        
        inputFileA.create()
        inputFileB.create()
        inputFileC.create()
        inputFileD.create()
        inputFileE.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFileA)
        testFileset.addFile(inputFileB)
        testFileset.addFile(inputFileC)
        testFileset.addFile(inputFileD)
        testFileset.addFile(inputFileE)               
        testFileset.commit()
        
        self.testSubscription = Subscription(fileset = testFileset,
                                             workflow = self.testWorkflow,
                                             split_algo = "EventBased",
                                             type = "Processing")

        self.testMergeRecoSubscription = Subscription(fileset = self.recoOutputFileset,
                                                      workflow = self.testRecoMergeWorkflow,
                                                      split_algo = "WMBSMergeBySize",
                                                      type = "Merge")
        self.testMergeAodSubscription = Subscription(fileset = self.aodOutputFileset,
                                                     workflow = self.testAodMergeWorkflow,
                                                     split_algo = "WMBSMergeBySize",
                                                     type = "Merge")
        self.testMergeSkimSubscription = Subscription(fileset = self.skimOutputFileset,
                                                      workflow = self.testSkimMergeWorkflow,
                                                      split_algo = "WMBSMergeBySize",
                                                      type = "Merge")        
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAodSubscription.create()
        self.testMergeSkimSubscription.create()        
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = self.testSubscription)
        testJobGroup.create()
        
        self.testJobA = Job(name = "RecoJobA", files = [inputFileA])
        self.testJobA.create(group = testJobGroup)
        self.testJobA["state"] = "complete"
        self.testJobA.save()
        self.stateChangeAction.execute(jobs = [self.testJobA])

        self.setFWJRAction.execute(self.testJobA["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco1.xml"))

        self.testJobB = Job(name = "RecoJobB", files = [inputFileB])
        self.testJobB.create(group = testJobGroup)
        self.testJobB["state"] = "complete"
        self.testJobB.save()
        self.stateChangeAction.execute(jobs = [self.testJobB])

        self.setFWJRAction.execute(self.testJobB["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco2.xml"))

        self.testJobC = Job(name = "RecoJobC", files = [inputFileC])
        self.testJobC.create(group = testJobGroup)
        self.testJobC["state"] = "complete"
        self.testJobC.save()
        self.stateChangeAction.execute(jobs = [self.testJobC])

        self.setFWJRAction.execute(self.testJobC["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco3.xml"))

        self.testJobD = Job(name = "RecoJobD", files = [inputFileD])
        self.testJobD.create(group = testJobGroup)
        self.testJobD["state"] = "complete"
        self.testJobD.save()
        self.stateChangeAction.execute(jobs = [self.testJobD])

        self.setFWJRAction.execute(self.testJobD["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco4.xml"))

        self.testJobE = Job(name = "RecoJobE", files = [inputFileE])
        self.testJobE.create(group = testJobGroup)
        self.testJobE["state"] = "complete"
        self.testJobE.save()
        self.stateChangeAction.execute(jobs = [self.testJobE])

        self.setFWJRAction.execute(self.testJobE["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco5.xml"))                
        return

    def setupDBForUnmergedRedneckRecoMerge(self):
        """
        _setupDBForUnmergedRedneckRecoMerge_

        Setup merges in the database for the unmerged redneck reco test.  The
        merge jobs will be constructed in the following fashion:
          reco merge 1: job1, job2
          reco merge 2: job3, job4, job5
          aod merge 1: job1, job2, job3
          aod merge 2: job4, job5
          skim merge: job1, job2, job3, job4, job5

        This function must be run after the accountant has finished run over all
        the processing jobs as the files output from those jobs need to be in
        the database.
        """
        recoFile1 = File(lfn = "/path/to/some/reco/file/1.root")
        recoFile2 = File(lfn = "/path/to/some/reco/file/2.root")
        recoFile3 = File(lfn = "/path/to/some/reco/file/3.root")
        recoFile4 = File(lfn = "/path/to/some/reco/file/4.root")
        recoFile5 = File(lfn = "/path/to/some/reco/file/5.root")        
        recoFile1.load()
        recoFile2.load()
        recoFile3.load()
        recoFile4.load()
        recoFile5.load()        

        aodFile1 = File(lfn = "/path/to/some/aod/file/1.root")
        aodFile2 = File(lfn = "/path/to/some/aod/file/2.root")
        aodFile3 = File(lfn = "/path/to/some/aod/file/3.root")
        aodFile4 = File(lfn = "/path/to/some/aod/file/4.root")
        aodFile5 = File(lfn = "/path/to/some/aod/file/5.root")        
        aodFile1.load()
        aodFile2.load()
        aodFile3.load()
        aodFile4.load()
        aodFile5.load()

        skimFile1 = File(lfn = "/path/to/some/skim/file/1.root")
        skimFile2 = File(lfn = "/path/to/some/skim/file/2.root")
        skimFile3 = File(lfn = "/path/to/some/skim/file/3.root")
        skimFile4 = File(lfn = "/path/to/some/skim/file/4.root")
        skimFile5 = File(lfn = "/path/to/some/skim/file/5.root")
        skimFile1.load()
        skimFile2.load()
        skimFile3.load()
        skimFile4.load()
        skimFile5.load()
        
        recoMergeGroup = JobGroup(subscription = self.testMergeRecoSubscription)
        recoMergeGroup.create()
        aodMergeGroup = JobGroup(subscription = self.testMergeAodSubscription)
        aodMergeGroup.create()
        skimMergeGroup = JobGroup(subscription = self.testMergeSkimSubscription)
        skimMergeGroup.create()                

        self.skimMergeJob = Job(name = "SkimMergeJob", files = [skimFile1, skimFile2,
                                                                skimFile3, skimFile4,
                                                                skimFile5])
        self.skimMergeJob.create(group = skimMergeGroup)
        self.skimMergeJob["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.skimMergeJob])
        self.setFWJRAction.execute(self.skimMergeJob["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeSkim.xml"))

        self.aodMergeJob1 = Job(name = "AodMergeJob1", files = [aodFile1, aodFile2,
                                                                aodFile3])
        self.aodMergeJob1.create(group = aodMergeGroup)
        self.aodMergeJob1["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.aodMergeJob1])
        self.setFWJRAction.execute(self.aodMergeJob1["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeAod1.xml"))

        self.aodMergeJob2 = Job(name = "AodMergeJob2", files = [aodFile4, aodFile5])
        self.aodMergeJob2.create(group = aodMergeGroup)
        self.aodMergeJob2["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.aodMergeJob2])
        self.setFWJRAction.execute(self.aodMergeJob2["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeAod2.xml"))

        self.recoMergeJob1 = Job(name = "RecoMergeJob1", files = [recoFile1, recoFile2])
        self.recoMergeJob1.create(group = recoMergeGroup)
        self.recoMergeJob1["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.recoMergeJob1])
        self.setFWJRAction.execute(self.recoMergeJob1["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeReco1.xml"))

        self.recoMergeJob2 = Job(name = "RecoMergeJob2", files = [recoFile3, recoFile4,
                                                                 recoFile5])
        self.recoMergeJob2.create(group = recoMergeGroup)
        self.recoMergeJob2["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.recoMergeJob2])
        self.setFWJRAction.execute(self.recoMergeJob2["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeReco2.xml"))        

        return

    def testUnmergedRedneckReco(self):
        """
        _testUnmergedRedneckReco_

        Run the unmerged redneck reco test.  This will verify that accounting of
        all the processing jobs and then run the merge jobs through the
        accountant one at a time so that file status and parentage in WMBS and
        DBSBuffer can be verified as redneck parents arrive.
        """
        self.setupDBForUnmergedRedneckReco()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport1 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco1.xml"))
        jobReport2 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco2.xml"))
        jobReport3 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco3.xml"))
        jobReport4 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco4.xml"))
        jobReport5 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "UnmergedRedneckReco5.xml"))

        self.verifyFileMetaData(self.testJobA["id"], jobReport1[0].files)
        self.verifyFileMetaData(self.testJobB["id"], jobReport2[0].files)
        self.verifyFileMetaData(self.testJobC["id"], jobReport3[0].files)
        self.verifyFileMetaData(self.testJobD["id"], jobReport4[0].files)
        self.verifyFileMetaData(self.testJobE["id"], jobReport5[0].files)

        self.verifyJobSuccess(self.testJobA["id"])
        self.verifyJobSuccess(self.testJobB["id"])
        self.verifyJobSuccess(self.testJobC["id"])
        self.verifyJobSuccess(self.testJobD["id"])
        self.verifyJobSuccess(self.testJobE["id"])        

        self.recoOutputFileset.loadData()
        self.mergedRecoOutputFileset.loadData()
        self.aodOutputFileset.loadData()
        self.mergedAodOutputFileset.loadData()
        self.skimOutputFileset.loadData()
        self.mergedSkimOutputFileset.loadData()

        assert len(self.mergedRecoOutputFileset.getFiles(type = "list")) == 0, \
               "Error: No files should be in the merged reco fileset."
        assert len(self.recoOutputFileset.getFiles(type = "list")) == 5, \
               "Error: There should be 5 files in the reco output fileset."

        assert len(self.mergedAodOutputFileset.getFiles(type = "list")) == 0, \
               "Error: No files should be in the merged aod fileset."
        assert len(self.aodOutputFileset.getFiles(type = "list")) == 5, \
               "Error: There should be 5 files in the aod output fileset."

        assert len(self.mergedSkimOutputFileset.getFiles(type = "list")) == 0, \
               "Error: No files should be in the merged skim output fileset."
        assert len(self.skimOutputFileset.getFiles(type = "list")) == 5, \
               "Error: There should be 5 files in the skim output fileset."        

        self.setupDBForUnmergedRedneckRecoMerge()

        self.skimMergeJob["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.skimMergeJob])
        accountant.algorithm()

        skimReport = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeSkim.xml"))
        self.verifyFileMetaData(self.skimMergeJob["id"], skimReport[0].files)        
        self.verifyJobSuccess(self.skimMergeJob["id"])

        skimDBSFile = DBSBufferFile(lfn = "/some/path/to/a/merged/skim/file/1.root")
        skimDBSFile.load(parentage = 1)

        assert skimDBSFile["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert len(skimDBSFile["parents"]) == 0, \
               "Error: File should have no parents in DBSBuffer."

        self.aodMergeJob1["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.aodMergeJob1])
        accountant.algorithm()

        aodReport1 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeAod1.xml"))
        self.verifyFileMetaData(self.aodMergeJob1["id"], aodReport1[0].files)        
        self.verifyJobSuccess(self.aodMergeJob1["id"])

        aodDBSFile1 = DBSBufferFile(lfn = "/some/path/to/a/merged/aod/file/1.root")
        aodDBSFile1.load(parentage = 1)
        skimDBSFile.load(parentage = 1)

        assert aodDBSFile1["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert skimDBSFile["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert len(aodDBSFile1["parents"]) == 0, \
               "Error: File should have no parents in DBSBuffer."
        assert len(skimDBSFile["parents"]) == 1, \
               "Error: File should have 1 parent in DBSBuffer."        
        assert list(skimDBSFile["parents"])[0]["lfn"] == aodDBSFile1["lfn"], \
               "Error: Skim file should be the child of the aod file."
        
        self.recoMergeJob2["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.recoMergeJob2])
        accountant.algorithm()

        recoReport2 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                 "test/python/WMComponent_t/JobAccountant_t/",
                                                 "RedneckRecoMergeReco2.xml"))
        self.verifyFileMetaData(self.recoMergeJob2["id"], recoReport2[0].files)        
        self.verifyJobSuccess(self.recoMergeJob2["id"])
        dbsParents = ["/path/to/some/lfnC", "/path/to/some/lfnD",
                      "/path/to/some/lfnE"]
        self.verifyDBSBufferContents("Merge", dbsParents, recoReport2[0].files)        

        recoDBSFile2 = DBSBufferFile(lfn = "/some/path/to/a/merged/reco/file/2.root")
        recoDBSFile2.load(parentage = 1)
        aodDBSFile1.load(parentage = 1)
        skimDBSFile.load(parentage = 1)

        assert recoDBSFile2["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."
        assert aodDBSFile1["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert skimDBSFile["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert len(aodDBSFile1["parents"]) == 1, \
               "Error: File should have 1 parent in DBSBuffer."
        assert list(aodDBSFile1["parents"])[0]["lfn"] == recoDBSFile2["lfn"], \
               "Error: Aod file should be the child of the reco file."            
        assert len(skimDBSFile["parents"]) == 1, \
               "Error: File should have 1 parent in DBSBuffer."        
        assert list(skimDBSFile["parents"])[0]["lfn"] == aodDBSFile1["lfn"], \
               "Error: Skim file should be the child of the aod file."        

        self.aodMergeJob2["state"] = "complete"        
        self.stateChangeAction.execute(jobs = [self.aodMergeJob2])
        accountant.algorithm()

        aodReport2 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeAod2.xml"))
        self.verifyFileMetaData(self.aodMergeJob2["id"], aodReport2[0].files)        
        self.verifyJobSuccess(self.aodMergeJob2["id"])

        dbsParents = ["/some/path/to/a/merged/reco/file/2.root"]        
        self.verifyDBSBufferContents("Merge", dbsParents, aodReport2[0].files)

        aodDBSFile2 = DBSBufferFile(lfn = "/some/path/to/a/merged/aod/file/2.root")
        aodDBSFile2.load(parentage = 1)
        aodDBSFile1.load(parentage = 1)
        skimDBSFile.load(parentage = 1)

        assert aodDBSFile2["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."
        assert aodDBSFile1["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert skimDBSFile["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert len(aodDBSFile1["parents"]) == 1, \
               "Error: File should have 1 parent in DBSBuffer."
        assert len(aodDBSFile1["parents"]) == 1, \
               "Error: File should have 1 parent in DBSBuffer."        
        assert list(aodDBSFile2["parents"])[0]["lfn"] == recoDBSFile2["lfn"], \
               "Error: Aod file should be the child of the reco file."
        assert list(aodDBSFile1["parents"])[0]["lfn"] == recoDBSFile2["lfn"], \
               "Error: Aod file should be the child of the reco file."                    

        dbsParents = ["/some/path/to/a/merged/aod/file/1.root", 
                      "/some/path/to/a/merged/aod/file/2.root"]
        self.verifyDBSBufferContents("Merge", dbsParents, skimReport[0].files)

        self.recoMergeJob1["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.recoMergeJob1])        
        accountant.algorithm()        

        recoReport1 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                 "test/python/WMComponent_t/JobAccountant_t/",
                                                 "RedneckRecoMergeReco1.xml"))
        self.verifyFileMetaData(self.recoMergeJob1["id"], recoReport1[0].files)        
        self.verifyJobSuccess(self.recoMergeJob1["id"])

        dbsParents = ["/path/to/some/lfnA", "/path/to/some/lfnB"]
        self.verifyDBSBufferContents("Merge", dbsParents, recoReport1[0].files)
        dbsParents = ["/some/path/to/a/merged/reco/file/1.root",
                      "/some/path/to/a/merged/reco/file/2.root"]
        self.verifyDBSBufferContents("Merge", dbsParents, aodReport1[0].files)

        recoDBSFile1 = DBSBufferFile(lfn = "/some/path/to/a/merged/reco/file/1.root")
        recoDBSFile1.load()
        aodDBSFile2.load()
        aodDBSFile1.load()
        skimDBSFile.load()

        assert recoDBSFile1["status"] == "NOTUPLOADED", \
               "Error: File status is not correct."
        assert aodDBSFile1["status"] == "NOTUPLOADED", \
               "Error: File status is not correct."
        assert aodDBSFile2["status"] == "NOTUPLOADED", \
               "Error: File status is not correct."                
        assert skimDBSFile["status"] == "NOTUPLOADED", \
               "Error: File status is not correct."        
        
        return

    def setupDBForMergedRedneckReco(self):
        """
        _setupDBForMergedRedneckReco_

        Setup a test for verifying that the accountant handles redneck workflows
        that have straight to merge files correctly.  This test will have five
        processing jobs and two merge jobs.  The processing jobs will have also
        possible combinations of unmerged/merged files for a workflow that
        produces two outputs.  Two of the FWJRs only differ in the order of
        the output modules in the FWJR.
        """
        self.recoOutputFileset = Fileset(name = "RECO")
        self.recoOutputFileset.create()
        self.mergedRecoOutputFileset = Fileset(name = "MergedRECO")
        self.mergedRecoOutputFileset.create()        
        self.aodOutputFileset = Fileset(name = "AOD")
        self.aodOutputFileset.create()
        self.mergedAodOutputFileset = Fileset(name = "MergedAOD")
        self.mergedAodOutputFileset.create()

        self.testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                     name = "TestWF", task = "None")
        self.testWorkflow.create()
        self.testWorkflow.addOutput("recoOutputModule", self.recoOutputFileset)
        self.testWorkflow.addOutput("aodOutputModule", self.aodOutputFileset,
                                    "recoOutputModule")

        self.testRecoMergeWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                              name = "TestRecoMergeWF", task = "None")
        self.testRecoMergeWorkflow.create()
        self.testRecoMergeWorkflow.addOutput("merged", self.mergedRecoOutputFileset)

        self.testAodMergeWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                             name = "TestAodMergeWF", task = "None")
        self.testAodMergeWorkflow.create()
        self.testAodMergeWorkflow.addOutput("merged", self.mergedAodOutputFileset)        

        inputFileA = File(lfn = "/path/to/some/lfnA", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileB = File(lfn = "/path/to/some/lfnB", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileC = File(lfn = "/path/to/some/lfnC", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileD = File(lfn = "/path/to/some/lfnD", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")
        inputFileE = File(lfn = "/path/to/some/lfnE", size = 600000, events = 60000,
                         locations = "cmssrm.fnal.gov")        
        inputFileA.create()
        inputFileB.create()
        inputFileC.create()
        inputFileD.create()
        inputFileE.create()        

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testFileset.addFile(inputFileA)
        testFileset.addFile(inputFileB)
        testFileset.addFile(inputFileC)
        testFileset.addFile(inputFileD)
        testFileset.addFile(inputFileE)        
        testFileset.commit()
        
        self.testSubscription = Subscription(fileset = testFileset,
                                             workflow = self.testWorkflow,
                                             split_algo = "EventBased",
                                             type = "Processing")

        self.testMergeRecoSubscription = Subscription(fileset = self.recoOutputFileset,
                                                      workflow = self.testRecoMergeWorkflow,
                                                      split_algo = "WMBSMergeBySize",
                                                      type = "Merge")
        self.testMergeAodSubscription = Subscription(fileset = self.aodOutputFileset,
                                                     workflow = self.testAodMergeWorkflow,
                                                     split_algo = "WMBSMergeBySize",
                                                     type = "Merge")
        self.testSubscription.create()
        self.testMergeRecoSubscription.create()
        self.testMergeAodSubscription.create()
        self.testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription = self.testSubscription)
        testJobGroup.create()
        
        self.testJobA = Job(name = "RecoJobA", files = [inputFileA])
        self.testJobA.create(group = testJobGroup)
        self.testJobA["state"] = "complete"
        self.testJobA.save()
        self.stateChangeAction.execute(jobs = [self.testJobA])

        self.setFWJRAction.execute(self.testJobA["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco1.xml"))

        self.testJobB = Job(name = "RecoJobB", files = [inputFileB])
        self.testJobB.create(group = testJobGroup)
        self.testJobB["state"] = "complete"
        self.testJobB.save()
        self.stateChangeAction.execute(jobs = [self.testJobB])

        self.setFWJRAction.execute(self.testJobB["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco2.xml"))

        self.testJobC = Job(name = "RecoJobC", files = [inputFileC])
        self.testJobC.create(group = testJobGroup)
        self.testJobC["state"] = "complete"
        self.testJobC.save()
        self.stateChangeAction.execute(jobs = [self.testJobC])

        self.setFWJRAction.execute(self.testJobC["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco3.xml"))

        self.testJobD = Job(name = "RecoJobD", files = [inputFileD])
        self.testJobD.create(group = testJobGroup)
        self.testJobD["state"] = "complete"
        self.testJobD.save()
        self.stateChangeAction.execute(jobs = [self.testJobD])

        self.setFWJRAction.execute(self.testJobD["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco4.xml"))

        self.testJobE = Job(name = "RecoJobE", files = [inputFileE])
        self.testJobE.create(group = testJobGroup)
        self.testJobE["state"] = "complete"
        self.testJobE.save()
        self.stateChangeAction.execute(jobs = [self.testJobE])

        self.setFWJRAction.execute(self.testJobE["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco5.xml"))        

        return

    def setupDBForMergedRedneckRecoMerge(self):
        """
        _setupDBForMergedRedneckRecoMerge_

        Create the merge jobs for the straight to merged redneck workflow test.
        The merge jobs will merge together the two unmerged RECO files and the
        two unmerged AOD files.
        """
        recoFile3 = File(lfn = "/path/to/some/reco/file/3.root")
        recoFile5 = File(lfn = "/path/to/some/reco/file/5.root")        
        recoFile3.load()
        recoFile5.load()        

        aodFile3 = File(lfn = "/path/to/some/aod/file/3.root")
        aodFile4 = File(lfn = "/path/to/some/aod/file/4.root")
        aodFile3.load()
        aodFile4.load()
        
        recoMergeGroup = JobGroup(subscription = self.testMergeRecoSubscription)
        recoMergeGroup.create()
        aodMergeGroup = JobGroup(subscription = self.testMergeAodSubscription)
        aodMergeGroup.create()

        self.recoMergeJob = Job(name = "RecoMergeJob", files = [recoFile3, recoFile5])
        self.recoMergeJob.create(group = recoMergeGroup)
        self.recoMergeJob["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.recoMergeJob])
        self.setFWJRAction.execute(self.recoMergeJob["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeReco1.xml"))

        self.aodMergeJob = Job(name = "AodMergeJob", files = [aodFile3, aodFile4])
        self.aodMergeJob.create(group = aodMergeGroup)
        self.aodMergeJob["state"] = "executing"
        self.stateChangeAction.execute(jobs = [self.aodMergeJob])
        self.setFWJRAction.execute(self.aodMergeJob["id"],
                                   os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "RedneckRecoMergeAod1.xml"))        

        return

    def testMergedRedneckReco(self):
        """
        _testMergedRedneckReco_

        Verify the behavior of the accountwant with a redneck workflow that has
        straight to merged files.  This will run the processing jobs and the
        merge jobs in the worst case way and verify that file parentage and
        file status in DBS is correct.
        """
        self.setupDBForMergedRedneckReco()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReport1 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco1.xml"))
        jobReport2 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco2.xml"))
        jobReport3 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco3.xml"))
        jobReport4 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco4.xml"))
        jobReport5 = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergedRedneckReco5.xml"))        

        self.verifyFileMetaData(self.testJobA["id"], jobReport1[0].files)
        self.verifyFileMetaData(self.testJobB["id"], jobReport2[0].files)
        self.verifyFileMetaData(self.testJobC["id"], jobReport3[0].files)
        self.verifyFileMetaData(self.testJobD["id"], jobReport4[0].files)
        self.verifyFileMetaData(self.testJobE["id"], jobReport5[0].files)        

        self.verifyJobSuccess(self.testJobA["id"])
        self.verifyJobSuccess(self.testJobB["id"])
        self.verifyJobSuccess(self.testJobC["id"])
        self.verifyJobSuccess(self.testJobD["id"])
        self.verifyJobSuccess(self.testJobE["id"])        

        dbsParents = {"/path/to/some/merged/reco/file/1.root":
                        ["/path/to/some/lfnA"],
                      "/path/to/some/merged/aod/file/1.root":
                        ["/path/to/some/merged/reco/file/1.root"]}
        self.verifyDBSBufferContents("Processing", dbsParents, jobReport1[0].files)

        dbsParents = {"/path/to/some/merged/reco/file/2.root":
                        ["/path/to/some/lfnB"],
                      "/path/to/some/merged/aod/file/2.root":
                        ["/path/to/some/merged/reco/file/2.root"]}
        self.verifyDBSBufferContents("Processing", dbsParents, jobReport2[0].files)

        dbsParents = ["/path/to/some/lfnD"]
        self.verifyDBSBufferContents("Processing", dbsParents, jobReport4[0].files)

        aodDBSFile1 = DBSBufferFile(lfn = "/path/to/some/merged/aod/file/1.root")
        aodDBSFile1.load(parentage = 1)
        aodDBSFile2 = DBSBufferFile(lfn = "/path/to/some/merged/aod/file/2.root")
        aodDBSFile2.load(parentage = 1)        
        aodDBSFile5 = DBSBufferFile(lfn = "/path/to/some/merged/aod/file/5.root")
        aodDBSFile5.load(parentage = 1)

        recoDBSFile1 = DBSBufferFile(lfn = "/path/to/some/merged/reco/file/1.root")
        recoDBSFile1.load(parentage = 1)
        recoDBSFile2 = DBSBufferFile(lfn = "/path/to/some/merged/reco/file/2.root")
        recoDBSFile2.load(parentage = 1)
        recoDBSFile4 = DBSBufferFile(lfn = "/path/to/some/merged/reco/file/4.root")
        recoDBSFile4.load(parentage = 1)                

        assert aodDBSFile1["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."
        assert aodDBSFile2["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."        
        assert aodDBSFile5["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert recoDBSFile1["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."
        assert recoDBSFile2["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."
        assert recoDBSFile4["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."        

        self.setupDBForMergedRedneckRecoMerge()

        self.aodMergeJob["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.aodMergeJob])        
        accountant.algorithm()        
        
        aodReport = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                               "test/python/WMComponent_t/JobAccountant_t/",
                                               "RedneckRecoMergeAod1.xml"))
        self.verifyFileMetaData(self.aodMergeJob["id"], aodReport[0].files)        
        self.verifyJobSuccess(self.aodMergeJob["id"])

        aodMergedDBSFile = DBSBufferFile(lfn = "/some/path/to/a/merged/aod/file/1.root")
        aodMergedDBSFile.load(parentage = 1)
        aodDBSFile5.load(parentage = 1)

        assert aodMergedDBSFile["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."
        assert aodDBSFile5["status"] == "WaitingForParents", \
               "Error: Status for file in DBSBuffer is wrong."

        self.recoMergeJob["state"] = "complete"
        self.stateChangeAction.execute(jobs = [self.recoMergeJob])        
        accountant.algorithm()        

        recoReport = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                               "test/python/WMComponent_t/JobAccountant_t/",
                                               "RedneckRecoMergeReco1.xml"))
        self.verifyFileMetaData(self.recoMergeJob["id"], recoReport[0].files)        
        self.verifyJobSuccess(self.recoMergeJob["id"])

        dbsParents = ["/path/to/some/lfnC", "/path/to/some/lfnE"]
        self.verifyDBSBufferContents("Merge", dbsParents, recoReport[0].files)
        dbsParents = ["/some/path/to/a/merged/reco/file/1.root",
                      "/path/to/some/merged/reco/file/4.root"]
        self.verifyDBSBufferContents("Merge", dbsParents, aodReport[0].files)        
        dbsParents = ["/some/path/to/a/merged/reco/file/1.root"]
        self.verifyDBSBufferContents("Processing", dbsParents, jobReport5[0].files)

        aodMergedDBSFile.load(parentage = 1)
        aodDBSFile5.load(parentage = 1)

        assert aodMergedDBSFile["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."
        assert aodDBSFile5["status"] == "NOTUPLOADED", \
               "Error: Status for file in DBSBuffer is wrong."

        return

    def setupDBForLoadTest(self):
        """
        _setupDBForLoadTest_

        Setup the database for a load test using the framework job reports in
        the DBSBuffer directory.  The job reports are from repack jobs that have
        several outputs, so configure the workflows accordingly.
        """
        config = self.createConfig(workerThreads = 1)

        inputFileset = Fileset(name = "TestFileset")
        inputFileset.create()

        caloFileset = Fileset(name = "Calo")
        caloFileset.create()
        cosmicsFileset = Fileset(name = "Cosmics")
        cosmicsFileset.create()
        hcalFileset = Fileset(name = "HCal")
        hcalFileset.create()
        minbiasFileset = Fileset(name = "MinBias")
        minbiasFileset.create()
        ranFileset = Fileset(name = "Random")
        ranFileset.create()
        calFileset = Fileset(name = "Cal")
        calFileset.create()
        hltFileset = Fileset(name = "HLT")
        hltFileset.create()

        testWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                name = "TestWF", task = "None")
        testWorkflow.create()
        testWorkflow.addOutput("write_A_Calo_RAW", caloFileset)
        testWorkflow.addOutput("write_A_Cosmics_RAW", cosmicsFileset)
        testWorkflow.addOutput("write_A_HcalHPDNoise_RAW", hcalFileset)
        testWorkflow.addOutput("write_A_MinimumBias_RAW", minbiasFileset)
        testWorkflow.addOutput("write_A_RandomTriggers_RAW", ranFileset)
        testWorkflow.addOutput("write_A_Calibration_TestEnables_RAW", calFileset)
        testWorkflow.addOutput("write_HLTDEBUG_Monitor_RAW", hltFileset)
        
        self.testSubscription = Subscription(fileset = inputFileset,
                                             workflow = testWorkflow,
                                             split_algo = "FileBased",
                                             type = "Processing")
        self.testSubscription.create()

        self.jobs = []
        for i in range(100):
            testJobGroup = JobGroup(subscription = self.testSubscription)
            testJobGroup.create()

            testJob = Job(name = makeUUID())
            testJob.create(group = testJobGroup)
            testJob["state"] = "complete"
            self.stateChangeAction.execute(jobs = [testJob])

            newFile = File(lfn = "/some/lfn/for/job/%s" % testJob["id"], size = 600000, events = 60000,
                           locations = "cmssrm.fnal.gov", merged = True)
            newFile.create()
            inputFileset.addFile(newFile)
            testJob.addFile(newFile)
            testJob.associateFiles()

            fwjrPath = os.path.join(os.getenv("WMCOREBASE"),
                                    "test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports",
                                    "FrameworkJobReport-45%02d.xml" % i)
            self.jobs.append((testJob["id"], fwjrPath))
            self.setFWJRAction.execute(jobID = testJob["id"], fwjrPath = fwjrPath)

        inputFileset.commit()
        return

    def testOneProcessLoadTest(self):
        """
        _testOneProcessLoadTest_

        Run the load test using one worker process.
        """
        logging.debug("  Filling DB...")
        self.setupDBForLoadTest()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()

        logging.debug("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        endTime = time.time()
        logging.debug("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            logging.debug("  Validating %s, %s" % (jobID, fwjrPath))
            jobReports = readJobReport(fwjrPath)

            # There are some job reports missing, so we'll just ignore the
            # reports that don't parse correctly.  There are other unit tests
            # that verify that the accountant handles this case correctly.
            if len(jobReports) == 0:
                continue
            
            self.verifyFileMetaData(jobID, jobReports[0].files)
            self.verifyJobSuccess(jobID)
            self.verifyDBSBufferContents("Processing",
                                         ["/some/lfn/for/job/%s" % jobID],
                                         jobReports[0].files)

        return

    def testTwoProcessLoadTest(self):
        """
        _testTwoProcessLoadTest_

        Run the load test using two worker processes.
        """
        logging.debug("Two process load test:")
        logging.debug("  Filling DB...")
        self.setupDBForLoadTest()
        config = self.createConfig(workerThreads = 2)

        accountant = JobAccountantPoller(config)
        accountant.setup()

        logging.debug("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        endTime = time.time()
        logging.debug("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            logging.debug("  Validating %s, %s" % (jobID, fwjrPath))
            jobReports = readJobReport(fwjrPath)

            # There are some job reports missing, so we'll just ignore the
            # reports that don't parse correctly.  There are other unit tests
            # that verify that the accountant handles this case correctly.
            if len(jobReports) == 0:
                continue
            
            self.verifyFileMetaData(jobID, jobReports[0].files)
            self.verifyJobSuccess(jobID)
            self.verifyDBSBufferContents("Processing",
                                         ["/some/lfn/for/job/%s" % jobID],
                                         jobReports[0].files)

        return

    def testFourProcessLoadTest(self):
        """
        _testFourProcessLoadTest_

        Run the load test using four workers processes.
        """
        logging.debug("Four process load test:")
        logging.debug("  Filling DB...")
        self.setupDBForLoadTest()
        config = self.createConfig(workerThreads = 4)

        accountant = JobAccountantPoller(config)
        accountant.setup()

        logging.debug("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        endTime = time.time()
        logging.debug("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            logging.debug("  Validating %s, %s" % (jobID, fwjrPath))
            jobReports = readJobReport(fwjrPath)

            # There are some job reports missing, so we'll just ignore the
            # reports that don't parse correctly.  There are other unit tests
            # that verify that the accountant handles this case correctly.
            if len(jobReports) == 0:
                continue
            
            self.verifyFileMetaData(jobID, jobReports[0].files)
            self.verifyJobSuccess(jobID)
            self.verifyDBSBufferContents("Processing",
                                         ["/some/lfn/for/job/%s" % jobID],
                                         jobReports[0].files)

        return

    def testEightProcessLoadTest(self):
        """
        _testEightProcessLoadTest_

        Run the load test using eight workers processes.
        """
        logging.debug("Eight process load test:")
        logging.debug("  Filling DB...")
        self.setupDBForLoadTest()
        config = self.createConfig(workerThreads = 8)

        accountant = JobAccountantPoller(config)
        accountant.setup()

        logging.debug("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        endTime = time.time()
        logging.debug("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            logging.debug("  Validating %s, %s" % (jobID, fwjrPath))
            jobReports = readJobReport(fwjrPath)

            # There are some job reports missing, so we'll just ignore the
            # reports that don't parse correctly.  There are other unit tests
            # that verify that the accountant handles this case correctly.
            if len(jobReports) == 0:
                continue
            
            self.verifyFileMetaData(jobID, jobReports[0].files)
            self.verifyJobSuccess(jobID)
            self.verifyDBSBufferContents("Processing",
                                         ["/some/lfn/for/job/%s" % jobID],
                                         jobReports[0].files)

        return

    def testSixteenProcessLoadTest(self):
        """
        _testSixteenProcessLoadTest_

        Run the load test using sixteen workers processes.
        """
        logging.debug("Sixteen process load test:")
        logging.debug("  Filling DB...")
        self.setupDBForLoadTest()
        config = self.createConfig(workerThreads = 16)

        accountant = JobAccountantPoller(config)
        accountant.setup()

        logging.debug("  Running accountant...")

        startTime = time.time()
        accountant.algorithm()
        endTime = time.time()
        logging.debug("  Performance: %s fwjrs/sec" % (100 / (endTime - startTime)))

        for (jobID, fwjrPath) in self.jobs:
            logging.debug("  Validating %s, %s" % (jobID, fwjrPath))
            jobReports = readJobReport(fwjrPath)

            # There are some job reports missing, so we'll just ignore the
            # reports that don't parse correctly.  There are other unit tests
            # that verify that the accountant handles this case correctly.
            if len(jobReports) == 0:
                continue
            
            self.verifyFileMetaData(jobID, jobReports[0].files)
            self.verifyJobSuccess(jobID)
            self.verifyDBSBufferContents("Processing",
                                         ["/some/lfn/for/job/%s" % jobID],
                                         jobReports[0].files)

        return


    def testNoFileReport(self):
        """
        _testNoFileReport_
        
        See that the Accountant does not crash if there are no files.
        """


        self.setupDBForMergeSuccess()
        config = self.createConfig(workerThreads = 1)

        accountant = JobAccountantPoller(config)
        accountant.setup()
        accountant.algorithm()

        jobReports = readJobReport(os.path.join(os.getenv("WMCOREBASE"),
                                                "test/python/WMComponent_t/JobAccountant_t/",
                                                "MergeSuccessNoFiles.xml"))
        self.verifyJobSuccess(self.testJob["id"])



    
if __name__ == '__main__':
    unittest.main()
