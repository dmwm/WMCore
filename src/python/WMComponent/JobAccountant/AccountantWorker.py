#!/usr/bin/env python
"""
_AccountantWorker_

Used by the JobAccountant to do the actual processing of completed jobs.
"""

__revision__ = "$Id: AccountantWorker.py,v 1.4 2009/10/26 16:54:58 sryu Exp $"
__version__ = "$Revision: 1.4 $"

import os
import time
import threading
import logging
import simplejson
import sys

from WMCore.Agent.Configuration import Configuration
from WMQuality.TestInit import TestInit

from WMCore.FwkJobReport.ReportParser import readJobReport

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.Transaction import Transaction

from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine import DefaultConfig

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

class AccountantWorker:
    def __init__(self, **kwargs):
        """
        __init__

        Create all DAO objects that are used by this class.
        """
        myThread = threading.currentThread()
        self.transaction = myThread.transaction
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.dbsDaoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)        

        self.newLocationAction = self.daoFactory(classname = "Locations.New")
        self.getOutputMapAction = self.daoFactory(classname = "Jobs.GetOutputMap")
        self.getOutputDBSParentAction = self.daoFactory(classname = "Jobs.GetOutputParentLFNs")
        self.bulkAddToFilesetAction = self.daoFactory(classname = "Fileset.BulkAdd")
        self.getJobTypeAction = self.daoFactory(classname = "Jobs.GetType")

        config = Configuration()
        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = kwargs["couchURL"]
        config.JobStateMachine.couchDBName = kwargs["couchDBName"]

        self.stateChanger = ChangeState(config,
                                        config.JobStateMachine.couchDBName)
        return

    def loadJobReport(self, jobReportPath):
        """
        _loadJobReport_

        Given a framework job report on disk, load it and return a
        FwkJobReport instance.  If there is any problem loading or parsing the
        framework job report return None.
        """
        # The jobReportPath may be prefixed with "file://" which needs to be
        # removed so it doesn't confuse the FwkJobReport() parser.
        jobReportPath = jobReportPath.replace("file://","")

        if not os.path.exists(jobReportPath):
            logging.error("Bad FwkJobReport Path: %s" % jobReportPath)
            return None

        if os.path.getsize(jobReportPath) == 0:
            logging.error("Empty FwkJobReport: %s" % jobReportPath)
            return None

        try:
            jobReports = readJobReport(jobReportPath)
        except Exception, msg:
            logging.error("Cannot load %s: %s" % (jobReportPath, msg))
            return None

        # The readJobReport() function will return a list of job reports,
        # but the accountant currently only supports jobs that return a single
        # job report.
        if len(jobReports) == 0:
            logging.error("Bad FWJR: %s" % jobReportPath)
            return None

        return jobReports[0]
   
    def __call__(self, parameters):
        """
        __call__

        Handle a completed job.  The parameters dictionary will contain the job
        ID and the path to the framework job report.
        """
        self.transaction.begin()
        
        fwkJobReport = self.loadJobReport(parameters["fwjr_path"])

        if fwkJobReport == None or fwkJobReport.status != "Success":
            self.handleFailed(jobID = parameters["id"],
                              fwkJobReport = fwkJobReport)
        else:
            self.handleSuccessful(jobID = parameters["id"],
                                  fwkJobReport = fwkJobReport)

        self.transaction.commit()
        return parameters["id"]

    def outputFilesetsForJob(self, outputMap, merged, moduleLabel):
        """
        _outputFilesetsForJob_

        Determine if the file should be placed in any other fileset.  Note that
        this will not return the JobGroup output fileset as all jobs will have
        their output placed their.

        If a file is merged and the output map has it going to a merge
        subscription the file will be placed in the output fileset of the merge
        subscription.
        """
        if not outputMap.has_key(moduleLabel):
            return None

        if merged == False:
            return outputMap[moduleLabel]["fileset"]

        if len(outputMap[moduleLabel]["children"]) == 1:
            outputChild = outputMap[moduleLabel]["children"][0]
            if outputChild["child_sub_type"] == "Merge":
                return outputChild["child_sub_output_fset"]

        return outputMap[moduleLabel]["fileset"]        

    def addFileToDBS(self, jobReportFile, fileParentLFNs):
        """
        _addFileToDBS_

        Add a file that was output from a job to the DBS buffer.
        """
        datasetInfo = jobReportFile.dataset[0]

        dbsFile = DBSBufferFile(lfn = jobReportFile["LFN"],
                                size = jobReportFile["Size"],
                                events = jobReportFile["TotalEvents"],
                                cksum = jobReportFile["Checksum"])

        dbsFile.setAlgorithm(appName = datasetInfo["ApplicationName"],
                             appVer = datasetInfo["ApplicationVersion"],
                             appFam = datasetInfo["OutputModuleName"],
                             psetHash = "GIBBERISH", configContent = "MOREGIBBERISH")
        
        dbsFile.setDatasetPath("/%s/%s/%s" % (datasetInfo["PrimaryDataset"],
                                              datasetInfo["ProcessedDataset"],
                                              datasetInfo["DataTier"]))

        for run in jobReportFile.runs.keys():
            newRun = Run(runNumber = run)
            newRun.extend(jobReportFile.runs[run])
            dbsFile.addRun(newRun)

        dbsFile.create()
        dbsFile.setLocation(se = jobReportFile["SEName"])
        dbsFile.addParents(fileParentLFNs)
        return

    def addFileToWMBS(self, jobType, fwjrFile, jobMask, inputFiles,
                      dbsParentLFNs):
        """
        _addFileToWMBS_

        Add a file that was produced in a job to WMBS.  Take care of file
        parentage as well as adding the file to DBS if necessary.  All merged
        files are added to DBS.
        """
        firstEvent = jobMask["FirstEvent"]
        lastEvent = jobMask["LastEvent"]

        if firstEvent == None:
            firstEvent = 0
        if lastEvent == None:
            lastEvent = int(fwjrFile["TotalEvents"])            

        if fwjrFile["MergedBySize"] == "True" or jobType == "Merge":
            merged = True
        else:
            merged = False

        self.newLocationAction.execute(siteName = fwjrFile["SEName"],
                                       conn = self.transaction.conn,
                                       transaction = True)
                                
        wmbsFile = File(lfn = fwjrFile["LFN"],
                        size = fwjrFile["Size"],
                        events = fwjrFile["TotalEvents"],
                        cksum = fwjrFile["Checksum"],
                        locations = fwjrFile["SEName"],
                        first_event = firstEvent,
                        last_event = lastEvent,
                        merged = merged)

        for run in fwjrFile.runs.keys():
            newRun = Run(runNumber = run)
            newRun.extend(fwjrFile.runs[run])
            wmbsFile.addRun(newRun)

        wmbsFile.create()

        for inputFile in inputFiles:
            wmbsFile.addParent(inputFile["lfn"])

        if merged:
            self.addFileToDBS(fwjrFile, dbsParentLFNs)

        return (wmbsFile["id"], fwjrFile["ModuleLabel"],
                fwjrFile["MergedBySize"])

    def handleSuccessful(self, jobID, fwkJobReport):
        """
        _handleSuccessful_

        Handle a successful job, parsing the job report and updating the job in
        WMBS.
        """
        wmbsJob = Job(id = jobID)
        wmbsJob.loadData()
        jobFiles = wmbsJob.getFiles()
        wmbsJob["outcome"] = "success"
        wmbsJob.save()
        wmbsJob.getMask()

        wmbsJob["fwjr"] = fwkJobReport
        self.stateChanger.propagate([wmbsJob], "success", "complete")

        dbsParentLFNs = self.getOutputDBSParentAction.execute(jobID = jobID,
                                                              conn = self.transaction.conn,
                                                              transaction = True)
        outputMap = self.getOutputMapAction.execute(jobID = jobID,
                                                    conn = self.transaction.conn,
                                                    transaction = True)

        wmbsJobGroup = JobGroup(id = wmbsJob["jobgroup"])
        wmbsJobGroup.load()

        jobType = self.getJobTypeAction.execute(jobID = jobID,
                                                conn = self.transaction.conn,
                                                transaction = True)

        filesetAssoc = []
        for fwjrFile in fwkJobReport.files:
            (fileID, moduleLabel, merged) = \
                     self.addFileToWMBS(jobType, fwjrFile, wmbsJob["mask"],
                                        jobFiles, dbsParentLFNs)

            outputFileset = self.outputFilesetsForJob(outputMap, merged, moduleLabel)
            if outputFileset != None:
                filesetAssoc.append({"fileid": fileID, "fileset": outputFileset})

            filesetAssoc.append({"fileid": fileID, "fileset": wmbsJobGroup.output.id})

        self.bulkAddToFilesetAction.execute(binds = filesetAssoc,
                                            conn = self.transaction.conn,
                                            transaction = True)
        wmbsJob.completeInputFiles()
        
        self.stateChanger.propagate([wmbsJob], "closeout", "success")
        return
        
    def handleFailed(self, jobID, fwkJobReport):
        """
        _handleFailed_

        Handle a failed job.  Update the job's metadata marking the outcome as
        "failure" and incrementing the retry count.  Mark all the files used as
        input for the job as failed.  Finally, update the job's state.
        """
        wmbsJob = Job(id = jobID)
        wmbsJob.load()

        wmbsJob["outcome"] = "failure"
        wmbsJob["retry_count"] += 1
        wmbsJob.save()
        wmbsJob.failInputFiles()
        
        # We'll fake the rest of the state transitions here as the rest of the
        # WMAgent job submission framework is not yet complete.
        wmbsJob["fwjr"] = fwkJobReport
        self.stateChanger.propagate([wmbsJob], "jobfailed", "complete")
        self.stateChanger.propagate([wmbsJob], "jobcooloff", "jobfailed")
        self.stateChanger.propagate([wmbsJob], "created", "jobcooloff")
        self.stateChanger.propagate([wmbsJob], "executing", "created")
        return

