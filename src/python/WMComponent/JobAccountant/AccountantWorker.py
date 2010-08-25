#!/usr/bin/env python
#pylint: disable-msg=E1101, E1103
#E1101 doesn't allow you to define config sections using .section_()
#E1103 doesn't recognize objects attached to the thread
"""
_AccountantWorker_

Used by the JobAccountant to do the actual processing of completed jobs.
"""

__revision__ = "$Id: AccountantWorker.py,v 1.10 2009/11/17 18:48:01 mnorman Exp $"
__version__ = "$Revision: 1.10 $"

import os
import threading
import logging

from WMCore.Agent.Configuration import Configuration

from WMCore.FwkJobReport.ReportParser import readJobReport
from WMCore.FwkJobReport.FwkJobReport import FwkJobReport

from WMCore.DAOFactory import DAOFactory

from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File import File
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

class AccountantWorker:
    """
    Class that actually does the work of parsing FWJRs for the Accountant
    Run through ProcessPool
    """
    
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

    def loadJobReport(self, parameters):
        """
        _loadJobReport_

        Given a framework job report on disk, load it and return a
        FwkJobReport instance.  If there is any problem loading or parsing the
        framework job report return None.
        """
        # The jobReportPath may be prefixed with "file://" which needs to be
        # removed so it doesn't confuse the FwkJobReport() parser.

        jobReportPath = parameters['fwjr_path']
        jobReportPath = jobReportPath.replace("file://","")

        if not os.path.exists(jobReportPath):
            logging.error("Bad FwkJobReport Path: %s" % jobReportPath)
            return self.createMissingFWKJR(parameters, 99999, 'Cannot find file in jobReport path')

        if os.path.getsize(jobReportPath) == 0:
            logging.error("Empty FwkJobReport: %s" % jobReportPath)
            return self.createMissingFWKJR(parameters, 99998, 'jobReport of size 0')

        try:
            jobReports = readJobReport(jobReportPath)
        except Exception, msg:
            logging.error("Cannot load %s: %s" % (jobReportPath, msg))
            return self.createMissingFWKJR(parameters, 99997, 'Cannot load jobReport')

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
        logging.info("Handling %s" % parameters["fwjr_path"])

        self.transaction.begin()

        fwkJobReport = self.loadJobReport(parameters)
        jobSuccess = None

        if fwkJobReport == None or fwkJobReport.status != "Success":
            if fwkJobReport == None:
                fwkJobReport = self.createMissingFWKJR(parameters)
            logging.error("I have a bad jobReport for %i" %(parameters['id']))
            self.handleFailed(jobID = parameters["id"],
                              fwkJobReport = fwkJobReport)
            jobSuccess = False
        else:
            self.handleSuccessful(jobID = parameters["id"],
                                  fwkJobReport = fwkJobReport)
            logging.error("This is a FWJR dummy!")
            logging.error(fwkJobReport.__class__)
            logging.error(fwkJobReport)
            jobSuccess = True

        self.transaction.commit()
        return {'id': parameters["id"], 'jobSuccess': jobSuccess}

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
            logging.info("Output module label missing.")
            return None

        if merged == False:
            logging.info("Unmerged...")
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
            if inputFile["lfn"] not in wmbsFile.getParentLFNs():
                wmbsFile.addParent(inputFile["lfn"])

        if merged:
            self.addFileToDBS(fwjrFile, dbsParentLFNs)

        return (wmbsFile["id"], fwjrFile["ModuleLabel"], merged)

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

            logging.info("Job: %s, %s, %s, %s" % (jobID, outputMap, merged, moduleLabel)) 
            outputFileset = self.outputFilesetsForJob(outputMap, merged, moduleLabel)
            logging.info("Output fileset: %s" % outputFileset)
            if outputFileset != None:
                filesetAssoc.append({"fileid": fileID, "fileset": outputFileset})

            filesetAssoc.append({"fileid": fileID, "fileset": wmbsJobGroup.output.id})

        self.bulkAddToFilesetAction.execute(binds = filesetAssoc,
                                            conn = self.transaction.conn,
                                            transaction = True)
        wmbsJob.completeInputFiles()
        
        self.stateChanger.propagate([wmbsJob], "success", "complete")

        return
        
    def handleFailed(self, jobID, fwkJobReport):
        """
        _handleFailed_

        Handle a failed job.  Update the job's metadata marking the outcome as
        'failure' and incrementing the retry count.  Mark all the files used as
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
        return


    def createMissingFWKJR(self, parameters, errorCode = 999, errorDescription = 'Failure of unknown type'):
        """
        _createMissingFWJR_
        
        Create a missing FWJR if the report can't be found by the code in the path location
        """

        report = FwkJobReport()
        report.addError(errorCode, errorDescription)
        report.status = 'Failed'
        report.name   = parameters['id']

        return report

