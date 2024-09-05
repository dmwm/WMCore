#!/usr/bin/env python
# pylint: disable=E1103, E1101, C0301
# E1103: Use DB objects attached to thread
# E1101: Create config sections
# C0301: The names for everything are so ridiculously long
# that I'm disabling this.  The rest of you will have to get
# bigger monitors.
"""
_AccountantWorker_

Used by the JobAccountant to do the actual processing of completed jobs.
"""

import collections
import gc
import logging
import os
import threading

from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.FwkJobReport.Report import Report
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.WMBS.File import File
from WMCore.WMBS.Job import Job
from WMCore.WMConnectionBase import WMConnectionBase
from WMCore.WMException import WMException


class AccountantWorkerException(WMException):
    """
    _AccountantWorkerException_

    WMException based specific class
    """


class AccountantWorker(WMConnectionBase):
    """
    Class that actually does the work of parsing FWJRs for the Accountant
    Run through ProcessPool
    """

    def __init__(self, config):
        """
        __init__

        Create all DAO objects that are used by this class.
        """
        WMConnectionBase.__init__(self, "WMCore.WMBS")
        myThread = threading.currentThread()
        self.dbsDaoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                        logger=myThread.logger,
                                        dbinterface=myThread.dbi)

        self.getOutputMapAction = self.daofactory(classname="Jobs.GetOutputMap")
        self.bulkAddToFilesetAction = self.daofactory(classname="Fileset.BulkAddByLFN")
        self.bulkParentageAction = self.daofactory(classname="Files.AddBulkParentage")
        self.getJobTypeAction = self.daofactory(classname="Jobs.GetType")
        self.getParentInfoAction = self.daofactory(classname="Files.GetParentAndGrandParentInfo")
        self.setParentageByJob = self.daofactory(classname="Files.SetParentageByJob")
        self.setParentageByMergeJob = self.daofactory(classname="Files.SetParentageByMergeJob")
        self.setFileRunLumi = self.daofactory(classname="Files.AddRunLumi")
        self.setFileLocation = self.daofactory(classname="Files.SetLocationByLFN")
        self.setFileAddChecksum = self.daofactory(classname="Files.AddChecksumByLFN")
        self.addFileAction = self.daofactory(classname="Files.Add")
        self.jobCompleteInput = self.daofactory(classname="Jobs.CompleteInput")
        self.setBulkOutcome = self.daofactory(classname="Jobs.SetOutcomeBulk")
        self.getWorkflowSpec = self.daofactory(classname="Workflow.GetSpecAndNameFromTask")
        self.getJobInfoByID = self.daofactory(classname="Jobs.LoadFromID")
        self.getFullJobInfo = self.daofactory(classname="Jobs.LoadForErrorHandler")
        self.getJobTaskNameAction = self.daofactory(classname="Jobs.GetFWJRTaskName")
        self.pnn2Psn = self.daofactory(classname="Locations.GetPNNtoPSNMapping").execute()

        self.dbsStatusAction = self.dbsDaoFactory(classname="DBSBufferFiles.SetStatus")
        self.dbsParentStatusAction = self.dbsDaoFactory(classname="DBSBufferFiles.GetParentStatus")
        self.dbsChildrenAction = self.dbsDaoFactory(classname="DBSBufferFiles.GetChildren")
        self.dbsCreateFiles = self.dbsDaoFactory(classname="DBSBufferFiles.Add")
        self.dbsSetLocation = self.dbsDaoFactory(classname="DBSBufferFiles.SetLocationByLFN")
        self.dbsInsertLocation = self.dbsDaoFactory(classname="DBSBufferFiles.AddLocation")
        self.dbsSetChecksum = self.dbsDaoFactory(classname="DBSBufferFiles.AddChecksumByLFN")
        self.dbsSetRunLumi = self.dbsDaoFactory(classname="DBSBufferFiles.AddRunLumi")
        self.dbsGetWorkflow = self.dbsDaoFactory(classname="ListWorkflow")

        self.dbsLFNHeritage = self.dbsDaoFactory(classname="DBSBufferFiles.BulkHeritageParent")

        self.stateChanger = ChangeState(config)

        # Decide whether or not to attach jobReport to returned value
        self.returnJobReport = getattr(config.JobAccountant, 'returnReportFromWorker', False)

        # Store location for the specs for DBS
        self.specDir = getattr(config.JobAccountant, 'specDir', None)

        # maximum RAW EDM size for Repack output before data is put into Error dataset and skips PromptReco
        self.maxAllowedRepackOutputSize = getattr(config.JobAccountant, 'maxAllowedRepackOutputSize',
                                                  12 * 1024 * 1024 * 1024)

        # ACDC service
        self.dataCollection = DataCollectionService(url=config.ACDC.couchurl,
                                                    database=config.ACDC.database)

        self.jobDBName = config.JobStateMachine.couchDBName
        self.jobCouchdb = CouchServer(config.JobStateMachine.couchurl)
        self.fwjrCouchDB = None
        self.localWMStats = WMStatsWriter(config.TaskArchiver.localWMStatsURL, appName="WMStatsAgent")

        # Hold data for later commital
        self.dbsFilesToCreate = []
        self.wmbsFilesToBuild = []
        self.wmbsMergeFilesToBuild = []
        self.mergedOutputFiles = []
        self.listOfJobsToSave = []
        self.listOfJobsToFail = []
        self.filesetAssoc = []
        self.parentageBinds = []
        self.parentageBindsForMerge = []
        self.jobsWithSkippedFiles = {}
        self.count = 0
        self.datasetAlgoID = collections.deque(maxlen=1000)
        self.datasetAlgoPaths = collections.deque(maxlen=1000)
        self.dbsLocations = set()
        self.workflowIDs = collections.deque(maxlen=1000)
        self.workflowPaths = collections.deque(maxlen=1000)

        return

    def reset(self):
        """
        _reset_

        Reset all global vars between runs.
        """
        self.dbsFilesToCreate = []
        self.wmbsFilesToBuild = []
        self.wmbsMergeFilesToBuild = []
        self.mergedOutputFiles = []
        self.listOfJobsToSave = []
        self.listOfJobsToFail = []
        self.filesetAssoc = []
        self.parentageBinds = []
        self.parentageBindsForMerge = []
        self.jobsWithSkippedFiles = {}
        gc.collect()
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
        if not jobReportPath:
            logging.error("Bad FwkJobReport Path: %s", jobReportPath)
            return self.createMissingFWKJR(99999, "FWJR path is empty")

        jobReportPath = jobReportPath.replace("file://", "")
        if not os.path.exists(jobReportPath):
            logging.error("Bad FwkJobReport Path: %s", jobReportPath)
            return self.createMissingFWKJR(99999, 'Cannot find file in jobReport path: %s' % jobReportPath)

        if os.path.getsize(jobReportPath) == 0:
            logging.error("Empty FwkJobReport: %s", jobReportPath)
            return self.createMissingFWKJR(99998, 'jobReport of size 0: %s ' % jobReportPath)

        jobReport = Report()

        try:
            jobReport.load(jobReportPath)
        except UnicodeDecodeError:
            logging.error("Hit UnicodeDecodeError exception while loading jobReport: %s", jobReportPath)
            return self.createMissingFWKJR(99997, 'Found undecodable data in jobReport: {}'.format(jobReportPath))
        except Exception as ex:
            msg = "Error loading jobReport: {}\nDetails: {}".format(jobReportPath, str(ex))
            logging.error(msg)
            return self.createMissingFWKJR(99997, 'Cannot load jobReport')

        if not jobReport.listSteps():
            logging.error("FwkJobReport with no steps: %s", jobReportPath)
            return self.createMissingFWKJR(99997, 'jobReport with no steps: %s ' % jobReportPath)

        return jobReport

    def isTaskExistInFWJR(self, jobReport, jobStatus):
        """
        If taskName is not available in the FWJR, then tries to
        recover it getting data from the SQL database.
        """
        if not jobReport.getTaskName():
            logging.warning("Trying to recover a corrupted FWJR for a %s job with job id %s", jobStatus,
                            jobReport.getJobID())
            jobInfo = self.getJobTaskNameAction.execute(jobId=jobReport.getJobID(),
                                                        conn=self.getDBConn(),
                                                        transaction=self.existingTransaction())

            jobReport.setTaskName(jobInfo['taskName'])
            jobReport.save(jobInfo['fwjr_path'])
            if not jobReport.getTaskName():
                msg = "Report to developers. Failed to recover corrupted fwjr for %s job id %s" % (jobStatus,
                                                                                                   jobReport.getJobID())
                raise AccountantWorkerException(msg)
            else:
                logging.info("TaskName '%s' successfully recovered and added to fwjr id %s.", jobReport.getTaskName(),
                             jobReport.getJobID())

        return

    def __call__(self, parameters):
        """
        __call__

        Handle a completed job.  The parameters dictionary will contain the job
        ID and the path to the framework job report.
        """
        returnList = []
        self.reset()

        for job in parameters:
            logging.info("Handling %s", job["fwjr_path"])

            # Load the job and set the ID
            fwkJobReport = self.loadJobReport(job["fwjr_path"])
            fwkJobReport.setJobID(job['id'])

            jobSuccess = self.handleJob(jobID=job["id"],
                                        fwkJobReport=fwkJobReport)

            if self.returnJobReport:
                returnList.append({'id': job["id"], 'jobSuccess': jobSuccess,
                                   'jobReport': fwkJobReport})
            else:
                returnList.append({'id': job["id"], 'jobSuccess': jobSuccess})

            self.count += 1

        existingTransaction = self.beginTransaction()

        # Now things done at the end of the job
        # Do what we can with WMBS files
        self.handleWMBSFiles(self.wmbsFilesToBuild, self.parentageBinds)

        # handle merge files separately since parentage need to set
        # separately to support robust merge
        self.handleWMBSFiles(self.wmbsMergeFilesToBuild, self.parentageBindsForMerge)

        # Create DBSBufferFiles
        self.createFilesInDBSBuffer()

        # Handle filesetAssoc
        if self.filesetAssoc:
            self.bulkAddToFilesetAction.execute(binds=self.filesetAssoc,
                                                conn=self.getDBConn(),
                                                transaction=self.existingTransaction())

        # Move successful jobs to successful
        if self.listOfJobsToSave:
            idList = [x['id'] for x in self.listOfJobsToSave]
            outcomeBinds = [{'jobid': x['id'], 'outcome': x['outcome']} for x in self.listOfJobsToSave]
            self.setBulkOutcome.execute(binds=outcomeBinds,
                                        conn=self.getDBConn(),
                                        transaction=self.existingTransaction())

            self.jobCompleteInput.execute(id=idList,
                                          lfnsToSkip=self.jobsWithSkippedFiles,
                                          conn=self.getDBConn(),
                                          transaction=self.existingTransaction())
            self.stateChanger.propagate(self.listOfJobsToSave, "success", "complete")

        # If we have failed jobs, fail them
        if self.listOfJobsToFail:
            outcomeBinds = [{'jobid': x['id'], 'outcome': x['outcome']} for x in self.listOfJobsToFail]
            self.setBulkOutcome.execute(binds=outcomeBinds,
                                        conn=self.getDBConn(),
                                        transaction=self.existingTransaction())
            self.stateChanger.propagate(self.listOfJobsToFail, "jobfailed", "complete")

        # Arrange WMBS parentage
        if self.parentageBinds:
            self.setParentageByJob.execute(binds=self.parentageBinds,
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())
        if self.parentageBindsForMerge:
            self.setParentageByMergeJob.execute(binds=self.parentageBindsForMerge,
                                                conn=self.getDBConn(),
                                                transaction=self.existingTransaction())

        # Straighten out DBS Parentage
        if self.mergedOutputFiles:
            self.handleDBSBufferParentage()

        if self.jobsWithSkippedFiles:
            self.handleSkippedFiles()

        self.commitTransaction(existingTransaction)

        return returnList

    def outputFilesetsForJob(self, outputMap, merged, moduleLabel, datatier):
        """
        _outputFilesetsForJob_

        Determine if the file should be placed in any other fileset.  Note that
        this will not return the JobGroup output fileset as all jobs will have
        their output placed there.
        """
        # output map identifier uses output module + datatier
        moduleLabel += datatier
        if moduleLabel not in outputMap:
            logging.info("Output module label missing from output map.")
            return []

        outputFilesets = []
        for outputFileset in outputMap[moduleLabel]:
            if merged is False and outputFileset["output_fileset"] is not None:
                outputFilesets.append(outputFileset["output_fileset"])
            else:
                if outputFileset["merged_output_fileset"] is not None:
                    outputFilesets.append(outputFileset["merged_output_fileset"])

        return outputFilesets

    def addFileToDBS(self, jobReportFile, task, errorDataset=False):
        """
        _addFileToDBS_

        Add a file that was output from a job to the DBS buffer.
        """
        datasetInfo = jobReportFile["dataset"]

        dbsFile = DBSBufferFile(lfn=jobReportFile["lfn"],
                                size=jobReportFile["size"],
                                events=jobReportFile["events"],
                                checksums=jobReportFile["checksums"],
                                status="NOTUPLOADED",
                                inPhedex=0)
        dbsFile.setAlgorithm(appName=datasetInfo["applicationName"],
                             appVer=datasetInfo["applicationVersion"],
                             appFam=jobReportFile["module_label"],
                             psetHash="GIBBERISH",
                             configContent=jobReportFile.get('configURL'))

        if errorDataset:
            dbsFile.setDatasetPath("/%s/%s/%s" % (datasetInfo["primaryDataset"] + "-Error",
                                                  datasetInfo["processedDataset"],
                                                  datasetInfo["dataTier"]))
        else:
            dbsFile.setDatasetPath("/%s/%s/%s" % (datasetInfo["primaryDataset"],
                                                  datasetInfo["processedDataset"],
                                                  datasetInfo["dataTier"]))

        dbsFile.setValidStatus(validStatus=jobReportFile.get("validStatus", None))
        dbsFile.setProcessingVer(ver=jobReportFile.get('processingVer', 0))
        dbsFile.setAcquisitionEra(era=jobReportFile.get('acquisitionEra', None))
        dbsFile.setGlobalTag(globalTag=jobReportFile.get('globalTag', None))
        # TODO need to find where to get the prep id
        dbsFile.setPrepID(prep_id=jobReportFile.get('prep_id', None))
        dbsFile['task'] = task
        dbsFile['runs'] = jobReportFile['runs']

        dbsFile.setLocation(pnn=list(jobReportFile["locations"])[0], immediateSave=False)
        self.dbsFilesToCreate.append(dbsFile)
        return

    def findDBSParents(self, lfn):
        """
        _findDBSParents_

        Find the parent of the file in DBS
        This is meant to be called recursively
        """
        parentsInfo = self.getParentInfoAction.execute([lfn],
                                                       conn=self.getDBConn(),
                                                       transaction=self.existingTransaction())
        newParents = set()
        logging.debug("Found %d potential parents for lfn: %s", len(parentsInfo), lfn)
        for parentInfo in parentsInfo:
            # This will catch straight to merge files that do not have redneck
            # parents.  We will mark the straight to merge file from the job
            # as a child of the merged parent.
            if int(parentInfo["merged"]) == 1 and not parentInfo["lfn"].startswith("/store/unmerged/"):
                newParents.add(parentInfo["lfn"])

            elif parentInfo['gpmerged'] is None:
                continue

            # Handle the files that result from merge jobs that aren't redneck
            # children.  We have to setup parentage and then check on whether or
            # not this file has any redneck children and update their parentage
            # information.
            elif int(parentInfo["gpmerged"]) == 1 and not parentInfo["gplfn"].startswith("/store/unmerged/"):
                newParents.add(parentInfo["gplfn"])

            # If that didn't work, we've reached the great-grandparents
            # And we have to work via recursion
            else:
                parentSet = self.findDBSParents(lfn=parentInfo['gplfn'])
                for parent in parentSet:
                    newParents.add(parent)

        return newParents

    def addFileToWMBS(self, jobType, fwjrFile, jobMask, task, jobID=None):
        """
        _addFileToWMBS_

        Add a file that was produced in a job to WMBS.
        """
        fwjrFile["first_event"] = jobMask["FirstEvent"]

        if fwjrFile["first_event"] is None:
            fwjrFile["first_event"] = 0

        if jobType == "Merge" and fwjrFile["module_label"] != "logArchive":
            setattr(fwjrFile["fileRef"], 'merged', True)
            fwjrFile["merged"] = True

        wmbsFile = self.createFileFromDataStructsFile(fname=fwjrFile, jobID=jobID)

        if jobType == "Merge":
            self.wmbsMergeFilesToBuild.append(wmbsFile)
        else:
            self.wmbsFilesToBuild.append(wmbsFile)

        if fwjrFile["merged"]:
            self.addFileToDBS(fwjrFile, task,
                              jobType == "Repack" and fwjrFile["size"] > self.maxAllowedRepackOutputSize)

        return wmbsFile

    def handleJob(self, jobID, fwkJobReport):
        """
        _handleJob_

        Figure out if a job was successful or not, handle it appropriately
        (parse FWJR, update WMBS) and return the success status as a boolean

        """
        jobSuccess = fwkJobReport.taskSuccessful()

        outputMap = self.getOutputMapAction.execute(jobID=jobID,
                                                    conn=self.getDBConn(),
                                                    transaction=self.existingTransaction())

        jobType = self.getJobTypeAction.execute(jobID=jobID,
                                                conn=self.getDBConn(),
                                                transaction=self.existingTransaction())

        if jobSuccess:
            fileList = fwkJobReport.getAllFiles()

            # Consistency check comparing outputMap to fileList
            # they should match except for some limited special cases related to Tier-0:
            #     Repack Merge: May not produce Error output modules
            #     Express Merge: May not write RAW output
            # as of #7998, workflow output identifier is made of output module name and datatier
            outputModules = set([])
            for fwjrFile in fileList:
                outputModules.add(fwjrFile['outputModule'] + fwjrFile['dataset'].get('dataTier', ''))

            if set(outputMap) == outputModules:
                pass
            elif jobType == "LogCollect" and not outputMap and outputModules == {'LogCollect'}:
                pass
            elif jobType == "Merge" and (set(outputMap) & {'MergedErrorRAW', 'MergedErrorL1SCOUT', 'MergedErrorHLTSCOUT'}):
                pass
            elif jobType == "Express" and set(outputMap).difference(outputModules) == {'write_RAWRAW'}:
                pass
            else:
                # any job that is not multi-step and Processing/Production must FAIL!
                if jobType in ["Processing", "Production"]:
                    cmsRunSteps = sum([1 for step in fwkJobReport.listSteps() if step.startswith("cmsRun")])
                    if cmsRunSteps == 1:
                        jobSuccess = False
                    else:
                        msg = f"Job {jobID} accepted for multi-step CMSSW, even though "
                        msg += "the expected outputModules does not match content of the FWJR."
                        logging.warning(msg)
                else:
                    jobSuccess = False

                if jobSuccess is False:
                    sortedOutputMap = sorted(outputMap.keys())
                    errMsg = f"Job {jobID}, expected output modules: {sortedOutputMap}, "
                    errMsg += f"but has FWJR output modules: {sorted(outputModules)}"
                    logging.error(errMsg)
                    # override file list by the logArch1 output only
                    fileList = fwkJobReport.getAllFilesFromStep(step='logArch1')
        else:
            fileList = fwkJobReport.getAllFilesFromStep(step='logArch1')

        # Workaround: make sure every file has a valid location. See:
        # https://github.com/dmwm/WMCore/issues/9353 and https://github.com/dmwm/WMCore/issues/12092
        for fwjrFile in fileList:
            # T0 has analysis file without any location, see:
            # https://github.com/dmwm/WMCore/issues/9497
            if not fwjrFile.get("locations") and fwjrFile.get("lfn", "").endswith(".root"):
                logging.warning("The following file does not have any location: %s", fwjrFile)
                jobSuccess = False
                fileList = fwkJobReport.getAllFilesFromStep(step='logArch1')
                break

        if jobSuccess:
            logging.info("Job %d , handle successful job", jobID)
        else:
            logging.warning("Job %d , bad jobReport, failing job", jobID)

        # make sure the task name is present in FWJR (recover from WMBS if needed)
        if fileList:
            if jobSuccess:
                self.isTaskExistInFWJR(fwkJobReport, "success")
            else:
                self.isTaskExistInFWJR(fwkJobReport, "failed")

        # special check for LogCollect jobs
        skipLogCollect = False
        if jobSuccess and jobType == "LogCollect":
            for fwjrFile in fileList:
                try:
                    # this assumes there is only one file for LogCollect jobs, not sure what happend if that changes
                    self.associateLogCollectToParentJobsInWMStats(fwkJobReport, fwjrFile["lfn"],
                                                                  fwkJobReport.getTaskName())
                except Exception as ex:
                    skipLogCollect = True
                    logging.error("Error occurred: associating log collect location, will try again\n %s", str(ex))
                    break

        # now handle the job (unless the special LogCollect check failed)
        if not skipLogCollect:

            wmbsJob = Job(id=jobID)
            wmbsJob.load()
            outputID = wmbsJob.loadOutputID()
            wmbsJob.getMask()

            wmbsJob["fwjr"] = fwkJobReport

            if jobSuccess:
                wmbsJob["outcome"] = "success"
            else:
                wmbsJob["outcome"] = "failure"

            for fwjrFile in fileList:

                logging.debug("Job %d , register output %s", jobID, fwjrFile["lfn"])

                wmbsFile = self.addFileToWMBS(jobType, fwjrFile, wmbsJob["mask"],
                                              jobID=jobID, task=fwkJobReport.getTaskName())
                merged = fwjrFile['merged']
                moduleLabel = fwjrFile["module_label"]

                if merged:
                    self.mergedOutputFiles.append(wmbsFile)

                self.filesetAssoc.append({"lfn": wmbsFile["lfn"], "fileset": outputID})

                # LogCollect jobs have no output fileset
                if jobType == "LogCollect":
                    pass
                # Repack jobs that wrote too large merged output skip output filesets
                elif jobType == "Repack" and merged and wmbsFile["size"] > self.maxAllowedRepackOutputSize:
                    pass
                else:
                    dataTier = fwjrFile['dataset'].get('dataTier', '')
                    outputFilesets = self.outputFilesetsForJob(outputMap, merged, moduleLabel, dataTier)
                    for outputFileset in outputFilesets:
                        self.filesetAssoc.append({"lfn": wmbsFile["lfn"], "fileset": outputFileset})

            # Check if the job had any skipped files, put them in ACDC containers
            # We assume full file processing (no job masks)
            if jobSuccess:
                skippedFiles = fwkJobReport.getAllSkippedFiles()
                if skippedFiles and jobType not in ['LogCollect', 'Cleanup']:
                    self.jobsWithSkippedFiles[jobID] = skippedFiles

            if jobSuccess:
                self.listOfJobsToSave.append(wmbsJob)
            else:
                self.listOfJobsToFail.append(wmbsJob)

        return jobSuccess

    def associateLogCollectToParentJobsInWMStats(self, fwkJobReport, logAchiveLFN, task):
        """
        _associateLogCollectToParentJobsInWMStats_

        Associate a logArchive output to its parent job
        """
        if self.fwjrCouchDB is None:
            self.fwjrCouchDB = self.jobCouchdb.connectDatabase("%s/fwjrs" % self.jobDBName)

        inputFileList = fwkJobReport.getAllInputFiles()
        requestName = task.split('/')[1]
        keys = []
        for inputFile in inputFileList:
            keys.append([requestName, inputFile["lfn"]])
        resultRows = self.fwjrCouchDB.loadView("FWJRDump", 'jobsByOutputLFN',
                                               options={"stale": "update_after"},
                                               keys=keys)['rows']
        if resultRows:
            # get data from wmbs
            parentWMBSJobIDs = []
            for row in resultRows:
                parentWMBSJobIDs.append({"jobid": row["value"]})
            # update Job doc in wmstats
            results = self.getJobInfoByID.execute(parentWMBSJobIDs)
            parentJobNames = []

            if isinstance(results, list):
                for jobInfo in results:
                    parentJobNames.append(jobInfo['name'])
            else:
                parentJobNames.append(results['name'])

            self.localWMStats.updateLogArchiveLFN(parentJobNames, logAchiveLFN)
        else:
            # TODO: if the couch db is consistent with DB this should be removed (checking resultRow > 0)
            # It need to be failed and retried.
            logging.error(
                "job report is missing for updating log archive mapping\n Input file list\n %s", inputFileList)

        return

    def createMissingFWKJR(self, errorCode=999, errorDescription='Failure of unknown type'):
        """
        _createMissingFWJR_

        Create a missing FWJR if the report can't be found by the code in the
        path location.
        """
        report = Report()
        report.addError("cmsRun1", errorCode, "MissingJobReport", errorDescription)
        report.data.cmsRun1.status = "Failed"
        return report

    def createFilesInDBSBuffer(self):
        """
        _createFilesInDBSBuffer_
        It does the actual job of creating things in DBSBuffer
        WARNING: This assumes all files in a job have the same final location
        """
        if not self.dbsFilesToCreate:
            # Whoops, nothing to do!
            return

        dbsFileTuples = []
        dbsFileLoc = []
        dbsCksumBinds = []
        runLumiBinds = []
        jobLocations = set()

        for dbsFile in self.dbsFilesToCreate:
            # Append a tuple in the format specified by DBSBufferFiles.Add
            # Also run insertDatasetAlgo

            assocID = None
            datasetAlgoPath = '%s:%s:%s:%s:%s:%s:%s:%s' % (dbsFile['datasetPath'],
                                                           dbsFile["appName"],
                                                           dbsFile["appVer"],
                                                           dbsFile["appFam"],
                                                           dbsFile["psetHash"],
                                                           dbsFile['processingVer'],
                                                           dbsFile['acquisitionEra'],
                                                           dbsFile['globalTag'])
            # First, check if this is in the cache
            if datasetAlgoPath in self.datasetAlgoPaths:
                for da in self.datasetAlgoID:
                    if da['datasetAlgoPath'] == datasetAlgoPath:
                        assocID = da['assocID']
                        break

            if not assocID:
                # Then we have to get it ourselves
                try:
                    assocID = dbsFile.insertDatasetAlgo()
                    self.datasetAlgoPaths.append(datasetAlgoPath)
                    self.datasetAlgoID.append({'datasetAlgoPath': datasetAlgoPath,
                                               'assocID': assocID})
                except WMException:
                    raise
                except Exception as ex:
                    msg = "Unhandled exception while inserting datasetAlgo: %s\n" % datasetAlgoPath
                    msg += str(ex)
                    logging.error(msg)
                    raise AccountantWorkerException(msg)

            # Associate the workflow to the file using the taskPath and the requestName
            # TODO: debug why it happens and then drop/recover these cases automatically
            taskPath = dbsFile.get('task')
            if not taskPath:
                msg = "Can't do workflow association, report this error to a developer.\n"
                msg += "DbsFile : %s" % str(dbsFile)
                raise AccountantWorkerException(msg)
            workflowName = taskPath.split('/')[1]
            workflowPath = '%s:%s' % (workflowName, taskPath)
            if workflowPath in self.workflowPaths:
                for wf in self.workflowIDs:
                    if wf['workflowPath'] == workflowPath:
                        workflowID = wf['workflowID']
                        break
            else:
                result = self.dbsGetWorkflow.execute(workflowName, taskPath, conn=self.getDBConn(),
                                                     transaction=self.existingTransaction())
                workflowID = result['id']

            self.workflowPaths.append(workflowPath)
            self.workflowIDs.append({'workflowPath': workflowPath, 'workflowID': workflowID})

            lfn = dbsFile['lfn']
            selfChecksums = dbsFile['checksums']
            jobLocation = dbsFile.getLocations()[0]
            jobLocations.add(jobLocation)
            dbsFileTuples.append((lfn, dbsFile['size'],
                                  dbsFile['events'], assocID,
                                  dbsFile['status'], workflowID, dbsFile['in_phedex']))

            dbsFileLoc.append({'lfn': lfn, 'pnn': jobLocation})
            if dbsFile['runs']:
                runLumiBinds.append({'lfn': lfn, 'runs': dbsFile['runs']})

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums:
                    dbsCksumBinds.append({'lfn': lfn, 'cksum': selfChecksums[entry],
                                          'cktype': entry})

        try:

            diffLocation = jobLocations.difference(self.dbsLocations)
            self.dbsInsertLocation.execute(siteName=diffLocation,
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())
            self.dbsLocations.union(diffLocation)  # update the component cache location list

            self.dbsCreateFiles.execute(files=dbsFileTuples,
                                        conn=self.getDBConn(),
                                        transaction=self.existingTransaction())

            self.dbsSetLocation.execute(binds=dbsFileLoc,
                                        conn=self.getDBConn(),
                                        transaction=self.existingTransaction())

            self.dbsSetChecksum.execute(bulkList=dbsCksumBinds,
                                        conn=self.getDBConn(),
                                        transaction=self.existingTransaction())

            if runLumiBinds:
                self.dbsSetRunLumi.execute(file=runLumiBinds,
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())
        except WMException:
            raise
        except Exception as ex:
            msg = "Got exception while inserting files into DBSBuffer!\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Listing binds:")
            logging.debug("jobLocation: %s", jobLocation)
            logging.debug("dbsFiles: %s", dbsFileTuples)
            logging.debug("dbsFileLoc: %s", dbsFileLoc)
            logging.debug("Checksum binds: %s", dbsCksumBinds)
            logging.debug("RunLumi binds: %s", runLumiBinds)
            raise AccountantWorkerException(msg)

        # Now that we've created those files, clear the list
        self.dbsFilesToCreate = []
        return

    def handleWMBSFiles(self, wmbsFilesToBuild, parentageBinds):
        """
        _handleWMBSFiles_

        Do what can be done in bulk in bulk
        """
        if not wmbsFilesToBuild:
            # Nothing to do
            return

        runLumiBinds = []
        fileCksumBinds = []
        fileLocations = []
        fileCreate = []

        for wmbsFile in wmbsFilesToBuild:
            lfn = wmbsFile['lfn']
            if lfn is None:
                continue

            selfChecksums = wmbsFile['checksums']
            # by jobType add to different parentage relation
            # if it is the merge job, don't include the parentage on failed input files.
            # otherwise parentage is set for all input files.
            parentageBinds.append({'child': lfn, 'jobid': wmbsFile['jid']})

            if wmbsFile['runs']:
                runLumiBinds.append({'lfn': lfn, 'runs': wmbsFile['runs']})

            if wmbsFile.getLocations():
                outpnn = wmbsFile.getLocations()[0]
                if self.pnn2Psn.get(outpnn, None):
                    fileLocations.append({'lfn': lfn, 'location': outpnn})
                else:
                    msg = "PNN doesn't exist in wmbs_pnns table: %s (investigate)" % outpnn
                    logging.error(msg)
                    raise AccountantWorkerException(msg)

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums:
                    fileCksumBinds.append({'lfn': lfn, 'cksum': selfChecksums[entry],
                                           'cktype': entry})

            fileCreate.append([lfn,
                               wmbsFile['size'],
                               wmbsFile['events'],
                               None,
                               wmbsFile["first_event"],
                               wmbsFile['merged']])

        if not fileCreate:
            return

        try:

            self.addFileAction.execute(files=fileCreate,
                                       conn=self.getDBConn(),
                                       transaction=self.existingTransaction())

            if runLumiBinds:
                self.setFileRunLumi.execute(file=runLumiBinds,
                                            conn=self.getDBConn(),
                                            transaction=self.existingTransaction())

            self.setFileAddChecksum.execute(bulkList=fileCksumBinds,
                                            conn=self.getDBConn(),
                                            transaction=self.existingTransaction())

            self.setFileLocation.execute(lfn=fileLocations,
                                         conn=self.getDBConn(),
                                         transaction=self.existingTransaction())


        except WMException:
            raise
        except Exception as ex:
            msg = "Error while adding files to WMBS!\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Printing binds:")
            logging.debug("FileCreate binds: %s", fileCreate)
            logging.debug("Runlumi binds: %s", runLumiBinds)
            logging.debug("Checksum binds: %s", fileCksumBinds)
            logging.debug("FileLocation binds: %s", fileLocations)
            raise AccountantWorkerException(msg)

        # Clear out finished files
        wmbsFilesToBuild = []
        return

    def createFileFromDataStructsFile(self, fname, jobID):
        """
        _createFileFromDataStructsFile_

        This function will create a WMBS File given a DataStructs file
        """
        wmbsFile = File()
        wmbsFile.update(fname)

        if fname["locations"] and isinstance(fname["locations"], set):
            pnn = list(fname["locations"])[0]
        elif fname["locations"] and isinstance(fname["locations"], list):
            if len(fname['locations']) > 1:
                logging.error("Have more then one location for a file in job %i", jobID)
                logging.error("Choosing location %s", fname['locations'][0])
            pnn = fname["locations"][0]
        else:
            pnn = fname["locations"]

        wmbsFile["locations"] = set()

        if pnn != None:
            wmbsFile.setLocation(pnn=pnn, immediateSave=False)
        wmbsFile['jid'] = jobID

        return wmbsFile

    def handleDBSBufferParentage(self):
        """
        _handleDBSBufferParentage_

        Handle all the DBSBuffer Parentage in bulk if you can
        """
        outputLFNs = [f['lfn'] for f in self.mergedOutputFiles]
        bindList = []
        for lfn in outputLFNs:
            newParents = self.findDBSParents(lfn=lfn)
            for parentLFN in newParents:
                bindList.append({'child': lfn, 'parent': parentLFN})

        # Now all the parents should exist
        # Commit them to DBSBuffer
        logging.info("About to commit DBSBuffer heritage information for: %d binds", len(bindList))

        if bindList:
            try:
                self.dbsLFNHeritage.execute(binds=bindList,
                                            conn=self.getDBConn(),
                                            transaction=self.existingTransaction())
            except WMException:
                raise
            except Exception as ex:
                msg = "Error while trying to handle the DBS LFN heritage\n"
                msg += str(ex)
                msg += "BindList: %s" % bindList
                logging.error(msg)
                raise AccountantWorkerException(msg)
        return

    def handleSkippedFiles(self):
        """
        _handleSkippedFiles_

        Handle all the skipped files in bulk,
        the way it handles the skipped files
        imposes an important restriction:
        Skipped files should have been processed by a single job
        in the task and no job mask exists in it.
        This is suitable for jobs using ParentlessMergeBySize/FileBased/MinFileBased
        splitting algorithms.
        Here ACDC records and created and the file are moved
        to wmbs_sub_files_failed from completed.
        """
        jobList = self.getFullJobInfo.execute([{'jobid': x} for x in self.jobsWithSkippedFiles],
                                              fileSelection=self.jobsWithSkippedFiles,
                                              conn=self.getDBConn(),
                                              transaction=self.existingTransaction())
        self.dataCollection.failedJobs(jobList, useMask=False)
        return
