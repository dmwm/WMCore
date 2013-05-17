#!/usr/bin/env python
#pylint: disable-msg=E1103, W6501, E1101, C0301
#E1103: Use DB objects attached to thread
#W6501: Allow string formatting in error messages
#E1101: Create config sections
#C0301: The names for everything are so ridiculously long
# that I'm disabling this.  The rest of you will have to get
# bigger monitors.
"""
_AccountantWorker_

Used by the JobAccountant to do the actual processing of completed jobs.
"""

import os
import shutil
import threading
import logging
import gc
import collections

from WMCore.FwkJobReport.Report  import Report
from WMCore.DAOFactory           import DAOFactory
from WMCore.WMConnectionBase     import WMConnectionBase
from WMCore.WMException          import WMException

from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File       import File
from WMCore.WMBS.Job        import Job

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import sanitizeURL
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.ACDC.DataCollectionService  import DataCollectionService

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
        self.dbsDaoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        self.getOutputMapAction      = self.daofactory(classname = "Jobs.GetOutputMap")
        self.bulkAddToFilesetAction  = self.daofactory(classname = "Fileset.BulkAddByLFN")
        self.bulkParentageAction     = self.daofactory(classname = "Files.AddBulkParentage")
        self.getJobTypeAction        = self.daofactory(classname = "Jobs.GetType")
        self.getParentInfoAction     = self.daofactory(classname = "Files.GetParentInfo")
        self.setParentageByJob       = self.daofactory(classname = "Files.SetParentageByJob")
        self.setFileRunLumi          = self.daofactory(classname = "Files.AddRunLumi")
        self.setFileLocation         = self.daofactory(classname = "Files.SetLocationByLFN")
        self.setFileAddChecksum      = self.daofactory(classname = "Files.AddChecksumByLFN")
        self.addFileAction           = self.daofactory(classname = "Files.Add")
        self.jobCompleteInput        = self.daofactory(classname = "Jobs.CompleteInput")
        self.setBulkOutcome          = self.daofactory(classname = "Jobs.SetOutcomeBulk")
        self.getWorkflowSpec         = self.daofactory(classname = "Workflow.GetSpecAndNameFromTask")
        self.getJobInfoByID         = self.daofactory(classname = "Jobs.LoadFromID")
        self.getFullJobInfo         = self.daofactory(classname = "Jobs.LoadForErrorHandler")

        self.dbsStatusAction = self.dbsDaoFactory(classname = "DBSBufferFiles.SetStatus")
        self.dbsParentStatusAction = self.dbsDaoFactory(classname = "DBSBufferFiles.GetParentStatus")
        self.dbsChildrenAction = self.dbsDaoFactory(classname = "DBSBufferFiles.GetChildren")
        self.dbsCreateFiles    = self.dbsDaoFactory(classname = "DBSBufferFiles.Add")
        self.dbsSetLocation    = self.dbsDaoFactory(classname = "DBSBufferFiles.SetLocationByLFN")
        self.dbsInsertLocation = self.dbsDaoFactory(classname = "DBSBufferFiles.AddLocation")
        self.dbsSetChecksum    = self.dbsDaoFactory(classname = "DBSBufferFiles.AddChecksumByLFN")
        self.dbsSetRunLumi     = self.dbsDaoFactory(classname = "DBSBufferFiles.AddRunLumi")
        self.dbsGetWorkflow    = self.dbsDaoFactory(classname = "ListWorkflow")

        self.dbsLFNHeritage      = self.dbsDaoFactory(classname = "DBSBufferFiles.BulkHeritageParent")

        self.stateChanger = ChangeState(config)

        # Decide whether or not to attach jobReport to returned value
        self.returnJobReport = getattr(config.JobAccountant, 'returnReportFromWorker', False)

        # Store location for the specs for DBS
        self.specDir = getattr(config.JobAccountant, 'specDir', None)

        # ACDC service
        self.dataCollection = DataCollectionService(url = config.ACDC.couchurl,
                                                    database = config.ACDC.database)

        jobDBurl = sanitizeURL(config.JobStateMachine.couchurl)['url']
        jobDBName = config.JobStateMachine.couchDBName
        jobCouchdb  = CouchServer(jobDBurl)
        self.fwjrCouchDB = jobCouchdb.connectDatabase("%s/fwjrs" % jobDBName)
        self.localWMStats = WMStatsWriter(config.TaskArchiver.localWMStatsURL)

        # Hold data for later commital
        self.dbsFilesToCreate  = []
        self.wmbsFilesToBuild  = []
        self.fileLocation      = None
        self.mergedOutputFiles = []
        self.listOfJobsToSave  = []
        self.listOfJobsToFail  = []
        self.filesetAssoc      = []
        self.parentageBinds    = []
        self.jobsWithSkippedFiles = {}
        self.count = 0
        self.datasetAlgoID     = collections.deque(maxlen = 1000)
        self.datasetAlgoPaths  = collections.deque(maxlen = 1000)
        self.dbsLocations      = collections.deque(maxlen = 1000)
        self.workflowIDs       = collections.deque(maxlen = 1000)
        self.workflowPaths     = collections.deque(maxlen = 1000)

        self.phedex = PhEDEx()
        self.locLists = self.phedex.getNodeMap()


        return

    def reset(self):
        """
        _reset_

        Reset all global vars between runs.
        """
        self.dbsFilesToCreate  = []
        self.wmbsFilesToBuild  = []
        self.fileLocation      = None
        self.mergedOutputFiles = []
        self.listOfJobsToSave  = []
        self.listOfJobsToFail  = []
        self.filesetAssoc      = []
        self.parentageBinds    = []
        self.jobsWithSkippedFiles = {}
        gc.collect()
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
        jobReportPath = parameters.get("fwjr_path", None)
        if not jobReportPath:
            logging.error("Bad FwkJobReport Path: %s" % jobReportPath)
            return self.createMissingFWKJR(parameters, 99999, "FWJR path is empty")

        jobReportPath = jobReportPath.replace("file://","")
        if not os.path.exists(jobReportPath):
            logging.error("Bad FwkJobReport Path: %s" % jobReportPath)
            return self.createMissingFWKJR(parameters, 99999, 'Cannot find file in jobReport path: %s' % jobReportPath)

        if os.path.getsize(jobReportPath) == 0:
            logging.error("Empty FwkJobReport: %s" % jobReportPath)
            return self.createMissingFWKJR(parameters, 99998, 'jobReport of size 0: %s ' % jobReportPath)

        jobReport = Report()

        try:
            jobReport.load(jobReportPath)
        except Exception, ex:
            msg =  "Error loading jobReport %s\n" % jobReportPath
            msg += str(ex)
            logging.error(msg)
            logging.debug("Failing job: %s\n" % parameters)
            return self.createMissingFWKJR(parameters, 99997, 'Cannot load jobReport')

        if len(jobReport.listSteps()) == 0:
            logging.error("FwkJobReport with no steps: %s" % jobReportPath)
            return self.createMissingFWKJR(parameters, 99997, 'jobReport with no steps: %s ' % jobReportPath)

        return jobReport

    def didJobSucceed(self, jobReport):
        """
        _didJobSucceed_

        Get the status of the jobReport.  This will loop through all the steps
        and make sure the status is 'Success'.  If a step does not return
        'Success', the job will fail.
        """
        if not hasattr(jobReport, 'data'):
            return False

        if not hasattr(jobReport.data, 'steps'):
            return False

        if not jobReport.taskSuccessful():
            return False


        return True

    def __call__(self, parameters):
        """
        __call__

        Handle a completed job.  The parameters dictionary will contain the job
        ID and the path to the framework job report.
        """
        returnList = []
        self.reset()

        for job in parameters:
            logging.info("Handling %s" % job["fwjr_path"])

            # Load the job and set the ID
            fwkJobReport = self.loadJobReport(job)
            fwkJobReport.setJobID(job['id'])
            jobSuccess = None

            if not self.didJobSucceed(fwkJobReport):
                logging.error("I have a bad jobReport for %i" %(job['id']))
                self.handleFailed(jobID = job["id"],
                                  fwkJobReport = fwkJobReport)
                jobSuccess = False
            else:
                self.handleSuccessful(jobID = job["id"],
                                      fwkJobReport = fwkJobReport,
                                      fwkJobReportPath = job['fwjr_path'])
                jobSuccess = True

            if self.returnJobReport:
                returnList.append({'id': job["id"], 'jobSuccess': jobSuccess,
                                   'jobReport': fwkJobReport})
            else:
                returnList.append({'id': job["id"], 'jobSuccess': jobSuccess})
            self.count += 1

        self.beginTransaction()

        # Now things done at the end of the job
        # Do what we can with WMBS files
        self.handleWMBSFiles()

        # Create DBSBufferFiles
        self.createFilesInDBSBuffer()

        # Handle filesetAssoc
        if len(self.filesetAssoc) > 0:
            self.bulkAddToFilesetAction.execute(binds = self.filesetAssoc,
                                                conn = self.getDBConn(),
                                                transaction = self.existingTransaction())

        # Move successful jobs to successful
        if len(self.listOfJobsToSave) > 0:
            idList = [x['id'] for x in self.listOfJobsToSave]
            outcomeBinds = [{'jobid': x['id'], 'outcome': x['outcome']} for x in self.listOfJobsToSave]
            self.setBulkOutcome.execute(binds = outcomeBinds,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

            self.jobCompleteInput.execute(id = idList,
                                          lfnsToSkip = self.jobsWithSkippedFiles,
                                          conn = self.getDBConn(),
                                          transaction = self.existingTransaction())
            self.stateChanger.propagate(self.listOfJobsToSave, "success", "complete")

        # If we have failed jobs, fail them
        if len(self.listOfJobsToFail) > 0:
            outcomeBinds = [{'jobid': x['id'], 'outcome': x['outcome']} for x in self.listOfJobsToFail]
            self.setBulkOutcome.execute(binds = outcomeBinds,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
            self.stateChanger.propagate(self.listOfJobsToFail, "jobfailed", "complete")

        # Arrange WMBS parentage
        if len(self.parentageBinds) > 0:
            self.setParentageByJob.execute(binds = self.parentageBinds,
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())

        # Straighten out DBS Parentage
        if len(self.mergedOutputFiles) > 0:
            self.handleDBSBufferParentage()

        if len(self.jobsWithSkippedFiles) > 0:
            self.handleSkippedFiles()

        self.commitTransaction(existingTransaction = False)

        return returnList

    def outputFilesetsForJob(self, outputMap, merged, moduleLabel):
        """
        _outputFilesetsForJob_

        Determine if the file should be placed in any other fileset.  Note that
        this will not return the JobGroup output fileset as all jobs will have
        their output placed there.
        """
        if not outputMap.has_key(moduleLabel):
            logging.info("Output module label missing from output map.")
            return []

        outputFilesets = []
        for outputFileset in outputMap[moduleLabel]:
            if merged == False and outputFileset["output_fileset"] != None:
                outputFilesets.append(outputFileset["output_fileset"])
            else:
                if outputFileset["merged_output_fileset"] != None:
                    outputFilesets.append(outputFileset["merged_output_fileset"])

        return outputFilesets

    def addFileToDBS(self, jobReportFile, task):
        """
        _addFileToDBS_

        Add a file that was output from a job to the DBS buffer.
        """
        datasetInfo = jobReportFile["dataset"]

        dbsFile = DBSBufferFile(lfn = jobReportFile["lfn"],
                                size = jobReportFile["size"],
                                events = jobReportFile["events"],
                                checksums = jobReportFile["checksums"],
                                status = "NOTUPLOADED")
        dbsFile.setAlgorithm(appName = datasetInfo["applicationName"],
                             appVer = datasetInfo["applicationVersion"],
                             appFam = jobReportFile["module_label"],
                             psetHash = "GIBBERISH",
                             configContent = jobReportFile.get('configURL'))

        dbsFile.setDatasetPath("/%s/%s/%s" % (datasetInfo["primaryDataset"],
                                              datasetInfo["processedDataset"],
                                              datasetInfo["dataTier"]))
        dbsFile.setValidStatus(validStatus = jobReportFile.get("validStatus", None))
        dbsFile.setProcessingVer(ver = jobReportFile.get('processingVer', None))
        dbsFile.setAcquisitionEra(era = jobReportFile.get('acquisitionEra', None))
        dbsFile.setGlobalTag(globalTag = jobReportFile.get('globalTag', None))
        dbsFile['task'] = task

        for run in jobReportFile["runs"]:
            newRun = Run(runNumber = run.run)
            newRun.extend(run.lumis)
            dbsFile.addRun(newRun)


        dbsFile.setLocation(se = list(jobReportFile["locations"])[0], immediateSave = False)
        self.dbsFilesToCreate.append(dbsFile)
        return

    def findDBSParents(self, lfn):
        """
        _findDBSParents_

        Find the parent of the file in DBS
        This is meant to be called recursively
        """
        parentsInfo = self.getParentInfoAction.execute([lfn],
                                                       conn = self.getDBConn(),
                                                       transaction = self.existingTransaction())
        newParents = set()
        for parentInfo in parentsInfo:
            # This will catch straight to merge files that do not have redneck
            # parents.  We will mark the straight to merge file from the job
            # as a child of the merged parent.
            if int(parentInfo["merged"]) == 1:
                newParents.add(parentInfo["lfn"])

            elif parentInfo['gpmerged'] == None:
                continue

            # Handle the files that result from merge jobs that aren't redneck
            # children.  We have to setup parentage and then check on whether or
            # not this file has any redneck children and update their parentage
            # information.
            elif int(parentInfo["gpmerged"]) == 1:
                newParents.add(parentInfo["gplfn"])

            # If that didn't work, we've reached the great-grandparents
            # And we have to work via recursion
            else:
                parentSet = self.findDBSParents(lfn = parentInfo['gplfn'])
                for parent in parentSet:
                    newParents.add(parent)

        return newParents

    def addFileToWMBS(self, jobType, fwjrFile, jobMask, task, jobID = None):
        """
        _addFileToWMBS_

        Add a file that was produced in a job to WMBS.
        """
        fwjrFile["first_event"] = jobMask["FirstEvent"]

        if fwjrFile["first_event"] == None:
            fwjrFile["first_event"] = 0

        if jobType == "Merge" and fwjrFile["module_label"] != "logArchive":
            setattr(fwjrFile["fileRef"], 'merged', True)
            fwjrFile["merged"] = True

        wmbsFile = self.createFileFromDataStructsFile(file = fwjrFile, jobID = jobID)

        if fwjrFile["merged"]:
            self.addFileToDBS(fwjrFile, task)

        return wmbsFile


    def _mapLocation(self, fwkJobReport):
        for file in fwkJobReport.getAllFileRefs():
            if file and hasattr(file, 'location'):
                file.location = self.phedex.getBestNodeName(file.location, self.locLists)


    def handleSuccessful(self, jobID, fwkJobReport, fwkJobReportPath = None):
        """
        _handleSuccessful_

        Handle a successful job, parsing the job report and updating the job in
        WMBS.
        """
        wmbsJob = Job(id = jobID)
        wmbsJob.load()
        wmbsJob["outcome"] = "success"
        wmbsJob.getMask()
        outputID = wmbsJob.loadOutputID()

        wmbsJob["fwjr"] = fwkJobReport

        outputMap = self.getOutputMapAction.execute(jobID = jobID,
                                                    conn = self.getDBConn(),
                                                    transaction = self.existingTransaction())

        jobType = self.getJobTypeAction.execute(jobID = jobID,
                                                conn = self.getDBConn(),
                                                transaction = self.existingTransaction())

        fileList = fwkJobReport.getAllFiles()
        
        bookKeepingSuccess = True
        
        for fwjrFile in fileList:
            # associate logArchived file for parent jobs on wmstats assuming fileList is length is 1.
            if jobType == "LogCollect":
                try:
                    self.associateLogCollectToParentJobsInWMStats(fwkJobReport, fwjrFile["lfn"], fwkJobReport.getTaskName())
                except Exception, ex:
                    bookKeepingSuccess = False
                    logging.error("Error occurred: associating log collect location, will try again\n %s" % str(ex))
                    break
                
            wmbsFile = self.addFileToWMBS(jobType, fwjrFile, wmbsJob["mask"],
                                          jobID = jobID, task = fwkJobReport.getTaskName())
            merged = fwjrFile['merged']
            moduleLabel = fwjrFile["module_label"]

            if merged:
                self.mergedOutputFiles.append(wmbsFile)

            self.filesetAssoc.append({"lfn": wmbsFile["lfn"], "fileset": outputID})
            outputFilesets = self.outputFilesetsForJob(outputMap, merged, moduleLabel)
            for outputFileset in outputFilesets:
                self.filesetAssoc.append({"lfn": wmbsFile["lfn"], "fileset": outputFileset})

        # Check if the job had any skipped files
        # Put them in ACDC containers, we assume full file processing
        # No job masks
        skippedFiles = fwkJobReport.getAllSkippedFiles()
        if skippedFiles:
            self.jobsWithSkippedFiles[jobID] = skippedFiles

        if bookKeepingSuccess:
            # Only save once job is done, and we're sure we made it through okay
            self._mapLocation(wmbsJob['fwjr'])
            self.listOfJobsToSave.append(wmbsJob)
        #wmbsJob.save()

        return
    
    def associateLogCollectToParentJobsInWMStats(self, fwkJobReport, logAchiveLFN, task):
        inputFileList = fwkJobReport.getAllInputFiles()
        requestName = task.split('/')[1]
        keys = []
        for inputFile in inputFileList:
            keys.append([requestName, inputFile["lfn"]])
        resultRows = self.fwjrCouchDB.loadView("FWJRDump", 'jobsByOutputLFN', 
                                               options = {"stale": "update_after"},
                                               keys = keys)['rows']
        #get data from wmbs
        parentWMBSJobIDs = []
        for row in resultRows:
            parentWMBSJobIDs.append({"jobid": row["value"]})
        #update Job doc in wmstats
        results = self.getJobInfoByID.execute(parentWMBSJobIDs)
        parentJobNames = []
        
        if type(results) == list:
            for jobInfo in results:
                parentJobNames.append(jobInfo['name'])
        else:
            parentJobNames.append(results['name'])
        
        self.localWMStats.updateLogArchiveLFN(parentJobNames, logAchiveLFN)
       
    def handleFailed(self, jobID, fwkJobReport):
        """
        _handleFailed_

        Handle a failed job.  Update the job's metadata marking the outcome as
        'failure' and incrementing the retry count.  Mark all the files used as
        input for the job as failed.  Finally, update the job's state.
        """
        wmbsJob = Job(id = jobID)
        wmbsJob.load()
        outputID = wmbsJob.loadOutputID()
        wmbsJob["outcome"] = "failure"
        #wmbsJob.save()

        # We'll fake the rest of the state transitions here as the rest of the
        # WMAgent job submission framework is not yet complete.
        wmbsJob["fwjr"] = fwkJobReport

        outputMap = self.getOutputMapAction.execute(jobID = jobID,
                                                    conn = self.getDBConn(),
                                                    transaction = self.existingTransaction())

        jobType = self.getJobTypeAction.execute(jobID = jobID,
                                                conn = self.getDBConn(),
                                                transaction = self.existingTransaction())

        fileList = fwkJobReport.getAllFilesFromStep(step = 'logArch1')

        for fwjrFile in fileList:
            wmbsFile = self.addFileToWMBS(jobType, fwjrFile, wmbsJob["mask"],
                                          jobID = jobID, task = fwkJobReport.getTaskName())
            merged = fwjrFile['merged']
            moduleLabel = fwjrFile["module_label"]

            if merged:
                self.mergedOutputFiles.append(wmbsFile)

            self.filesetAssoc.append({"lfn": wmbsFile["lfn"], "fileset": outputID})
            outputFilesets = self.outputFilesetsForJob(outputMap, merged, moduleLabel)
            for outputFileset in outputFilesets:
                self.filesetAssoc.append({"lfn": wmbsFile["lfn"], "fileset": outputFileset})

        self._mapLocation(wmbsJob['fwjr'])
        self.listOfJobsToFail.append(wmbsJob)

        return


    def createMissingFWKJR(self, parameters, errorCode = 999,
                           errorDescription = 'Failure of unknown type'):
        """
        _createMissingFWJR_

        Create a missing FWJR if the report can't be found by the code in the
        path location.
        """
        report = Report()
        report.addError("cmsRun1", 84, errorCode, errorDescription)
        report.data.cmsRun1.status = "Failed"
        return report

    def createFilesInDBSBuffer(self):
        """
        _createFilesInDBSBuffer_
        It does the actual job of creating things in DBSBuffer
        WARNING: This assumes all files in a job have the same final location
        """
        if len(self.dbsFilesToCreate) == 0:
            # Whoops, nothing to do!
            return

        dbsFileTuples = []
        dbsLocations  = []
        dbsFileLoc    = []
        dbsCksumBinds = []
        runLumiBinds  = []
        selfChecksums = None

        for dbsFile in self.dbsFilesToCreate:
            # Append a tuple in the format specified by DBSBufferFiles.Add
            # Also run insertDatasetAlgo

            assocID         = None
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
                except Exception, ex:
                    msg =  "Unhandled exception while inserting datasetAlgo: %s\n" % datasetAlgoPath
                    msg += str(ex)
                    logging.error(msg)
                    raise AccountantWorkerException(msg)

            # Associate the workflow to the file using the taskPath and the requestName
            taskPath     = str(dbsFile.get('task'))
            if not taskPath:
                msg = "Can't do workflow association, this is not acceptable.\n"
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
                result = self.dbsGetWorkflow.execute(workflowName, taskPath, conn = self.getDBConn(),
                                                         transaction = self.existingTransaction())
                workflowID = result['id']

            self.workflowPaths.append(workflowPath)
            self.workflowIDs.append({'workflowPath': workflowPath, 'workflowID': workflowID})

            lfn           = dbsFile['lfn']
            selfChecksums = dbsFile['checksums']
            jobLocation   = dbsFile.getLocations()[0]

            dbsFileTuples.append((lfn, dbsFile['size'],
                                  dbsFile['events'], assocID,
                                  dbsFile['status'], workflowID))

            dbsFileLoc.append({'lfn': lfn, 'sename' : jobLocation})
            if dbsFile['runs']:
                runLumiBinds.append({'lfn': lfn, 'runs': dbsFile['runs']})

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums.keys():
                    dbsCksumBinds.append({'lfn': lfn, 'cksum' : selfChecksums[entry],
                                          'cktype' : entry})

        try:

            if not jobLocation in self.dbsLocations:
                self.dbsInsertLocation.execute(siteName = jobLocation,
                                               conn = self.getDBConn(),
                                               transaction = self.existingTransaction())
                self.dbsLocations.append(jobLocation)

            self.dbsCreateFiles.execute(files = dbsFileTuples,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            self.dbsSetLocation.execute(binds = dbsFileLoc,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            self.dbsSetChecksum.execute(bulkList = dbsCksumBinds,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            if len(runLumiBinds) > 0:
                self.dbsSetRunLumi.execute(file = runLumiBinds,
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())
        except WMException:
            raise
        except Exception, ex:
            msg =  "Got exception while inserting files into DBSBuffer!\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Listing binds:")
            logging.debug("jobLocation: %s\n" % jobLocation)
            logging.debug("dbsFiles: %s\n" % dbsFileTuples)
            logging.debug("dbsFileLoc: %s\n" %dbsFileLoc)
            logging.debug("Checksum binds: %s\n" % dbsCksumBinds)
            logging.debug("RunLumi binds: %s\n" % runLumiBinds)
            raise AccountantWorkerException(msg)


        # Now that we've created those files, clear the list
        self.dbsFilesToCreate = []
        return


    def handleWMBSFiles(self):
        """
        _handleWMBSFiles_

        Do what can be done in bulk in bulk
        """
        if len(self.wmbsFilesToBuild) == 0:
            # Nothing to do
            return

        runLumiBinds   = []
        fileCksumBinds = []
        fileLocations  = []
        fileCreate     = []

        for wmbsFile in self.wmbsFilesToBuild:
            lfn           = wmbsFile['lfn']
            if lfn == None:
                continue

            selfChecksums = wmbsFile['checksums']
            self.parentageBinds.append({'child': lfn, 'jobid': wmbsFile['jid']})
            if wmbsFile['runs']:
                runLumiBinds.append({'lfn': lfn, 'runs': wmbsFile['runs']})

            if len(wmbsFile.getLocations()) > 0:
                fileLocations.append({'lfn': lfn, 'location': wmbsFile.getLocations()[0]})

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums.keys():
                    fileCksumBinds.append({'lfn': lfn, 'cksum' : selfChecksums[entry],
                                           'cktype' : entry})

            fileCreate.append([lfn,
                               wmbsFile['size'],
                               wmbsFile['events'],
                               None,
                               wmbsFile["first_event"],
                               wmbsFile['merged']])

        if len(fileCreate) == 0:
            return

        try:

            self.addFileAction.execute(files = fileCreate,
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

            if runLumiBinds:
                self.setFileRunLumi.execute(file = runLumiBinds,
                                            conn = self.getDBConn(),
                                            transaction = self.existingTransaction())

            self.setFileAddChecksum.execute(bulkList = fileCksumBinds,
                                            conn = self.getDBConn(),
                                            transaction = self.existingTransaction())

            self.setFileLocation.execute(lfn = fileLocations,
                                         location = self.fileLocation,
                                         conn = self.getDBConn(),
                                         transaction = self.existingTransaction())


        except WMException:
            raise
        except Exception, ex:
            msg =  "Error while adding files to WMBS!\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Printing binds: \n")
            logging.debug("FileCreate binds: %s\n" % fileCreate)
            logging.debug("Runlumi binds: %s\n" % runLumiBinds)
            logging.debug("Checksum binds: %s\n" % fileCksumBinds)
            logging.debug("FileLocation binds: %s\n" % fileLocations)
            raise AccountantWorkerException(msg)

        # Clear out finished files
        self.wmbsFilesToBuild = []
        return

    def createFileFromDataStructsFile(self, file, jobID):
        """
        _createFileFromDataStructsFile_

        This function will create a WMBS File given a DataStructs file
        """
        wmbsFile = File()
        wmbsFile.update(file)

        if type(file["locations"]) == set:
            seName = list(file["locations"])[0]
        elif type(file["locations"]) == list:
            if len(file['locations']) > 1:
                logging.error("Have more then one location for a file in job %i" % (jobID))
                logging.error("Choosing location %s" % (file['locations'][0]))
            seName = file["locations"][0]
        else:
            seName = file["locations"]

        wmbsFile["locations"] = set()

        if seName != None:
            wmbsFile.setLocation(se = seName, immediateSave = False)
        wmbsFile['jid'] = jobID
        self.wmbsFilesToBuild.append(wmbsFile)

        return wmbsFile

    def handleDBSBufferParentage(self):
        """
        _handleDBSBufferParentage_

        Handle all the DBSBuffer Parentage in bulk if you can
        """
        outputLFNs = [f['lfn'] for f in self.mergedOutputFiles]
        bindList         = []
        for lfn in outputLFNs:
            newParents = self.findDBSParents(lfn = lfn)
            for parentLFN in newParents:
                bindList.append({'child': lfn, 'parent': parentLFN})

        # Now all the parents should exist
        # Commit them to DBSBuffer
        logging.info("About to commit all DBSBuffer Heritage information")
        logging.info(len(bindList))

        if len(bindList) > 0:
            try:
                self.dbsLFNHeritage.execute(binds = bindList,
                                            conn = self.getDBConn(),
                                            transaction = self.existingTransaction())
            except WMException:
                raise
            except Exception, ex:
                msg =  "Error while trying to handle the DBS LFN heritage\n"
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
        jobList = self.getFullJobInfo.execute([{'jobid' : x} for x in self.jobsWithSkippedFiles.keys()],
                                              fileSelection = self.jobsWithSkippedFiles,
                                              conn = self.getDBConn(),
                                              transaction = self.existingTransaction())
        self.dataCollection.failedJobs(jobList, useMask = False)
        return
