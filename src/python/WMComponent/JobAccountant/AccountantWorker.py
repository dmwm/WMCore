#!/usr/bin/env python
"""
_AccountantWorker_

Used by the JobAccountant to do the actual processing of completed jobs.
"""

__revision__ = "$Id: AccountantWorker.py,v 1.14 2010/02/25 22:44:06 sfoulkes Exp $"
__version__ = "$Revision: 1.14 $"

import os
import threading
import logging

from WMCore.Agent.Configuration import Configuration
from WMCore.FwkJobReport.Report import Report
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

        self.getOutputMapAction = self.daoFactory(classname = "Jobs.GetOutputMap")
        self.bulkAddToFilesetAction = self.daoFactory(classname = "Fileset.BulkAdd")
        self.bulkParentageAction = self.daoFactory(classname = "Files.AddBulkParentage")
        self.getJobTypeAction = self.daoFactory(classname = "Jobs.GetType")
        self.getParentInfoAction = self.daoFactory(classname = "Files.GetParentInfo")
        self.getMergedChildrenAction = self.daoFactory(classname = "Files.GetMergedChildren")

        self.dbsStatusAction = self.dbsDaoFactory(classname = "DBSBufferFiles.SetStatus")
        self.dbsParentStatusAction = self.dbsDaoFactory(classname = "DBSBufferFiles.GetParentStatus")
        self.dbsChildrenAction = self.dbsDaoFactory(classname = "DBSBufferFiles.GetChildren")

        config = Configuration()
        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = kwargs["couchURL"]
        config.JobStateMachine.couchDBName = kwargs["couchDBName"]

        self.stateChanger = ChangeState(config)
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

        jobReport = Report()

        try:
            jobReport.load(jobReportPath)
        except Exception, msg:
            logging.error("Cannot load %s: %s" % (jobReportPath, msg))
            return self.createMissingFWKJR(parameters, 99997, 'Cannot load jobReport')

        return jobReport

    def didJobSucceed(self, jobReport):
        """
        _getJobReportStatus_
        
        Get the status of the jobReport.  This will loop through all the steps
        and make sure the status is 'Success'.  If a step does not return
        'Success', the job will fail.
        """
        for step in jobReport.data.steps:
            report = getattr(jobReport.data, step)
            if report.status != 'Success' and report.status != 0:
                return False

        return True
   
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

        if not self.didJobSucceed(fwkJobReport):
            logging.error("I have a bad jobReport for %i" %(parameters['id']))
            self.handleFailed(jobID = parameters["id"],
                              fwkJobReport = fwkJobReport)
            jobSuccess = False
        else:
            self.handleSuccessful(jobID = parameters["id"],
                                  fwkJobReport = fwkJobReport)
            jobSuccess = True

        self.transaction.commit()
        return {'id': parameters["id"], 'jobSuccess': jobSuccess}

    def outputFilesetsForJob(self, outputMap, merged, moduleLabel):
        """
        _outputFilesetsForJob_

        Determine if the file should be placed in any other fileset.  Note that
        this will not return the JobGroup output fileset as all jobs will have
        their output placed there.

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

    def addFileToDBS(self, jobReportFile):
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
                             appFam = jobReportFile["ModuleLabel"],
                             psetHash = "GIBBERISH", configContent = "MOREGIBBERISH")
        
        dbsFile.setDatasetPath("/%s/%s/%s" % (datasetInfo["primaryDataset"],
                                              datasetInfo["processedDataset"],
                                              datasetInfo["dataTier"]))
        for run in jobReportFile["runs"]:
            newRun = Run(runNumber = run.run)
            newRun.extend(run.lumis)
            dbsFile.addRun(newRun)

        dbsFile.create()
        dbsFile.setLocation(se = list(jobReportFile["locations"])[0])
        return

    def fixupDBSFileStatus(self, redneckChildren):
        """
        _fixupDBSFileStatus_

        Fixup file status in DBS for redneck children.  Given a list of redneck
        children this method will determine if the redneck parents for the given
        child have been merged and update the file status in DBS accordingly.
        If a redneck child has it's status changed from "WaitingForParents" to
        "NOTUPLOADED" all of it's redneck children will be checked as well to
        determine if their status can be updated.
        """
        while len(redneckChildren) > 0:
            redneckChild = redneckChildren.pop()

            # Find all the redneck parents for this redneck child and iterate
            # through them.  For each redneck parent we check to see if it has
            # a merged child.  If it doesn't, we bail out of the loop and update
            # the child's status to be "WaitingForParents".  If all of the
            # redneck parents have merged children we can safely change the
            # status of the redneck child to "NOTUPLOADED" so that it is picked
            # up by the DBSUpload component.
            parentsInfo = self.getParentInfoAction.execute([redneckChild],
                                                           conn = self.transaction.conn,
                                                           transaction = True)
            for parentInfo in parentsInfo:
                if parentInfo["redneck_parent_fileset"] != None:
                    # We need to determine the correct input LFN for the
                    # GetMergedChildren DAO.  The GetParentInfo DAO will return
                    # the file's parent and grand parent LFN but does not state
                    # which was the input to the processing job, which is what
                    # we're really after.  Here, I'm going to assume that if the
                    # parent file is merged than it was the input to the
                    # processing job.  This assumption prevents us from running
                    # redneck workflows over unmerged files, but that seems like
                    # a reasonable compromise.
                    if int(parentInfo["merged"]) == 1:
                        inputLFN = parentInfo["lfn"]
                    else:
                        inputLFN = parentInfo["gplfn"]
                        
                    children = self.getMergedChildrenAction.execute(inputLFN = inputLFN,
                                                                    parentFileset = parentInfo["redneck_parent_fileset"],
                                                                    conn = self.transaction.conn,
                                                                    transaction = True)

                    if len(children) == 0:
                        self.dbsStatusAction.execute([redneckChild], "WaitingForParents",
                                                     conn = self.transaction.conn,
                                                     transaction = True)
                        break
            else:
                # All of the redneck parents for this file have been merged but
                # we still need to verify that the parents themselves are not
                # waiting for their parents to be merged.  This will check the
                # status of all parents to verify that they aren't waiting for
                # parents.
                parentStatus = self.dbsParentStatusAction.execute(lfn = redneckChild,
                                                                  conn = self.transaction.conn,
                                                                  transaction = True)

                if "WaitingForParents" not in parentStatus:
                    children = self.dbsChildrenAction.execute(redneckChild,
                                                              conn = self.transaction.conn,
                                                              transaction = True)
                    for child in children:
                        redneckChildren.add(child)
                    self.dbsStatusAction.execute([redneckChild], "NOTUPLOADED",
                                                 conn = self.transaction.conn, transaction = True)

        return

    def setupDBSFileParentage(self, outputFile):
        """
        _setupDBSFileParentage_

        Setup file parentage inside DBSBuffer, properly handling redneck
        parentage.
        """
        parentsInfo = self.getParentInfoAction.execute([outputFile["lfn"]],
                                                       conn = self.transaction.conn,
                                                       transaction = True)

        newParents = set()
        redneckChildren = set()
        for parentInfo in parentsInfo:
            # This will catch straight to merge files that do not have redneck
            # parents.  We will mark the straight to merge file from the job
            # as a child of the merged parent.
            if int(parentInfo["merged"]) == 1 and parentInfo["redneck_parent_fileset"] == None:
                newParents.add(parentInfo["lfn"])

                # If there are any merged redneck children for this file we need
                # to find them, add this file as their parent and add them to
                # the redneck children list so we can fixup their status later.
                if parentInfo["redneck_child_fileset"] != None:
                    children = self.getMergedChildrenAction.execute(inputLFN = parentInfo["lfn"],
                                                                    parentFileset = parentInfo["redneck_child_fileset"],
                                                                    conn = self.transaction.conn, transaction = True)
                    for child in children:
                        dbsFile = DBSBufferFile(lfn = child)
                        dbsFile.load()
                        dbsFile.addParents([outputFile["lfn"]])
                        redneckChildren.add(child)

            # This will catch redneck children.  We need to discover any merged
            # parents that already exist and add them as parents of the output
            # file.  We also need to add the output file to the list of redneck
            # children so that it's status can be updated.
            elif parentInfo["redneck_parent_fileset"] != None:
                redneckChildren.add(outputFile["lfn"])
                self.dbsStatusAction.execute([outputFile["lfn"]], "WaitingForParents",
                                             conn = self.transaction.conn, transaction = True)                
                children = self.getMergedChildrenAction.execute(inputLFN = parentInfo["gplfn"],
                                                                parentFileset = parentInfo["redneck_parent_fileset"],
                                                                conn = self.transaction.conn, transaction = True)
                for child in children:
                    newParents.add(child)

                # A redneck child can be a redneck parent at the same time.  If
                # that is the case then we need to discover any merged redneck
                # children and setup the parentage information accordingly.
                if parentInfo["redneck_child_fileset"] != None:
                    children = self.getMergedChildrenAction.execute(inputLFN = parentInfo["gplfn"],
                                                                    parentFileset = parentInfo["redneck_child_fileset"],
                                                                    conn = self.transaction.conn, transaction = True)
                    for child in children:
                        dbsFile = DBSBufferFile(lfn = child)
                        dbsFile.load()
                        dbsFile.addParents([outputFile["lfn"]])
                        redneckChildren.add(child)     
            # Handle the files that result from merge jobs that aren't redneck
            # children.  We have to setup parentage and then check on whether or
            # not this file has any redneck children and update their parentage
            # information.
            else:
                if int(parentInfo["gpmerged"]) == 1:
                    newParents.add(parentInfo["gplfn"])
                if parentInfo["redneck_child_fileset"] != None:
                    children = self.getMergedChildrenAction.execute(inputLFN = parentInfo["gplfn"],
                                                                    parentFileset = parentInfo["redneck_child_fileset"],
                                                                    conn = self.transaction.conn, transaction = True)
                    for child in children:
                        dbsFile = DBSBufferFile(lfn = child)
                        dbsFile.load()
                        dbsFile.addParents([outputFile["lfn"]])
                        redneckChildren.add(child)

        if len(newParents) > 0:
            dbsFile = DBSBufferFile(lfn = outputFile["lfn"])
            dbsFile.load()
            dbsFile.addParents(list(newParents))

        self.fixupDBSFileStatus(redneckChildren)
        return

    def addFileToWMBS(self, jobType, fwjrFile, jobMask, inputFiles):
        """
        _addFileToWMBS_

        Add a file that was produced in a job to WMBS.  
        """
        fwjrFile["first_event"] = jobMask["FirstEvent"]
        fwjrFile["last_event"] = jobMask["LastEvent"]

        if fwjrFile["first_event"] == None:
            fwjrFile["first_event"] = 0
        if fwjrFile["last_event"] == None:
            fwjrFile["last_event"] = fwjrFile["events"]

        if jobType == "Merge":
            fwjrFile["merged"] = True

        if type(fwjrFile["locations"]) == set:
            s = fwjrFile["locations"].copy()
            seName = s.pop()
        elif type(fwjrFile["locations"]) == list:
            seName = fwjrFile["locations"][0]
        else:
            seName = fwjrFile["locations"]

        wmbsFile = File()
        wmbsFile.loadFromDataStructsFile(file = fwjrFile)

        for inputFile in inputFiles:
            if inputFile["lfn"] not in wmbsFile.getParentLFNs():
                try:
                    wmbsFile.addParent(inputFile["lfn"])
                except Exception, ex:
                    msg = str(ex)
                    logging.error("Exception in adding parents\n%s" %(msg))
                    return None, None, None

        if fwjrFile["merged"]:
            self.addFileToDBS(fwjrFile)

        return (wmbsFile, fwjrFile["ModuleLabel"], fwjrFile["merged"])

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

        outputMap = self.getOutputMapAction.execute(jobID = jobID,
                                                    conn = self.transaction.conn,
                                                    transaction = True)

        wmbsJobGroup = JobGroup(id = wmbsJob["jobgroup"])
        wmbsJobGroup.load()

        jobType = self.getJobTypeAction.execute(jobID = jobID,
                                                conn = self.transaction.conn,
                                                transaction = True)

        filesetAssoc = []
        outputFiles = {}
        mergedOutputFiles = []
        fileList = fwkJobReport.getAllFiles()
        if not fileList:
            # Well, then we failed somewhere in getting the files
            # Ergo: the job should fail
            self.transaction.rollback()
            self.transaction.begin()
            self.handleFailed(jobID = jobID, fwkJobReport = fwkJobReport)
            return

        for fwjrFile in fileList:
            (wmbsFile, moduleLabel, merged) = \
                     self.addFileToWMBS(jobType, fwjrFile, wmbsJob["mask"], jobFiles)
            if not wmbsFile and not moduleLabel:
                # Something got screwed up in addFileToWMBS.  Send job to FAIL
                self.transaction.rollback()
                self.transaction.begin()
                self.handleFailed(jobID = jobID, fwkJobReport = fwkJobReport)
                return
            outputFiles[moduleLabel] = wmbsFile

            if merged:
                mergedOutputFiles.append(wmbsFile)

            filesetAssoc.append({"fileid": wmbsFile["id"], "fileset": wmbsJobGroup.output.id})
            outputFileset = self.outputFilesetsForJob(outputMap, wmbsFile["merged"], moduleLabel)
            if outputFileset != None:
                filesetAssoc.append({"fileid": wmbsFile["id"], "fileset": outputFileset})


        if len(filesetAssoc) > 0:
            self.bulkAddToFilesetAction.execute(binds = filesetAssoc,
                                                conn = self.transaction.conn,
                                                transaction = True)

        wmbsJob.completeInputFiles()
        self.stateChanger.propagate([wmbsJob], "success", "complete")

        for mergedOutputFile in mergedOutputFiles:
            self.setupDBSFileParentage(mergedOutputFile)

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
