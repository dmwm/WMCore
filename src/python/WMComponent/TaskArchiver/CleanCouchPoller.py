"""
Perform cleanup actions
"""
from __future__ import division
from builtins import range
from future.utils import viewitems

from future import standard_library
standard_library.install_aliases()

import http.client
import json
import logging
import os.path
import re
import shutil
import threading
import time
import urllib.request
from contextlib import closing
from Utils.Timers import timeFunction
from WMComponent.JobCreator.CreateWorkArea import getMasterName
from WMComponent.JobCreator.JobCreatorPoller import retrieveWMSpec
from WMComponent.TaskArchiver.DataCache import DataCache
from WMCore.Algorithms import MathAlgos
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.LumiList import LumiList
from WMCore.DataStructs.MathStructs.DiscreteSummaryHistogram import DiscreteSummaryHistogram
from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Lexicon import sanitizeURL
from WMCore.Services.FWJRDB.FWJRDBAPI import FWJRDBAPI
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMException import WMException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class CleanCouchPollerException(WMException):
    """
    _CleanCouchPollerException_

    Customized exception for the CleanCouchPoller
    """


class CleanCouchPoller(BaseWorkerThread):
    """
    Cleans up local couch db according the the given condition.
    1. Cleans local couch db when request is completed and reported to cental db.
       This will clean up local couchdb, local summary db, local queue

    2. Cleans old couchdoc which is created older than the time threshold

    """

    def __init__(self, config):
        """
        Initialize config
        """
        BaseWorkerThread.__init__(self)
        # set the workqueue service for REST call
        self.config = config

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.dbsDaoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                        logger=myThread.logger,
                                        dbinterface=myThread.dbi)

        self.config = config
        self.jobCacheDir = self.config.JobCreator.jobCacheDir

        self.maxProcessSize = getattr(self.config.TaskArchiver, 'maxProcessSize', 250)
        self.timeout = getattr(self.config.TaskArchiver, "timeOut", None)
        self.nOffenders = getattr(self.config.TaskArchiver, 'nOffenders', 3)

        # Set up optional histograms
        self.histogramKeys = getattr(self.config.TaskArchiver, "histogramKeys", [])
        self.histogramBins = getattr(self.config.TaskArchiver, "histogramBins", 10)
        self.histogramLimit = getattr(self.config.TaskArchiver, "histogramLimit", 5.0)

        # Set defaults for reco performance reporting
        self.interestingPDs = getattr(config.TaskArchiver, "perfPrimaryDatasets", ['SingleMu', 'MuHad'])
        self.dqmUrl = getattr(config.TaskArchiver, "dqmUrl", 'https://cmsweb.cern.ch/dqm/dev/')
        self.perfDashBoardMinLumi = getattr(config.TaskArchiver, "perfDashBoardMinLumi", 50)
        self.perfDashBoardMaxLumi = getattr(config.TaskArchiver, "perfDashBoardMaxLumi", 9000)
        self.dashBoardUrl = getattr(config.TaskArchiver, "dashBoardUrl", None)
        self.DataKeepDays = getattr(config.TaskArchiver, "DataKeepDays", 0.125)  # 3 hours

        # Initialise with None all setup defined variables:
        self.teamName = None
        self.useReqMgrForCompletionCheck = None
        self.archiveDelayHours = None
        self.wmstatsCouchDB = None
        self.centralRequestDBReader = None
        self.centralRequestDBWriter = None
        self.deletableState = None
        self.reqmgr2Svc = None
        self.jobCouchdb = None
        self.jobsdatabase = None
        self.fwjrdatabase = None
        self.fwjrService = None
        self.workCouchdb = None
        self.workdatabase = None
        self.statsumdatabase = None

    def setup(self, parameters=None):
        """
        Called at startup
        """
        self.teamName = self.config.Agent.teamName
        # set the connection for local couchDB call
        self.useReqMgrForCompletionCheck = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)
        self.archiveDelayHours = getattr(self.config.TaskArchiver, 'archiveDelayHours', 0)
        self.wmstatsCouchDB = WMStatsWriter(self.config.TaskArchiver.localWMStatsURL, appName="WMStatsAgent")

        # TODO: we might need to use local db for Tier0
        self.centralRequestDBReader = RequestDBReader(self.config.AnalyticsDataCollector.centralRequestDBURL,
                                                      couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)

        if self.useReqMgrForCompletionCheck:
            self.deletableState = "announced"
            self.centralRequestDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.centralRequestDBURL,
                                                          couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)
            self.reqmgr2Svc = ReqMgr(self.config.General.ReqMgr2ServiceURL)
        else:
            # Tier0 case
            self.deletableState = "completed"
            # use local for update
            self.centralRequestDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.localT0RequestDBURL,
                                                          couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)

        jobDBName = self.config.JobStateMachine.couchDBName
        self.jobCouchdb = CouchServer(self.config.JobStateMachine.couchurl)
        self.jobsdatabase = self.jobCouchdb.connectDatabase("%s/jobs" % jobDBName)
        self.fwjrdatabase = self.jobCouchdb.connectDatabase("%s/fwjrs" % jobDBName)
        self.fwjrService = FWJRDBAPI(self.fwjrdatabase)

        workDBName = getattr(self.config.TaskArchiver, 'workloadSummaryCouchDBName',
                             'workloadsummary')
        workDBurl = getattr(self.config.TaskArchiver, 'workloadSummaryCouchURL')
        self.workCouchdb = CouchServer(workDBurl)
        self.workdatabase = self.workCouchdb.connectDatabase(workDBName)

        statSummaryDBName = self.config.JobStateMachine.summaryStatsDBName
        self.statsumdatabase = self.jobCouchdb.connectDatabase(statSummaryDBName)

        logging.debug("Using url %s/%s for job",
                      sanitizeURL(self.config.JobStateMachine.couchurl)['url'], jobDBName)
        logging.debug("Writing to  %s/%s for workloadSummary", sanitizeURL(workDBurl)['url'], workDBName)

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Get information from wmbs, workqueue and local couch and:
          - It deletes old wmstats docs
          - deletes all JobCouch data for archived workflows
          - creates the workload summary for completed workflows
          - Archive workflows
        """
        try:
            logging.info("Cleaning up old request docs from local wmstats")
            report = self.wmstatsCouchDB.deleteOldDocs(self.DataKeepDays)
            logging.info("%s docs deleted", report)
        except Exception as e:
            msg = "Local wmstats clean up failed with error: %s" % str(e)
            logging.exception(msg)

        try:
            logging.info("Cleaning up all local couch data for archived requests")
            report = self.cleanAlreadyArchivedWorkflows()
            logging.info("%s archived workflows deleted", report)
        except Exception as e:
            msg = "Local couch clean up for archived requests failed with error: %s" % str(e)
            logging.exception(msg)

        try:
            logging.info("Creating workload summary")
            finishedwfsWithLogCollectAndCleanUp = DataCache.getFinishedWorkflows()
            logging.info("Total %s workload summary will be created", len(finishedwfsWithLogCollectAndCleanUp))
            self.archiveSummaryAndPublishToDashBoard(finishedwfsWithLogCollectAndCleanUp)
            logging.info("All workload summary docs were uploaded")

            logging.info("Cleaning up couch db")
            self.cleanCouchDBAndChangeToArchiveStatus()
            logging.info("Done: cleaning up couch db")
        except Exception as e:
            msg = "Error creating workload and/or cleaning up couch: %s" % str(e)
            logging.exception(msg)

        try:
            logging.info("Cleaning up wmbs and disk")
            self.deleteWorkflowFromWMBSAndDisk()
            logging.info("Done: cleaning up wmsbs and disk")
        except Exception as ex:
            msg = "Error cleaning up wmbs and disk: %s" % str(ex)
            logging.exception(msg)

    def archiveWorkflows(self, workflows, archiveState):
        updated = 0
        for workflowName in workflows:
            if not self.isUploadedToWMArchive(workflowName):
                continue
            if self.cleanAllLocalCouchDB(workflowName):
                if not self.useReqMgrForCompletionCheck:
                    #  only update tier0 case, for Prodcuction/Processing reqmgr will update status
                    self.centralRequestDBWriter.updateRequestStatus(workflowName, archiveState)
                updated += 1
        return updated

    def archiveSummaryAndPublishToDashBoard(self, finishedwfsWithLogCollectAndCleanUp):
        """
        _archiveSummaryAndPublishToDashBoard_

        This method will call several auxiliary methods to do the following:
        1. Archive workloadsumary when all the subscription including CleanUp and LogCollect tasks are finished.
        2. Pulblish to Dashboard
        3. TODO: update LogCollect and Cleanup status in central couchdb
        """
        # Upload summary to couch
        for workflow in finishedwfsWithLogCollectAndCleanUp:
            spec = retrieveWMSpec(wmWorkloadURL=finishedwfsWithLogCollectAndCleanUp[workflow]["spec"])
            if spec:
                self.archiveWorkflowSummary(spec=spec)
                # Send Reconstruciton performance information to DashBoard
                if self.dashBoardUrl is not None:
                    self.publishRecoPerfToDashBoard(spec)
            else:
                logging.warning("Workflow spec was not found for %s", workflow)

        return

    def cleanCouchDBAndChangeToArchiveStatus(self):
        # archiving only workflows that I own (same team)
        logging.info("Getting requests in '%s' state for team '%s'", self.deletableState,
                     self.teamName)
        endTime = int(time.time()) - self.archiveDelayHours * 3600

        if self.useReqMgrForCompletionCheck:
            wfs = self.centralRequestDBReader.getRequestByTeamAndStatus(self.teamName,
                                                                        self.deletableState)
        else:
            # TO doesn't store team name in the requset document since there is only one team.
            wfs = self.centralRequestDBReader.getRequestByStatus(self.deletableState)

        commonWfs = self.centralRequestDBReader.getRequestByStatusAndEndTime(self.deletableState,
                                                                             False, endTime)

        finishedWfs = set(DataCache.getFinishedWorkflows().keys())
        deletableWorkflows = list(set(wfs) & set(commonWfs) & finishedWfs)
        logging.info("Ready to archive normal %s workflows", len(deletableWorkflows))
        numUpdated = self.archiveWorkflows(deletableWorkflows, "normal-archived")
        logging.info("archive normal %s workflows", numUpdated)

        abortedWorkflows = self.centralRequestDBReader.getRequestByStatus(["aborted-completed"])
        abortedWorkflows = list(set(abortedWorkflows) & finishedWfs)
        logging.info("Ready to archive aborted %s workflows", len(abortedWorkflows))
        numUpdated = self.archiveWorkflows(abortedWorkflows, "aborted-archived")
        logging.info("archive aborted %s workflows", numUpdated)

        rejectedWorkflows = self.centralRequestDBReader.getRequestByStatus(["rejected"])
        rejectedWorkflows = list(set(rejectedWorkflows) & finishedWfs)
        logging.info("Ready to archive rejected %s workflows", len(rejectedWorkflows))
        numUpdated = self.archiveWorkflows(rejectedWorkflows, "rejected-archived")
        logging.info("archive rejected %s workflows", numUpdated)

    def deleteWorkflowFromJobCouch(self, workflowName, db):
        """
        _deleteWorkflowFromCouch_

        If we are asked to delete the workflow from couch, delete it
        to clear up some space.

        Load the document IDs and revisions out of couch by workflowName,
        then order a delete on them.
        """
        options = {"startkey": [workflowName], "endkey": [workflowName, {}], "reduce": False}

        if db == "JobDump":
            couchDB = self.jobsdatabase
            view = "jobsByWorkflowName"
        elif db == "FWJRDump":
            couchDB = self.fwjrdatabase
            view = "fwjrsByWorkflowName"
        elif db == "SummaryStats":
            couchDB = self.statsumdatabase
            view = None
        elif db == "WMStatsAgent":
            couchDB = self.wmstatsCouchDB.getDBInstance()
            view = "allWorkflows"
            options = {"key": workflowName, "reduce": False}

        if view is None:
            try:
                committed = couchDB.delete_doc(workflowName)
            except CouchNotFoundError as ex:
                return {'status': 'warning', 'message': "%s: %s" % (workflowName, str(ex))}
        else:
            try:
                jobs = couchDB.loadView(db, view, options=options)['rows']
            except Exception as ex:
                errorMsg = "Error on loading jobs for %s" % workflowName
                logging.warning("%s/n%s", str(ex), errorMsg)
                return {'status': 'error', 'message': errorMsg}

            for j in jobs:
                doc = {}
                doc["_id"] = j['value']['id']
                doc["_rev"] = j['value']['rev']
                couchDB.queueDelete(doc)
            committed = couchDB.commit()

        if committed:
            # create the error report
            errorReport = {}
            deleted = 0
            status = "ok"
            for data in committed:
                if 'error' in data:
                    errorReport.setdefault(data['error'], 0)
                    errorReport[data['error']] += 1
                    status = "error"
                else:
                    deleted += 1
            return {'status': status, 'delete': deleted, 'message': errorReport}
        else:
            return {'status': 'warning', 'message': "no %s exist" % workflowName}

    def cleanAllLocalCouchDB(self, workflowName):
        logging.info("Deleting %s from JobCouch", workflowName)

        jobReport = self.deleteWorkflowFromJobCouch(workflowName, "JobDump")
        logging.debug("%s docs deleted from JobDump", jobReport)

        fwjrReport = self.deleteWorkflowFromJobCouch(workflowName, "FWJRDump")
        logging.debug("%s docs deleted from FWJRDump", fwjrReport)

        summaryReport = self.deleteWorkflowFromJobCouch(workflowName, "SummaryStats")
        logging.debug("%s docs deleted from SummaryStats", summaryReport)

        wmstatsReport = self.deleteWorkflowFromJobCouch(workflowName, "WMStatsAgent")
        logging.debug("%s docs deleted from wmagent_summary", wmstatsReport)

        # if one of the procedure fails return False
        if jobReport["status"] == "error" or fwjrReport["status"] == "error" or wmstatsReport["status"] == "error":
            return False
        # other wise return True.
        return True

    def isUploadedToWMArchive(self, workflowName):

        if hasattr(self.config, "ArchiveDataReporter") and self.config.ArchiveDataReporter.WMArchiveURL:
            try:
                return self.fwjrService.isAllFWJRArchived(workflowName)
            except Exception:
                logging.error("Fail to check FWJR upload status: %s", workflowName)
                return False
        return True

    def cleanAlreadyArchivedWorkflows(self):
        """
        loop through the workflows in couchdb, if archived delete all the data in couchdb
        """

        numDeletedRequests = 0
        try:
            localWMStats = self.wmstatsCouchDB.getDBInstance()
            options = {"group_level": 1, "reduce": True}

            results = localWMStats.loadView("WMStatsAgent", "allWorkflows", options=options)['rows']
            requestNames = [x['key'] for x in results]
            logging.info("There are %s workflows to check for archived status", len(requestNames))

            workflowDict = self.centralRequestDBReader.getStatusAndTypeByRequest(requestNames)

            for request, value in viewitems(workflowDict):
                if value[0].endswith("-archived"):
                    self.cleanAllLocalCouchDB(request)
                    numDeletedRequests += 1

        except Exception as ex:
            errorMsg = "Error on loading workflow list from wmagent_summary db"
            logging.warning("%s/n%s", errorMsg, str(ex))

        return numDeletedRequests

    def deleteWorkflowFromWMBSAndDisk(self):
        # Get the finished workflows, in descending order
        deletableWorkflowsDAO = self.daoFactory(classname="Workflow.GetDeletableWorkflows")
        deletablewfs = deletableWorkflowsDAO.execute()

        # For T0 subtract the workflows which are not having all their blocks deleted yet:
        if not self.useReqMgrForCompletionCheck:
            undeletedBlocksByWorkflowDAO = self.dbsDaoFactory(classname="CountUndeletedBlocksByWorkflow")
            wfsWithUndeletedBlocks = [record['name'] for record in undeletedBlocksByWorkflowDAO.execute()]
            for workflow in list(deletablewfs):
                if workflow in wfsWithUndeletedBlocks:
                    msg = "Removing workflow: %s from the list of deletable workflows. It still has blocks NOT deleted."
                    self.logger.info(msg, workflow)
                    deletablewfs.pop(workflow)

        # Only delete those where the upload and notification succeeded
        logging.info("Found %d candidate workflows for deletion.", len(deletablewfs))
        # update the completed flag in dbsbuffer_workflow table so blocks can be closed
        # create updateDBSBufferWorkflowComplete DAO
        if len(deletablewfs) == 0:
            return
        safeStatesToDelete = ["normal-archived", "aborted-archived", "rejected-archived"]
        wfsToDelete = {}
        for workflow in deletablewfs:
            try:
                spec = retrieveWMSpec(wmWorkloadURL=deletablewfs[workflow]["spec"])

                # This is used both tier0 and normal agent case
                result = self.centralRequestDBWriter.getStatusAndTypeByRequest(workflow)
                wfStatus = result[workflow][0]
                if wfStatus in safeStatesToDelete:
                    wfsToDelete[workflow] = {"spec": spec, "workflows": deletablewfs[workflow]["workflows"]}
                else:
                    logging.debug("%s is in %s, will be deleted later", workflow, wfStatus)

            except Exception as ex:
                # Something didn't go well on couch, abort!!!
                msg = "Couldn't delete %s\nException message: %s" % (workflow, str(ex))
                logging.exception(msg)

        logging.info("Time to kill %d workflows.", len(wfsToDelete))
        self.killWorkflows(wfsToDelete)

    def killWorkflows(self, workflows):
        """
        _killWorkflows_

        Delete all the information in couch and WMBS about the given
        workflow, go through all subscriptions and delete one by
        one.
        The input is a dictionary with workflow names as keys, fully loaded WMWorkloads and
        subscriptions lists as values
        """
        logging.info("Deleting %s workflows by subscription (from disk)", len(workflows))
        for workflow in workflows:
            logging.info("Deleting workflow %s", workflow)
            try:
                # Get the task-workflow ids, sort them by ID,
                # higher ID first so we kill
                # the leaves of the tree first, root last
                workflowsIDs = list(workflows[workflow]["workflows"])
                workflowsIDs.sort(reverse=True)

                # Now go through all tasks and load the WMBS workflow objects
                wmbsWorkflows = []
                for wfID in workflowsIDs:
                    wmbsWorkflow = Workflow(id=wfID)
                    wmbsWorkflow.load()
                    wmbsWorkflows.append(wmbsWorkflow)

                # Time to shoot one by one
                for wmbsWorkflow in wmbsWorkflows:
                    # Load all the associated subscriptions and shoot them one by one
                    subIDs = workflows[workflow]["workflows"][wmbsWorkflow.id]
                    for subID in subIDs:
                        subscription = Subscription(id=subID)
                        subscription['workflow'] = wmbsWorkflow
                        subscription.load()
                        subscription.deleteEverything()

                    # Check that the workflow is gone
                    if wmbsWorkflow.exists():
                        # Something went bad, this workflow
                        # should be gone by now
                        msg = "Workflow %s, Task %s was not deleted completely" % (wmbsWorkflow.name,
                                                                                   wmbsWorkflow.task)
                        raise CleanCouchPollerException(msg)

                    # Now delete directories
                    _, taskDir = getMasterName(startDir=self.jobCacheDir,
                                               workflow=wmbsWorkflow)
                    logging.debug("About to delete work directory %s", taskDir)
                    if os.path.exists(taskDir):
                        if os.path.isdir(taskDir):
                            shutil.rmtree(taskDir)
                        else:
                            # What we think of as a working directory is not a directory
                            # This should never happen and there is no way we can recover
                            # from this here. Bail out now and have someone look at things.
                            msg = "Work directory is not a directory, this should never happen: %s" % taskDir
                            raise CleanCouchPollerException(msg)
                    else:
                        msg = "Attempted to delete work directory but it was already gone: %s" % taskDir
                        logging.debug(msg)

                if workflows[workflow]["spec"] is None:
                    logging.warning("Workflow spec not found for %s", workflow)
                    continue

                spec = workflows[workflow]["spec"]
                topTask = spec.getTopLevelTask()[0]

                # Now take care of the sandbox
                sandbox = getattr(topTask.data.input, 'sandbox', None)
                if sandbox:
                    sandboxDir = os.path.dirname(sandbox)
                    if os.path.isdir(sandboxDir):
                        shutil.rmtree(sandboxDir)
                        logging.debug("Sandbox dir deleted")
                    else:
                        logging.error("Attempted to delete sandbox dir but it was already gone: %s", sandboxDir)

            except Exception as ex:
                msg = "Critical error while deleting workflow %s\nError: %s" % (workflow, str(ex))
                logging.exception(msg)

    def archiveWorkflowSummary(self, spec):
        """
        _archiveWorkflowSummary_

        For each workflow pull its information from couch and WMBS and turn it into
        a summary for archiving
        """

        failedJobs = []

        workflowData = {'retryData': {}}
        workflowName = spec.name()

        # First make sure that we didn't upload something already
        # Could be the that the WMBS deletion epic failed,
        # so we can skip this if there is a summary already up there
        # TODO: With multiple agents sharing workflows, we will need to differentiate and combine summaries for a request
        if self.workdatabase.documentExists(workflowName):
            logging.debug("Workload summary for %s already exists, proceeding only with cleanup", workflowName)
            return

        # Set campaign
        workflowData['campaign'] = spec.getCampaign()
        # Set inputdataset
        workflowData['inputdatasets'] = spec.listInputDatasets()
        # Set histograms
        histograms = {'workflowLevel': {'failuresBySite': DiscreteSummaryHistogram('Failed jobs by site', 'Site')},
                      'taskLevel': {},
                      'stepLevel': {}}

        # Get a list of failed job IDs
        # Make sure you get it for ALL tasks in the spec
        for taskName in spec.listAllTaskPathNames():
            failedTmp = self.jobsdatabase.loadView("JobDump", "failedJobsByWorkflowName",
                                                   options={"startkey": [workflowName, taskName],
                                                            "endkey": [workflowName, taskName],
                                                            "stale": "update_after"})['rows']
            for entry in failedTmp:
                failedJobs.append(entry['value'])

        retryData = self.jobsdatabase.loadView("JobDump", "retriesByTask",
                                               options={'group_level': 3,
                                                        'startkey': [workflowName],
                                                        'endkey': [workflowName, {}],
                                                        "stale": "update_after"})['rows']
        for row in retryData:
            taskName = row['key'][2]
            count = str(row['key'][1])
            if taskName not in workflowData['retryData']:
                workflowData['retryData'][taskName] = {}
            workflowData['retryData'][taskName][count] = row['value']

        output = self.fwjrdatabase.loadView("FWJRDump", "outputByWorkflowName",
                                            options={"group_level": 2,
                                                     "startkey": [workflowName],
                                                     "endkey": [workflowName, {}],
                                                     "group": True,
                                                     "stale": "update_after"})['rows']
        outputList = {}
        try:
            outputListStr = self.fwjrdatabase.loadList("FWJRDump", "workflowOutputTaskMapping",
                                                       "outputByWorkflowName", options={"startkey": [workflowName],
                                                                                        "endkey": [workflowName, {}],
                                                                                        "reduce": False})
            outputList = json.loads(outputListStr)
        except Exception as ex:
            # Catch couch errors
            logging.error("Could not load the output task mapping list due to an error")
            logging.error("Error: %s", str(ex))
        perf = self.handleCouchPerformance(workflowName=workflowName)
        workflowData['performance'] = {}
        for key in perf:
            workflowData['performance'][key] = {}
            for attr in perf[key]:
                workflowData['performance'][key][attr] = perf[key][attr]

        workflowData["_id"] = workflowName
        try:
            workflowData["ACDCServer"] = sanitizeURL(self.config.ACDC.couchurl)['url']
            workflowData["ACDCDatabase"] = self.config.ACDC.database
        except AttributeError as ex:
            # We're missing the ACDC info.
            # Keep going
            logging.error("ACDC info missing from config.  Skipping this step in the workflow summary.")
            logging.error("Error: %s", str(ex))

        # Attach output
        workflowData['output'] = {}
        for e in output:
            entry = e['value']
            dataset = entry['dataset']
            workflowData['output'][dataset] = {}
            workflowData['output'][dataset]['nFiles'] = entry['count']
            workflowData['output'][dataset]['size'] = entry['size']
            workflowData['output'][dataset]['events'] = entry['events']
            workflowData['output'][dataset]['tasks'] = list(outputList.get(dataset, {}))

        # If the workflow was aborted, then don't parse all the jobs, cut at 5k
        try:
            reqDetails = self.centralRequestDBWriter.getRequestByNames(workflowName)
            wfStatus = reqDetails[workflowName]['RequestTransition'][-1]['Status']
            if wfStatus in ["aborted", "aborted-completed", "aborted-archived"]:
                logging.info("Workflow %s in status %s with a total of %d jobs, capping at 5000", workflowName,
                             wfStatus, len(failedJobs))
                failedJobs = failedJobs[:5000]
        except Exception as ex:
            logging.error("Failed to query getRequestByNames view. Will retry later.\n%s", str(ex))

        # Loop over all failed jobs
        workflowData['errors'] = {}

        # Get the job information from WMBS, a la ErrorHandler
        # This will probably take some time, better warn first
        logging.info("Starting to load  the failed job information")
        logging.info("This may take some time")

        # Let's split the list of failed jobs in chunks
        while len(failedJobs) > 0:
            chunkList = failedJobs[:self.maxProcessSize]
            failedJobs = failedJobs[self.maxProcessSize:]
            logging.info("Processing %d this cycle, %d jobs remaining", self.maxProcessSize, len(failedJobs))

            loadJobs = self.daoFactory(classname="Jobs.LoadForTaskArchiver")
            jobList = loadJobs.execute(chunkList)
            logging.info("Processing %d jobs,", len(jobList))
            for job in jobList:
                lastRegisteredRetry = None
                errorCouch = self.fwjrdatabase.loadView("FWJRDump", "errorsByJobID",
                                                        options={"startkey": [job['id'], 0],
                                                                 "endkey": [job['id'], {}],
                                                                 "stale": "update_after"})['rows']

                # Get the input files
                inputLFNs = [x['lfn'] for x in job['input_files']]
                runs = []
                for inputFile in job['input_files']:
                    runs.extend(inputFile.getRuns())

                # Get rid of runs that aren't in the mask
                mask = job['mask']
                runs = mask.filterRunLumisByMask(runs=runs)

                logging.info("Processing %d errors, for job id %s", len(errorCouch), job['id'])
                for err in errorCouch:
                    task = err['value']['task']
                    step = err['value']['step']
                    errors = err['value']['error']
                    logs = err['value']['logs']
                    start = err['value']['start']
                    stop = err['value']['stop']
                    errorSite = str(err['value']['site'])
                    retry = err['value']['retry']
                    if lastRegisteredRetry is None or lastRegisteredRetry != retry:
                        histograms['workflowLevel']['failuresBySite'].addPoint(errorSite, 'Failed Jobs')
                        lastRegisteredRetry = retry
                    if task not in histograms['stepLevel']:
                        histograms['stepLevel'][task] = {}
                    if step not in histograms['stepLevel'][task]:
                        histograms['stepLevel'][task][step] = {
                            'errorsBySite': DiscreteSummaryHistogram('Errors by site',
                                                                     'Site')}
                    errorsBySiteData = histograms['stepLevel'][task][step]['errorsBySite']
                    if task not in workflowData['errors']:
                        workflowData['errors'][task] = {'failureTime': 0}
                    if step not in workflowData['errors'][task]:
                        workflowData['errors'][task][step] = {}
                    workflowData['errors'][task]['failureTime'] += (stop - start)
                    stepFailures = workflowData['errors'][task][step]
                    for error in errors:
                        exitCode = str(error['exitCode'])
                        if exitCode not in stepFailures:
                            stepFailures[exitCode] = {"errors": [],
                                                      "jobs": 0,
                                                      "input": [],
                                                      "runs": {},
                                                      "logs": []}
                        stepFailures[exitCode]['jobs'] += 1  # Increment job counter
                        errorsBySiteData.addPoint(errorSite, str(exitCode))
                        if len(stepFailures[exitCode]['errors']) == 0 or exitCode == '99999':
                            # Only record the first error for an exit code
                            # unless exit code is 99999 (general panic)
                            stepFailures[exitCode]['errors'].append(error)
                        # Add input LFNs to structure
                        for inputLFN in inputLFNs:
                            if inputLFN not in stepFailures[exitCode]['input']:
                                stepFailures[exitCode]['input'].append(inputLFN)
                        # Add runs to structure
                        for run in runs:
                            if str(run.run) not in stepFailures[exitCode]['runs']:
                                stepFailures[exitCode]['runs'][str(run.run)] = []
                            logging.debug("number of lumis failed: %s", len(run.lumis))
                            nodupLumis = set(run.lumis)
                            for l in nodupLumis:
                                stepFailures[exitCode]['runs'][str(run.run)].append(l)
                        for log in logs:
                            if log not in stepFailures[exitCode]["logs"]:
                                stepFailures[exitCode]["logs"].append(log)
        # Now convert run/lumis into a compact list, to avoid monstrous lists
        # e.g. {"1": [3, 6, 9, 2, 7, 1, 19]} becomes {'1': [[1, 3], [6, 7], [9, 9], [19, 19]]}
        for task in workflowData['errors']:
            for step in workflowData['errors'][task]:
                stepFailures = workflowData['errors'][task][step]
                if not isinstance(stepFailures, dict):
                    continue
                for exitCode in stepFailures:
                    if stepFailures[exitCode]['runs']:
                        runLumiObj = LumiList(runsAndLumis=stepFailures[exitCode]['runs'])
                        stepFailures[exitCode]['runs'] = runLumiObj.getCompactList()

        # Adding logArchives per task
        logArchives = self.getLogArchives(spec)
        workflowData['logArchives'] = logArchives

        jsonHistograms = {'workflowLevel': {},
                          'taskLevel': {},
                          'stepLevel': {}}
        for histogram in histograms['workflowLevel']:
            jsonHistograms['workflowLevel'][histogram] = histograms['workflowLevel'][histogram].toJSON()
        for task in histograms['taskLevel']:
            jsonHistograms['taskLevel'][task] = {}
            for histogram in histograms['taskLevel'][task]:
                jsonHistograms['taskLevel'][task][histogram] = histograms['taskLevel'][task][histogram].toJSON()
        for task in histograms['stepLevel']:
            jsonHistograms['stepLevel'][task] = {}
            for step in histograms['stepLevel'][task]:
                jsonHistograms['stepLevel'][task][step] = {}
                for histogram in histograms['stepLevel'][task][step]:
                    jsonHistograms['stepLevel'][task][step][histogram] = histograms['stepLevel'][task][step][
                        histogram].toJSON()

        workflowData['histograms'] = jsonHistograms

        # No easy way to get the memory footprint of a python object.
        summarySize = len(json.dumps(workflowData)) / 1024
        if summarySize > 6 * 1024:  # 6MB
            msg = "Workload summary for %s is too big: %d Kb. " % (workflowName, summarySize)
            msg += "Wiping out the 'errors' section to make it smaller."
            logging.warning(msg)
            workflowData['errors'] = {}
        summarySize = len(json.dumps(workflowData)) / 1024

        # Now we have the workflowData in the right format, time to push it
        logging.info("About to commit %d Kb of data for workflow summary for %s", summarySize, workflowName)
        retval = self.workdatabase.commitOne(workflowData)
        logging.info("Finished committing summary,returned value: %s", retval)

        return

    def getLogArchives(self, spec):
        """
        _getLogArchives_

        Gets per Workflow/Task what are the log archives, sends it to the summary to be displayed on the page
        """
        try:
            logArchivesTaskStr = self.fwjrdatabase.loadList("FWJRDump", "logCollectsByTask",
                                                            "logArchivePerWorkflowTask", options={"reduce": False},
                                                            keys=spec.listAllTaskPathNames())
            logArchivesTask = json.loads(logArchivesTaskStr)
            return logArchivesTask
        except Exception as ex:
            logging.error("Couldn't load the logCollect list from CouchDB.")
            logging.error("Error: %s", str(ex))
            return {}

    def handleCouchPerformance(self, workflowName):
        """
        _handleCouchPerformance_

        The couch performance stuff is convoluted enough I think I want to handle it separately.
        """
        perf = self.fwjrdatabase.loadView("FWJRDump", "performanceByWorkflowName",
                                          options={"startkey": [workflowName],
                                                   "endkey": [workflowName],
                                                   "stale": "update_after"})['rows']

        failedJobs = self.getFailedJobs(workflowName)

        taskList = {}
        finalTask = {}

        for row in perf:
            taskName = row['value']['taskName']
            stepName = row['value']['stepName']
            if taskName not in taskList:
                taskList[taskName] = {}
            if stepName not in taskList[taskName]:
                taskList[taskName][stepName] = []
            value = row['value']
            taskList[taskName][stepName].append(value)

        for taskName in taskList:
            final = {}
            for stepName in taskList[taskName]:
                output = {'jobTime': []}
                outputFailed = {'jobTime': []}  # This will be same, but only for failed jobs
                final[stepName] = {}
                masterList = []

                # For each step put the data into a dictionary called output
                # keyed by the name of the value
                for row in taskList[taskName][stepName]:
                    masterList.append(row)
                    for key in row:
                        if key in ['startTime', 'stopTime', 'taskName', 'stepName', 'jobID']:
                            continue
                        if key not in output:
                            output[key] = []
                            if len(failedJobs) > 0:
                                outputFailed[key] = []
                        try:
                            output[key].append(float(row[key]))
                            if row['jobID'] in failedJobs:
                                outputFailed[key].append(float(row[key]))

                        except TypeError:
                            # Why do we get None values here?
                            # We may want to look into it
                            logging.debug("Got a None performance value for key %s", key)
                            if row[key] is None:
                                output[key].append(0.0)
                            else:
                                raise
                    try:
                        jobTime = row.get('stopTime', None) - row.get('startTime', None)
                        output['jobTime'].append(jobTime)
                        row['jobTime'] = jobTime
                        # Account job running time here only if the job has failed
                        if row['jobID'] in failedJobs:
                            outputFailed['jobTime'].append(jobTime)
                    except TypeError:
                        # One of those didn't have a real value
                        pass

                # Now that we've sorted the data, we process it one key at a time
                for key in output:
                    final[stepName][key] = {}
                    # Assemble the 'worstOffenders'
                    # These are the top [self.nOffenders] in that particular category
                    # i.e., those with the highest values
                    offenders = MathAlgos.getLargestValues(dictList=masterList, key=key,
                                                           n=self.nOffenders)
                    for x in offenders:
                        try:
                            logArchive = self.fwjrdatabase.loadView("FWJRDump", "logArchivesByJobID",
                                                                    options={"startkey": [x['jobID']],
                                                                             "endkey": [x['jobID'],
                                                                                        x['retry_count']],
                                                                             "stale": "update_after"})
                            logArchive = logArchive['rows'][0]['value']['lfn']
                            logCollectID = self.jobsdatabase.loadView("JobDump", "jobsByInputLFN",
                                                                      options={"startkey": [workflowName, logArchive],
                                                                               "endkey": [workflowName, logArchive],
                                                                               "stale": "update_after"})
                            logCollectID = logCollectID['rows'][0]['value']
                            logCollect = self.fwjrdatabase.loadView("FWJRDump", "outputByJobID",
                                                                    options={"startkey": logCollectID,
                                                                             "endkey": logCollectID,
                                                                             "stale": "update_after"})
                            logCollect = logCollect['rows'][0]['value']['lfn']
                            x['logArchive'] = logArchive.split('/')[-1]
                            x['logCollect'] = logCollect
                        except IndexError as ex:
                            logging.debug("Unable to find final logArchive tarball for %i", x['jobID'])
                            logging.debug(str(ex))
                        except KeyError as ex:
                            logging.debug("Unable to find final logArchive tarball for %i", x['jobID'])
                            logging.debug(str(ex))

                    if key in self.histogramKeys:
                        # Usual histogram that was always done
                        histogram = MathAlgos.createHistogram(numList=output[key],
                                                              nBins=self.histogramBins,
                                                              limit=self.histogramLimit)
                        final[stepName][key]['histogram'] = histogram
                        # Histogram only picking values from failed jobs
                        # Operators  can use it to find out quicker why a workflow/task/step is failing :
                        if len(failedJobs) > 0:
                            failedJobsHistogram = MathAlgos.createHistogram(numList=outputFailed[key],
                                                                            nBins=self.histogramBins,
                                                                            limit=self.histogramLimit)

                            final[stepName][key]['errorsHistogram'] = failedJobsHistogram
                    else:
                        average, stdDev = MathAlgos.getAverageStdDev(numList=output[key])
                        final[stepName][key]['average'] = average
                        final[stepName][key]['stdDev'] = stdDev

                    final[stepName][key]['worstOffenders'] = [{'jobID': x['jobID'], 'value': x.get(key, 0.0),
                                                               'log': x.get('logArchive', None),
                                                               'logCollect': x.get('logCollect', None)} for x in
                                                              offenders]

            finalTask[taskName] = final
        return finalTask

    def getFailedJobs(self, workflowName):
        # We want ALL the jobs, and I'm sorry, CouchDB doesn't support wildcards, above-than-absurd values will do:
        errorView = self.fwjrdatabase.loadView("FWJRDump", "errorsByWorkflowName",
                                               options={"startkey": [workflowName, 0, 0],
                                                        "endkey": [workflowName, 999999999, 999999],
                                                        "stale": "update_after"})['rows']
        failedJobs = []
        for row in errorView:
            jobId = row['value']['jobid']
            if jobId not in failedJobs:
                failedJobs.append(jobId)

        return failedJobs

    def publishRecoPerfToDashBoard(self, workload):

        listRunsWorkflow = self.dbsDaoFactory(classname="ListRunsWorkflow")

        interestingPDs = self.interestingPDs
        interestingDatasets = []
        # Are the datasets from this request interesting? Do they have DQM output? One might ask afterwards if they have harvest
        for dataset in workload.listOutputDatasets():
            (dummy, PD, dummyProcDS, dataTier) = dataset.split('/')
            if PD in interestingPDs and dataTier == "DQM":
                interestingDatasets.append(dataset)
        # We should have found 1 interesting dataset at least
        logging.debug("Those datasets are interesting %s", str(interestingDatasets))
        if len(interestingDatasets) == 0:
            return

        # Request will be only interesting for performance if it's a ReReco or PromptReco
        if workload.getRequestType() not in ['ReReco', 'PromptReco']:
            return

        logging.info("%s has interesting performance information, trying to publish to DashBoard", workload.name())
        release = workload.getCMSSWVersions()[0]
        if not release:
            logging.info("no release for %s, bailing out", workload.name())

        # If all is true, get the run numbers processed by this worklfow
        runList = listRunsWorkflow.execute(workflow=workload.name())
        # GO to DQM GUI, get what you want
        for dataset in interestingDatasets:
            (dummy, PD, dummyProcDS, dataTier) = dataset.split('/')
            worthPoints = {}
            for run in runList:
                responseJSON = self.getPerformanceFromDQM(self.dqmUrl, dataset, run)
                if responseJSON:
                    worthPoints.update(self.filterInterestingPerfPoints(responseJSON,
                                                                        self.perfDashBoardMinLumi,
                                                                        self.perfDashBoardMaxLumi))

            # Publish dataset performance to DashBoard.
            if self.publishPerformanceDashBoard(self.dashBoardUrl, PD, release, worthPoints) is False:
                logging.info("something went wrong when publishing dataset %s to DashBoard", dataset)

        return

    def getPerformanceFromDQM(self, dqmUrl, dataset, run):

        # Get the proxy, as CMSWEB doesn't allow us to use plain HTTP
        hostCert = os.getenv("X509_HOST_CERT")
        hostKey = os.getenv("X509_HOST_KEY")
        # it seems that curl -k works, but as we already have everything, I will just provide it

        # Make function to fetch this from DQM. Returning Null or False if it fails
        getUrl = "%sjsonfairy/archive/%s%s/DQM/TimerService/event_byluminosity" % (dqmUrl, run, dataset)
        logging.debug("Requesting performance information from %s", getUrl)

        regExp = re.compile('https://(.*)(/dqm.+)')
        regExpResult = regExp.match(getUrl)
        dqmHost = regExpResult.group(1)
        dqmPath = regExpResult.group(2)

        connection = http.client.HTTPSConnection(dqmHost, 443, hostKey, hostCert)
        try:
            connection.request('GET', dqmPath)
            response = connection.getresponse()
            responseData = response.read()
            responseJSON = json.loads(responseData)
            if response.status != 200:
                logging.info("Something went wrong while fetching Reco performance from DQM, response code %d",
                             response.code)
                return False
        except Exception as ex:
            logging.error('Couldnt fetch DQM Performance data for dataset %s , Run %s', dataset, run)
            logging.exception(str(ex))  # Let's print the stacktrace with generic Exception
            return False

        try:
            if "content" in responseJSON["hist"]["bins"]:
                return responseJSON
        except Exception as ex:
            logging.info("Actually got a JSON from DQM perf in for %s run %d , but content was bad, Bailing out",
                         dataset, run)
            logging.exception(str(ex))  # Let's print the stacktrace with generic Exception
            return False
        # If it gets here before returning False or responseJSON, it went wrong
        return False

    def filterInterestingPerfPoints(self, responseJSON, minLumi, maxLumi):
        worthPoints = {}
        points = responseJSON["hist"]["bins"]["content"]
        for i in range(responseJSON["hist"]["xaxis"]["first"]["id"], responseJSON["hist"]["xaxis"]["last"]["id"]):
            # is the point worth it? if yes add to interesting points dictionary.
            # 1 - non 0
            # 2 - between minimum and maximum expected luminosity
            # FIXME : 3 - population in dashboard for the bin interval < 100
            # Those should come from the config :
            if points[i] == 0:
                continue
            binSize = responseJSON["hist"]["xaxis"]["last"]["value"] // responseJSON["hist"]["xaxis"]["last"]["id"]
            # Fetching the important values
            instLuminosity = i * binSize
            timePerEvent = points[i]

            if instLuminosity > minLumi and instLuminosity < maxLumi:
                worthPoints[instLuminosity] = timePerEvent
        logging.debug("Got %d worthwhile performance points", len(worthPoints))

        return worthPoints

    def publishPerformanceDashBoard(self, dashBoardUrl, PD, release, worthPoints):
        dashboardPayload = []
        for instLuminosity in worthPoints:
            timePerEvent = int(worthPoints[instLuminosity])
            dashboardPayload.append({"primaryDataset": PD,
                                     "release": release,
                                     "integratedLuminosity": instLuminosity,
                                     "timePerEvent": timePerEvent})

        data = "{\"data\":%s}" % str(dashboardPayload).replace("\'", "\"")
        headers = {"Accept": "application/json"}

        logging.debug("Going to upload this payload %s", data)

        try:
            request = urllib.request.Request(dashBoardUrl, data, headers)
            with closing(urllib.request.urlopen(request)) as response:
                if response.code != 200:
                    logging.info("Something went wrong while uploading to DashBoard, response code %d", response.code)
                    return False
        except Exception as ex:
            logging.error('Performance data : DashBoard upload failed for PD %s Release %s', PD, release)
            logging.exception(ex)  # Let's print the stacktrace with generic Exception
            return False

        logging.debug("Uploaded it successfully, apparently")
        return True
