"""
Perform cleanup actions
"""
__all__ = []

import logging
import time
from httplib import HTTPException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.ReqMgr.ReqMgr         import ReqMgr
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import sanitizeURL
from WMCore.Database.CMSCouch import CouchNotFoundError

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
        
    def setup(self, parameters):
        """
        Called at startup
        """
        self.teamName = self.config.Agent.teamName
        # set the connection for local couchDB call
        self.useReqMgrForCompletionCheck = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)
        self.archiveDelayHours = getattr(self.config.TaskArchiver, 'archiveDelayHours', 0)
        self.wmstatsCouchDB = WMStatsWriter(self.config.TaskArchiver.localWMStatsURL, 
                                            "WMStatsAgent")
        
        #TODO: we might need to use local db for Tier0
        self.centralRequestDBReader = RequestDBReader(self.config.AnalyticsDataCollector.centralRequestDBURL, 
                                                      couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)
        
        if self.useReqMgrForCompletionCheck:
            self.deletableState = "announced"
            self.centralRequestDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.centralRequestDBURL, 
                                                          couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)
            self.reqmgr2Svc = ReqMgr(self.config.TaskArchiver.ReqMgr2ServiceURL)
            #TODO: remove this when reqmgr2 replace reqmgr completely (reqmgr2Only)
            self.reqmgrSvc = RequestManager({'endpoint': self.config.TaskArchiver.ReqMgrServiceURL})
        else:
            # Tier0 case
            self.deletableState = "completed"
            # use local for update
            self.centralRequestDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.localT0RequestDBURL, 
                                                          couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)
        
        jobDBurl = sanitizeURL(self.config.JobStateMachine.couchurl)['url']
        jobDBName = self.config.JobStateMachine.couchDBName
        self.jobCouchdb  = CouchServer(jobDBurl)
        self.jobsdatabase = self.jobCouchdb.connectDatabase("%s/jobs" % jobDBName)
        self.fwjrdatabase = self.jobCouchdb.connectDatabase("%s/fwjrs" % jobDBName)
        
        statSummaryDBName = self.config.JobStateMachine.summaryStatsDBName
        self.statsumdatabase = self.jobCouchdb.connectDatabase(statSummaryDBName)

    def algorithm(self, parameters):
        """
        Get information from wmbs, workqueue and local couch.
          - It deletes old wmstats docs
          - Archive workflows
        """
        try:
            logging.info("Cleaning up the old request docs")
            report = self.wmstatsCouchDB.deleteOldDocs(self.config.TaskArchiver.DataKeepDays)
            logging.info("%s docs deleted" % report)

            # archiving only workflows that I own (same team)
            logging.info("Getting requests in '%s' state for team '%s'" % (self.deletableState,
                                                                           self.teamName))
            endTime = int(time.time()) - self.archiveDelayHours * 3600
            wfs = self.centralRequestDBReader.getRequestByTeamAndStatus(self.teamName,
                                                                        self.deletableState)
            commonWfs = self.centralRequestDBReader.getRequestByStatusAndStartTime(self.deletableState, 
                                                                                   False, endTime)
            deletableWorkflows = list(set(wfs) & set(commonWfs))
            logging.info("Ready to archive normal %s workflows" % len(deletableWorkflows))
            numUpdated = self.archiveWorkflows(deletableWorkflows, "normal-archived")
            logging.info("archive normal %s workflows" % numUpdated)
            
            abortedWorkflows = self.centralRequestDBReader.getRequestByStatus(["aborted-completed"])
            logging.info("Ready to archive aborted %s workflows" % len(abortedWorkflows))
            numUpdated = self.archiveWorkflows(abortedWorkflows, "aborted-archived")
            logging.info("archive aborted %s workflows" % numUpdated)
            
            rejectedWorkflows = self.centralRequestDBReader.getRequestByStatus(["rejected"])
            logging.info("Ready to archive rejected %s workflows" % len(rejectedWorkflows))
            numUpdated = self.archiveWorkflows(rejectedWorkflows, "rejected-archived")
            logging.info("archive rejected %s workflows" % numUpdated)

        except Exception as ex:
            logging.error(str(ex))
            logging.error("Error occurred, will try again next cycle")
    
    def archiveWorkflows(self, workflows, archiveState):
        updated = 0
        for workflowName in workflows:
            if self.cleanAllLocalCouchDB(workflowName):
                if self.useReqMgrForCompletionCheck:
                    try:
                        #TODO: try reqmgr1 call if it fails (reqmgr2Only - remove this line when reqmgr is replaced)
                        self.reqmgrSvc.updateRequestStatus(workflowName, archiveState)
                        #And replace with this - remove all the excption
                        #self.reqmgr2Svc.updateRequestStatus(workflowName, archiveState)
                    except HTTPException as ex:
                        # If we get an HTTPException of 404 means reqmgr2 request
                        if ex.status == 404:
                            # try reqmgr2 call
                            msg = "%s : reqmgr2 request: %s" % (workflowName, str(ex))
                            logging.warning(msg)
                            self.reqmgr2Svc.updateRequestStatus(workflowName, archiveState)
                        else:
                            msg = "%s : fail to update status with HTTP error: %s" % (workflowName, str(ex))
                            logging.error(msg)
                            raise ex
                            
                    updated += 1 
                    logging.debug("status updated to %s %s" % (archiveState, workflowName))
                else:
                    # tier0 update case
                    self.centralRequestDBWriter.updateRequestStatus(workflowName, archiveState)
        return updated
    
    def deleteWorkflowFromJobCouch(self, workflowName, db):
        """
        _deleteWorkflowFromCouch_

        If we are asked to delete the workflow from couch, delete it
        to clear up some space.

        Load the document IDs and revisions out of couch by workflowName,
        then order a delete on them.
        """
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
            view = "jobsByStatusWorkflow"
        
        if view == None:
            try:
                committed = couchDB.delete_doc(workflowName)
            except CouchNotFoundError as ex:
                return {'status': 'warning', 'message': "%s: %s" % (workflowName, str(ex))}
        else:
            options = {"startkey": [workflowName], "endkey": [workflowName, {}], "reduce": False}
            try:
                jobs = couchDB.loadView(db, view, options = options)['rows']
            except Exception as ex:
                errorMsg = "Error on loading jobs for %s" % workflowName
                logging.warning("%s/n%s" % (str(ex), errorMsg))
                return {'status': 'error', 'message': errorMsg}
            
            for j in jobs:
                doc = {}
                doc["_id"]  = j['value']['id']
                doc["_rev"] = j['value']['rev']
                couchDB.queueDelete(doc)
            committed = couchDB.commit()
        
        if committed:
            #create the error report
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
        logging.info("Deleting %s from JobCouch" % workflowName)
        
        jobReport = self.deleteWorkflowFromJobCouch(workflowName, "JobDump")
        logging.debug("%s docs deleted from JobDump" % jobReport)
        
        fwjrReport = self.deleteWorkflowFromJobCouch(workflowName, "FWJRDump")
        logging.debug("%s docs deleted from FWJRDump" % fwjrReport)
        
        summaryReport = self.deleteWorkflowFromJobCouch(workflowName, "SummaryStats")
        logging.debug("%s docs deleted from SummaryStats" % summaryReport)
        
        wmstatsReport = self.deleteWorkflowFromJobCouch(workflowName, "WMStatsAgent")
        logging.debug("%s docs deleted from wmagent_summary" % wmstatsReport)
        
        # if one of the procedure fails return False
        if (jobReport["status"] == "error" or fwjrReport["status"] == "error" or 
            wmstatsReport["status"] == "error"):
            return False
        # other wise return True.
        return True
        
