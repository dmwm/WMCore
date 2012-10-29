"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import sanitizeURL

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
        # set the connection for local couchDB call
        self.wmstatsCouchDB = WMStatsWriter(self.config.CleanUpManager.localWMStatsURL)
        self.centralCouchDBWriter = WMStatsWriter(self.config.CleanUpManager.centralWMStatsURL)
        self.centralCouchDBReader = WMStatsReader(self.config.CleanUpManager.centralWMStatsURL)
        jobDBurl = sanitizeURL(self.config.JobStateMachine.couchurl)['url']
        jobDBName = self.config.JobStateMachine.couchDBName
        self.jobCouchdb  = CouchServer(jobDBurl)
        self.jobsdatabase = self.jobCouchdb.connectDatabase("%s/jobs" % jobDBName)
        self.fwjrdatabase = self.jobCouchdb.connectDatabase("%s/fwjrs" % jobDBName)

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Cleaning up the old request docs")
            report = self.wmstatsCouchDB.deleteOldDocs(self.config.CleanUpManager.DataKeepDays)
            logging.info("%s docs deleted" % report)
            logging.info("getting complete and announced requests")
            
            #TODO: define what is deletable status. Also add the code to delet summary document, 
            # request summary and job summary
            deletableWorkflows = self.centralCouchDBReader.workflowsByStatus(["announced"])
            
            logging.info("Ready to delete %s" % deletableWorkflows)     
            for workflowName in deletableWorkflows:
                if self.cleanAllLocalCouchDB(workflowName):
                    self.centralCouchDBWriter.updateRequestStatus(workflowName, "normal-archived")
                    logging.info("status updated to normal-archived %s" % workflowName)
            
            abortedWorkflows = self.centralCouchDBReader.workflowsByStatus(["aborted-completed"])
            logging.info("Ready to delete aborted %s" % abortedWorkflows)
            for workflowName in abortedWorkflows:
                if self.cleanAllLocalCouchDB(workflowName):
                    self.centralCouchDBWriter.updateRequestStatus(workflowName, "aborted-archived")
                    logging.info("status updated to aborted-archived %s" % workflowName)
            
            #TODO: following code is temproraly - remove after production archived data is cleaned 
            removableWorkflows = self.centralCouchDBReader.workflowsByStatus(["archived"])
            for workflowName in removableWorkflows:
                logging.info("Deleting %s from WMAgent Summary Couch" % workflowName)
                report = self.deleteWorkflowFromJobCouch(workflowName, "WMStats")
                logging.info("%s docs deleted from wmagent_summary" % report)
                # only updatet he status when delete is successful
                # TODO: need to handle the case when there are multiple agent running the same request.
                if report["status"] == "ok":
                    self.centralCouchDBWriter.updateRequestStatus(workflowName, "normal-archived")
                    logging.info("status updated to normal-archived from archived (this is temp solution for production) %s" % workflowName)

        except Exception, ex:
            logging.error(str(ex))
            raise

    def deleteWorkflowFromJobCouch(self, workflowName, db):
        """
        _deleteWorkflowFromCouch_

        If we are asked to delete the workflow from couch, delete it
        to clear up some space.

        Load the document IDs and revisions out of couch by workflowName,
        then order a delete on them.
        """
        if (db == "JobDump"):
            couchDB = self.jobsdatabase
            view = "jobsByWorkflowName"
        elif (db == "FWJRDump"):
            couchDB = self.fwjrdatabase
            view = "fwjrsByWorkflowName"
        elif (db == "WMStats"):
            couchDB = self.wmstatsCouchDB.getDBInstance()
            view = "jobsByStatusWorkflow"
        
        options = {"startkey": [workflowName], "endkey": [workflowName, {}], "stale": "ok", "reduce": False}
        jobs = couchDB.loadView(db, view, options = options)['rows']
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
                if data.has_key('error'):
                    errorReport.setdefault(data['error'], 0)
                    errorReport[data['error']] += 1
                    status =  "error"
                else:
                    deleted += 1
            return {'status': status, 'delete': deleted, 'message': errorReport}
        else:
            return {'status': 'warning', 'message': "no %s exist" % workflowName}

    def cleanAllLocalCouchDB(self, workflowName):
        logging.info("Deleting %s from JobCouch" % workflowName)
        
        jobReport = self.deleteWorkflowFromJobCouch(workflowName, "JobDump")
        logging.info("%s docs deleted from JobDump" % jobReport)
        
        fwjrReport = self.deleteWorkflowFromJobCouch(workflowName, "FWJRDump")
        logging.info("%s docs deleted from FWJRDump" % fwjrReport)
        
        wmstatsReport = self.deleteWorkflowFromJobCouch(workflowName, "WMStats")
        logging.info("%s docs deleted from wmagent_summary" % wmstatsReport)
        
        # if one of the procedure fails return False
        if (jobReport["status"] == "error" or fwjrReport["status"] == "error" or 
            wmstatsReport["status"] == "error"):
            return False
        
        # if the data doesn't exist on all the db return False
        if (jobReport["status"] == "warning" and 
            fwjrReport["status"] == "warning" and 
            wmstatsReport["status"] == "warning"):
            return False
        # other wise return True.
        return True