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
        self.useReqMgrForCompletionCheck   = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)
        self.wmstatsCouchDB = WMStatsWriter(self.config.TaskArchiver.localWMStatsURL)
        self.centralCouchDBWriter = WMStatsWriter(self.config.TaskArchiver.centralWMStatsURL)
        self.centralCouchDBReader = WMStatsReader(self.config.TaskArchiver.centralWMStatsURL)
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
            report = self.wmstatsCouchDB.deleteOldDocs(self.config.TaskArchiver.DataKeepDays)
            logging.info("%s docs deleted" % report)
            logging.info("getting complete and announced requests")
            
            #TODO: define what is deletable status. Also add the code to delet summary document, 
            # request summary and job summary
            if self.useReqMgrForCompletionCheck:
                deletableWorkflows = self.centralCouchDBReader.workflowsByStatus(["announced"])
            else:
                deletableWorkflows = self.centralCouchDBReader.workflowsByStatus(["completed"])
            
            logging.info("Ready to delete %s" % deletableWorkflows)     
            for workflowName in deletableWorkflows:
                logging.info("Deleting %s from JobCouch" % workflowName)
                
                report = self.deleteWorkflowFromJobCouch(workflowName, "JobDump")
                logging.info("%s docs deleted from JobDump" % report)
                report = self.deleteWorkflowFromJobCouch(workflowName, "FWJRDump")
                logging.info("%s docs deleted from FWJRDump" % report)
                
                self.centralCouchDBWriter.updateRequestStatus(workflowName, "archived")
                logging.info("status updated to archived %s" % workflowName)
                
        except Exception, ex:
            logging.error(str(ex))
            logging.error("Error occurred, will try again next cycle")

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
        
        options = {"startkey": [workflowName], "endkey": [workflowName, {}], "stale": "ok"}
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
            for data in committed:
                if data.has_key('error'):
                    errorReport.setdefault(data['error'], 0)
                    errorReport[data['error']] += 1
                else:
                    deleted += 1
            return {'delete': deleted, 'error': errorReport}
        else:
            return "nothing"
