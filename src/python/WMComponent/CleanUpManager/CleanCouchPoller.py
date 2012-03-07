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
            numOfDoc = self.wmstatsCouchDB.deleteOldDocs(self.config.CleanUpManager.DataKeepDays)
            logging.info("%s docs deleted" % numOfDoc)
            logging.info("Deleting the complete request docs")
            
            #TODO: define what is deletable status. Also add the code to delet summary document, 
            # request summary and job summary
            deletableWorkflows = self.centralCouchDBReader.workflowsByStatus(["completed", "announced"])
            for workflowName in deletableWorkflows:
                self.deleteWorkflowFromJobCouch(workflowName)
                self.centralCouchDBWriter.updateRequestStatus(workflowName, "deleted")            
        except Exception, ex:
            logging.error(str(ex))
            raise

    def deleteWorkflowFromJobCouch(self, workflowName):
        """
        _deleteWorkflowFromCouch_

        If we are asked to delete the workflow from couch, delete it
        to clear up some space.

        Load the document IDs and revisions out of couch by workflowName,
        then order a delete on them.
        """
       
        jobs = self.jobsdatabase.loadView("JobDump", "jobsByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName, {}]})['rows']
        for j in jobs:
            id  = j['value']['id']
            rev = j['value']['rev']
            self.jobsdatabase.delete_doc(id = id, rev = rev)

        jobs = self.fwjrdatabase.loadView("FWJRDump", "fwjrsByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName, {}]})['rows']

        for j in jobs:
            id  = j['value']['id']
            rev = j['value']['rev']
            self.fwjrdatabase.delete_doc(id = id, rev = rev)

        return
