"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter

class CleanCouchPoller(BaseWorkerThread):
    """
    Cleans up local couch db according the the given condition.
    1. TODO: Cleans local couch db when request is completed and reported to cental db.
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
        self.wmstatsCouchDB = WMStatsWriter(self.config.CleanUpManager.localWMStatsURL )

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Cleaning up the request docs")
            numOfDoc = self.wmstatsCouchDB.deleteOldDocs(self.config.CleanUpManager.DataKeepDays)
            logging.info("%s docs deleted" % numOfDoc)
        except Exception, ex:
            logging.error(str(ex))
            raise
