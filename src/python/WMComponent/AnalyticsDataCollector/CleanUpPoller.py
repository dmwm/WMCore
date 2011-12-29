"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter

class CleanUpPoller(BaseWorkerThread):
    """
    Cleans expired items, updates element status.
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
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        # set the connection for local couchDB call
        self.wmstatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL )

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Cleaning up the request docs")
            numOfDoc = self.wmstatsCouchDB.deleteOldDocs(self.config.AnalyticsDataCollector.DataKeepDays)
            logging.info("%s docs deleted" % numOfDoc)
        except Exception, ex:
            logging.error(str(ex))
            raise
