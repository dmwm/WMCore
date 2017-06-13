"""
Thread used to monitor the agent draining process. Should
eventually report no issues meaning that the agent is ready
to be shutdown and a new version be put in place.
"""
from __future__ import division

__all__ = []

import logging
import threading
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMComponent.AnalyticsDataCollector.DataCollectAPI import isDrainMode
from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil

class DrainStatusPoller(BaseWorkerThread):
    """
    Collects information related to the agent drain status
    """
    # class variable that contains drain statistics
    drainStats = {}

    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        self.config = config

    def setup(self, parameters):
        """
        """
        myThread = threading.currentThread()
        self.dbsUtil = DBSBufferUtil()

    def algorithm(self, parameters):
        """
        Update drainStats if agent is in drain mode
        """
        if isDrainMode(self.config):
            logging.info("Checking agent drain status...")

            try:
                DrainStatusPoller.drainStats = self.collectDrainInfo()
                logging.info("Finished collecting agent drain status.")

            except Exception as ex:
                msg = "Error occurred, will retry later:\n"
                msg += str(ex)
                logging.exception(msg)
        else:
            logging.info("Agent not in drain mode. Skipping drain check...")

    def collectDrainInfo(self):
        """
        Call methods to check the drain status
        """
        results = {}
        results['workflows_completed'] = self.dbsUtil.isAllWorkflowCompleted()
        logging.info("Drain stats - workflows completed: " + str(results['workflows_completed']))
        return results

    @classmethod
    def getDrainInfo(cls):
        """
        Return drainStats class variable
        """
        return cls.drainStats

