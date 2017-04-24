"""
Monitoring the agent drain status using CheckDrainStatus
"""
__all__ = []

import logging
import traceback
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMComponent.AnalyticsDataCollector.DataCollectAPI import isDrainMode


class DrainStatusPoller(BaseWorkerThread):
    """
    """
    # class variable that contains drain statistics
    drainStats = {}

    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        self.config = config

    def algorithm(self, parameters):
        """
        Update drainStats if agent is in drain mode
        """
        if isDrainMode(self.config):
            logging.info("Checking agent drain status...")

            try:
                DrainStatusPoller.drainStats = self.collectDrainInfo()
                logging.info("finished collecting agent drain status")

            except Exception as ex:
                msg = "Error occurred, will retry later:\n"
                msg += str(ex)
                msg += "Trace back: \n%s" % traceback.format_exc()
                logging.exception(msg)
        else:
            logging.info("Agent not in drain mode. Skipping drain check...")

    def collectDrainInfo(self):
        """
        call CheckDrainStatus here
        """
        # return empty dict until real stats are queried
        results = {}
        return results

    @classmethod
    def getDrainInfo(cls):
        return cls.drainStats

