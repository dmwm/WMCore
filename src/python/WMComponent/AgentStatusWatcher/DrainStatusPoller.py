"""
Monitoring the agent drain status using CheckDrainStatus
"""
__all__ = []

import threading
import logging
import time
import traceback
import json
from WMCore.Lexicon import sanitizeURL
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import WMAgentDBData, \
     DataUploadTime, initAgentInfo, isDrainMode


class DrainStatusPoller(BaseWorkerThread):
    """
    """

    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        self.config = config
        # need to get campaign, user, owner info
        self.agentInfo = initAgentInfo(self.config)
        self.summaryLevel = config.AnalyticsDataCollector.summaryLevel
        self.jsonFile = config.AgentStatusWatcher.jsonFile


    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """
        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)

    def algorithm(self, parameters):
        """
        do some stuff if agent is in drain mode
        """
        if isDrainMode(self.config):
            logging.info("Checking agent drain status...")

            try:
                drainInfo = self.collectDrainInfo()
                # set the uploadTime - should be the same for all docs
                logging.info("finished collecting agent drain status")
                uploadTime = int(time.time())
                # self.uploadAgentInfoToCentralWMStats(drainInfo, uploadTime)

                # save locally json file as well
                with open(self.jsonFile, 'w') as outFile:
                    json.dump(drainInfo, outFile, indent=2)

            except Exception as ex:
                logging.error("Error occurred, will retry later:")
                logging.error(str(ex))
                logging.error("Trace back: \n%s" % traceback.format_exc())
        else:
            logging.info("Skipping agent drain check...")

    def collectDrainInfo(self):
        """
        call CheckDrainStatus here
        """
        # return some basic drain info until real stats are queried
        results = {}
        results['drain_status'] = "draining"

        return results

