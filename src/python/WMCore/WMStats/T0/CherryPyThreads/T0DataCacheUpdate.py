'''

'''
from __future__ import (division, print_function)

import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

class T0DataCacheUpdate(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)
        self.cleanCouchDelayHours = getattr(config, "cleanCouchDelayHours", 0)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.gatherT0ActiveDataStats, 'duration': 300},
                                {'func': self.gatherT0ArchivedDataStats, 'duration': 300}]

    def gatherT0ActiveDataStats(self, config):
        """
        gather active data statistics
        """
        try:
            if DataCache.islatestJobDataExpired():
                wmstatsDB = WMStatsReader(config.wmstats_url, reqdbURL=config.reqmgrdb_url,
                                          reqdbCouchApp = "T0Request")
                jobData = wmstatsDB.getT0ActiveData(jobInfoFlag = True)
                DataCache.setlatestJobData(jobData)
                self.logger.info("DataCache is updated with T0 active Data. Number of records: %s", len(jobData))
        except Exception as ex:
            self.logger.error(str(ex))
        return

    def gatherT0ArchivedDataStats(self, config):
        """
        gather active data statistics
        """
        try:
            if DataCache.islatestJobDataExpired():
                wmstatsDB = WMStatsReader(config.wmstats_url, reqdbURL=config.reqmgrdb_url,
                                          reqdbCouchApp = "T0Request")
                startTime = int(time.time()) - int(self.cleanCouchDelayHours * 60 * 60)
                jobData = wmstatsDB.getT0ArchivedData(jobInfoFlag = True, startTime=startTime)
                DataCache.setlatestJobData(jobData)
                self.logger.info("DataCache is updated with T0 archived Data. Number of records: %s", len(jobData))
        except Exception as ex:
            self.logger.error(str(ex))
        return
