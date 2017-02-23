'''

'''
from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

class T0DataCacheUpdate(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.gatherT0ActiveDataStats, 'duration': 300}]

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
                self.logger.info("DataCache is updated: %s", len(jobData))
        except Exception as ex:
            self.logger.error(str(ex))
        return
