from __future__ import (division, print_function)

import time
import tracemalloc
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.ReqMgr.DataStructs.RequestStatus import WMSTATS_JOB_INFO, WMSTATS_NO_JOB_INFO

class DataCacheUpdate(CherryPyPeriodicTask):

    def __init__(self, rest, config):
        self.getJobInfo = getattr(config, "getJobInfo", False)

        super(DataCacheUpdate, self).__init__(config)
        tracemalloc.start()

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.gatherActiveDataStats, 'duration': 300}]

    def gatherActiveDataStats(self, config):
        """
        gather active data statistics
        """
        self.logger.info("Starting gatherActiveDataStats with jobInfo set to: %s", self.getJobInfo)
        try:
            tStart = time.time()
            if DataCache.islatestJobDataExpired():
                snapshot1 = tracemalloc.take_snapshot()
                wmstatsDB = WMStatsReader(config.wmstats_url, reqdbURL=config.reqmgrdb_url,
                                          reqdbCouchApp="ReqMgr", logger=self.logger)
                self.logger.info("Getting active data with job info for statuses: %s", WMSTATS_JOB_INFO)
                jobData = wmstatsDB.getActiveData(WMSTATS_JOB_INFO, jobInfoFlag=self.getJobInfo)
                self.logger.info("Getting active data with NO job info for statuses: %s", WMSTATS_NO_JOB_INFO)
                tempData = wmstatsDB.getActiveData(WMSTATS_NO_JOB_INFO, jobInfoFlag=False)
                jobData.update(tempData)
                snapshot2 = tracemalloc.take_snapshot()
                self.logger.info("Running setlatestJobData...")
                DataCache.setlatestJobData(jobData)
                self.logger.info("DataCache is up-to-date with %d requests data", len(jobData))
                snapshot3 = tracemalloc.take_snapshot()

                self.logger.info("Memory stats between snapshot2 and snapshot1")
                top_stats = snapshot2.compare_to(snapshot1, 'lineno')
                for thisStat in top_stats[:5]:
                    self.logger.info(thisStat)

                self.logger.info("Memory stats between snapshot3 and snapshot2")
                top_stats = snapshot3.compare_to(snapshot2, 'lineno')
                for thisStat in top_stats[:5]:
                    self.logger.info(thisStat)
        except Exception as ex:
            self.logger.exception("Exception updating DataCache. Error: %s", str(ex))
        self.logger.info("Total time loading data from ReqMgr2 and WMStats: %s", time.time() - tStart)
        return
