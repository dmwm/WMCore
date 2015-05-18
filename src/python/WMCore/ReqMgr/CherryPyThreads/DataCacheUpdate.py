'''

'''
from WMCore.ReqMgr.DataStructs.DataCache import DataCache
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS
from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

class DataCacheUpdate(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.agentDataUpdate, 'duration': config.agentUpdateDuration}]

    def gatherActiveDataStats(self, config):
        """
        gather active data statistics
        """
        try:
            if DataCache.islatestJobDataExpired():
                reqDB = RequestDBReader(config.requestDBURL)
                wmstatsDB = WMStatsReader(config.wmstatsURL)
                
                requestNames = reqDB.getRequestByStatus(ACTIVE_STATUS)
                jobData = wmstatsDB.getLatestJobInfoByRequests(requestNames)
                DataCache.setlatestJobData(jobData)
            
        except Exception as ex:
            self.logger.error(str(ex))
        return