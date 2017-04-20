from __future__ import (division, print_function)

import traceback
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfo
from WMCore.WMStats.DataStructs.DataCache import DataCache

class CleanUpTask(CherryPyPeriodicTask):
    """
    This class is used for both T0WMStats and WMStats
    controlled by config.reqdb_couch_app value
    """

    def __init__(self, rest, config):

        super(CleanUpTask, self).__init__(config)
        self.wmstatsDB = WMStatsWriter(config.wmstats_url, reqdbURL=config.reqmgrdb_url,
                              reqdbCouchApp=config.reqdb_couch_app)

    def _deleteWMStatsDocsByRequest(self, req):
        self.logger.info("deleting %s data", req)
        try:
            result = self.wmstatsDB.deleteDocsByWorkflow(req)
        except Exception as ex:
            self.logger.error("deleting %s failed: %s", req, str(ex))
            for line in traceback.format_exc().rstrip().split("\n"):
                self.logger.error(" " + line)
        else:
            self.logger.info("%s deleted", len(result))

        return
    
    def setConcurrentTasks(self, config):
        """
        sets the list of functions which runs concurrently
        """
        self.concurrentTasks = [{'func': self.cleanUpOldRequests, 'duration': (config.DataKeepDays * 24 * 60 * 60)},
                                {'func': self.cleanUpArchivedRequests, 'duration': config.archivedCleanUpDuration},
                                {'func': self.cleauUpAgentDocs, 'duration': config.archivedCleanUpDuration}]

    def cleanUpOldRequests(self, config):
        """
        clean up wmstats data older then given days
        """
        self.logger.info("deleting %s hours old docs", (config.DataKeepDays * 24))
        result = self.wmstatsDB.deleteOldDocs(config.DataKeepDays)
        self.logger.info("%s old doc deleted", result)
        return

    def cleanUpArchivedRequests(self, config):
        """
        loop through the workflows in couchdb, if archived delete all the data in couchdb
        """
        self.logger.info("getting archived data")
        requestNames = self.wmstatsDB.getArchivedRequests()
        self.logger.info("archived list %s", requestNames)

        for req in requestNames:
            self._deleteWMStatsDocsByRequest(self, req)
        return

    def cleauUpAgentDocs(self, config):
        """
        delete wmstats document if all the job is finished including logCollect and Clean up
        It won't be completely sync with agent deletion, hence there is always chance for race conditoin
        which cause move to archive status before agent clean up everything
        """
        statusFilter = {"RequestStatus": ["aborted-completed", "rejected", "announced"]}
        mask = ["AgentJobInfo"]
        for requestData in DataCache.filterDataByRequest(statusFilter, mask):
            reqInfo = RequestInfo(requestData)
            if reqInfo.isWorkflowFinished():
                self._deleteWMStatsDocsByRequest(self, requestData["RequestName"])