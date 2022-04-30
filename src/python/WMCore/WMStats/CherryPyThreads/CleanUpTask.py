from __future__ import (division, print_function)

import traceback
import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.ReqMgr.DataStructs.RequestStatus import ARCHIVED_STATUS

class CleanUpTask(CherryPyPeriodicTask):
    """
    This class is used for both T0WMStats and WMStats
    controlled by config.reqdb_couch_app value
    """

    def __init__(self, rest, config):

        super(CleanUpTask, self).__init__(config)
        self.wmstatsDB = WMStatsWriter(config.wmstats_url, reqdbURL=config.reqmgrdb_url,
                              reqdbCouchApp=config.reqdb_couch_app)
        self.cleanCouchDelayHours = getattr(config, "cleanCouchDelayHours", 0)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which runs concurrently
        """
        self.concurrentTasks = [{'func': self.cleanUpArchivedRequests, 'duration': config.archivedCleanUpDuration}]

    def cleanUpArchivedRequests(self, config):
        """
        loop through the workflows in couchdb, if archived delete all the data in couchdb
        """
        self.logger.info("getting archived data")
        if self.cleanCouchDelayHours > 0:
            satartTime = int(time.time()) - int(self.cleanCouchDelayHours * 60 * 60)
            requestNames = self.wmstatsDB.getRequestByStatusAndStartTime(ARCHIVED_STATUS, startTime=startTime, detail=False, jobInfoFlag=False)
        else:
            requestNames = self.wmstatsDB.getArchivedRequests()
        self.logger.info("archived list %s", requestNames)

        for req in requestNames:
            self.logger.info("Deleting data for: %s", req)
            try:
                result = self.wmstatsDB.deleteDocsByWorkflow(req)
            except Exception as ex:
                self.logger.error("deleting %s failed: %s", req, str(ex))
                for line in traceback.format_exc().rstrip().split("\n"):
                    self.logger.error(" " + line)
            else:
                if result is None:
                    self.logger.info("there were no documents to delete.")
                else:
                    self.logger.info("%s docs deleted", len(result))
        return
