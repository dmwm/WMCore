"""
Created on Aug 13, 2014
@author: sryu
"""

from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.LogDB.LogDB import LogDB

class LogDBTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):
        super(LogDBTasks, self).__init__(config)
        self.logdb = LogDB(config.central_logdb_url, config.log_reporter)

    def setConcurrentTasks(self, config):
        """
        Set logDBCleanUp method to be executed in a polling cycle
        """
        self.concurrentTasks = [{'func': self.logDBCleanUp, 'duration': config.logDBCleanDuration}]

    def logDBCleanUp(self, config):
        """
        gather active data statistics
        """
        self.logger.info("Cleaning documents from LogDB WMStats")
        docs = self.logdb.cleanup(config.keepDocsAge)
        self.logger.info("Deleted %d old documents", len(docs))

        return
