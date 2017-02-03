'''
Created on Aug 13, 2014

@author: sryu
'''
from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.LogDB.LogDB import LogDB

class LogDBTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(LogDBTasks, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.logDBCleanUp, 'duration': config.logDBCleanDuration}]

    def logDBCleanUp(self, config):
        """
        gather active data statistics
        """
        
        logdb = LogDB(config.central_logdb_url, config.log_reporter)
        logdb.cleanup(config.logDBCleanDuration)
        
        self.logger.info("cleaned up log db")        
        return
