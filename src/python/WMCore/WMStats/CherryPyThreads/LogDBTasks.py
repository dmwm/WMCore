'''
Created on Aug 13, 2014

@author: sryu
'''
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.WMStats.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask

class LogDBTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

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
