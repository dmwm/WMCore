'''
Created on Aug 13, 2014

@author: sryu
'''
from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMDataMining.Utils import getherWMDataMiningStats

class WMDataMining(CherryPyPeriodicTask):
    
    def __init__(self, rest, config):
        
        CherryPyPeriodicTask.__init__(self, config)
        
    def setConcurrentTasks(self, config):
        """
        sets the list of functions which 
        """
        self.concurrentTasks = [{'func': self.getherActiveDataStats, 'duration': config.activeDuration}, 
                                {'func': self.getherArchivedDataStats, 'duration': config.archiveDuration}] 
        
    def getherActiveDataStats(self, config):
        """
        gether active data statistics
        """
        getherWMDataMiningStats(config.wmstats_url, config.reqmgrdb_url, 
                                       config.wmdatamining_url)
        return
    
    def getherArchivedDataStats(self, config):
        """
        gether archived data statistics
        """
        getherWMDataMiningStats(config.wmstats_url, config.reqmgrdb_url, 
                                       config.wmdatamining_url, True)
        return
    