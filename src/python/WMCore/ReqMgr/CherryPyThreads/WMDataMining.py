'''
Created on Aug 13, 2014

@author: sryu
'''
import cherrypy
from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMDataMining.Utils import gatherWMDataMiningStats

class WMDataMining(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.gatherActiveDataStats, 'duration': config.activeDuration},
                                {'func': self.gatherArchivedDataStats, 'duration': config.archiveDuration}]

    def gatherActiveDataStats(self, config):
        """
        gather active data statistics
        """
        gatherWMDataMiningStats(wmstatsUrl=config.wmstats_url, reqmgrUrl=config.reqmgrdb_url,
                                wmMiningUrl=config.wmdatamining_url, mcmUrl=config.mcm_url,
                                mcmCert=config.mcm_cert, mcmKey=config.mcm_key, tmpDir=config.mcm_tmp_dir,
                                archived = False, log = cherrypy.log)
        return

    def gatherArchivedDataStats(self, config):
        """
        gather archived data statistics
        """
        gatherWMDataMiningStats(wmstatsUrl=config.wmstats_url, reqmgrUrl=config.reqmgrdb_url,
                                wmMiningUrl=config.wmdatamining_url, mcmUrl=config.mcm_url,
                                mcmCert=config.mcm_cert, mcmKey=config.mcm_key, tmpDir=config.mcm_tmp_dir,
                                archived = True, log = cherrypy.log)
        return
