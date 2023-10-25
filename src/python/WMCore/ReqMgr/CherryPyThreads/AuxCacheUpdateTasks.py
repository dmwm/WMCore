"""
Created on May 19, 2015
"""
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.pycurl_manager import RequestHandler


class AuxCacheUpdateTasks(CherryPyPeriodicTask):
    """
    Updates Aux db update periodically. (i.e. TagCollector)
    """

    def __init__(self, rest, config):
        super(AuxCacheUpdateTasks, self).__init__(config)
        self.reqmgrAux = ReqMgrAux(config.reqmgr2_url, logger=self.logger)
        self.mgr = RequestHandler()

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.updateAuxiliarDocs, 'duration': config.tagCollectDuration}]

    def updateAuxiliarDocs(self, config):
        """
        Update the central couch database with auxiliary documents
        that need to be continuously updated
        """
        self.logger.info("Updating CMSSW document in Central Couch ...")
        self.reqmgrAux.populateCMSSWVersion(config.tagcollect_url, **config.tagcollect_args)
