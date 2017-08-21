"""
Created on May 19, 2015
"""

from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux

class AuxCacheUpdateTasks(CherryPyPeriodicTask):
    """
    Updates Aux db update periodically. (i.e. TagCollector)
    """

    def __init__(self, rest, config):

        super(AuxCacheUpdateTasks, self).__init__(config)
        self.reqmgrAux = ReqMgrAux(config.reqmgr2_url, logger=self.logger)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.updateCMSSW, 'duration': config.tagCollectDuration}]

    def updateCMSSW(self, config):
        """
        gather active data statistics
        """
        self.reqmgrAux.populateCMSSWVersion(config.tagcollect_url, **config.tagcollect_args)
        self.logger.info("Updated CMSSW versions in the auxiliar db")
