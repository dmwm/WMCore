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
        Update the central couch document which contains a map of
        releases and their ScramArch
        """
        self.logger.info("Updating the CMSSW/ScramArch map document ...")

        res = self.reqmgrAux.populateCMSSWVersion(config.tagcollect_url, **config.tagcollect_args)
        if 'error' in res:
            self.logger.error("Failed to update releases. Response: %s", res)
        else:
            self.logger.info("CMSSW releases successfully updated.")
