"""
Created on May 19, 2015
"""

from __future__ import (division, print_function)

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
        that need to be constanly updated whenever an update is
        made at the data source
        """
        self.logger.info("Updating auxiliary couch documents ...")

        res = self.reqmgrAux.populateCMSSWVersion(config.tagcollect_url, **config.tagcollect_args)
        self.checkResponse(res, "CMSSW")

        try:
            data = self.mgr.getdata(config.unified_url, params={},
                                    headers={'Accept': 'application/json'})
        except Exception as ex:
            msg = "Failed to retrieve unified configuration from github. Error: %s" % str(ex)
            msg += "\nRetrying again in the next cycle"
            self.logger.error(msg)
            return

        res = self.reqmgrAux.updateUnifiedConfig(data, docName="config")
        self.checkResponse(res, "Unified config")


    def checkResponse(self, response, docName=""):
        """
        Check the response returned by CouchDB
        """
        if 'error' in response:
            self.logger.error("Failed to update the %s doc. Response: %s", docName, response)
        else:
            self.logger.info("%s document successfully updated.", docName)
