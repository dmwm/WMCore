"""
File       : MSCore.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSCore class provides core functionality of the MS.
"""
# futures
from __future__ import division, print_function

# WMCore modules
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx


class MSCore(object):
    """
    This class provides core functionality for both
    MSTransferor and MSMonitor classes.
    """

    def __init__(self, msConfig, logger=None):
        """
        Provides setup for MSTransferor and MSMonitor classes

        :param config: MS service configuration
        :param logger: logger object (optional)
        """
        self.logger = getMSLogger(getattr(msConfig, 'verbose', False), logger)
        self.msConfig = msConfig
        self.logger.info(
            "Configuration including default values:\n%s", self.msConfig)

        self.reqmgr2 = ReqMgr(self.msConfig['reqmgrUrl'], logger=self.logger)
        self.reqmgrAux = ReqMgrAux(self.msConfig['reqmgrUrl'],
                                   httpDict={'cacheduration': 60},
                                   logger=self.logger)

        # hard code it to production DBS otherwise PhEDEx subscribe API fails to match TMDB data
        dbsUrl = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        # eventually will change it to Rucio
        self.phedex = PhEDEx(httpDict={'cacheduration': 10 * 60},
                             dbsUrl=dbsUrl, logger=self.logger)

    def unifiedConfig(self):
        """
        Fetches the unified configuration
        :return: unified configuration content
        """
        res = self.reqmgrAux.getUnifiedConfig(docName="config")
        if res:
            if isinstance(res, list):
                return res[0]
            return res
        else:
            return {}

    def change(self, reqName, reqStatus, prefix='###'):
        """
        Update the request status in ReqMgr2
        """
        self.logger.info('%s updating %s status to %s', prefix, reqName, reqStatus)
        try:
            if not self.msConfig['readOnly']:
                self.reqmgr2.updateRequestStatus(reqName, reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))
