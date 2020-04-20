"""
File       : MSCore.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSCore class provides core functionality of the MS.
"""
# futures
from __future__ import division, print_function

from Utils.Utilities import usingRucio
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.Rucio.Rucio import Rucio


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
        self.logger.info("Configuration including default values:\n%s", self.msConfig)

        self.reqmgr2 = ReqMgr(self.msConfig['reqmgr2Url'], logger=self.logger)
        self.reqmgrAux = ReqMgrAux(self.msConfig['reqmgr2Url'],
                                   httpDict={'cacheduration': 1.0},
                                   logger=self.logger)

        # hard code it to production DBS otherwise PhEDEx subscribe API fails to match TMDB data
        dbsUrl = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        if usingRucio():
            # FIXME: we cannot use Rucio in write mode yet
            # self.rucio = Rucio(self.msConfig['rucioAccount'], configDict={"logger": self.logger})
            self.phedex = PhEDEx(httpDict={'cacheduration': 0.5},
                                 dbsUrl=dbsUrl, logger=self.logger)
        else:
            self.phedex = PhEDEx(httpDict={'cacheduration': 0.5},
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
        try:
            if self.msConfig['enableStatusTransition']:
                self.logger.info('%s updating %s status to: %s', prefix, reqName, reqStatus)
                self.reqmgr2.updateRequestStatus(reqName, reqStatus)
            else:
                self.logger.info('DRY-RUN:: %s updating %s status to: %s', prefix, reqName, reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))

    def updateReportDict(self, reportDict, keyName, value):
        """
        Provided a key name and value, validate the key name
        and update the report dictionary if it passes the validation
        :param reportDict: dictionary with a summary of the service
        :param keyName: string with the key name in the report
        :param value: string/integer value with the content of a metric
        :return: the updated dictionary
        """
        if keyName not in reportDict:
            self.logger.error("Report metric '%s' is not supported", keyName)
        else:
            reportDict[keyName] = value
        return reportDict
