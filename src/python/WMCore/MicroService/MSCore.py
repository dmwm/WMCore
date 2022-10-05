"""
File       : MSCore.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSCore class provides core functionality of the MS.
"""
# futures
from __future__ import division, print_function

from builtins import object
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI


class MSCore(object):
    """
    This class provides core functionality for
    MSTransferor, MSMonitor, MSOutput, MSRuleCleaner, MSUnmerged classes.
    """

    def __init__(self, msConfig, **kwargs):
        """
        Provides a basic setup for all the microservices

        :param config: MS service configuration
        :param kwargs: can be used to skip the initialization of specific services, such as:
            logger: logger object
            skipReqMgr: boolean to skip ReqMgr initialization
            skipReqMgrAux: boolean to skip ReqMgrAux initialization
            skipRucio: boolean to skip Rucio initialization
        """
        self.logger = getMSLogger(getattr(msConfig, 'verbose', False), kwargs.get("logger"))
        self.msConfig = msConfig
        self.logger.info("Configuration including default values:\n%s", self.msConfig)

        if not kwargs.get("skipReqMgr", False):
            self.reqmgr2 = ReqMgr(self.msConfig['reqmgr2Url'], logger=self.logger)
        if not kwargs.get("skipReqMgrAux", False):
            self.reqmgrAux = ReqMgrAux(self.msConfig['reqmgr2Url'],
                                       httpDict={'cacheduration': 1.0}, logger=self.logger)

        self.phedex = None
        self.rucio = None
        if not kwargs.get("skipRucio", False):
            self.rucio = Rucio(acct=self.msConfig['rucioAccount'],
                               hostUrl=self.msConfig['rucioUrl'],
                               authUrl=self.msConfig['rucioAuthUrl'],
                               configDict={"logger": self.logger, "user_agent": "wmcore-microservices"})
        self.alertManagerUrl = self.msConfig.get("alertManagerUrl", None)
        self.alertManagerApi = AlertManagerAPI(self.alertManagerUrl, logger=self.logger)

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

    def sendAlert(self, alertName, severity, summary, description, service, endSecs=1 * 60 * 60):
        """
        Sends an alert to Prometheus.
        :param alertName: string with unique alert name
        :param severity: string with the alert severity
        :param summary: string with a short summary of the alert
        :param description: string with a longer description of the alert
        :param service: string with the service name generating this alert
        :param endSecs: integer with an expiration time
        :return: none
        """
        try:
            self.alertManagerApi.sendAlert(alertName, severity, summary, description,
                                           service, endSecs=endSecs)
        except Exception as ex:
            self.logger.exception("Failed to send alert to %s. Error: %s", self.alertManagerUrl, str(ex))
