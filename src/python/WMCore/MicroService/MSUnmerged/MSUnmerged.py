"""
File       : MSUnmerged.py

Description:

This MicroService is meant to remove all no longer needed files from the unmerged
area of the CMS LFN Namespace. The cleanup process is supposed to happen on a per
RSE basis.
"""

# futures
from __future__ import division, print_function

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import UNMERGED_REPORT
from WMCore.MicroService.MSCore import MSCore
# from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI
from WMCore.WMException import WMException


class MSUnmergedException(WMException):
    """
    General Exception Class for MSUnmerged Module in WMCore MicroServices
    """
    def __init__(self, message):
        self.myMessage = "MSUnmergedException: %s" % message
        super(MSUnmergedException, self).__init__(self.myMessage)


class MSUnmerged(MSCore):
    """
    MSUnmerged.py class provides the logic for cleaning the unmerged area of
    the CMS LFN Namespace.
    """

    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSUnmerged module
        :param msConfig: micro service configuration
        """
        super(MSUnmerged, self).__init__(msConfig, logger=logger)

        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 60)
        self.msConfig.setdefault('limitRSEsPerInstance', 100)
        self.msConfig.setdefault('limitTiersPerInstance', ['T1', 'T2', 'T3'])
        self.msConfig.setdefault("rucioAccount", "FIXME_RUCIO_ACCT")
        self.alertServiceName = "ms-unmerged"
        # TODO: Add 'alertManagerUrl' to msConfig'
        # self.alertManagerAPI = AlertManagerAPI(self.msConfig.get("alertManagerUrl", None), logger=logger)

    def execute(self):
        """
        Executes the whole MSUnmerged logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(UNMERGED_REPORT)

        try:
            rseList = self.getRSEList()
            self.updateReportDict(summary, "total_num_rses", len(rseList))
            msg = "  retrieved %s RSEs. " % len(rseList)
            msg += "Service set to process up to %s RSEs per instance." % self.msConfig["limitRSEsPerInstance"]
            self.logger.info(msg)
        except Exception as err:  # general error
            msg = "Unknown exception while fetching RSEs from CRIC. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        # this one is put here just for example.
        self.updateReportDict(summary, "error", 42)
        return summary

    def getRSEList(self):
        """
        Queries CRIC for the proper RSE lists to iterate through.
        return: List of RSEs.
        """
        # NOTE: To filter out the granularity (Tier*) level.
        rseList = []

        return rseList
