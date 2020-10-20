"""
File       : MSRuleCleaner.py

Description:
This MicroService is meant to remove Rucio rules that are no longer needed in the Workload Management system, such as:
 * block-level rules created by WMAgent, against the origin RSE where data is getting produced
 * container and block-level rules created by MSTransferor, for input data that is no longer in the system
In addition to that, this MicroService is now also responsible for the workflow archival, which is the final status that
workflows remain.
"""

# futures
from __future__ import division, print_function

# system modules
from retry import retry

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import RULECLEANER_REPORT
from WMCore.MicroService.Unified.MSCore import MSCore
from Utils.EmailAlert import EmailAlert


class MSRuleCleaner(MSCore):
    """
    MSRuleCleaner.py class provides the logic used to clean the Rucio
    block level data placement rules created by WMAgent.
    """

    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSRuleCleaner module
        :param msConfig: micro service configuration
        """
        super(MSRuleCleaner, self).__init__(msConfig, logger=logger)

        self.msConfig.setdefault("limitRequestsPerCycle", 500)
        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 60)
        self.msConfig.setdefault("services", ['ruleCleaner'])
        self.msConfig.setdefault("rucioAccount", "FIXME_RUCIO_ACCT")
        self.uConfig = {}
        self.emailAlert = EmailAlert(self.msConfig)

    def execute(self, reqStatus):
        """
        Executes the whole ruleCleaner logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(RULECLEANER_REPORT)

        try:
            requestRecords = self.getRequestRecords(reqStatus)
            self.updateReportDict(summary, "total_num_requests", len(requestRecords))
            msg = "  retrieved %s requests. " % len(requestRecords)
            msg += "Service set to process up to %s requests per cycle." % self.msConfig["limitRequestsPerCycle"]
            self.logger.info(msg)
        except Exception as err:  # general error
            msg = "Unknown exception while fetching requests from ReqMgr2. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        # this one is put here just for example.
        self.updateReportDict(summary, "error", 42)
        return summary

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status.
        """

        requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
        self.logger.info('  retrieved %s requests in status: %s', len(requests), reqStatus)

        return requests
