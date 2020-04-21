"""
File       : MSOtput.py

Description: MSOutput.py class provides the whole logic behind
the Output data placement in WMCore MicroServices.
"""

# futures
from __future__ import division, print_function

# system modules
from retry import retry

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import OUTPUT_REPORT,\
    OUTPUT_MONGO_DOC
from WMCore.MicroService.DataStructs.Workflow import Workflow
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.Services.DDM.DDM import DDM, DDMReqTemplate
from Utils.EmailAlert import EmailAlert


class MSOutput(MSCore):
    """
    MSOutput.py class provides the whole logic behind the Output data placement
    in MicroServices.
    """

    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSOutput module
        :param microConfig: microservice configuration
        """
        super(MSOutput, self).__init__(msConfig, logger)

        self.msConfig.setdefault("limitRequestsPerCycle", 500)
        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 600)
        self.msConfig.setdefault("services", ['output'])
        self.msConfig.setdefault("defaultDataManSys", "DDM")
        self.msConfig.setdefault("defaultGroup", "DataOps")
        self.msConfig.setdefault("enableAggSubscr", True)
        self.msConfig.setdefault("enableDataPlacement", False)
        self.msConfig.setdefault("excludeDataTier", ['NANOAOD'])
        self.msConfig.setdefault("rucioAccount", 'wma_test')
        self.uConfig = {}
        self.emailAlert = EmailAlert(self.msConfig)

    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the output logic, e.g.:
        * unified configuration
        """
        self.uConfig = self.unifiedConfig()
        if not self.uConfig:
            raise RuntimeWarning("Failed to fetch the unified configuration")

    def execute(self, reqStatus):
        """
        Executes the whole output data placement logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        # NOTE:
        #    Here we should make the whole logic - like:
        #    * Calling the system to fetch the workflows from;
        #    * Creating the workflow objects;
        #    * Pushing them into the back end database system we choose for bookkeeping
        #    * Updating their status in that system, both MsStatus (subscribed,
        #      processing, etc.) and also the Reqmgr status
        #    * Associate and keep track of the requestID/subscriptionID/ruleID
        #      returned by the Data Management System and the workflow
        #      object (through the bookkeeping machinery we choose/develop)
        summary = dict(OUTPUT_REPORT)

        # We should decide here what variable name to use request vs. workflow
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

        try:
            self.updateCaches()
        except RuntimeWarning as ex:
            msg = "All retries exhausted! Last error was: '%s'" % str(ex)
            msg += "\nRetrying to update caches again in the next cycle."
            self.logger.error(msg)
            self.updateReportDict(summary, "error", msg)
            return summary
        except Exception as ex:
            msg = "Unknown exception updating caches. Error: %s" % str(ex)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

        # this one is put here just for example.
        self.updateReportDict(summary, "ddm_request_id", 42)
        return summary

    def makeSubscriptions(self, workflows=[]):
        """
        The common function to make the final subscriptions. It depends on the
        default Data Management System configured through msConfig. Based on that
        The relevant service wrapper is called.
        :return: A list of results from the REST interface of the DMS in question
        """

        # NOTE:
        #    Here is just an example construction of the function. None of the
        #    data structures used to visualise it is correct. To Be Updated
        results = []
        if self.msConfig['defaultDataManSys'] == 'DDM':
            # TODO: Here to put the dryrun mode: True/False
            ddm = DDM(
                url=self.msConfig['ddmUrl'],
                logger=self.logger,
                enableDataPlacement=self.msConfig['enableDataPlacement'])

            ddmReqList = []
            for workflow in workflows:
                for output in workflow['output']:
                    ddmReqList.append(DDMReqTemplate('copy', item=output))

            if self.msConfig['enableAggSubscr']:
                results = ddm.makeAggRequests(ddmReqList, aggKey='item')
            else:
                for ddmReq in ddmReqList:
                    results.append(ddm.makeRequests(ddmReqList, aggKey='item'))

        elif self.msConfig['defaultDataManSys'] == 'PhEDEx':
            pass

        elif self.msConfig['defaultDataManSys'] == 'Rucio':
            pass

        return results

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status.
        NOTE: to be taken from MSTransferor with minor changes
        """

        # NOTE:
        #    If we are about to use an additional database for book keeping like
        #    MongoDb, we can fetch up to 'limitRequestsPerCycle' and keep track
        #    their status.

        # The following is taken from MSMonitor, just for an example.
        # get requests from ReqMgr2 data-service for given status
        # here with detail=False we get back list of records
        requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
        self.logger.info('  retrieved %s requests in status: %s', len(requests), reqStatus)

        return requests

    def _pushToMongo(self, reqStatus):
        """
        An auxiliary function to push documents with workflow/request
        representation into mongoDB
        """
        pass
