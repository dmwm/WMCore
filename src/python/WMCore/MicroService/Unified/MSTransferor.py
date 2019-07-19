#!/usr/bin/env python
"""
_MSTransferor_

Class to hold the whole logic behind the transferor module
"""
from __future__ import division, print_function

import hashlib
from pprint import pformat
from Utils.IteratorTools import grouper
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux


class MSTransferor(object):
    def __init__(self, microConfig, status, logger=None):
        """
        Runs the basic setup and initialization for the MS Transferor module
        :param microConfig: microservice configuration
        """
        self.msConfig = microConfig
        self.status = status
        self.uConfig = {}
        self.logger = getMSLogger(microConfig['verbose'], logger=logger)
        self.reqmgr2 = ReqMgr(microConfig['reqmgrUrl'], logger=self.logger)
        self.reqmgrAux = ReqMgrAux(microConfig['reqmgrUrl'], httpDict={'cacheduration': 60}, logger=self.logger)
        # eventually will change it to Rucio
        self.phedex = PhEDEx(httpDict={'cacheduration': 10 * 60},
                             dbsUrl=microConfig['dbsUrl'], logger=self.logger)

    def prep(self):
        """
        Runs any preparation tasks before executing the actual algorithm.
        For now:
         * fetches the unified configuration
        :return: False if it fail to run this action, otherwise True
        """
        self.uConfig = self.reqmgrAux.getUnifiedConfig(docName="config")
        return bool(self.uConfig)

    def execute(self):
        """
        Executes the whole transferor logic
        :param reqStatus: request status to that matters for this module
        :return:
        """
        if not self.prep():
            self.logger.warning("Failed to fetch the latest unified config. Skipping this cycle")
            return
        self.uConfig = self.uConfig[0]

        requestRecords = []
        try:
            # get requests from ReqMgr2 data-service for given status
            requests = self.reqmgr2.getRequestByStatus([self.status], detail=True)
            if requests:
                requests = requests[0]
            self.logger.info("### transferor found %s requests in '%s' state", len(requests), self.status)
            if requests:
                for _, wfData in requests.iteritems():
                    requestRecords.append(self.requestRecord(wfData))
        except Exception as err:
            self.logger.exception('### transferor error: %s', str(err))

        if not requestRecords:
            return

        try:
            reqInfo = RequestInfo(self.msConfig, self.uConfig, self.logger)
            for reqSlice in grouper(requestRecords, 50):
                reqResults = reqInfo(reqSlice)
                self.logger.info("%d requests completely processed.", len(reqResults))
                self.logger.info("Working on the data subscription and status change...")
                # process all requests
                for req in reqResults:
                    reqName = req['name']
                    # perform transfer
                    tid = self.transferRequest(req)
                    if tid:
                        # Once all transfer requests were successfully made, update: assigned -> staging
                        self.logger.debug("### transfer request for %s successfull", reqName)
                        self.change(req, 'staging', '### transferor')
                        # if there is nothing to be transferred (no input at all),
                        # then update the request status once again staging -> staged
                        # self.change(req, 'staged', '### transferor')
        except Exception as err:  # general error
            self.logger.exception('### transferor error: %s', str(err))

    def post(self):
        """
        Runs any post tasks before exiting the execution cycle
        :return:
        """
        pass

    def requestRecord(self, wfData):
        """
        Selects only important information for a request dictionary

        Returns: a dictionary
        """
        datasets = []
        if "TaskChain" in wfData or "StepChain" in wfData:
            innerDicts = []
            for i in range(1, wfData.get("TaskChain", wfData.get("StepChain")) + 1):
                innerDicts.append(wfData.get("Task%d" % i, wfData.get("Step%d" % i)))
        else:
            # ReReco and DQMHarvesting
            innerDicts = [wfData]
        for item in innerDicts:
            for key in ['InputDataset', 'MCPileup', 'DataPileup']:
                dataset = item.get(key)
                if dataset:
                    datasets.append({'type': key, 'name': dataset})

        return {'name': wfData.get('RequestName'),
                'reqStatus': wfData.get('RequestStatus'),
                'SiteWhiteList': wfData.get('SiteWhitelist', []),
                'SiteBlackList': wfData.get('SiteBlacklist', []),
                'datasets': datasets,
                'campaign': []}

    def transferRequest(self, req):
        "Send request to Phedex and return status of request subscription"
        datasets = req.get('datasets', [])
        sites = req.get('sites', [])
        if datasets and sites:
            self.logger.debug("### creating subscription for: %s", pformat(req))
            subscription = PhEDExSubscription(datasets, sites, self.msConfig['group'])
            # TODO: implement how to get transfer id
            tid = hashlib.md5(str(subscription)).hexdigest()
            # TODO: when ready enable submit subscription step
            # self.phedex.subscribe(subscription)
            return tid

    def change(self, req, reqStatus, prefix='###'):
        """
        Change request status, internally it is done via PUT request to ReqMgr2:
        curl -X PUT -H "Content-Type: application/json" \
             -d '{"RequestStatus":"staging", "RequestName":"bla-bla"}' \
             https://xxx.yyy.zz/reqmgr2/data/request
        """
        self.logger.debug('%s updating %s status to %s', prefix, req['name'], reqStatus)
        try:
            if not self.msConfig['readOnly']:
                self.reqmgr2.updateRequestStatus(req['name'], reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))