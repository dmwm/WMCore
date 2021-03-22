from __future__ import division, print_function

from builtins import bytes, str
from future.utils import viewitems

import time
from WMCore.REST.HeartbeatMonitorBase import HeartbeatMonitorBase
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer


def _getCampaign(propertyValue):
    """
    WMStats server can return this argument in three different data types.
    If there are multiple values, return only the first one.
    """
    if not propertyValue:  # Empty string is Ok; it should never be None though
        return ["NONE"]
    elif isinstance(propertyValue, (str, bytes)):
        return [propertyValue]
    # then it's a list
    return propertyValue


def _getRequestNumEvents(propertyValue):
    """
    WMStats server can return this argument in three different data types.
    If there are multiple values, return only the first one.
    """
    if not propertyValue:
        return 0
    elif isinstance(propertyValue, list):
        return propertyValue[0]
    # then it's integer
    return propertyValue


def initMetrics():
    """
    Initialize the metrics dictionary
    :param veryActiveStatus: a small list of active status
    """
    results = {"requestsByStatus": {},
               "requestsByStatusAndCampaign": {},
               "requestsByStatusAndPrio": {},
               "requestsByStatusAndNumEvts": {}}

    for st in ACTIVE_STATUS:
        results["requestsByStatus"].setdefault(st, 0)
        results["requestsByStatusAndNumEvts"].setdefault(st, 0)
        results["requestsByStatusAndCampaign"].setdefault(st, {})
        results["requestsByStatusAndPrio"].setdefault(st, {})

    return results


class HeartbeatMonitor(HeartbeatMonitorBase):

    def __init__(self, rest, config):
        super(HeartbeatMonitor, self).__init__(rest, config)
        self.producer = "reqmgr2"
        self.docTypeAMQ = "cms_%s_info" % self.producer

    def addAdditionalMonitorReport(self, config):
        """
        _addAdditionalMonitorReport_

        Collect general request information and post it to both central
        couchdb and MonIT. Items to fetch:
         * number of requests in each status (excluding archived ones)
         * for requests in assignment-approved:
           * their priority
           * their RequestNumEvents (if there is no input dataset)
         * for assignment-approved, assigned, acquired, running-* and completed:
           * number of requests in each campaign
        """
        self.logger.info("Collecting ReqMgr2 statistics...")
        wmstatsSvc = WMStatsServer(config.wmstatsSvc_url, logger=self.logger)

        results = initMetrics()

        inputConditon = {}
        outputMask = ["RequestStatus", "RequestType", "RequestPriority",
                      "Campaign", "RequestNumEvents"]
        startT = int(time.time())
        for reqInfo in wmstatsSvc.getFilteredActiveData(inputConditon, outputMask):
            status = reqInfo['RequestStatus']
            results['requestsByStatus'][status] += 1

            for campaign in _getCampaign(reqInfo["Campaign"]):
                results["requestsByStatusAndCampaign"][status].setdefault(campaign, 0)
                results['requestsByStatusAndCampaign'][status][campaign] += 1

            requestPrio = reqInfo['RequestPriority']
            results["requestsByStatusAndPrio"][status].setdefault(requestPrio, 0)
            results['requestsByStatusAndPrio'][status][requestPrio] += 1

            results['requestsByStatusAndNumEvts'][status] += _getRequestNumEvents(reqInfo['RequestNumEvents'])

        endT = int(time.time())
        results["total_query_time"] = endT - startT

        if self.postToAMQ:
            allDocs = self.buildMonITDocs(results)
            self.uploadToAMQ(allDocs)

        return results

    def buildMonITDocs(self, stats):
        """
        Given the statistics that are uploaded to wmstats, create different
        documents to post to MonIT AMQ (aggregation-friendly docs).
        """
        commonInfo = {"agent_url": "reqmgr2"}

        docs = []
        for status, numReq in viewitems(stats['requestsByStatus']):
            doc = {}
            doc["type"] = "reqmgr2_status"
            doc["request_status"] = status
            doc["num_requests"] = numReq
            doc.update((commonInfo))
            docs.append(doc)

        for status, items in viewitems(stats['requestsByStatusAndCampaign']):
            for campaign, numReq in viewitems(items):
                doc = {}
                doc["type"] = "reqmgr2_campaign"
                doc["request_status"] = status
                doc["campaign"] = campaign
                doc["num_requests"] = numReq
                doc.update((commonInfo))
                docs.append(doc)

        for status, items in viewitems(stats['requestsByStatusAndPrio']):
            for prio, numReq in viewitems(items):
                doc = {}
                doc["type"] = "reqmgr2_prio"
                doc["request_status"] = status
                doc["request_priority"] = prio
                doc["num_requests"] = numReq
                doc.update((commonInfo))
                docs.append(doc)

        for status, evts in viewitems(stats['requestsByStatusAndNumEvts']):
            doc = {}
            doc["type"] = "reqmgr2_events"
            doc["request_status"] = status
            doc["total_num_events"] = evts
            doc.update((commonInfo))
            docs.append(doc)

        self.logger.info("%i docs created to post to MonIT", len(docs))
        return docs
