"""
File       : MSMonitor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSMonitor class provide whole logic behind
the transferor monitoring module.
"""
# futures
from __future__ import division, print_function

# system modules
import time

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import MONITOR_REPORT
from WMCore.MicroService.Unified.MSCore import MSCore


class MSMonitor(MSCore):
    """
    MSMonitor class provide whole logic behind
    the transferor monitoring module.
    """

    def __init__(self, msConfig, logger=None):
        super(MSMonitor, self).__init__(msConfig, logger)
        # update interval is used to check records in CouchDB and update them
        # after this interval, default 6h
        self.updateInterval = self.msConfig.get('updateInterval', 6 * 60 * 60)

    def updateCaches(self):
        """
        Fetch some data required for the monitoring logic, e.g.:
         * all campaign configuration
         * all transfer records from backend DB
        :return: True if all of them succeeded, else False
        """
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        transferRecords = self.reqmgrAux.getTransferInfo('ALL_DOCS')
        cdict = {}
        if not campaigns:
            self.logger.warning("Failed to fetch campaign configurations")
        if not transferRecords:
            self.logger.warning("Failed to fetch transfer records")
        else:
            for camp in campaigns:
                cdict[camp['CampaignName']] = camp
        return cdict, transferRecords

    def filterTransferDocs(self, requests, transferDocs):
        """
        Given a list of requests in the `staging` status and all the
        transfer documents; select the transfer documents that:
         * match against a workflow in requests
         * haven't been updated over the last updateInterval seconds
        :param requests: list of workflow names
        :param transferDocs: list of transfer documents
        :return: a filtered out list of transfer documents
        """
        now = time.time()
        newTransferDocs = []
        self.logger.info("Matching %d requests to %d transfer documents...",
                         len(requests), len(transferDocs))
        for record in transferDocs:
            if record['workflowName'] in requests:
                if now - record['lastUpdate'] > self.updateInterval:
                    newTransferDocs.append(record)
        msg = "Only %d transfer documents passed the status and timestamp filter."
        self.logger.info(msg, len(newTransferDocs))
        return newTransferDocs

    def execute(self, reqStatus):
        """
        Executes the MS monitoring logic, see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Monitor

        :param reqStatus: request status to process
        :return: a summary of the activity of the last cycle
        """
        summary = dict(MONITOR_REPORT)
        try:
            # get requests from ReqMgr2 data-service for given status
            # here with detail=False we get back list of records
            requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
            self.logger.info('  retrieved %s requests in status: %s', len(requests), reqStatus)

            campaigns, transferRecords = self.updateCaches()
            self.updateReportDict(summary, "total_num_campaigns", len(campaigns))
            self.updateReportDict(summary, "total_num_transfers", len(transferRecords))
            if not campaigns or not transferRecords:
                # then wait until the next cycle
                msg = "Failed to fetch data from one of the data sources. Retrying again in the next cycle"
                self.logger.error(msg)
                self.updateReportDict(summary, "error", msg)
                return summary
            transferRecords = self.filterTransferDocs(requests, transferRecords)
            self.updateReportDict(summary, "filtered_transfer_docs", len(transferRecords))
        except Exception as ex:  # general error
            msg = 'Unknown exception bootstrapping the MSMonitor thread. Error: %s', str(ex)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

        try:
            # keep track of request and their new statuses
            self.getTransferInfo(transferRecords)
            requestsToStage = self.getCompletedWorkflows(transferRecords, campaigns)
            failedDocs = self.updateTransferDocs(transferRecords)
            self.updateReportDict(summary, "success_transfer_doc_update",
                                  len(transferRecords) - len(failedDocs))
            self.updateReportDict(summary, "failed_transfer_doc_update", len(failedDocs))
            # finally, update statuses for requests
            for reqName in requestsToStage:
                if reqName in failedDocs:
                    msg = "Can't proceed with status transition for %s, because" % reqName
                    msg += "the transfer document failed to get updated"
                    self.logger.warning(msg)
                    continue
                self.change(reqName, 'staged', self.__class__.__name__)
            self.updateReportDict(summary, "request_status_updated",
                                  summary['success_transfer_doc_update'] - summary['failed_transfer_doc_update'])
            msg = "%s processed %d transfer records, where " % (self.__class__.__name__, len(transferRecords))
            msg += "%d completed their data transfers and " % len(requestsToStage)
            msg += "%d failed to get their transfer documents updated in CouchDB." % len(failedDocs)
            self.logger.info(msg)
        except Exception as ex:
            msg = "Unknown exception processing the transfer records. Error: %s", str(ex)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
        return summary

    def getTransferInfo(self, transferRecords):
        """
        Contact the data management tool in order to get a status
        update for the transfer request.
        :param transferRecords: list of transfer records
        """
        # FIXME: create concurrent phedex calls using multi_getdata
        tstamp = int(time.time())
        for doc in transferRecords:
            self.logger.debug("Checking transfers for: %s", doc['workflowName'])
            for rec in doc.get('transfers', []):
                # obtain new transfer ids and completion for given dataset
                completion = self._getTransferstatus(rec['dataset'], rec['transferIDs'])
                # Per Alan request, we'll update only completion and not tids
                rec['completion'].append(round(completion, 2))
            doc['lastUpdate'] = tstamp

    def _getTransferstatus(self, dataset, requestList):
        """
        Fetch the transfer request status from the data management tool
        :param dataset: dataset name string
        :param requestList: list of request IDs
        :return: the transfer percent completion normalized to the nodes

        The subscription API response structure is something like:
        {u'phedex': {u'call_time': 0.09359,
                     u'dataset': [{u'block': [{u'bytes': 2827742840,
                                               u'files': 3,
                                               u'id': u'7702606',
                                               u'is_open': u'n',
                                               u'name': u'/HLTPhysicsIsolatedBunch/Run2016H-v1/RAW#92049f6e-92f9-11e6-b150-001e67abf228',
                                               u'subscription': [{u'custodial': u'n',
                                                                  u'group': u'DataOps',
                                                                  u'percent_files': 100,
                                                                  ...
        """
        completion = []
        for reqId in requestList:
            # if we query by dataset and the subscription was at block level,
            # we get an empty response. So always wildcard the block parameter
            if hasattr(self, "rucio"):
                # FIXME: then it should check the rule status
                data = self.phedex.subscriptions(block=dataset + '#*', request=reqId)
            else:
                data = self.phedex.subscriptions(block=dataset + '#*', request=reqId)
            if not data or not data['phedex']['dataset']:
                self.logger.error("Failed to retrieve information for dataset: %s and request ID: %s",
                                  dataset, reqId)
            else:
                self.logger.debug("Subscription result for dataset: %s and request ID: %s was: %s",
                                  dataset, reqId, data)
            # the response structure is nested as hell!
            for dsetRow in data['phedex']['dataset']:
                # TODO: check what happens when we subscribe both the primary and parent in the same request
                for blockRow in dsetRow['block']:
                    for subs in blockRow['subscription']:
                        if subs['percent_files'] is None:
                            completion.append(0)
                        else:
                            completion.append(int(subs['percent_files']))
        if not completion:
            return 0
        return sum(completion) / len(completion)

    def getCompletedWorkflows(self, transfers, campaigns):
        """
        Parse the transfer documents, compare against the campaign settings
        and decide whether the workflow is completed or not.
        :param transfers: list of transfers records
        :param campaigns: dictionary of campaigns
        :return: completion status
        """
        completedWfs = []
        for record in transfers:
            reqName = record['workflowName']
            if not record['transfers']:
                self.logger.info("%s OK, no input data transfers, move it on.", reqName)
                completedWfs.append(reqName)
                continue
            # check completion of all transfers
            statuses = []
            for transfer in record['transfers']:
                cdict = campaigns[transfer['campaignName']]
                # compare against the last completion number, which is from the last cycle execution
                if transfer['completion'][-1] >= cdict['PartialCopy'] * 100:
                    status = 1
                else:
                    status = 0
                statuses.append(status)
            if all(statuses):
                self.logger.info("%s OK, all transfers completed or above threshold, move it on.", reqName)
                completedWfs.append(reqName)
        return completedWfs

    def updateTransferDocs(self, docs):
        """
        Given a list of transfer documents, update all of them in
        ReqMgrAux database.
        :param docs: list of transfer docs
        :return: a list of request names that failed to be updated
        """
        failedWfs = []
        for rec in docs:
            if not self.reqmgrAux.updateTransferInfo(rec['workflowName'], rec):
                # then it failed to update the doc, ReqMgrAux client is logging it already
                failedWfs.append(rec['workflowName'])
        return failedWfs
