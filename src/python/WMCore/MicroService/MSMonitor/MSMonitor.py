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
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.Services.Rucio.Rucio import Rucio


class MSMonitor(MSCore):
    """
    MSMonitor class provide whole logic behind
    the transferor monitoring module.
    """

    def __init__(self, msConfig, logger=None):
        super(MSMonitor, self).__init__(msConfig, logger=logger)
        # update interval is used to check records in CouchDB and update them
        # after this interval, default 6h
        self.updateInterval = self.msConfig.get('updateInterval', 6 * 60 * 60)
        self.rucio = Rucio(acct=self.msConfig['rucioAccount'],
                           hostUrl=self.msConfig['rucioUrl'],
                           authUrl=self.msConfig['rucioAuthUrl'],
                           configDict={"logger": self.logger, "user_agent": "WMCore-MSMonitor"})

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
            skippedWorkflows = self.getTransferInfo(transferRecords)
            requestsToStage = self.getCompletedWorkflows(transferRecords, campaigns)
            failedDocs = self.updateTransferDocs(transferRecords, skippedWorkflows)
            self.updateReportDict(summary, "success_transfer_doc_update",
                                  len(transferRecords) - len(failedDocs) - len(skippedWorkflows))
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
            msg += "%d completed their data transfers, " % len(requestsToStage)
            msg += "%d failed to contact the DM system and were skipped in this cycle and " % len(skippedWorkflows)
            msg += "%d failed to get their transfer documents updated in CouchDB." % len(failedDocs)
            self.logger.info(msg)
        except Exception as ex:
            msg = "Unknown exception processing the transfer records. Error: %s" % str(ex)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
        return summary

    def getTransferInfo(self, transferRecords):
        """
        Contact the data management tool in order to get a status
        update for the transfer request.
        :param transferRecords: list of transfer records
        :return skippedWorkflows: a list of workflow names which a call to the data
        management system did not succeed
        """
        # FIXME: create concurrent rucio calls using multi_getdata
        skippedWorkflows = []
        tstamp = int(time.time())
        for doc in transferRecords:
            self.logger.debug("Checking transfers for: %s", doc['workflowName'])
            if not doc['transfers']:
                # nothing to be done, simply update the document last timestamp
                doc['lastUpdate'] = tstamp
                continue

            try:
                for rec in doc['transfers']:
                    # obtain new transfer ids and completion for given dataset
                    completion = self._getRucioTransferstatus(rec['transferIDs'])
                    rec['completion'].append(round(completion, 3))
                doc['lastUpdate'] = tstamp
            except Exception as exc:
                msg = "Unknown exception checking workflow %s. Error: %s"
                self.logger.exception(msg, doc['workflowName'], str(exc))
                skippedWorkflows.append(doc['workflowName'])
        return skippedWorkflows

    def _getRucioTransferstatus(self, rulesList):
        """
        Given a list of Rucio rules ID - for a given input data - check the
        overall transfer status from Rucio
        :param rulesList: list of rules ID
        :return: the overall transfers percent completion

        The Rucio getRule API returns data in the form of:
            {u'account': u'transfer_ops',
             u'grouping': u'ALL',
             u'id': u'40cbe787a42b4f6e991611f6fac3bb11',
             u'locked': True,
             u'locks_ok_cnt': 8,
             u'locks_replicating_cnt': 0,
             u'locks_stuck_cnt': 0,
             u'meta': None,
             etc etc
        NOTE: completion in Rucio is different than in PhEDEx. PhEDEx gives the
        percentage value; while Rucio gives the ratio (0 - 1).
        """
        completion = []
        for ruleID in rulesList:
            # if we query by dataset and the subscription was at block level,
            # we get an empty response. So always wildcard the block parameter
            data = self.rucio.getRule(ruleID)
            if not data:
                msg = "Failed to retrieve rule information from Rucio for rule ID: {}".format(ruleID)
                raise RuntimeError(msg)

            if data['state'] == "OK":
                lockCompletion = 100.0
            else:
                totalLocks = data['locks_ok_cnt'] + data['locks_replicating_cnt'] + data['locks_stuck_cnt']
                try:
                    lockCompletion = (data['locks_ok_cnt'] / totalLocks) * 100
                except ZeroDivisionError:
                    self.logger.warning("Rule does not have any lock counts yet. Rule data: %s", data)
                    lockCompletion = 0
            completion.append(lockCompletion)
            self.logger.info("Rule ID: %s has a completion rate of: %s%%", ruleID, lockCompletion)
            self.logger.debug("Rule ID: %s, DID: %s, state: %s, grouping: %s, rse_expression: %s",
                              ruleID, data['name'], data['state'], data['grouping'], data['rse_expression'])
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

    def updateTransferDocs(self, docs, workflowsToSkip):
        """
        Given a list of transfer documents, update all of them in
        ReqMgrAux database.
        :param docs: list of transfer docs
        :param workflowsToSkip: list of workflow names that should not be updated in CouchDB
        :return: a list of request names that failed to be updated
        """
        failedWfs = []
        for rec in docs:
            if rec['workflowName'] in workflowsToSkip:
                self.logger.warning("Not updating transfer record in CouchDB for: %s", rec['workflowName'])
                continue
            if not self.reqmgrAux.updateTransferInfo(rec['workflowName'], rec):
                # then it failed to update the doc, ReqMgrAux client is logging it already
                failedWfs.append(rec['workflowName'])
        return failedWfs
