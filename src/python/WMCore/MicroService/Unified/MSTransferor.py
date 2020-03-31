"""
File       : MSTransferor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSTransferor class provide whole logic behind
the transferor module.

This is NOT a thread-safe module, even though some internal
tasks might be extended to multi-threading in the future.
"""
# futures
from __future__ import division, print_function

# system modules
from httplib import HTTPException
from operator import itemgetter
from pprint import pformat
from retry import retry
from random import randint
from copy import deepcopy

# WMCore modules
from Utils.IteratorTools import grouper
from WMCore.MicroService.DataStructs.DefaultStructs import TRANSFEROR_REPORT,\
    TRANSFER_RECORD, TRANSFER_COUCH_DOC
from WMCore.MicroService.Unified.Common import gigaBytes, teraBytes
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.MicroService.Unified.RSEQuotas import RSEQuotas
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from Utils.EmailAlert import EmailAlert

def newTransferRec(dataIn):
    """
    Create a basic transfer record to be appended to a transfer document
    :param dataIn: dictionary with information relevant to this transfer doc
    :return: a transfer record dictionary
    """
    record = deepcopy(TRANSFER_RECORD)
    record["dataset"] = dataIn['name']
    record["dataType"] = dataIn['type']
    record["campaignName"] = dataIn['campaign']
    return record


def newTransferDoc(reqName, transferRecords):
    """
    Create a transfer document which is meant to be created in
    central CouchDB
    :param reqName: string with the workflow name
    :param transferRecords: list of dictionaries with transfer records
    :return: a transfer document dictionary
    """
    doc = dict(TRANSFER_COUCH_DOC)
    doc["workflowName"] = reqName
    doc["transfers"] = transferRecords
    return doc

class MSTransferor(MSCore):
    """
    MSTransferor class provide whole logic behind
    the transferor module.
    """

    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MS Transferor module
        :param microConfig: microservice configuration
        """
        super(MSTransferor, self).__init__(msConfig, logger)

        # url for fetching the storage quota
        self.msConfig.setdefault("detoxUrl",
                                 "http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt")
        # minimum percentage completion for dataset/blocks subscribed
        self.msConfig.setdefault("minPercentCompletion", 99)
        # minimum available storage to consider a resource good for receiving data
        self.msConfig.setdefault("minimumThreshold", 1 * (1000 ** 4))  # 1TB
        # limit MSTransferor to this amount of requests per cycle
        self.msConfig.setdefault("limitRequestsPerCycle", 500)
        # Send warning messages for any data transfer above this threshold.
        # Set to negative to ignore.
        self.msConfig.setdefault("warningTransferThreshold", 100. * (1000 ** 4))  # 100TB
        # Set default email settings
        self.msConfig.setdefault("toAddr", "cms-comp-ops-workflow-team@cern.ch")
        self.msConfig.setdefault("fromAddr", "noreply@cern.ch")
        self.msConfig.setdefault("smtpServer", "localhost")
        # enable or not the PhEDEx requests auto-approval (!request_only)
        if self.msConfig.setdefault("phedexRequestOnly", True):
            self.msConfig["phedexRequestOnly"] = "y"
        else:
            self.msConfig["phedexRequestOnly"] = "n"

        self.rseQuotas = RSEQuotas(self.msConfig['detoxUrl'], self.msConfig["quotaAccount"],
                                   self.msConfig["quotaUsage"], useRucio=self.msConfig["useRucio"],
                                   minimumThreshold=self.msConfig["minimumThreshold"],
                                   verbose=self.msConfig['verbose'], logger=logger)
        self.reqInfo = RequestInfo(msConfig, logger)

        self.cric = CRIC(logger=self.logger)
        self.inputMap = {"InputDataset": "primary",
                         "MCPileup": "secondary",
                         "DataPileup": "secondary"}
        self.uConfig = {}
        self.campaigns = {}
        self.psn2pnnMap = {}
        self.dsetCounter = 0
        self.blockCounter = 0
        self.emailAlert = EmailAlert(self.msConfig)

    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the transferor logic, e.g.:
         * quota from detox (or Rucio)
         * storage usage and available from Rucio (or PhEDEx)
         * unified configuration
         * all campaign configuration
         * PSN to PNN map from CRIC
        """
        self.logger.info("Updating RSE/PNN quota and usage")
        self.rseQuotas.fetchStorageQuota()
        self.rseQuotas.fetchStorageUsage(getattr(self, "rucio", self.phedex))
        self.rseQuotas.evaluateQuotaExceeded()
        if not self.rseQuotas.getNodeUsage():
            raise RuntimeWarning("Failed to fetch storage usage stats")

        self.logger.info("Updating all local caches...")
        self.dsetCounter = 0
        self.blockCounter = 0
        self.uConfig = self.unifiedConfig()
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        self.psn2pnnMap = self.cric.PSNtoPNNMap()
        if not self.uConfig:
            raise RuntimeWarning("Failed to fetch the unified configuration")
        elif not campaigns:
            raise RuntimeWarning("Failed to fetch the campaign configurations")
        elif not self.psn2pnnMap:
            raise RuntimeWarning("Failed to fetch PSN x PNN map from CRIC")
        else:
            # let's make campaign look-up easier and more efficient
            self.campaigns = {}
            for camp in campaigns:
                self.campaigns[camp['CampaignName']] = camp
        self.rseQuotas.printQuotaSummary()

    def execute(self, reqStatus):
        """
        Executes the whole transferor logic
        :param reqStatus: request status to process
        :return:
        """
        counterWorkflows = 0
        counterFailedRequests = 0
        counterSuccessRequests = 0
        summary = dict(TRANSFEROR_REPORT)
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
            self.updateReportDict(summary, "total_num_campaigns", len(self.campaigns))
            self.updateReportDict(summary, "nodes_out_of_space", list(self.rseQuotas.getOutOfSpaceRSEs()))
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

        # process all requests
        for reqSlice in grouper(requestRecords, 100):
            self.logger.info("Processing workflows from %d to %d.",
                             counterWorkflows + 1, counterWorkflows + len(reqSlice))
            # get complete requests information
            # based on Unified Transferor logic
            reqResults = self.reqInfo(reqSlice)
            self.logger.info("%d requests information completely processed.", len(reqResults))

            for wflow in reqResults:
                # first check which data is already in place and filter them out
                self.checkDataLocation(wflow)

                try:
                    success, transfers = self.makeTransferRequest(wflow)
                except Exception as ex:
                    success = False
                    msg = "Unknown exception while making Transfer Request for %s " % wflow.getName()
                    msg += "\tError: %s" % str(ex)
                    self.logger.exception(msg)
                if success:
                    self.logger.info("Transfer requests successful for %s. Summary: %s",
                                     wflow.getName(), pformat(transfers))                    # then create a document in ReqMgr Aux DB
                    if self.createTransferDoc(wflow.getName(), transfers):
                        self.logger.info("Transfer document successfully created in CouchDB for: %s", wflow.getName())
                        # then move this request to staging status
                        self.change(wflow.getName(), 'staging', self.__class__.__name__)
                        counterSuccessRequests += 1
                    else:
                        counterFailedRequests += 1
                else:
                    counterFailedRequests += 1
            # it can go slightly beyond the limit. It's evaluated for every slice
            if counterSuccessRequests >= self.msConfig["limitRequestsPerCycle"]:
                msg = "Transferor succeeded acting on %d workflows in this cycle. " % counterSuccessRequests
                msg += "Which exceeds the configuration limit set to: %s" % self.msConfig["limitRequestsPerCycle"]
                self.logger.info(msg)
                break
            counterWorkflows += len(reqSlice)

        self.logger.info("There were %d failed and %d success requests in this cycle",
                         counterFailedRequests, counterSuccessRequests)
        self.logger.info("%s subscribed %d datasets and %d blocks in this cycle",
                         self.__class__.__name__, self.dsetCounter, self.blockCounter)
        self.updateReportDict(summary, "success_request_transition", counterSuccessRequests)
        self.updateReportDict(summary, "failed_request_transition", counterFailedRequests)
        self.updateReportDict(summary, "num_datasets_subscribed", self.dsetCounter)
        self.updateReportDict(summary, "num_blocks_subscribed", self.blockCounter)
        self.updateReportDict(summary, "nodes_out_of_space", list(self.rseQuotas.getOutOfSpaceRSEs()))
        return summary

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status, sort them by priority
        and return a subset of each request with important information for the
        data placement algorithm.
        """
        self.logger.info("Fetching requests in status: %s", reqStatus)
        # get requests from ReqMgr2 data-service for given status
        reqData = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)

        # we need to first put these requests in order of priority, as done for GQ...
        orderedRequests = []
        for requests in reqData:
            orderedRequests = requests.values()
        orderedRequests.sort(key=itemgetter('RequestPriority'), reverse=True)

        return orderedRequests

    def checkDataLocation(self, wflow):
        """
        Check which data is already in place (according to the site lists)
        and remove those datasets/blocks from the data placement (next step).
        If workflow has XRootD/AAA enabled, data location can be outside of the
        SiteWhitelist.
        :param wflow: workflow object
        """
        pnns = self._getPNNs(wflow.getSitelist())
        for methodName in ("getPrimaryBlocks", "getParentBlocks"):
            primaryAAA = wflow.getReqParam("TrustSitelists")
            inputBlocks = getattr(wflow, methodName)()
            self.logger.info("Request %s has %d initial blocks from %s",
                             wflow.getName(), len(inputBlocks), methodName)

            for block, blockDict in inputBlocks.items():
                blockLocation = self._diskPNNs(blockDict['locations'])
                if primaryAAA and blockLocation:
                    msg = "Primary/parent block %s already in place (via AAA): %s" % (block, blockLocation)
                    self.logger.info(msg)
                    inputBlocks.pop(block)
                elif blockLocation:
                    commonLocation = pnns & set(blockLocation)
                    if commonLocation:
                        self.logger.info("Primary/parent block %s already in place: %s", block, commonLocation)
                        inputBlocks.pop(block)
            self.logger.info("Request %s has %d final blocks from %s",
                             wflow.getName(), len(getattr(wflow, methodName)()), methodName)

        pileupInput = wflow.getSecondarySummary()
        secondaryAAA = wflow.getReqParam("TrustPUSitelists")
        self.logger.info("Request %s with %d initial secondary dataset to be transferred",
                         wflow.getName(), len(pileupInput))
        for dset, dsetDict in pileupInput.items():
            datasetLocation = self._diskPNNs(dsetDict['locations'])
            if secondaryAAA and datasetLocation:
                self.logger.info("Secondary dataset %s already in place (via AAA): %s",
                                  dset, datasetLocation)
                pileupInput.pop(dset)
            elif datasetLocation:
                commonLocation = pnns & set(datasetLocation)
                if commonLocation:
                    self.logger.info("Secondary dataset %s already in place: %s", dset, commonLocation)
                    pileupInput.pop(dset)
        self.logger.info("Request %s with %d final secondary dataset to be transferred",
                         wflow.getName(), len(wflow.getSecondarySummary()))

    def makeTransferRequest(self, wflow):
        """
        Send request to PhEDEx and return status of request subscription
        This method does the following:
          1. return if there is no workflow data to be transferred
          2. check if the data input campaign is in the database, skip if not
          3. _getValidSites: using the workflow site lists and the campaign configuration,
             find a common list of sites (converted to PNNs). If the PNN is out of quota,
             it's also removed from this list
          4. create the transfer record dictionary
          5. for every final node
             5.1. if it's a pileup dataset, pick a random node and subscribe the whole dataset
             5.2. else, retrieve chunks of blocks to be subscribed (evenly distributed)
             5.3. update node usage with the amount of data subscribed
          6. re-evaluate nodes with quota exceeded
          7. return the transfer record, with a list of transfer IDs
        :param wflow: workflow object
        :return: boolean whether it succeeded or not, and a subscription dictionary {"dataset":transferIDs}
        """
        response = []
        success = True
        if not (wflow.getParentBlocks() or wflow.getPrimaryBlocks() or wflow.getSecondarySummary()):
            self.logger.info("Request %s does not have any further data to transfer", wflow.getName())
            return success, response

        self.logger.info("Handling data subscriptions for request: %s", wflow.getName())

        for dataIn in wflow.getDataCampaignMap():
            if dataIn["type"] == "parent":
                msg = "Skipping 'parent' data subscription (done with the 'primary' data), for: %s" % dataIn
                self.logger.info(msg)
                continue
            elif dataIn["type"] == "secondary" and dataIn['name'] not in wflow.getSecondarySummary():
                # secondary already in place
                continue
            if dataIn['campaign'] not in self.campaigns:
                msg = "Data placement can't proceed because campaign '%s' was not found." % dataIn["campaign"]
                msg += " Skipping this workflow until the campaign gets created."
                self.logger.warning(msg)
                return False, response

            nodes = self._getValidSites(wflow, dataIn)
            if not nodes:
                msg = "There are no RSEs with available space for %s. " % wflow.getName()
                msg += "Skipping this workflow until RSEs get enough free space"
                self.logger.warning(msg)
                return False, response

            transRec = newTransferRec(dataIn)
            for blocks, dataSize, idx in self._decideDataDestination(wflow, dataIn, len(nodes)):
                if not blocks and dataIn["type"] == "primary":
                    # no valid files in any blocks, it will likely fail in global workqueue
                    return success, response
                if blocks:
                    subLevel = "block"
                    data = {dataIn['name']: blocks}
                else:
                    # then it's a dataset level subscription
                    subLevel = "dataset"
                    data = None

                subscription = PhEDExSubscription(datasetPathList=dataIn['name'],
                                                  nodeList=nodes[idx],
                                                  group=self.msConfig['quotaAccount'],
                                                  level=subLevel,
                                                  priority="low",
                                                  request_only=self.msConfig["phedexRequestOnly"],
                                                  blocks=data,
                                                  comments="WMCore MicroService automated subscription")
                msg = "Creating '%s' level subscription for %s dataset: %s" % (subscription.level,
                                                                               dataIn['type'],
                                                                               dataIn['name'])
                if wflow.getParentDataset():
                    msg += ", where parent blocks have also been added for dataset: %s" % wflow.getParentDataset()
                self.logger.info(msg)

                if self.msConfig.get('enableDataTransfer', True):
                    # Force request-only subscription
                    # to any data transfer going above some threshold (do not auto-approve)
                    aboveWarningThreshold = self.msConfig.get('warningTransferThreshold') > 0. and \
                        dataSize > self.msConfig.get('warningTransferThreshold')
                    if aboveWarningThreshold and subscription.request_only != "y":
                        subscription.request_only = "y"

                    # Then make the data subscription, for real!!!
                    success, transferId = self._subscribeData(subscription, wflow.getName(), dataIn['name'])
                    if not success:
                        break
                    if transferId:
                        transRec['transferIDs'].add(transferId)

                    # Warn about data transfer subscriptions going above some treshold
                    if aboveWarningThreshold:
                        emailSubject = "[MS] Large pending data transfer under request id: {transferid}".format(
                            transferid=transferId)
                        emailMsg = "Workflow: {}\nhas a large amount of ".format(wflow.getName())
                        emailMsg += "data subscribed: {} TB,\n".format(teraBytes(dataSize))
                        emailMsg += "for {} data: {}.""".format(dataIn['type'], dataIn['name'])
                        self.emailAlert.send(emailSubject, emailMsg)
                        self.logger.info(emailMsg)

                    # and update some instance caches
                    self.rseQuotas.updateNodeUsage(nodes[idx], dataSize)
                    if subLevel == 'dataset':
                        self.dsetCounter += 1
                    else:
                        self.blockCounter += len(blocks)
                else:
                    self.logger.info("DRY-RUN: making subscription: %s", subscription)

            transRec['transferIDs'] = list(transRec['transferIDs'])
            response.append(transRec)

        # once the workflow has been completely processed, update the node usage
        self.rseQuotas.evaluateQuotaExceeded()
        return success, response

    def _subscribeData(self, subscriptionObj, wflowName, dsetName):
        """
        Make the actual PhEDEx subscription - or create a Rucio rule - for the
        input data placement
        :param subscriptionObj:
        :param wflowName:
        :param dsetName:
        :return:
        """
        success, transferId = True, 0
        try:
            if hasattr(self, "rucio"):
                # FIXME: then it should create a Rucio rule instead
                res = self.phedex.subscribe(subscriptionObj)
            else:
                res = self.phedex.subscribe(subscriptionObj)
            self.logger.debug("Subscription done, result: %s", res)
        except HTTPException as ex:
            # It might be that the block has no more replicas (all files invalidated)
            # let it go and fail at global workqueue level
            msg = "Subscription failed for workflow: %s and dataset: %s " % (wflowName, dsetName)
            if ex.status == 400 and "request matched no data in TMDB" in ex.reason:
                msg += "because data cannot be found in TMDB. Bypassing this/these blocks..."
                self.logger.warning(msg)
            else:
                msg += "with status code: %s and result: %s. " % (ex.status, ex.result)
                msg += "It will be retried in the next cycle"
                self.logger.error(msg)
                success = False
        except Exception as ex:
            msg = "Unknown exception while subscribing data for workflow: %s " % wflowName
            msg += "and dataset: %s. Will retry again later. Error details: %s" % (dsetName, str(ex))
            self.logger.exception(msg)
            success = False
        else:
            transferId = res['phedex']['request_created'][0]['id']

        return success, transferId

    def _getValidSites(self, wflow, dataIn):
        """
        Given a workflow object and the data short summary, find out
        the Campaign name, the workflow SiteWhitelist, map the PSNs to
        PNNs and finally remove PNNs without space
        can still receive data
        :param wflow: the workflow object
        :param dataIn: short summary of data to be transferred
        :return: a unique and ordered list of PNNs to take data
        """
        dsetName = dataIn["name"]
        campConfig = self.campaigns[dataIn['campaign']]
        psns = wflow.getSitelist()

        if dataIn["type"] == "primary":
            if campConfig['SiteWhiteList']:
                psns = set(psns) & set(campConfig['SiteWhiteList'])
            if campConfig['SiteBlackList']:
                psns = set(psns) - set(campConfig['SiteBlackList'])
        elif dataIn["type"] == "secondary":
            # if the dataset has a location list, use solely that one
            if campConfig['Secondaries'].get(dsetName, []):
                psns = set(psns) & set(campConfig['Secondaries'][dsetName])
            else:
                if dsetName.startswith("/Neutrino"):
                    # different PU type use different campaign attributes...
                    psns = set(psns) & set(campConfig['SecondaryLocation'])
                else:
                    if campConfig['SiteWhiteList']:
                        psns = set(psns) & set(campConfig['SiteWhiteList'])
                    if campConfig['SiteBlackList']:
                        psns = set(psns) - set(campConfig['SiteBlackList'])

        self.logger.info("  final list of PSNs to be use: %s", psns)
        pnns = self._getPNNs(psns)

        self.logger.info("List of out-of-space RSEs dropped for '%s' is: %s",
                         wflow.getName(), pnns & self.rseQuotas.getOutOfSpaceRSEs())
        return list(pnns & self.rseQuotas.getAvailableRSEs())


    def _decideDataDestination(self, wflow, dataIn, numNodes):
        """
        Given a global list of blocks and the campaign configuration,
        decide which blocks have to be transferred and to where.
        :param wflow: workflow object
        :param dataIn: dictionary with a summary of the data to be placed
        :param numNodes: amount of nodes/RSEs that can receive data
        :return: yield a block list, the total chunk size and a node index
        """
        # FIXME: implement multiple copies (MaxCopies > 1)
        blockList = []
        dsetName = dataIn["name"]

        ### NOTE: data placement done in a block basis
        if dataIn["type"] == "primary":
            listBlockSets, listSetsSize = wflow.getChunkBlocks(numNodes)
            if not listBlockSets:
                self.logger.warning("  found 0 primary/parent blocks for dataset: %s, moving on...", dsetName)
                yield blockList, 0, 0
            for idx, blocksSet in enumerate(listBlockSets):
                self.logger.info("Have a chunk of %d blocks (%s GB) for dataset: %s",
                                 len(blocksSet), gigaBytes(listSetsSize[idx]), dsetName)
                yield blocksSet, listSetsSize[idx], idx
        ### NOTE: data placement done in a dataset basis
        elif dataIn["type"] == "secondary":
            # secondary datasets are transferred as a whole, until better days...
            dsetSize = wflow.getSecondarySummary()
            dsetSize = dsetSize[dsetName]['dsetSize']
            # randomly pick one of the PNNs to put the whole pileup dataset in
            idx = randint(0, numNodes - 1)
            self.logger.info("Have whole PU dataset: %s (%s GB)", dsetName, gigaBytes(dsetSize))
            yield blockList, dsetSize, idx

    def _getFinalPNNs(self, psns):
        """
        Given a list of sites/PSNs, get their associated PNN (dropping
        PNNs invalid for data placement).
        Lastly, filter out PNNs with no more space left for data placement.
        :param psns: list of sites
        :return: a flat list of PNNs
        """
        self.logger.info("  final list of PSNs to be use: %s", psns)
        # TODO: how to evenly distribute data on sites with > 1 PNNs?
        pnns = set()
        for psn in psns:
            for pnn in self.psn2pnnMap.get(psn, []):
                if pnn == "T2_CH_CERNBOX" or pnn.startswith("T3_") or pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    pass
                else:
                    pnns.add(pnn)

        self.logger.info("List of out-of-space RSEs dropped for this dataset: %s",
                         pnns & self.rseQuotas.getOutOfSpaceRSEs())
        return list(pnns & self.rseQuotas.getAvailableRSEs())

    def createTransferDoc(self, reqName, transferRecords):
        """
        Enrich the records returned from the data placement logic, wrap them up
        in a single document and post it to CouchDB
        :param reqName: the workflow name
        :param transferRecords: list of dictionaries records, or empty if no input at all
        :return: True if operation is successful, else False
        """
        doc = newTransferDoc(reqName, transferRecords)
        # Use the update/put method, otherwise it will fail if the document already exists
        if self.reqmgrAux.updateTransferInfo(reqName, doc):
            return True
        self.logger.error("Failed to create transfer document in CouchDB. Will retry again later.")
        return False

    def _getPNNs(self, psnList):
        """
        Given a list/set of PSNs, return a list of valid PNNs
        """
        pnns = set()
        for psn in psnList:
            for pnn in self.psn2pnnMap.get(psn, []):
                if pnn == "T2_CH_CERNBOX" or pnn.startswith("T3_") or pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    pass
                else:
                    pnns.add(pnn)
        return pnns

    def _diskPNNs(self, pnnList):
        """
        Provided a list of PNN locations, return another list of
        PNNs without mass storage and T3 sites
        :param pnnList: list of PNN strings
        :return: list of strings with filtered out PNNs
        """
        diskPNNs = set()
        for pnn in pnnList:
            if pnn == "T2_CH_CERNBOX" or pnn.startswith("T3_") or pnn.endswith("_MSS") or pnn.endswith("_Export"):
                pass
            else:
                diskPNNs.add(pnn)
        return list(diskPNNs)
