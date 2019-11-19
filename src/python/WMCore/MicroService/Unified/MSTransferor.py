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

# WMCore modules
from Utils.IteratorTools import grouper
from WMCore.MicroService.Unified.Common import gigaBytes
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.MicroService.Unified.RSEQuotas import RSEQuotas
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription


def newTransferRec(dataIn):
    """
    Create a basic transfer record to be appended to a transfer document
    :param dataIn: dictionary with information relevant to this transfer doc
    :return: a transfer record dictionary
    """
    transferRecord = {"dataset": dataIn['name'],
                      "dataType": dataIn['type'],
                      "transferIDs": set(),  # casted to list before going to couch
                      "campaignName": dataIn['campaign'],
                      "completion": [0.0]}
    return transferRecord


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
        detoxUrl = self.msConfig.get("detoxUrl",
                                     "http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt")
        quotaFraction = self.msConfig.get("quotaUsage", 0.8)  # use only 80% of the quota
        # now a PhEDEx group name, then the Rucio account name in the near future
        dataAcct = self.msConfig.get("quotaAccount", "DataOps")
        # minimum available storage to consider a resource good for receiving data
        minimumThreshold = self.msConfig.get("minimumThreshold", 1 * (1000 ** 4))  # 1TB

        self.rseQuotas = RSEQuotas(detoxUrl, dataAcct, quotaFraction, useRucio=hasattr(self, "rucio"),
                                   logger=logger, verbose=self.msConfig.get('verbose'),
                                   minimumThreshold=minimumThreshold)
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
        # also clear the RequestInfo pileup cache
        self.reqInfo.clearPileupCache()
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

    def execute(self, reqStatus):
        """
        Executes the whole transferor logic
        :param reqStatus: request status to process
        :return:
        """
        try:
            requestRecords = self.getRequestRecords(reqStatus)
            self.logger.info('  retrieved %s requests.', len(requestRecords))
        except Exception as err:  # general error
            self.logger.exception('Unknown exception while fetching requests from ReqMgr2', str(err))

        try:
            self.updateCaches()
        except RuntimeWarning as ex:
            msg = "All retries exhausted! Last error was: '%s'" % str(ex)
            msg += "\nRetrying to update caches again in the next cycle."
            self.logger.error(msg)
            return
        except Exception as ex:
            self.logger.exception("Unknown exception updating caches. Error: %s", str(ex))
            return

        # process all requests
        for reqSlice in grouper(requestRecords, 100):
            # get complete requests information
            # based on Unified Transferor logic
            reqResults = self.reqInfo(reqSlice)
            self.logger.info("%d requests completely processed.", len(reqResults))
            # process all requests
            for wflow in reqResults:
                self.logger.info("Working on the data subscription and status change for: %s", wflow)
                # perform transfer
                success, transfers = self.makeTransferRequest(wflow)
                if success:
                    self.logger.info("Transfers successfull for %s. Summary: %s", wflow.getName(), pformat(transfers))
                    # then create a document in ReqMgr Aux DB
                    if self.createTransferDoc(wflow.getName(), transfers):
                        self.logger.info("Transfer document successfully created in CouchDB for: %s", wflow.getName())
                        # then move this request to staging status
                        self.change(wflow.getName(), 'staging', self.__class__.__name__)
        self.logger.info("%s subscribed %d datasets and %d blocks in this cycle",
                         self.__class__.__name__, self.dsetCounter, self.blockCounter)

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status, sort them by priority
        and return a subset of each request with important information for the
        data placement algorithm.
        """
        self.logger.info("Fetching requests in status: %s", reqStatus)
        # get requests from ReqMgr2 data-service for given statue
        reqData = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)

        # we need to first put these requests in order of priority, as done for GQ...
        orderedRequests = []
        for requests in reqData:
            orderedRequests = requests.values()
        orderedRequests.sort(key=itemgetter('RequestPriority'), reverse=True)

        return orderedRequests

    def makeTransferRequest(self, wflow):
        """
        Send request to PhEDEx and return status of request subscription
        :param wflow: workflow object
        :return: subscription dictionary {"dataset":transferIDs}
        """
        success = True
        self.logger.info("Handling data subscriptions for request: %s", wflow.getName())

        response = []
        for dataIn in wflow.getDataCampaignMap():
            if dataIn["type"] == "parent":
                msg = "Skipping 'parent' data subscription (done with the 'primary' data), for: %s" % dataIn
                self.logger.info(msg)
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
                                                  group=self.msConfig['group'],
                                                  level=subLevel,  # use dataset for premix
                                                  priority="low",
                                                  request_only="y",
                                                  blocks=data,
                                                  comments="WMCore MicroService automated subscription")
                msg = "Creating '%s' level subscription for %s dataset: %s" % (subscription.level,
                                                                               dataIn['type'],
                                                                               dataIn['name'])
                if wflow.getParentDataset():
                    msg += ", where parent blocks have also been added for dataset: %s" % wflow.getParentDataset()
                self.logger.info(msg)

                if self.msConfig.get('readOnly', True):
                    self.logger.info("TODO readOnly mode, subscription not made. Faking id to 1111")
                    transRec['transferIDs'].add(1111)  # any fake number
                else:
                    # Then make the data subscription, for real!!!
                    success, transferId = self._subscribeData(subscription, wflow.getName(), dataIn['name'])
                    if not success:
                        break
                    if transferId:
                        transRec['transferIDs'].add(transferId)

                    # and update some instance caches
                    self.rseQuotas.updateNodeUsage(nodes[idx], dataSize)
                    if subLevel == 'dataset':
                        self.dsetCounter += 1
                    else:
                        self.blockCounter += len(blocks)

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
        pnns = set()
        for psn in psns:
            for pnn in self.psn2pnnMap.get(psn, []):
                if pnn == "T2_CH_CERNBOX" or pnn.startswith("T3_") or pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    pass
                else:
                    pnns.add(pnn)

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
            for idx in range(numNodes):
                self.logger.info("Have whole PU dataset: %s (%s GB)", dsetName, gigaBytes(dsetSize[dsetName]))
                yield blockList, dsetSize[dsetName], idx

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
        doc = {"workflowName": reqName,
               "lastUpdate": 0,
               "transfers": transferRecords}
        resp = self.reqmgrAux.postTransferInfo(reqName, doc)
        if resp and resp[0].get("ok", False):
            return True
        msg = "Failed to create transfer document in CouchDB. Will retry again later."
        msg += "Error: %s" % resp
        self.logger.error(msg)
        return False
