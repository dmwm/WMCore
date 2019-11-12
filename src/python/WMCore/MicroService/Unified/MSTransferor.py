"""
File       : MSTransferor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSTransferor class provide whole logic behind
the transferor module.
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
from WMCore.MicroService.Unified.Common import getDetoxQuota
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription


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
        self.msConfig.setdefault("quotaUsage", 0.8)  # use only 80% of the quota
        # now a PhEDEx group name, then the Rucio account name in the near future
        self.msConfig.setdefault("quotaAccount", "DataOps")

        self.reqInfo = RequestInfo(msConfig, logger)
        self.cric = CRIC(logger=self.logger)
        self.inputMap = {"InputDataset": "primary",
                         "MCPileup": "secondary",
                         "DataPileup": "secondary"}
        self.uConfig = {}
        self.campaigns = {}
        self.psn2pnnMap = {}
        # information about RSE/PNNs and their quota/usage
        self.nodesUsage = {}
        # list of RSE/PNNs with less than 1TB available, skipped from data placement
        self.fullRSEs = set()
        self.dsetCounter = 0
        self.blockCounter = 0

    @retry(tries=2, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the transferor logic, e.g.:
         * quota from detox (or Rucio)
         * storage usage and available from Rucio (or PhEDEx)
         * unified configuration
         * all campaign configuration
         * PSN to PNN map from CRIC
        """
        self._getStorageQuota()
        self._getStorageUsage()

        self.dsetCounter = 0
        self.blockCounter = 0
        self.fullRSEs = set()
        self.uConfig = self.unifiedConfig()
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        self.psn2pnnMap = self.cric.PSNtoPNNMap()
        # also clear the RequestInfo pileup cache
        self.reqInfo.clearPileupCache()
        if not self.uConfig:
            raise RuntimeWarning("Failed to fetch the unified configuration. Retrying...")
        elif not campaigns:
            raise RuntimeWarning("Failed to fetch the campaign configurations. Retrying...")
        elif not self.psn2pnnMap:
            raise RuntimeWarning("Failed to fetch PSN x PNN map from CRIC. Retrying...")
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
            self.logger.debug('### transferor found %s requests in %s state',
                              len(requestRecords), reqStatus)
        except Exception as err:  # general error
            self.logger.exception('### transferor error: %s', str(err))

        try:
            self.updateCaches()
        except RuntimeWarning:
            msg = "All retries exhausted! Retrying to update caches again in the next cycle"
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

            transRec = self._makeTransferRec(dataIn)
            for nodes, blocks in self._decideDataDestination(wflow, dataIn):
                if not nodes and not blocks:
                    # no valid files in any blocks, it will likely fail in global workqueue
                    return success, response
                if not blocks:
                    # then it's a dataset level subscription
                    subLevel = "dataset"
                    data = None
                else:
                    subLevel = "block"
                    data = {dataIn['name']: blocks}

                subscription = PhEDExSubscription(datasetPathList=dataIn['name'],
                                                  nodeList=nodes,
                                                  group=self.msConfig['group'],
                                                  level=subLevel,  # use dataset for premix
                                                  priority="low",
                                                  request_only="y",
                                                  blocks=data,
                                                  comments="WMCore MicroService automated subscription")
                msg = "Creating '%s' level subscription for %s dataset: %s" % (subscription.level,
                                                                               dataIn['type'],
                                                                               dataIn['name'])
                if dataIn['type'] == "parent":
                    msg += ", where parent blocks have also been added for dataset: %s" % wflow.getParentDataset()
                self.logger.info(msg)

                if self.msConfig.get('readOnly', True):
                    self.logger.info("TODO readOnly mode, subscription not made. Faking id to 1111")
                    transRec['transferIDs'].add(1111)  # any fake number
                else:
                    try:
                        if hasattr(self, "rucio"):
                            # FIXME: then it should create a Rucio rule instead
                            res = self.phedex.subscribe(subscription)
                        else:
                            res = self.phedex.subscribe(subscription)
                        self.logger.debug("Subscription done, result: %s", res)
                    except HTTPException as ex:
                        # It might be that the block has no more replicas (all files invalidated)
                        # let it go and fail at global workqueue level
                        msg = "Subscription failed for workflow: %s and dataset: %s " % (wflow.getName(),
                                                                                         dataIn['name'])
                        if ex.status == 400 and "request matched no data in TMDB" in ex.reason:
                            msg += "because data cannot be found in TMDB. Bypassing this/these blocks..."
                            self.logger.warning(msg)
                        else:
                            msg += "with status code: %s and result: %s. " % (ex.status, ex.result)
                            msg += "It will be retried in the next cycle"
                            self.logger.error(msg)
                            success = False
                            break
                    except Exception as ex:
                        msg = "Unknown exception while subscribing data for workflow: %s " % wflow.getName()
                        msg += "and dataset: %s. Will retry again later. Error details: %s" % (dataIn['name'],
                                                                                               str(ex))
                        self.logger.exception(msg)
                        success = False
                        break
                    else:
                        transferId = res['phedex']['request_created'][0]['id']
                        transRec['transferIDs'].add(transferId)
            transRec['transferIDs'] = list(transRec['transferIDs'])
            response.append(transRec)
        return success, response

    def _decideDataDestination(self, wflow, dataIn):
        """
        Given a global list of blocks and the campaign configuration,
        decide which blocks have to be transferred and to where.
        :param wflow: workflow object
        :param dataIn: dictionary with a summary of the data to be placed
        :return: yield the node and a list of block names to be transferred
        """
        # FIXME: implement multiple copies (MaxCopies > 1)
        blockList = []
        dsetName = dataIn["name"]

        campConfig = self.campaigns[dataIn['campaign']]
        psns = wflow.getSitelist()

        ### NOTE: data placement done in a block basis
        if dataIn["type"] == "primary":
            if campConfig['SiteWhiteList']:
                psns = set(psns) & set(campConfig['SiteWhiteList'])
            if campConfig['SiteBlackList']:
                psns = set(psns) - set(campConfig['SiteBlackList'])
            nodes = self._getFinalPNNs(psns)

            listBlockSets = wflow.getChunkBlocks(len(nodes))
            if not listBlockSets:
                # use empty nodes to avoid making a dataset level subscription
                self.logger.warning("  found 0 primary/parent blocks for dataset: %s, moving on...", dsetName)
                nodes = []
            for idx, blocksSet in enumerate(listBlockSets):
                self.logger.info("Placing %d blocks for dataset: %s at %s", len(blocksSet), dsetName, nodes[idx])
                self.blockCounter += len(blocksSet)
                yield nodes, blockList
        ### NOTE: data placement done in a dataset basis
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
            nodes = self._getFinalPNNs(psns)

            # secondary datasets are transferred as a whole, until better days...
            self.dsetCounter += 1
            for node in nodes:
                self.logger.info("Placing pileup dataset: %s at %s", dsetName, node)
                yield node, blockList

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
                if pnn == "T2_CH_CERNBOX":
                    self.logger.debug("%s maps to %s, skipping it.", psn, pnn)
                elif pnn.startswith("T3_"):
                    self.logger.debug("%s maps to a T3 PNN resource: %s, skipping it.", psn, pnn)
                elif pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    self.logger.debug("%s maps to a archival storage PNN: %s, skipping it.", psn, pnn)
                else:
                    pnns.add(pnn)

        self.logger.info("List of out-of-space RSEs dropped: %s", pnns & self.fullRSEs)
        return list(pnns - self.fullRSEs)

    def _makeTransferRec(self, dataIn):
        """
        Create a basic transfer record to be appended to a transfer document
        :param data:
        :return: a dictionary
        """
        transferRecord = {"dataset": dataIn['name'],
                          "dataType": dataIn['type'],
                          "transferIDs": set(),  # casted to list before going to couch
                          "campaignName": dataIn['campaign'],
                          "completion": [0.0]}
        return transferRecord

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


    def _getStorageQuota(self):
        """
        Fetch the DataOps quota from Detox. At this stage, we do not do
        any manipulation with the quota value (Unified uses 80% of the quota),
        use it as is!

        :return: update the cache self.nodesUsage with the quota value from
          Detox

        NOTE: code extracted/modified from Unified, see `fetch_detox_info` in
          https://github.com/CMSCompOps/WmAgentScripts/blob/master/utils.py#L2514
        """
        # FIXME: extremely fragile code that has to be replaced by a proper
        # CRIC/Rucio API in the very near future
        info = getDetoxQuota(self.msConfig['detoxUrl'])

        doRead = False
        for line in info:
            if 'DDM Partition:' in line and self.msConfig['quotaAccount'] in line:
                doRead = True
                continue
            elif 'DDM Partition:' in line:
                doRead = False
                continue
            elif line.startswith('#'):
                continue

            if not doRead:
                continue

            _, quota, _, _, pnn = line.split()

            if pnn.endswith("_MSS") or pnn.endswith("_Export"):
                continue
            # convert from TB to bytes
            self.nodesUsage[pnn]['quota'] = int(quota) * (1024 ** 4)

    def _getStorageUsage(self):
        """
        Fetch the storage usage from either Rucio or PhEDEx, which will then
        be used as part of the data placement mechanism.
        Also calculate the available quota - given the configurable quota
        fraction - and mark RSEs with less than 1TB available as NOT usable.

        Keys definition is:
         * quota: the PhEDEx group quota provided by Detox
         * bytes_limit: either the PhEDEx quota or the account quota from Rucio
         * bytes: data volume placed by Rucio (or subscribed in PhEDEx)
         * bytes_remaining: storage available for our account/group
         * quota_avail: space left (in bytes) that we can use for data placement
        :return: update our cache in place with the up-to-date values
        """
        if hasattr(self, "rucio"):
            for item in self.rucio.getAccountUsage(self.msConfig['rucioAccount']):
                self.nodesUsage[item['rse']].update({'bytes_limit': item['bytes_limit'],
                                                     'bytes': item['bytes'],
                                                     'bytes_remaining': item['bytes_remaining']})
        else:
            # for PhEDEx, we have also to remap the key's to keep in sync with Rucio
            res = self.phedex.getGroupUsage(group=self.msConfig['group'])
            for item in res['phedex']['node']:
                quota = self.nodesUsage[item['name']]['quota']
                self.nodesUsage[item['name']].update({'bytes_limit': quota,
                                                      'bytes': item['dest_bytes'],
                                                      'bytes_remaining': quota - item['dest_bytes']})

        # given a configurable sub-fraction of our quota, recalculate how much storage is left
        for rse, info in self.nodesUsage.items():
            quota_avail = info['quota'] * self.msConfig["quotaUsage"]
            info['quota_avail'] = min(quota_avail, info['bytes_remaining'])
            if info['quota_avail'] < 1000 * (1024 ** 3):
                self.logger.info("RSE: %s out of quota, skipping this storage!!")
                self.fullRSEs.add(rse)
