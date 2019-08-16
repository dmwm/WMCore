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

# WMCore modules
from Utils.IteratorTools import grouper
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

    def updateCaches(self):
        """
        Fetch some data required for the transferor logic, e.g.:
         * unified configuration
         * all campaign configuration
         * PSN to PNN map from CRIC
        :return: True if all of them succeeded, else False
        """
        self.dsetCounter = 0
        self.blockCounter = 0
        self.uConfig = self.unifiedConfig()
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        self.psn2pnnMap = self.cric.PSNtoPNNMap()
        # also clear the RequestInfo pileup cache
        self.reqInfo.clearPileupCache()
        if not self.uConfig:
            self.logger.warning("Failed to fetch the unified configuration")
        elif not campaigns:
            self.logger.warning("Failed to fetch the campaign configurations")
        elif not self.psn2pnnMap:
            self.logger.warning("Failed to fetch PSN x PNN map from CRIC")
        else:
            # let's make campaign look-up easier and more efficient
            self.campaigns = {}
            for camp in campaigns:
                self.campaigns[camp['CampaignName']] = camp
            return True

        return False

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

        if not self.updateCaches():
            # then wait until the next cycle
            msg = "Failed to fetch data from one of the data sources. Retrying again in the next cycle"
            self.logger.error(msg)
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
        Send request to Phedex and return status of request subscription
        :param req: request object
        :return: subscriptoin dictionary {"dataset":transferIDs}
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
        :param sites: list of all sites that can host part of the data
        :param campaign: campaign name
        :param dataType: type of data we're going to transfer
        :param blocks: list of block names and their size (and parents, if any)
        :return: yield the node and a list of block names to be transferred
        """
        # FIXME: implement multiple copies (MaxCopies > 1)
        blockList = []
        dsetName = dataIn["name"]

        campConfig = self.campaigns[dataIn['campaign']]
        psns = wflow.getSitelist()

        if dataIn["type"] == "primary":
            if campConfig['SiteWhiteList']:
                psns = set(psns) & set(campConfig['SiteWhiteList'])
            if campConfig['SiteBlackList']:
                psns = set(psns) - set(campConfig['SiteBlackList'])
            nodes = self._getFinalPNNs(psns)
            blockList = wflow.getPrimaryBlocks().keys()
            # YES, parents go together with the primary data
            if wflow.getParentDataset():
                blockList.extend(wflow.getParentBlocks().keys())
            # FIXME: use whatever algorithm Unified uses to distribute blocks
            self.logger.info("Placing %d blocks for dataset: %s at %s", len(blockList), dsetName, nodes)
            self.blockCounter += len(blockList)
            yield nodes, blockList
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
        Given a list of sites/PSNs, get their associated PNN.
        Also applies some basic filters on the PNN resources
        :param psns: list of sites
        :return: a flat list of PNNs
        """
        self.logger.info("  final list of PSNs to be use: %s", psns)
        # TODO: how to equally distribute data  on sites with > 1 PNNs?
        pnns = set()
        for psn in psns:
            for pnn in self.psn2pnnMap.get(psn, []):
                if pnn == "T2_CH_CERNBOX":
                    self.logger.info("%s maps to %s, skipping it.", psn, pnn)
                elif pnn.startswith("T3_"):
                    self.logger.info("%s maps to a T3 PNN resource: %s, skipping it.", psn, pnn)
                elif pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    self.logger.info("%s maps to a archival storage PNN: %s, skipping it.", psn, pnn)
                else:
                    pnns.add(pnn)
        return list(pnns)

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
