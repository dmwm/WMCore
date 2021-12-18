"""
File       : MSTransferor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSTransferor class provides the whole logic for
central production workflow's input data placement.

This is NOT a thread-safe module, even though some internal
tasks might be extended to multi-threading in the future.
"""
# futures
from __future__ import division, print_function
from future.utils import viewitems, listvalues, listitems
from future import standard_library
standard_library.install_aliases()

# system modules
from operator import itemgetter
from pprint import pformat
from retry import retry
from random import randint, choice
from copy import deepcopy

# WMCore modules
from Utils.IteratorTools import grouper
from WMCore.MicroService.DataStructs.DefaultStructs import TRANSFEROR_REPORT,\
    TRANSFER_RECORD, TRANSFER_COUCH_DOC
from WMCore.MicroService.Tools.Common import gigaBytes, teraBytes
from WMCore.MicroService.MSCore import MSCore
from WMCore.MicroService.MSTransferor.RequestInfo import RequestInfo
from WMCore.MicroService.MSTransferor.RSEQuotas import RSEQuotas
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI

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
        super(MSTransferor, self).__init__(msConfig, logger=logger)

        # minimum percentage completion for dataset/blocks subscribed
        self.msConfig.setdefault("minPercentCompletion", 99)
        # minimum available storage to consider a resource good for receiving data
        self.msConfig.setdefault("minimumThreshold", 1 * (1000 ** 4))  # 1TB
        # limit MSTransferor to this amount of requests per cycle
        self.msConfig.setdefault("limitRequestsPerCycle", 500)
        # Send warning messages for any data transfer above this threshold.
        # Set to negative to ignore.
        self.msConfig.setdefault("warningTransferThreshold", 100. * (1000 ** 4))  # 100TB
        # weight expression for the input replication rules
        self.msConfig.setdefault("rucioRuleWeight", 'ddm_quota')

        quotaAccount = self.msConfig["rucioAccount"]
        self.rseQuotas = RSEQuotas(quotaAccount, self.msConfig["quotaUsage"],
                                   minimumThreshold=self.msConfig["minimumThreshold"],
                                   verbose=self.msConfig['verbose'], logger=logger)

        # FIXME TODO: remove the setdefault once deployment is properly updated
        quotaAccountRelVal = self.msConfig.setdefault("rucioAccountRelVal", "wmcore_transferor_relval")
        self.rseQuotasRelVal = RSEQuotas(quotaAccountRelVal, self.msConfig["quotaUsage"],
                                         minimumThreshold=self.msConfig["minimumThreshold"],
                                         verbose=self.msConfig['verbose'], logger=logger)

        self.reqInfo = RequestInfo(self.msConfig, self.rucio, self.logger)

        self.cric = CRIC(logger=self.logger)
        self.inputMap = {"InputDataset": "primary",
                         "MCPileup": "secondary",
                         "DataPileup": "secondary"}
        self.uConfig = {}
        self.campaigns = {}
        self.psn2pnnMap = {}
        self.pnn2psnMap = {}
        self.dsetCounter = 0
        self.blockCounter = 0
        # service name used to route alerts via AlertManager
        self.alertServiceName = "ms-transferor"
        self.alertManagerUrl = self.msConfig.get("alertManagerUrl", None)
        self.alertManagerApi = AlertManagerAPI(self.alertManagerUrl, logger=logger)

    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the transferor logic, e.g.:
         * unified configuration
         * all campaign configuration
         * PSN to PNN map from CRIC
         * account limits from Rucio
         * account usage from Rucio
        """
        self.logger.info("Updating all local caches...")
        self.dsetCounter = 0
        self.blockCounter = 0
        self.uConfig = self.unifiedConfig()
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        self.psn2pnnMap = self.cric.PSNtoPNNMap()
        self.pnn2psnMap = self.cric.PNNtoPSNMap()
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
        for clsObject in (self.rseQuotas, self.rseQuotasRelVal):
            self.logger.info("Updating RSE quota/usage for Rucio account: %s", clsObject.dataAcct)
            getattr(clsObject, "fetchStorageQuota")(self.rucio)
            getattr(clsObject, "fetchStorageUsage")(self.rucio)
            getattr(clsObject, "evaluateQuotaExceeded")()
            getattr(clsObject, "printQuotaSummary")()
            if not getattr(clsObject, "getNodeUsage")():
                msg = "Failed to fetch storage usage stats for account: {}".format(clsObject.dataAcct)
                raise RuntimeWarning(msg)

    def execute(self, reqStatus):
        """
        Executes the whole transferor logic
        :param reqStatus: request status to process
        :return:
        """
        counterWorkflows = 0
        counterFailedRequests = 0
        counterProblematicRequests = 0
        counterSuccessRequests = 0
        summary = dict(TRANSFEROR_REPORT)
        try:
            requestRecords = self.getRequestRecords(reqStatus)
            self.updateReportDict(summary, "total_num_requests", len(requestRecords))
            msg = "  retrieved %s requests. " % len(requestRecords)
            msg += "Service set to process up to %s requests per cycle." % self.msConfig["limitRequestsPerCycle"]
            self.logger.info(msg)
        except Exception as err:  # general error
            requestRecords = []
            msg = "Unknown exception while fetching requests from ReqMgr2. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        try:
            self.updateCaches()
            self.updateReportDict(summary, "total_num_campaigns", len(self.campaigns))
            self.updateReportDict(summary, "nodes_out_of_space", list(self.rseQuotas.getOutOfSpaceRSEs()))
            self.updateReportDict(summary, "relval_nodes_out_of_space", list(self.rseQuotasRelVal.getOutOfSpaceRSEs()))
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
                if not self.verifyCampaignExist(wflow):
                    counterProblematicRequests += 1
                    continue

                # first, check whether any pileup dataset is already in place
                self.checkPUDataLocation(wflow)
                if wflow.getSecondarySummary() and not wflow.getPURSElist():
                    # then we still have pileup to be transferred, but with incorrect locations
                    msg = "Workflow: %s cannot proceed due to some PU misconfiguration. Check previous logs..."
                    self.logger.critical(msg, wflow.getName())
                    # FIXME: this needs to be logged somewhere and workflow be set to failed
                    counterProblematicRequests += 1
                    continue

                # now check where input primary and parent blocks will need to go
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
                                     wflow.getName(), pformat(transfers))
                    # then create a document in ReqMgr Aux DB
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

        self.logger.info("Summary for this cycle is:")
        self.logger.info("    * there were %d problematic requests;", counterProblematicRequests)
        self.logger.info("    * there were %d failed requests;", counterFailedRequests)
        self.logger.info("    * there were %d successful requests;", counterSuccessRequests)
        self.logger.info("    * a total of %d datasets were subscribed;", self.dsetCounter)
        self.logger.info("    * a total of %d blocks were subscribed.", self.blockCounter)
        self.updateReportDict(summary, "success_request_transition", counterSuccessRequests)
        self.updateReportDict(summary, "failed_request_transition", counterFailedRequests)
        self.updateReportDict(summary, "problematic_requests", counterProblematicRequests)
        self.updateReportDict(summary, "num_datasets_subscribed", self.dsetCounter)
        self.updateReportDict(summary, "num_blocks_subscribed", self.blockCounter)
        self.updateReportDict(summary, "nodes_out_of_space", list(self.rseQuotas.getOutOfSpaceRSEs()))
        self.updateReportDict(summary, "relval_nodes_out_of_space", list(self.rseQuotasRelVal.getOutOfSpaceRSEs()))
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
            orderedRequests = listvalues(requests)
        orderedRequests.sort(key=itemgetter('RequestPriority'), reverse=True)

        return orderedRequests

    def verifyCampaignExist(self, wflow):
        """
        Check whether the campaigns associated to all the input datasets
        exist in the database.
        :param wflow: a workflow object
        :return: True if campaigns exist, False otherwise
        """
        for dataIn in wflow.getDataCampaignMap():
            if dataIn['campaign'] not in self.campaigns:
                msg = "Workflow: %s has to transfer dataset: %s under the campaign: %s. "
                msg += "This campaign does not exist and needs to be created. Skipping this workflow!"
                self.logger.warning(msg, wflow.getName(), dataIn['name'], dataIn['campaign'])
                return False
        return True

    def checkDataLocation(self, wflow):
        """
        Check which data is already in place (according to the site lists)
        and remove them from the data placement to be performed next.
        If workflow has XRootD/AAA enabled, data location can be outside of the
        SiteWhitelist.
        :param wflow: workflow object
        """
        if not wflow.getInputDataset():
            return

        wflowPnns = self._getPNNsFromPSNs(wflow.getSitelist())
        primaryAAA = wflow.getReqParam("TrustSitelists")
        msg = "Checking data location for request: %s, TrustSitelists: %s, request white/black list PNNs: %s"
        self.logger.info(msg, wflow.getName(), primaryAAA, wflowPnns)

        if not wflow.getPileupDatasets():
            # perfect, it does not depend on pileup location then
            pass
        elif primaryAAA:
            # perfect, data can be anywhere
            pass
        elif wflow.getSecondarySummary() and wflow.getPURSElist():
            # still pileup datasets to be transferred
            wflowPnns = wflow.getPURSElist()
            self.logger.info("using: %s for primary/parent/pileup data placement", wflowPnns)
            finalPNN = self._checkPrimaryDataVolume(wflow, wflowPnns)
            self.logger.info("Forcing all primary/parent data to be placed under: %s", finalPNN)
            wflow.setPURSElist(finalPNN)
            wflowPnns = finalPNN
        elif wflow.getPURSElist():
            # all pileup datasets are already in place
            wflowPnns = wflow.getPURSElist()
            self.logger.info("using: %s for primary/parent data placement", wflowPnns)
            finalPNN = self._checkPrimaryDataVolume(wflow, wflowPnns)
            self.logger.info("Forcing all primary/parent data to be placed under: %s", finalPNN)
            wflow.setPURSElist(finalPNN)
            wflowPnns = finalPNN
        else:
            self.logger.error("Unexpected condition for request: %s ...", wflow.getName())

        for methodName in ("getPrimaryBlocks", "getParentBlocks"):
            inputBlocks = getattr(wflow, methodName)()
            self.logger.info("Request %s has %d initial blocks from %s",
                             wflow.getName(), len(inputBlocks), methodName)

            for block, blockDict in listitems(inputBlocks):  # dict can change size here
                blockLocation = self._diskPNNs(blockDict['locations'])
                if primaryAAA and blockLocation:
                    msg = "Primary/parent block %s already in place (via AAA): %s" % (block, blockLocation)
                    self.logger.info(msg)
                    inputBlocks.pop(block)
                elif blockLocation:
                    commonLocation = wflowPnns & set(blockLocation)
                    if commonLocation:
                        self.logger.info("Primary/parent block %s already in place: %s", block, commonLocation)
                        inputBlocks.pop(block)
                    else:
                        self.logger.info("block: %s will need data placement!!!", block)
                else:
                    self.logger.info("Primary/parent block %s not available in any disk storage", block)

            self.logger.info("Request %s has %d final blocks from %s",
                             wflow.getName(), len(getattr(wflow, methodName)()), methodName)

    def _checkPrimaryDataVolume(self, wflow, wflowPnns):
        """
        Calculate the total data volume already available in the
        restricted list of PNNs, such that we can minimize primary/
        parent data transfers
        :param wflow: a workflow object
        :param wflowPnns: set with the allowed PNNs to receive data
        :return: the PNN which contains most of the data already in
        """
        msg = "Checking primary data volume for: %s, allowed PNNs: %s"
        self.logger.info(msg, wflow.getName(), wflowPnns)

        volumeByPNN = dict()
        for pnn in wflowPnns:
            volumeByPNN.setdefault(pnn, 0)

        for methodName in ("getPrimaryBlocks", "getParentBlocks"):
            inputBlocks = getattr(wflow, methodName)()
            self.logger.info("Request %s has %d initial blocks from %s",
                             wflow.getName(), len(inputBlocks), methodName)

            for block, blockDict in viewitems(inputBlocks):
                blockLocation = self._diskPNNs(blockDict['locations'])
                commonLocation = wflowPnns & set(blockLocation)
                if not commonLocation:
                    continue
                for pnn in commonLocation:
                    volumeByPNN[pnn] += blockDict['blockSize']

        maxSize = 0
        finalPNN = set()
        self.logger.info("Primary/parent data volume currently available:")
        for pnn, size in viewitems(volumeByPNN):
            self.logger.info("  PNN: %s\t\tData volume: %s GB", pnn, gigaBytes(size))
            if size > maxSize:
                maxSize = size
                finalPNN = {pnn}
            elif size == maxSize:
                finalPNN.add(pnn)
        self.logger.info("The PNN that would require less data to be transferred is: %s", finalPNN)
        if len(finalPNN) > 1:
            # magically picks one site from the list. It could pick the one with highest
            # available quota, but that might overload that one site...
            # make sure it's a set object
            finalPNN = choice(list(finalPNN))
            finalPNN = {finalPNN}
            self.logger.info("Randomly picked PNN: %s as final location", finalPNN)

        return finalPNN

    def checkPUDataLocation(self, wflow):
        """
        Check the workflow configuration - in terms of AAA - and the secondary
        pileup distribution; and if possible remove the pileup dataset from the
        next step where data is placed.
        If workflow has XRootD/AAA enabled, data location can be outside of the
        SiteWhitelist.
        :param wflow: workflow object
        """
        pileupInput = wflow.getSecondarySummary()
        if not pileupInput:
            # nothing to be done here
            return

        wflowPnns = self._getPNNsFromPSNs(wflow.getSitelist())
        secondaryAAA = wflow.getReqParam("TrustPUSitelists")
        msg = "Checking secondary data location for request: {}, ".format(wflow.getName())
        msg += "TrustPUSitelists: {}, request white/black list PNNs: {}".format(secondaryAAA, wflowPnns)
        self.logger.info(msg)

        if secondaryAAA:
            # what matters is to have pileup dataset(s) available in ANY disk storage
            for dset, dsetDict in listitems(pileupInput):  # dict can change size here
                datasetLocation = self._diskPNNs(dsetDict['locations'])
                msg = "it has secondary: %s, total size: %s GB, disk locations: %s"
                self.logger.info(msg, dset, gigaBytes(dsetDict['dsetSize']), datasetLocation)
                if datasetLocation:
                    self.logger.info("secondary dataset %s already in place through AAA: %s",
                                     dset, datasetLocation)
                    pileupInput.pop(dset)
                else:
                    self.logger.info("secondary dataset %s not available even through AAA", dset)
        else:
            if len(pileupInput) == 1:
                for dset, dsetDict in listitems(pileupInput):  # dict can change size here
                    datasetLocation = self._diskPNNs(dsetDict['locations'])
                    msg = "it has secondary: %s, total size: %s GB, current disk locations: %s"
                    self.logger.info(msg, dset, gigaBytes(dsetDict['dsetSize']), datasetLocation)
                    commonLocation = wflowPnns & set(datasetLocation)
                    if commonLocation:
                        msg = "secondary dataset: %s already in place. "
                        msg += "Common locations with site white/black list is: %s"
                        self.logger.info(msg, dset, commonLocation)
                        pileupInput.pop(dset)
                        wflow.setPURSElist(commonLocation)
                    else:
                        self.logger.info("secondary: %s will need data placement!!!", dset)
            elif len(pileupInput) >= 2:
                # then make sure multiple pileup datasets are available at the same location
                # Note: avoid transferring the biggest one
                largestSize = 0
                largestDset = ""
                for dset, dsetDict in viewitems(pileupInput):
                    if dsetDict['dsetSize'] > largestSize:
                        largestSize = dsetDict['dsetSize']
                        largestDset = dset
                datasetLocation = self._diskPNNs(pileupInput[largestDset]['locations'])
                msg = "it has multiple pileup datasets, the largest one is: %s,"
                msg += "total size: %s GB, current disk locations: %s"
                self.logger.info(msg, largestDset, gigaBytes(largestSize), datasetLocation)
                commonLocation = wflowPnns & set(datasetLocation)
                if commonLocation:
                    self.logger.info("Largest secondary dataset %s already in place: %s",
                                     largestDset, datasetLocation)
                    pileupInput.pop(largestDset)
                    wflow.setPURSElist(commonLocation)
                else:
                    self.logger.info("Largest secondary dataset %s not available in a common location. This is BAD!")
                # now iterate normally through the pileup datasets
                for dset, dsetDict in listitems(pileupInput):  # dict can change size here
                    datasetLocation = self._diskPNNs(dsetDict['locations'])
                    msg = "it has secondary: %s, total size: %s GB, current disk locations: %s"
                    self.logger.info(msg, dset, gigaBytes(dsetDict['dsetSize']), datasetLocation)
                    commonLocation = wflowPnns & set(datasetLocation)
                    if not commonLocation:
                        msg = "secondary dataset: %s not in any common location. Its current locations are: %s"
                        self.logger.info(msg, dset, datasetLocation)
                    elif commonLocation and not wflow.getPURSElist():
                        # then it's the first pileup dataset available within the SiteWhitelist,
                        # force its common location for the workflow from now on
                        msg = "secondary dataset: %s already in place: %s, common location: %s"
                        msg += ". Forcing the whole workflow to this new common location."
                        self.logger.info(msg, dset, datasetLocation, commonLocation)
                        pileupInput.pop(dset)
                        wflow.setPURSElist(commonLocation)
                    else:
                        # pileup RSE list has already been defined. Get the new common location
                        newCommonLocation = commonLocation & wflow.getPURSElist()
                        if newCommonLocation:
                            msg = "secondary dataset: %s already in place. "
                            msg += "New common locations with site white/black list is: %s"
                            self.logger.info(msg, dset, newCommonLocation)
                            pileupInput.pop(dset)
                            wflow.setPURSElist(newCommonLocation)
                        else:
                            msg = "secondary dataset: %s is currently available within the site white/black list: %s"
                            msg += " But there is no common location with the other(s) pileup datasets: %s"
                            msg += " It will need data placement!!!"
                            self.logger.info(msg, dset, commonLocation, wflow.getPURSElist())

        # check if there are remaining pileups to be placed
        # we need to figure out its location NOW!
        if wflow.getSecondarySummary() and not wflow.getPURSElist():
            pnns = self._findFinalPULocation(wflow)
            wflow.setPURSElist(pnns)

    def _findFinalPULocation(self, wflow):
        """
        Given a workflow object, find the secondary datasets left to be
        placed and decide which destination to be used, based on the campaign
        configuration and the site with more quota available
        :param wflow: the workflow object
        :return: a string with the final pileup destination PNN
        """
        # FIXME: workflows should be marked as failed if there is no common
        # site between SiteWhitelist and secondary location
        psns = wflow.getSitelist()
        self.logger.info("Finding final pileup destination for request: %s", wflow.getName())

        for dataIn in wflow.getDataCampaignMap():
            if dataIn["type"] == "secondary" and dataIn['name'] in wflow.getSecondarySummary():
                # secondary still to be transferred
                dsetName = dataIn["name"]
                campConfig = self.campaigns[dataIn['campaign']]

                commonPsns = set()
                # if the dataset has a location list, use solely that one
                if campConfig['Secondaries'].get(dsetName, []):
                    campSecPSNs = self._getPSNsFromPNNs(campConfig['Secondaries'][dsetName])
                    commonPsns = set(psns) & campSecPSNs
                    if not commonPsns:
                        msg = "Workflow has been incorrectly assigned: %s. The secondary dataset: %s,"
                        msg += "belongs to the campaign: %s, with Secondaries location set to: %s. "
                        msg += "While the workflow has been assigned to: %s"
                        self.logger.error(msg, wflow.getName(), dsetName, dataIn['campaign'],
                                          campSecPSNs, psns)
                else:
                    if dsetName.startswith("/Neutrino"):
                        # different PU type use different campaign attributes...
                        campSecPSNs = self._getPSNsFromPNNs(campConfig['SecondaryLocation'])
                        commonPsns = set(psns) & campSecPSNs
                        if not commonPsns:
                            msg = "Workflow has been incorrectly assigned: %s. The secondary dataset: %s,"
                            msg += "belongs to the campaign: %s, with SecondaryLocation set to: %s. "
                            msg += "While the workflow has been assigned to: %s"
                            self.logger.error(msg, wflow.getName(), dsetName, dataIn['campaign'],
                                              campSecPSNs, psns)
                    else:
                        if campConfig['SiteWhiteList']:
                            commonPsns = set(psns) & set(campConfig['SiteWhiteList'])
                        if campConfig['SiteBlackList']:
                            commonPsns = set(psns) - set(campConfig['SiteBlackList'])
                        if not commonPsns:
                            msg = "Workflow has been incorrectly assigned: %s. The secondary dataset: %s,"
                            msg += "belongs to the campaign: %s, which does not match the campaign SiteWhiteList: %s "
                            msg += "and SiteBlackList: %s. While the workflow has been assigned to: %s"
                            self.logger.error(msg, wflow.getName(), dsetName, dataIn['campaign'],
                                              campConfig['SiteWhiteList'], campConfig['SiteBlackList'], psns)
                if not commonPsns:
                    # returns an empty set, which will make this workflow to be skipped for the moment
                    return commonPsns

        pnns = self._getPNNsFromPSNs(commonPsns)
        self.logger.info("  found a PSN list: %s, which maps to a list of PNNs: %s", commonPsns, pnns)
        return pnns

    def makeTransferRequest(self, wflow):
        """
        Checks which input data has to be transferred, select the final destination if needed,
        create the transfer record to be stored in Couch, and create the DM placement request.
        This method does the following:
          1. return if there is no workflow data to be transferred
          2. check if the data input campaign is in the database, skip if not
          3. _getValidSites: using the workflow site lists and the campaign configuration,
             find a common list of sites (converted to PNNs). If the PNN is out of quota,
             it's also removed from this list
          4. create the transfer record dictionary
          5. for every final node
             5.1. if it's a pileup dataset, pick a random node and subscribe the whole container
             5.2. else, retrieve chunks of blocks to be subscribed (evenly distributed)
             5.3. update node usage with the amount of data subscribed
          6. re-evaluate nodes with quota exceeded
          7. return the transfer record, with a list of transfer IDs
        :param wflow: workflow object
        :return: boolean whether it succeeded or not, and a list of transfer records
        """
        response = []
        success = True
        if not (wflow.getParentBlocks() or wflow.getPrimaryBlocks() or wflow.getSecondarySummary()):
            self.logger.info("Request %s does not have any further data to transfer", wflow.getName())
            return success, response

        self.logger.info("Handling data subscriptions for request: %s", wflow.getName())

        rseQuotaObject = self.rseQuotasRelVal if wflow.isRelVal() else self.rseQuotas
        for dataIn in wflow.getDataCampaignMap():
            if dataIn["type"] == "parent":
                msg = "Skipping 'parent' data subscription (done with the 'primary' data), for: %s" % dataIn
                self.logger.info(msg)
                continue
            elif dataIn["type"] == "secondary" and dataIn['name'] not in wflow.getSecondarySummary():
                # secondary already in place
                continue

            if wflow.getPURSElist():
                # then the whole workflow is very much limited to a single site
                nodes = list(wflow.getPURSElist() & rseQuotaObject.getAvailableRSEs())
                if not nodes:
                    msg = "Workflow: %s can only run in RSEs with no available space: %s. "
                    msg += "Skipping this workflow until space gets released"
                    self.logger.warning(msg, wflow.getName(), wflow.getPURSElist())
                    return False, response
            else:
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
                else:
                    # enforce a container-level Rucio rule
                    subLevel = "container"
                    blocks = None

                success, transferId = self.makeTransferRucio(wflow, dataIn, subLevel,
                                                             blocks, dataSize, nodes, idx)

                if not success:
                    # stop any other data placement for this workflow
                    msg = "There were failures transferring data for workflow: %s. Will retry again later."
                    self.logger.warning(msg, wflow.getName())
                    break
                if transferId:
                    if isinstance(transferId, (set, list)):
                        transRec['transferIDs'].update(transferId)
                    else:
                        transRec['transferIDs'].add(transferId)
                    rseQuotaObject.updateNodeUsage(nodes[idx], dataSize)

                # and update some instance caches
                if subLevel == 'container':
                    self.dsetCounter += 1
                else:
                    self.blockCounter += len(blocks)

            transRec['transferIDs'] = list(transRec['transferIDs'])
            response.append(transRec)

        # once the workflow has been completely processed, update the node usage
        rseQuotaObject.evaluateQuotaExceeded()
        return success, response

    def makeTransferRucio(self, wflow, dataIn, subLevel, blocks, dataSize, nodes, nodeIdx):
        """
        Creates a Rucio rule object and make a replication rule in Rucio

        :param wflow: the workflow object
        :param dataIn: short summary of the data to be placed
        :param subLevel: subscription level (container or block)
        :param blocks: list of blocks to be subscribed (or None if dataset level)
        :param dataSize: amount of data being placed by this rule
        :param nodes: list of nodes/RSE
        :param nodeIdx: index of the node/RSE to be used in the replication rule
        :return: a boolean flagging whether it succeeded or not, and the rule id
        """
        success, transferId = True, set()
        subLevel = "ALL" if subLevel == "container" else "DATASET"
        dids = blocks if blocks else [dataIn['name']]
        rucioAcct = self.msConfig['rucioAccountRelVal'] if wflow.isRelVal() else self.msConfig['rucioAccount']

        ruleAttrs = {'copies': 1,
                     'activity': 'Production Input',
                     'lifetime': self.msConfig['rulesLifetime'],
                     'account': rucioAcct,
                     'grouping': subLevel,
                     'weight': self.msConfig['rucioRuleWeight'],
                     'meta': {'workflow_group': wflow.getWorkflowGroup()},
                     'comment': 'WMCore MSTransferor input data placement'}

        if wflow.getParentDataset():
            # then we need to make sure the child and its parent blocks end up in the same RSE
            rseExpr = nodes[nodeIdx]
            msg = "Primary data placement with parent blocks, putting all in the same RSE: {}".format(rseExpr)
            self.logger.info(msg)
        elif ruleAttrs['grouping'] == "ALL":
            # this means we are placing the whole container under the same RSE.
            # Ask Rucio which RSE we should use, provided a list of them
            rseExpr = "|".join(nodes)
            rseTuple = self.rucio.pickRSE(rseExpr)
            if not rseTuple:
                self.logger.error("PickRSE did not return any valid RSE for expression: %s", rseExpr)
                return False, transferId
            self.logger.info("Placing whole container, picked RSE: %s out of an RSE list: %s",
                             rseTuple[0], rseExpr)
            rseExpr = rseTuple[0]
        else:
            # then grouping is by DATASET, and there is no parent dataset
            # we can proceed with the primary blocks data placement in all RSEs
            rseExpr = "|".join(nodes)
            msg = "Primary data placement without any parent dataset, "
            msg += "using all RSEs for the rule creation: {}".format(rseExpr)
            self.logger.info(msg)

        if self.msConfig.get('enableDataTransfer', True):
            # Force request-only subscription
            # to any data transfer going above some threshold (do not auto-approve)
            aboveWarningThreshold = self.msConfig.get('warningTransferThreshold') > 0. and \
                                    dataSize > self.msConfig.get('warningTransferThreshold')

            # Then make the data subscription, for real!!!
            self.logger.info("Creating rule for workflow %s with %d DIDs in container %s, RSEs: %s, grouping: %s",
                             wflow.getName(), len(dids), dataIn['name'], rseExpr, subLevel)
            try:
                res = self.rucio.createReplicationRule(dids, rseExpr, **ruleAttrs)
            except Exception as exc:
                msg = "Hit a bad exception while creating replication rules for DID: %s. Error: %s"
                self.logger.error(msg, dids, str(exc))
                success = False
            else:
                if res:
                    # it could be that some of the DIDs already had such a rule in
                    # place, so we might be retrieving a bunch of rule ids instead of
                    # a single one
                    self.logger.info("Rules successful created for %s : %s", dataIn['name'], res)
                    transferId.update(res)
                    # send an alert, if needed
                    self.notifyLargeData(aboveWarningThreshold, transferId, wflow.getName(), dataSize, dataIn)
                else:
                    self.logger.error("Failed to create rule for %s, will retry later", dids)
                    success = False
        else:
            msg = "DRY-RUN: making Rucio rule for workflow: %s, dids: %s, rse: %s, kwargs: %s"
            self.logger.info(msg, wflow.getName(), dids, rseExpr, ruleAttrs)
        return success, transferId

    def notifyLargeData(self, aboveWarningThreshold, transferId, wflowName, dataSize, dataIn):
        """
        Evaluates whether the amount of data placed is too big, if so, send an alert
        notification to a few persons
        :param aboveWarningThreshold: boolean flag saying if the thresholds was exceeded or not
        :param transferId: rule/transfer request id
        :param wflowName: name of the workflow
        :param dataSize: total amount of data subscribed
        :param dataIn: short summary of the workflow data
        """
        # Warn about data transfer subscriptions going above some threshold
        if aboveWarningThreshold:
            alertName = "{}: input data transfer over threshold: {}".format(self.alertServiceName,
                                                                            wflowName)
            alertSeverity = "high"
            alertSummary = "[MS] Large pending data transfer under request id: {}".format(transferId)
            alertDescription = "Workflow: {} has a large amount of ".format(wflowName)
            alertDescription += "data subscribed: {} TB, ".format(teraBytes(dataSize))
            alertDescription += "for {} data: {}.""".format(dataIn['type'], dataIn['name'])

            try:
                # alert to expiry in an hour from now
                self.alertManagerApi.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                                               self.alertServiceName, endSecs=1 * 60 * 60)
            except Exception as ex:
                self.logger.exception("Failed to send alert to %s. Error: %s", self.alertManagerUrl, str(ex))
            self.logger.warning(alertDescription)

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
        campConfig = self.campaigns[dataIn['campaign']]
        psns = wflow.getSitelist()

        if dataIn["type"] == "primary":
            if campConfig['SiteWhiteList']:
                psns = set(psns) & set(campConfig['SiteWhiteList'])
            if campConfig['SiteBlackList']:
                psns = set(psns) - set(campConfig['SiteBlackList'])

        self.logger.info("  final list of PSNs to be use: %s", psns)
        pnns = self._getPNNsFromPSNs(psns)

        rseQuotaObject = self.rseQuotasRelVal if wflow.isRelVal() else self.rseQuotas
        self.logger.info("List of out-of-space RSEs dropped for '%s' is: %s",
                         wflow.getName(), pnns & rseQuotaObject.getOutOfSpaceRSEs())
        return list(pnns & rseQuotaObject.getAvailableRSEs())

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
            # Except for DQMHarvest workflows, which must have a data placement of the
            # whole dataset within the same location
            if wflow.getReqType() == "DQMHarvest":
                numNodes = 1
            # if there is no parent data, just make one big rule for all the primary data
            # against all RSEs available for the workflow (intersection with PU data
            if not wflow.getParentBlocks():
                numNodes = 1
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

    def _getPNNsFromPSNs(self, psnList):
        """
        Given a list/set of PSNs, return a set of valid PNNs.
        Note that T3, Tape and a few other PNNs are never returned.
        """
        pnns = set()
        for psn in psnList:
            for pnn in self.psn2pnnMap.get(psn, []):
                if pnn == "T2_CH_CERNBOX" or pnn.startswith("T3_"):
                    pass
                elif pnn.endswith("_Tape") or pnn.endswith("_MSS") or pnn.endswith("_Export"):
                    pass
                else:
                    pnns.add(pnn)
        return pnns

    def _getPSNsFromPNNs(self, pnnList):
        """
        Given a list/set of PNNs, return a set of valid PSNs.
        Note that T3 sites are never returned.
        """
        psns = set()
        for pnn in pnnList:
            for psn in self.pnn2psnMap.get(pnn, []):
                if psn.startswith("T3_"):
                    pass
                else:
                    psns.add(psn)
        return psns

    def _diskPNNs(self, pnnList):
        """
        Provided a list of PNN locations, return another list of
        PNNs without mass storage and T3 sites
        :param pnnList: list of PNN strings
        :return: a set of strings with filtered out PNNs
        """
        diskPNNs = set()
        for pnn in pnnList:
            if pnn == "T2_CH_CERNBOX" or pnn.startswith("T3_"):
                pass
            elif pnn.endswith("_Tape") or pnn.endswith("_MSS") or pnn.endswith("_Export"):
                pass
            else:
                diskPNNs.add(pnn)
        return diskPNNs
