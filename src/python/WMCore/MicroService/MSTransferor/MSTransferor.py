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
from future.utils import listvalues, listitems
from future import standard_library
standard_library.install_aliases()

# system modules
from operator import itemgetter
from pprint import pformat
from retry import retry
from copy import deepcopy

# WMCore modules
from Utils.IteratorTools import grouper
from WMCore.MicroService.DataStructs.DefaultStructs import TRANSFEROR_REPORT,\
    TRANSFER_RECORD, TRANSFER_COUCH_DOC
from WMCore.MicroService.Tools.Common import gigaBytes, teraBytes, isRelVal
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.MicroService.MSTransferor.RequestInfo import RequestInfo
from WMCore.MicroService.MSTransferor.DataStructs.RSEQuotas import RSEQuotas
from WMCore.Services.CRIC.CRIC import CRIC


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
        # Workflows with open running timeout are used for growing input dataset, thus
        # make a container level rule for the whole container whenever the open running
        # timeout is larger than what is configured (or the default of 7 days below)
        self.msConfig.setdefault("openRunning", 7 * 24 * 60 * 60)

        quotaAccount = self.msConfig["rucioAccount"]

        self.rseQuotas = RSEQuotas(quotaAccount, self.msConfig["quotaUsage"],
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

    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the transferor logic, e.g.:
         * account limits from Rucio
         * account usage from Rucio
         * unified configuration
         * all campaign configuration
         * PSN to PNN map from CRIC
        """
        self.logger.info("Updating RSE/PNN quota and usage")
        self.rseQuotas.fetchStorageQuota(self.rucio)
        self.rseQuotas.fetchStorageUsage(self.rucio)
        self.rseQuotas.evaluateQuotaExceeded()
        if not self.rseQuotas.getNodeUsage():
            raise RuntimeWarning("Failed to fetch storage usage stats")

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
        self.rseQuotas.printQuotaSummary()

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
        self.logger.info("Service set to process up to %s requests per cycle.",
                         self.msConfig["limitRequestsPerCycle"])
        try:
            requestRecords = self.getRequestRecords(reqStatus)
            self.updateReportDict(summary, "total_num_requests", len(requestRecords))
            self.logger.info("Retrieved %s requests.", len(requestRecords))
        except Exception as err:  # general error
            requestRecords = []
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
                if not self.verifyCampaignExist(wflow):
                    counterProblematicRequests += 1
                    continue

                # first, check whether any pileup dataset is already in place
                self.checkPUDataLocation(wflow)
                if wflow.getSecondarySummary() and not wflow.getPURSElist():
                    # then we still have pileup to be transferred, but with incorrect locations
                    self.alertPUMisconfig(wflow.getName())
                    counterProblematicRequests += 1
                    continue

                # now check where input primary and parent blocks will need to go
                self.checkDataLocation(wflow)

                try:
                    success, transfers = self.makeTransferRequest(wflow)
                except Exception as ex:
                    success = False
                    self.alertUnknownTransferError(wflow.getName())
                    msg = "Unknown exception while making transfer request for %s " % wflow.getName()
                    msg = "\tError: %s" % str(ex)
                    self.logger.exception(msg)
                if success:
                    # then create a document in ReqMgr Aux DB
                    self.logger.info("Transfer requests successful for %s. Summary: %s",
                                     wflow.getName(), pformat(transfers))
                    if self.createTransferDoc(wflow.getName(), transfers):
                        self.logger.info("Transfer document successfully created in CouchDB for: %s", wflow.getName())
                        # then move this request to staging status
                        self.change(wflow.getName(), 'staging', self.__class__.__name__)
                        counterSuccessRequests += 1
                    else:
                        counterFailedRequests += 1
                        self.alertTransferCouchDBError(wflow.getName())
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
        Check which data is already in place (according to the site lists
        and pileup data location) and remove them from the data placement
        if already available anywhere.
        If workflow has XRootD/AAA enabled, data location can be outside of
        the SiteWhitelist.
        :param wflow: workflow object
        :return: None
        """
        if not wflow.getInputDataset():
            return

        wflowPnns = self._getPNNsFromPSNs(wflow.getSitelist())
        primaryAAA = wflow.getReqParam("TrustSitelists")
        msg = "Checking data location for request: %s, TrustSitelists: %s, request white/black list PNNs: %s"
        self.logger.info(msg, wflow.getName(), primaryAAA, wflowPnns)

        wflowPnns = wflow.getPURSElist()

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

    def checkPUDataLocation(self, wflow):
        """
        Check the workflow pileup current location, compare it to what is defined
        in the campaign configuration and ensure that each location defined in the
        campaign gets a rule created, regardless whether AAA is enabled or not.

        Use the workflow sitelists and the expected pileup(s) location(s) to decide
        where primary and parent data must be placed.

        :param wflow: workflow object
        :return: None
        """
        pileupInput = wflow.getSecondarySummary()
        if not pileupInput:
            # nothing to be done here
            return

        psns = wflow.getSitelist()
        wflowPnns = self._getPNNsFromPSNs(psns)
        secAAA = wflow.getReqParam("TrustPUSitelists")
        msg = "Checking secondary data location for request: {}, ".format(wflow.getName())
        msg += "TrustPUSitelists: {}, request white/black list PNNs: {}".format(secAAA, wflowPnns)
        self.logger.info(msg)

        # this variable will contain a set of each pileup location, according
        # to what has been defined in the campaign configuration. In the end,
        # their intersection will be the final location for primary and parents
        campBasedLocation = []
        for dataIn in wflow.getDataCampaignMap():
            if dataIn["type"] == "secondary":
                dsetName = dataIn["name"]
                campConfig = self.campaigns[dataIn['campaign']]
                secSize = pileupInput[dsetName]['dsetSize']
                secLocation = pileupInput[dsetName]['locations']
                # and a special case for RelVal workflows, which do not define
                # secondary datasets and their location
                if isRelVal(wflow.data):
                    campSecLocations = wflowPnns
                else:
                    campSecLocations = campConfig['Secondaries'].get(dsetName, [])
                campBasedLocation.append(set(campSecLocations))

                if not campSecLocations:
                    msg = "Workflow has been incorrectly assigned: %s. The secondary dataset: %s, "
                    msg += "belongs to the campaign: %s, which does not define the secondary "
                    msg += "dataset or it has defined an empty location list."
                    self.logger.error(msg, wflow.getName(), dsetName, dataIn['campaign'])
                    return

                # compare the expected locations against the current availability
                missingDestinations = list(set(campSecLocations) - set(secLocation))
                msg = "it has secondary pileup: %s, with a total size of: %s GB, "
                msg += "currently at: %s, campaign expected at: %s and missing replicas at: %s"
                self.logger.info(msg, dsetName, gigaBytes(secSize), secLocation,
                                 campSecLocations, missingDestinations)
                if missingDestinations:
                    # then update this pileup location to get rule(s) on it
                    self.logger.info("pileup %s will get container rules on: %s", dsetName, missingDestinations)
                    pileupInput[dsetName]['locations'] = missingDestinations
                else:
                    self.logger.info("pileup %s already available at the expected locations", dsetName)
                    # then remove it from the samples to get a data placement rule
                    pileupInput.pop(dsetName)

        # consider the workflow sitelist for this final location
        if len(campBasedLocation) == 1:
            # then there is only one pileup dataset in the workflow
            wflowFinalLocation = campBasedLocation[0] & wflowPnns
        else:
            # then there are multiple pileups, meaning possibly different
            # campaigns and expected locations. Use their location
            # intersection as final workflow destination
            wflowFinalLocation = campBasedLocation[0].intersection(*campBasedLocation)
        self.logger.info("Final location for workflow: %s is: %s", wflow.getName(), wflowFinalLocation)
        wflow.setPURSElist(wflowFinalLocation)

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

        for dataIn in wflow.getDataCampaignMap():
            dsetName = dataIn['name']
            if dataIn["type"] == "parent":
                msg = "Skipping 'parent' data subscription (done with the 'primary' data), for: %s" % dataIn
                self.logger.info(msg)
                continue
            elif dataIn["type"] == "secondary" and dsetName not in wflow.getSecondarySummary():
                # secondary already in place
                continue

            if wflow.getPURSElist() and not isRelVal(wflow.data):
                rses = list(wflow.getPURSElist() & self.rseQuotas.getAvailableRSEs())
            else:
                rses = self._getValidSites(wflow, dataIn)
            if not rses:
                msg = "Workflow: %s can only run in RSEs with no available space: %s. "
                msg += "Skipping this workflow until space gets released"
                self.logger.warning(msg, wflow.getName(), wflow.getPURSElist())
                return False, response

            # create a transfer record data structure
            transRec = newTransferRec(dataIn)
            # figure out dids, number of copies and which grouping to use
            if dataIn["type"] == "primary":
                dids, didsSize = wflow.getInputData()
                grouping = wflow.getRucioGrouping()
                copies = wflow.getReplicaCopies()
                if not dids:
                    # no valid files in any blocks, it will likely fail in global workqueue
                    self.logger.warning("  found 0 primary/parent blocks for dataset: %s, moving on...", dataIn['name'])
                    return success, response
            # then it's secondary type
            else:
                # we can have multiple pileup datasets
                puSummary = wflow.getSecondarySummary()
                dids = [dsetName]
                didsSize = puSummary[dsetName]['dsetSize']
                grouping = "ALL"
                # one replica for each RSE
                copies = len(rses)

            success, transferId = self.makeTransferRucio(wflow, dataIn, dids, didsSize,
                                                         grouping, copies, rses)
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

            # and update some instance caches
            if dataIn["type"] == "secondary":
                self.dsetCounter += 1
            else:
                self.blockCounter += len(dids)

        transRec['transferIDs'] = list(transRec['transferIDs'])
        response.append(transRec)

        return success, response

    def makeTransferRucio(self, wflow, dataIn, dids, dataSize, grouping, copies, nodes):
        """
        Creates a Rucio replication rule

        :param wflow: the workflow object
        :param dataIn: short summary of the data to be placed
        :param dids: a list of the DIDs to be added to the rule
        :param dataSize: amount of data being placed by this rule
        :param grouping: whether blocks need to be placed altogether (ALL)
                         or if the can be scattered around (DATASET).
        :param copies: integer with the number of copies to use in the rule
        :param nodes: list of nodes/RSE
        :return: a boolean flagging whether it succeeded or not, and the rule id
        """
        success, transferId = True, set()

        ruleAttrs = {'copies': copies,
                     'activity': 'Production Input',
                     'lifetime': self.msConfig['rulesLifetime'],
                     'account': self.msConfig['rucioAccount'],
                     'grouping': grouping,
                     'weight': self.msConfig['rucioRuleWeight'],
                     'meta': {'workflow_group': wflow.getWorkflowGroup()},
                     'comment': 'WMCore MSTransferor input data placement'}

        rseExpr = "|".join(nodes)

        if self.msConfig.get('enableDataTransfer', True):
            # Force request-only subscription
            # to any data transfer going above some threshold (do not auto-approve)
            aboveWarningThreshold = (self.msConfig.get('warningTransferThreshold') > 0. and
                                     dataSize > self.msConfig.get('warningTransferThreshold'))

            # Then make the data subscription, for real!!!
            self.logger.info("Creating rule for workflow %s with %d DIDs in container %s, RSEs: %s, grouping: %s",
                             wflow.getName(), len(dids), dataIn['name'], rseExpr, grouping)
            try:
                res = self.rucio.createReplicationRule(dids, rseExpr, **ruleAttrs)
            except Exception as exc:
                msg = "Hit a bad exception while creating replication rules for DID: %s. Error: %s"
                self.logger.error(msg, dids, str(exc))
                success = False
            else:
                if res:
                    # it could be that some of the DIDs already had such rule in
                    # place, so we might be retrieving a bunch of rule ids instead of
                    # a single one
                    self.logger.info("Rules successful created for %s : %s", dataIn['name'], res)
                    transferId.update(res)
                    # send an alert, if needed
                    self.alertLargeInputData(aboveWarningThreshold, transferId, wflow.getName(), dataSize, dataIn)
                else:
                    self.logger.error("Failed to create rule for %s, will retry later", dids)
                    success = False
        else:
            msg = "DRY-RUN: making Rucio rule for workflow: %s, dids: %s, rse: %s, kwargs: %s"
            self.logger.info(msg, wflow.getName(), dids, rseExpr, ruleAttrs)
        return success, transferId

    def alertPUMisconfig(self, workflowName):
        """
        Send alert to Prometheus with PU misconfiguration error
        """
        alertName = "{}: PU misconfiguration error. Workflow: {}".format(self.alertServiceName,
                                                                         workflowName)
        alertSeverity = "high"
        alertSummary = "[MSTransferor] Workflow cannot proceed due to some PU misconfiguration."
        alertDescription = "Workflow: {} could not proceed due to some PU misconfiguration,".format(workflowName)
        alertDescription += "so it will be skipped."
        self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                       self.alertServiceName)
        self.logger.critical(alertDescription)

    def alertUnknownTransferError(self, workflowName):
        """
        Send alert to Prometheus with unknown transfer error
        """
        alertName = "{}: Transfer request error. Workflow: {}".format(self.alertServiceName,
                                                                         workflowName)
        alertSeverity = "high"
        alertSummary = "[MSTransferor] Unknown exception while making transfer request."
        alertDescription = "Unknown exception while making Transfer request for workflow: {}".format(workflowName)
        self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                       self.alertServiceName)

    def alertTransferCouchDBError(self, workflowName):
        """
        Send alert to Prometheus with CouchDB transfer error
        """
        alertName = "{}: Failed to create a transfer document in CouchDB for workflow: {}".format(self.alertServiceName,
                                                                         workflowName)
        alertSeverity = "high"
        alertSummary = "[MSTransferor] Transfer document could not be created in CouchDB."
        alertDescription = "Workflow: {}, failed request  due to error posting to CouchDB".format(workflowName)
        self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                       self.alertServiceName)
        self.logger.warning(alertDescription)


    def alertLargeInputData(self, aboveWarningThreshold, transferId, wflowName, dataSize, dataIn):
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

            self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                           self.alertServiceName)
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

        if isRelVal(wflow.data):
            self.logger.info("RelVal workflow '%s' ignores sites out of quota", wflow.getName())
            return list(pnns)

        self.logger.info("List of out-of-space RSEs dropped for '%s' is: %s",
                         wflow.getName(), pnns & self.rseQuotas.getOutOfSpaceRSEs())
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
