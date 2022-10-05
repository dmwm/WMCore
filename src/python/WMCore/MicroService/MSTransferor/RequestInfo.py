#!/usr/bin/env python
"""
_RequestInfo_

Class to hold and parse all information related to a given request
"""
# futures
from __future__ import division, print_function
from future.utils import viewitems

# system modules
import datetime
import time
# WMCore modules
from pprint import pformat
from copy import deepcopy
from Utils.IteratorTools import grouper
from WMCore.DataStructs.LumiList import LumiList
from WMCore.MicroService.MSTransferor.DataStructs.DQMHarvestWorkflow import DQMHarvestWorkflow
from WMCore.MicroService.MSTransferor.DataStructs.GrowingWorkflow import GrowingWorkflow
from WMCore.MicroService.MSTransferor.DataStructs.NanoWorkflow import NanoWorkflow
from WMCore.MicroService.MSTransferor.DataStructs.RelValWorkflow import RelValWorkflow
from WMCore.MicroService.MSTransferor.DataStructs.Workflow import Workflow
from WMCore.MicroService.Tools.PycurlRucio import (getRucioToken, getPileupContainerSizesRucio,
                                                   listReplicationRules, getBlocksAndSizeRucio)
from WMCore.MicroService.Tools.Common import (elapsedTime, findBlockParents,
                                              findParent, getBlocksByDsetAndRun,
                                              getFileLumisInBlock, getRunsInBlock)
from WMCore.MicroService.MSCore.MSCore import MSCore


def isNanoWorkflow(reqDict):
    """
    Function to parse the request dictionary and decide whether it
    corresponds to a MiniAODSIM to NanoAODSIM workflow.
    :param reqDict: dictionary with the workflow description
    :return: a boolean True if workflow is Nano, False otherwise.
    """
    inputDset = ""
    if reqDict['RequestType'] == "TaskChain":
        inputDset = reqDict["Task1"].get("InputDataset", "")
    elif reqDict['RequestType'] == "StepChain":
        inputDset = reqDict["Step1"].get("InputDataset", "")

    if inputDset.endswith("/MINIAODSIM"):
        return True
    return False


class RequestInfo(MSCore):
    """
    RequestInfo class provides functionality to access and
    manipulate requests.
    """

    def __init__(self, msConfig, rucioObj, logger):
        """
        Basic setup for this RequestInfo module
        """
        extraArgs = {"skipReqMgr": True, "skipRucio": True}
        super(RequestInfo, self).__init__(msConfig, logger=logger, **extraArgs)

        self.rucio = rucioObj
        self.rucioToken = None
        self.tokenValidity = None
        self.openRunning = self.msConfig["openRunning"]

    def __call__(self, reqRecords):
        """
        Run the unified transferor box
        :param reqRecords: input records
        :return: output records
        """
        # obtain new unified Configuration
        uConfig = self.unifiedConfig()
        if not uConfig:
            self.logger.warning("Failed to fetch the latest unified config. Skipping this cycle")
            return []
        self.logger.info("Going to process %d requests.", len(reqRecords))

        # create a Workflow object representing the request, matching
        # against some specific templates
        workflows = self.classifyWorkflows(reqRecords)

        # setup the Rucio token
        self.setupRucio()
        # get complete requests information (based on Unified Transferor logic)
        self.unified(workflows)

        return workflows

    def classifyWorkflows(self, reqRecords):
        """
        This method classifies the provided workflows into their
        respective MS templates. Making it easier to retrieve
        input data and parameters for input data placement.

        :param reqRecords: list of workflow dictionaries (from ReqMgr2)
        :return: a custom python object for the workflow type
        """
        workflows = []
        for record in reqRecords:
            if record.get("SubRequestType") in ['RelVal', 'HIRelVal']:
                wflow = RelValWorkflow(record['RequestName'], record, logger=self.logger)
            elif record.get("RequestType") == "DQMHarvest":
                wflow = DQMHarvestWorkflow(record['RequestName'], record, logger=self.logger)
            elif record.get("OpenRunningTimeout", 0) > self.openRunning:
                wflow = GrowingWorkflow(record['RequestName'], record, logger=self.logger)
            elif isNanoWorkflow(record):
                wflow = NanoWorkflow(record['RequestName'], record, logger=self.logger)
            else:
                wflow = Workflow(record['RequestName'], record, logger=self.logger)

            workflows.append(wflow)
            msg = f"Processing request: {wflow.getName()}, "
            msg += f"with transferor template: {wflow.__class__.__name__}, "
            msg += f"with campaigns: {wflow.getCampaigns()} and "
            msg += f"input data as:\n{pformat(wflow.getDataCampaignMap())}"
            self.logger.info(msg)
        return workflows

    def setupRucio(self):
        """
        Check whether Rucio is enabled and create a new token, or renew it if needed
        """
        if not self.tokenValidity:
            # a brand new token needs to be created. To be done in the coming lines...
            pass
        elif self.tokenValidity:
            # then check the token lifetime
            dateTimeNow = int(datetime.datetime.utcnow().strftime("%s"))
            timeDiff = self.tokenValidity - dateTimeNow
            if timeDiff > 30 * 60: # 30min
                # current token still valid for a while
                return

        self.rucioToken, self.tokenValidity = getRucioToken(self.msConfig['rucioAuthUrl'],
                                                            self.msConfig['rucioAccount'])

    def unified(self, workflows):
        """
        Unified Transferor black box
        :param workflows: input workflow objects
        """
        # get aux info for dataset/blocks from inputs/parents/pileups
        # make subscriptions based on site white/black lists
        self.logger.info("Unified method processing %d requests", len(workflows))

        orig = time.time()
        # start by finding what are the parent datasets for requests requiring it
        time0 = time.time()
        parentMap = self.getParentDatasets(workflows)
        self.setParentDatasets(workflows, parentMap)
        self.logger.info(elapsedTime(time0, "### getParentDatasets"))

        # then check the secondary dataset sizes and locations
        time0 = time.time()
        sizeByDset, locationByDset = self.getSecondaryDatasets(workflows)
        locationByDset = self.resolveSecondaryRSEs(locationByDset)
        self.setSecondaryDatasets(workflows, sizeByDset, locationByDset)
        self.logger.info(elapsedTime(time0, "### getSecondaryDatasets"))

        # get final primary and parent list of valid blocks,
        # considering run, block and lumi lists
        time0 = time.time()
        blocksByDset = self.getInputDataBlocks(workflows)
        self.setInputDataBlocks(workflows, blocksByDset)
        self.logger.info(elapsedTime(time0, "### getInputDataBlocks"))

        # get a final list of parent blocks
        time0 = time.time()
        parentageMap = self.getParentChildBlocks(workflows)
        self.setParentChildBlocks(workflows, parentageMap)
        self.logger.info(elapsedTime(time0, "### getParentChildBlocks"))
        self.logger.info(elapsedTime(orig, '### total time for unified method'))
        self.logger.info("Unified method successfully processed %d requests", len(workflows))

        return workflows

    def _workflowRemoval(self, listOfWorkflows, workflowsToRetry):
        """
        Receives the initial list of workflows and another list of workflows
        that failed somewhere in the MS processing (like fetching information
        from the data-services); and remove those workflows from this cycle of
        the MSTransferor, such that they can be retried in the next cycle.
        :param listOfWorkflows: reference to the list of the initial workflows
        :param workflowsToRetry: list of workflows with missing information
        :return: nothing, the workflow removal is done in place
        """
        for wflow in set(workflowsToRetry):
            self.logger.warning("Removing workflow that failed processing in MSTransferor: %s", wflow.getName())
            listOfWorkflows.remove(wflow)

    def getParentDatasets(self, workflows):
        """
        Given a list of requests, find which requests need to process a parent
        dataset, and discover what the parent dataset name is.
        :return: dictionary with the child and the parent dataset
        """
        retryWorkflows = []
        retryDatasets = []
        datasetByDbs = {}
        parentByDset = {}
        for wflow in workflows:
            if wflow.hasParents():
                datasetByDbs.setdefault(wflow.getDbsUrl(), set())
                datasetByDbs[wflow.getDbsUrl()].add(wflow.getInputDataset())

        for dbsUrl, datasets in viewitems(datasetByDbs):
            self.logger.info("Resolving %d dataset parentage against DBS: %s", len(datasets), dbsUrl)
            # first find out what's the parent dataset name
            parentByDset.update(findParent(datasets, dbsUrl))

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in viewitems(parentByDset):
            if value is None:
                retryDatasets.append(dset)
        if retryDatasets:
            for wflow in workflows:
                if wflow.hasParents() and wflow.getInputDataset() in retryDatasets:
                    retryWorkflows.append(wflow)
            # remove workflows that failed one or more of the bulk queries to the data-service
            self._workflowRemoval(workflows, retryWorkflows)

        return parentByDset

    def setParentDatasets(self, workflows, parentageMap):
        """
        Set the parent dataset for workflows requiring parents
        """
        for wflow in workflows:
            if wflow.hasParents() and wflow.getInputDataset() in parentageMap:
                wflow.setParentDataset(parentageMap[wflow.getInputDataset()])

    def getSecondaryDatasets(self, workflows):
        """
        Given a list of requests, list all the pileup datasets and, find their
        total dataset sizes and which locations host completed and subscribed datasets.
        NOTE it only uses valid blocks (i.e., blocks with at least one replica!)
        :param workflows: a list of Workflow objects
        :return: two dictionaries keyed by the dataset.
           First contains dataset size as value.
           Second contains a list of locations as value.
        """
        retryWorkflows = []
        retryDatasets = []
        datasets = set()
        for wflow in workflows:
            datasets = datasets | wflow.getPileupDatasets()

        # retrieve pileup container size and locations from Rucio
        self.logger.info("Fetching pileup dataset sizes for %d datasets against Rucio: %s",
                         len(datasets), self.msConfig['rucioUrl'])
        sizesByDset = getPileupContainerSizesRucio(datasets, self.msConfig['rucioUrl'], self.rucioToken)

        # then fetch data location for locked data, under our own rucio account
        self.logger.info("Fetching pileup container location for %d containers against Rucio: %s",
                         len(datasets), self.msConfig['rucioUrl'])
        locationsByDset = listReplicationRules(datasets, self.msConfig['rucioAccount'],
                                               grouping="A", rucioUrl=self.msConfig['rucioUrl'],
                                               rucioToken=self.rucioToken)
        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in viewitems(sizesByDset):
            if value is None:
                retryDatasets.append(dset)
        for dset, value in viewitems(locationsByDset):
            if value is None:
                retryDatasets.append(dset)
        if retryDatasets:
            for wflow in workflows:
                for pileup in wflow.getPileupDatasets():
                    if pileup in retryDatasets:
                        retryWorkflows.append(wflow)
            # remove workflows that failed one or more of the bulk queries to the data-service
            self._workflowRemoval(workflows, retryWorkflows)
        return sizesByDset, locationsByDset

    def resolveSecondaryRSEs(self, rsesByContainer):
        """
        Given a dictionary with containers and their list of RSE
        expressions, resolve the RSE expressions into RSE names,
        dropping all the Tape RSEs.
        :param rsesByContainer: dict key'ed by the container with a list of expressions
        :return: a dictionary key'ed by the container name, with a flat list of unique
            RSE names.
        """
        self.logger.info("Resolving Rucio RSE expressions for %d containers", len(rsesByContainer))
        for contName in list(rsesByContainer):
            rseNames = []
            for rseExpr in rsesByContainer[contName]:
                rseNames.extend(self.rucio.evaluateRSEExpression(rseExpr, returnTape=False))
            rsesByContainer[contName] = list(set(rseNames))
        return rsesByContainer

    def setSecondaryDatasets(self, workflows, sizesByDset, locationsByDset):
        """
        Given dictionaries with the pileup dataset size and locations, set the
        workflow object accordingly.
        """
        for wflow in workflows:
            for dsetName in wflow.getPileupDatasets():
                wflow.setSecondarySummary(dsetName, sizesByDset[dsetName], locationsByDset[dsetName])

    def getInputDataBlocks(self, workflows):
        """
        Given a list of requests, list all the primary and parent datasets and, find
        their block sizes and which locations host completed and subscribed blocks
        NOTE it only uses valid blocks (i.e., blocks with at least one replica!)
        :param workflows: a list of Workflow objects
        :return: dictionary with dataset and a few block information
        """
        retryWorkflows = []
        retryDatasets = []
        datasets = set()
        for wflow in workflows:
            for dataIn in wflow.getDataCampaignMap():
                if dataIn['type'] in ["primary", "parent"]:
                    datasets.add(dataIn['name'])

        # fetch all block names and their sizes from Rucio
        self.logger.info("Fetching parent/primary block sizes for %d containers against Rucio: %s",
                         len(datasets), self.msConfig['rucioUrl'])
        blocksByDset = getBlocksAndSizeRucio(datasets, self.msConfig['rucioUrl'], self.rucioToken)

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dsetName in blocksByDset:
            if blocksByDset[dsetName] is None:
                retryDatasets.append(dsetName)
        if retryDatasets:
            for wflow in workflows:
                if wflow.getInputDataset() in retryDatasets or wflow.getParentDataset() in retryDatasets:
                    retryWorkflows.append(wflow)
            # remove workflows that failed one or more of the bulk queries to the data-service
            self._workflowRemoval(workflows, retryWorkflows)
        return blocksByDset

    def setInputDataBlocks(self, workflows, blocksByDset):
        """
        Provided a dictionary structure of dictionary, block name, and a couple of
        block information, set the workflow attributes accordingly.
        """
        retryWorkflows = []
        for wflow in workflows:
            try:
                for dataIn in wflow.getDataCampaignMap():
                    if dataIn['type'] == "primary":
                        newBlockDict = self._handleInputDataInfo(wflow, dataIn['name'],
                                                                 blocksByDset[dataIn['name']])
                        wflow.setPrimaryBlocks(newBlockDict)
                    elif dataIn['type'] == "parent":
                        newBlockDict = self._handleInputDataInfo(wflow, dataIn['name'],
                                                                 blocksByDset[dataIn['name']])
                        wflow.setParentBlocks(newBlockDict)
            except Exception:
                self.logger.error("Workflow: %s will be retried in the next cycle", wflow.getName())
                retryWorkflows.append(wflow)
        # remove workflows that failed one or more of the bulk queries to the data-service
        self._workflowRemoval(workflows, retryWorkflows)

    def _handleInputDataInfo(self, wflow, dset, blocksDict):
        """
        Applies any run/block/lumi list on the primary and parent
        blocks provided.
        It's a convoluted logic, such as:
         1) if there is no run/block/lumi list, just return the initial blocksDict
         2) if it has lumi list, filter runs from it and run block discovery
            given a dataset name and a list of runs
         3) if it has RunWhitelist, run block discovery for a given dataset name
            and a list of runs
         4) if it has only RunBlacklist, discover the run list of all initial blocks
            provided in blocksDict and remove blocks matching only the black list
         5) for the steps above, always check whether the block has replicas
         6) NOW that the block data discovery is completed (considering runs):
           * if LumiList is not enabled, just return the current list of blocks
           * else, fetch file/run/lumi information in bulk of blocks and compare it
           to the LumiList, skipping blocks without a single file that matches it.

        Note that the LumiList check is dealt with in a similar way
        as done in the WorkQueue StartPolicyInterface/getMaskedBlocks:

        :param wflow: the Workflow object
        :param dset: dataset name
        :param blocksDict: dictionary of blocks, their size and location
        :return: dictionary of block names and block size
        """
        finalBlocks = {}
        dbsUrl = wflow.getDbsUrl()
        runAllowedList = wflow.getRunWhitelist()
        runForbiddenList = set(wflow.getRunBlacklist())
        lumiList = wflow.getLumilist()
        # if there is no filter on the input data, simply return it
        if not (lumiList or runAllowedList or runForbiddenList):
            return self._removeZeroSizeBlocks(blocksDict)

        if lumiList:
            # LumiList has precedence over RunWhitelist
            runAllowedList = []
            for run in lumiList.getRuns():
                runAllowedList.append(int(run))
            runAllowedList = list(set(runAllowedList))
        if runAllowedList:
            # Run number 1 is not supported by DBSServer
            if int(runAllowedList[0]) == 1:
                finalBlocks = deepcopy(blocksDict)
            else:
                runAllowedList = list(set(runAllowedList) - runForbiddenList)
                self.logger.info("Fetching blocks matching a list of runs for %s", wflow.getName())
                try:
                    blocks = getBlocksByDsetAndRun(dset, runAllowedList, dbsUrl)
                except Exception as exc:
                    msg = "Failed to retrieve blocks by dataset '%s'and run: %s\n" % (dset, runAllowedList)
                    msg += "Error details: %s" % str(exc)
                    self.logger.error(msg)
                    raise
                for block in blocks:
                    if block in blocksDict:
                        finalBlocks[block] = deepcopy(blocksDict[block])
                    else:
                        self.logger.warning("Dropping block existent in DBS but not in Rucio: %s", block)
        elif runForbiddenList:
            # only run blacklist set
            self.logger.info("Fetching runs in blocks for RunBlacklist for %s", wflow.getName())
            try:
                blockRuns = getRunsInBlock(list(blocksDict), dbsUrl)
            except Exception as exc:
                self.logger.error("Failed to bulk retrieve runs per block. Details: %s", str(exc))
                raise
            for block, runs in viewitems(blockRuns):
                if not set(runs).difference(runForbiddenList):
                    self.logger.info("Dropping block with only blacklisted runs: %s", block)
                elif block in blocksDict:
                    finalBlocks[block] = deepcopy(blocksDict[block])

        if lumiList:
            self.logger.info("Fetching block/lumi information for %d blocks in %s",
                             len(finalBlocks), wflow.getName())
            self.logger.debug("with the following run whitelist: %s", runAllowedList)
            goodBlocks = set()
            # now with a smaller set of blocks in hand, we collect their lumi
            # information and discard any blocks not matching the lumi list
            for blockSlice in grouper(finalBlocks, 10):
                try:
                    blockFileLumis = getFileLumisInBlock(blockSlice, dbsUrl, validFileOnly=1)
                except Exception as exc:
                    self.logger.error("Failed to bulk retrieve run/lumi per block. Details: %s", str(exc))
                    raise
                for block, fileLumis in viewitems(blockFileLumis):
                    for fileLumi in fileLumis:
                        if int(fileLumi['run_num']) not in runAllowedList:
                            continue
                        runNumber = str(fileLumi['run_num'])
                        lumis = fileLumi['lumi_section_num']
                        fileMask = LumiList(runsAndLumis={runNumber: lumis})
                        if lumiList & fileMask:
                            # then it has lumis that we need, keep this block and move on
                            goodBlocks.add(block)
                            break
            # last but not least, drop any blocks that are not in the good list
            for block in list(finalBlocks):
                if block not in goodBlocks:
                    self.logger.info("Dropping block not matching LumiList: %s", block)
                    finalBlocks.pop(block)

        return self._removeZeroSizeBlocks(finalBlocks)

    def _removeZeroSizeBlocks(self, blocksDict):
        """
        Given a dictionary of blocks and their block size and location information,
        return only blocks with >0 bytes of block size (Rucio blocks with no replicas/
        files result in blocks with None size).
        :return: dictionary of block names and block size
        """
        finalBlocks = {}
        for blockName in blocksDict:
            if blocksDict[blockName]['blockSize']:
                finalBlocks[blockName] = blocksDict[blockName]
            else:
                self.logger.info("Dropping block: %s with no files and size: %s",
                                 blockName, blocksDict[blockName]['blockSize'])
        return finalBlocks

    def getParentChildBlocks(self, workflows):
        """
        Given a list of requests, get their children block, discover their parent blocks
        and finally filter out any parent blocks with only invalid files (without any replicas)
        :param workflows: list of workflow objects
        :return: nothing, updates the workflow attributes in place
        """
        retryWorkflows = []
        retryDatasets = []
        blocksByDbs = {}
        parentageMap = {}
        for wflow in workflows:
            blocksByDbs.setdefault(wflow.getDbsUrl(), set())
            if wflow.getParentDataset():
                blocksByDbs[wflow.getDbsUrl()] = blocksByDbs[wflow.getDbsUrl()] | set(wflow.getPrimaryBlocks().keys())

        for dbsUrl, blocks in viewitems(blocksByDbs):
            if not blocks:
                continue
            self.logger.debug("Fetching DBS parent blocks for %d children blocks...", len(blocks))
            # first find out what's the parent dataset name
            parentageMap.update(findBlockParents(blocks, dbsUrl))

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in viewitems(parentageMap):
            if value is None:
                retryDatasets.append(dset)
        if retryDatasets:
            for wflow in workflows:
                if wflow.getParentDataset() in retryDatasets:
                    retryWorkflows.append(wflow)
            # remove workflows that failed one or more of the bulk queries to the data-service
            self._workflowRemoval(workflows, retryWorkflows)
        return parentageMap

    def setParentChildBlocks(self, workflows, parentageMap):
        """
        Provided a dictionary with the dataset, the child block and a set
        of the parent blocks, set the workflow attribute accordingly
        """
        for wflow in workflows:
            if wflow.getParentDataset() and wflow.getInputDataset() in parentageMap:
                wflow.setChildToParentBlocks(parentageMap[wflow.getInputDataset()])
