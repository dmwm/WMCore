#!/usr/bin/env python
"""
_RequestInfo_

Class to hold and parse all information related to a given request
"""
# futures
from __future__ import division, print_function

# system modules
import datetime
import json
import pickle
import time
# WMCore modules
from pprint import pformat
from copy import deepcopy
from Utils.IteratorTools import grouper
from WMCore.DataStructs.LumiList import LumiList
from WMCore.MicroService.DataStructs.Workflow import Workflow
from WMCore.MicroService.Tools.PycurlRucio import (getRucioToken, getPileupContainerSizesRucio,
                                                   listReplicationRules, getBlocksAndSizeRucio)
from WMCore.MicroService.Unified.Common import \
    elapsedTime, cert, ckey, workflowsInfo, eventsLumisInfo, getIO, \
    dbsInfo, phedexInfo, getComputingTime, getNCopies, teraBytes, \
    findBlockParents, findParent, getBlocksByDsetAndRun, getFileLumisInBlock, \
    getBlockReplicasAndSize, getPileupDatasetSizes, getPileupSubscriptions, getRunsInBlock
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.SiteInfo import SiteInfo
from WMCore.Services.pycurl_manager import getdata \
    as multi_getdata, RequestHandler


class RequestInfo(MSCore):
    """
    RequestInfo class provides functionality to access and
    manipulate requests.
    """

    def __init__(self, msConfig, logger):
        """
        Basic setup for this RequestInfo module
        """
        extraArgs = {"skipReqMgr": True, "skipRucio": True}
        super(RequestInfo, self).__init__(msConfig, logger=logger, **extraArgs)

        self.rucioToken = None
        self.tokenValidity = None

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

        # create a Workflow object representing the request
        workflows = []
        for record in reqRecords:
            wflow = Workflow(record['RequestName'], record, logger=self.logger)
            workflows.append(wflow)
            msg = "Processing request: %s, with campaigns: %s, " % (wflow.getName(),
                                                                    wflow.getCampaigns())
            msg += "and input data as:\n%s" % pformat(wflow.getDataCampaignMap())
            self.logger.info(msg)

        # setup the Rucio token
        self.setupRucio()
        # get complete requests information (based on Unified Transferor logic)
        self.unified(workflows)

        return workflows

    def setupRucio(self):
        """
        Check whether Rucio is enabled and create a new token, or renew it if needed
        """
        if not self.msConfig['useRucio']:
            return

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
        self.logger.debug(elapsedTime(time0, "### getParentDatasets"))

        # then check the secondary dataset sizes and locations
        time0 = time.time()
        sizeByDset, locationByDset = self.getSecondaryDatasets(workflows)
        self.setSecondaryDatasets(workflows, sizeByDset, locationByDset)
        self.logger.debug(elapsedTime(time0, "### getSecondaryDatasets"))

        # get final primary and parent list of valid blocks,
        # considering run, block and lumi lists
        time0 = time.time()
        blocksByDset = self.getInputDataBlocks(workflows)
        self.setInputDataBlocks(workflows, blocksByDset)
        self.logger.debug(elapsedTime(time0, "### getInputDataBlocks"))

        # get a final list of parent blocks
        time0 = time.time()
        parentageMap = self.getParentChildBlocks(workflows)
        self.setParentChildBlocks(workflows, parentageMap)
        self.logger.debug(elapsedTime(time0, "### getParentChildBlocks"))
        self.logger.info(elapsedTime(orig, '### total time for unified method'))
        self.logger.info("Unified method successfully processed %d requests", len(workflows))

        return workflows

    def unifiedUnused(self):
        """
        FIXME FIXME TODO
        Leave this code in a different method until we evaluate what
        is needed and what is not, and refactor this thing...
        """
        # FIXME making pylint happy, remove these assignments
        requestNames = []
        uConfig = {}

        # requestNames = [r.getName() for r in workflows]
        # TODO: the logic below shows original unified port and it should be
        #       revisited wrt new proposal specs and unified codebase

        # get workflows from list of requests
        orig = time.time()
        time0 = time.time()
        requestWorkflows = self._getRequestWorkflows(requestNames)
        requestWorkflows = requestWorkflows.values()
        self.logger.debug(elapsedTime(time0, "### getWorkflows"))

        # get workflows info summaries and collect datasets we need to process
        winfo = workflowsInfo(requestWorkflows)
        datasets = [d for row in winfo.values() for d in row['datasets']]

        # find dataset info
        time0 = time.time()
        datasetBlocks, datasetSizes, _datasetTransfers = dbsInfo(datasets, self.msConfig['dbsUrl'])
        self.logger.debug(elapsedTime(time0, "### dbsInfo"))

        # find block nodes information for our datasets
        time0 = time.time()
        blockNodes = phedexInfo(datasets, self.msConfig['phedexUrl'])
        self.logger.debug(elapsedTime(time0, "### phedexInfo"))

        # find events-lumis info for our datasets
        time0 = time.time()
        eventsLumis = eventsLumisInfo(datasets, self.msConfig['dbsUrl'])
        self.logger.debug(elapsedTime(time0, "### eventsLumisInfo"))

        # get specs for all requests and re-use them later in getSiteWhiteList as cache
        reqSpecs = self._getRequestSpecs(requestNames)

        # get siteInfo instance once and re-use it later, it is time-consumed object
        siteInfo = SiteInfo(uConfig)

        requestsToProcess = []
        tst0 = time.time()
        totBlocks = totEvents = totSize = totCpuT = 0
        for wflow in requestWorkflows:
            for wname, wspec in wflow.items():
                time0 = time.time()
                cput = getComputingTime(wspec, eventsLumis=eventsLumis, dbsUrl=self.msConfig['dbsUrl'],
                                        logger=self.logger)
                ncopies = getNCopies(cput)

                attrs = winfo[wname]
                ndatasets = len(attrs['datasets'])
                npileups = len(attrs['pileups'])
                nblocks = nevts = nlumis = size = 0
                nodes = set()
                for dataset in attrs['datasets']:
                    blocks = datasetBlocks[dataset]
                    for blk in blocks:
                        for node in blockNodes.get(blk, []):
                            nodes.add(node)
                    nblocks += len(blocks)
                    size += datasetSizes[dataset]
                    edata = eventsLumis.get(dataset, {'num_event': 0, 'num_lumi': 0})
                    nevts += edata['num_event']
                    nlumis += edata['num_lumi']
                totBlocks += nblocks
                totEvents += nevts
                totSize += size
                totCpuT += cput
                sites = json.dumps(sorted(list(nodes)))
                self.logger.debug("### %s", wname)
                self.logger.debug(
                        "%s datasets, %s blocks, %s bytes (%s TB), %s nevts, %s nlumis, cput %s, copies %s, %s",
                        ndatasets, nblocks, size, teraBytes(size), nevts, nlumis, cput, ncopies, sites)
                # find out which site can serve given workflow request
                t0 = time.time()
                lheInput, primary, parent, secondary, allowedSites \
                    = self._getSiteWhiteList(uConfig, wspec, siteInfo, reqSpecs)
                if not isinstance(primary, list):
                    primary = [primary]
                if not isinstance(secondary, list):
                    secondary = [secondary]
                wflowDatasets = primary + secondary
                wflowDatasetsBlocks = []
                for dset in wflowDatasets:
                    for item in datasetBlocks.get(dset, []):
                        wflowDatasetsBlocks.append(item)
                rdict = dict(name=wname, datasets=wflowDatasets,
                             blocks=wflowDatasetsBlocks,
                             npileups=npileups, size=size,
                             nevents=nevts, nlumis=nlumis, cput=cput, ncopies=ncopies,
                             sites=sites, allowedSites=allowedSites, parent=parent,
                             lheInput=lheInput, primary=primary, secondary=secondary)
                requestsToProcess.append(rdict)
                self.logger.debug(elapsedTime(t0, "### getSiteWhiteList"))
        self.logger.debug("total # of workflows %s, datasets %s, blocks %s, evts %s, size %s (%s TB), cput %s (hours)",
                          len(winfo.keys()), len(datasets), totBlocks, totEvents, totSize, teraBytes(totSize), totCpuT)
        self.logger.debug(elapsedTime(tst0, '### workflows info'))
        self.logger.debug(elapsedTime(orig, '### total time'))
        return requestsToProcess

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

        for dbsUrl, datasets in datasetByDbs.items():
            self.logger.info("Resolving %d dataset parentage against DBS: %s", len(datasets), dbsUrl)
            # first find out what's the parent dataset name
            parentByDset.update(findParent(datasets, dbsUrl))

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in parentByDset.items():
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

        if self.rucioToken:
            # now fetch valid blocks from PhEDEx and calculate the total dataset size
            self.logger.info("Fetching pileup dataset sizes for %d datasets against Rucio: %s",
                             len(datasets), self.msConfig['rucioUrl'])
            sizesByDset = getPileupContainerSizesRucio(datasets, self.msConfig['rucioUrl'], self.rucioToken)

            # then fetch data location for subscribed data, under the group provided in the config
            self.logger.info("Fetching pileup dataset location for %d datasets against Rucio: %s",
                             len(datasets), self.msConfig['rucioUrl'])
            locationsByDset = listReplicationRules(datasets, self.msConfig['rucioAccount'],
                                                   grouping="A", rucioUrl=self.msConfig['rucioUrl'],
                                                   rucioToken=self.rucioToken)
        else:
            # now fetch valid blocks from PhEDEx and calculate the total dataset size
            self.logger.info("Fetching pileup dataset sizes for %d datasets against PhEDEx: %s",
                             len(datasets), self.msConfig['phedexUrl'])
            sizesByDset = getPileupDatasetSizes(datasets, self.msConfig['phedexUrl'])

            # then fetch data location for subscribed data, under the group provided in the config
            self.logger.info("Fetching pileup dataset location for %d datasets against PhEDEx: %s",
                             len(datasets), self.msConfig['phedexUrl'])
            locationsByDset = getPileupSubscriptions(datasets, self.msConfig['phedexUrl'],
                                                     percentMin=self.msConfig['minPercentCompletion'])

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in sizesByDset.items():
            if value is None:
                retryDatasets.append(dset)
        for dset, value in locationsByDset.items():
            if value is None:
                retryDatasets.append(dset)
        if retryDatasets:
            for wflow in workflows:
                for pileup in wflow.getPileupDatasets():
                    if pileup in  retryDatasets:
                        retryWorkflows.append(wflow)
            # remove workflows that failed one or more of the bulk queries to the data-service
            self._workflowRemoval(workflows, retryWorkflows)
        return sizesByDset, locationsByDset

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

        if self.rucioToken:
            # now fetch valid blocks from PhEDEx and calculate the total dataset size
            self.logger.info("Fetching parent/primary block info for %d datasets against Rucio: %s",
                             len(datasets), self.msConfig['rucioUrl'])
            blocksByDset = getBlocksAndSizeRucio(datasets, self.msConfig['rucioUrl'], self.rucioToken)
        else:
            # now fetch block names from PhEDEx
            self.logger.info("Fetching parent/primary block info for %d datasets against PhEDEx: %s",
                             len(datasets), self.msConfig['phedexUrl'])
            blocksByDset = getBlockReplicasAndSize(datasets, self.msConfig['phedexUrl'])

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in blocksByDset.items():
            if value is None:
                retryDatasets.append(dset)
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
        runWhite = wflow.getRunWhitelist()
        runBlack = set(wflow.getRunBlacklist())
        lumiList = wflow.getLumilist()
        if lumiList:
            # LumiList has precedence over RunWhitelist
            runWhite = []
            for run in lumiList.getRuns():
                runWhite.append(int(run))
            runWhite = list(set(runWhite))
        if runWhite:
            # Run number 1 is not supported by DBSServer
            if int(runWhite[0]) == 1:
                finalBlocks = deepcopy(blocksDict)
            else:
                runWhite = list(set(runWhite) - runBlack)
                self.logger.info("Fetching blocks matching a list of runs for %s", wflow.getName())
                try:
                    blocks = getBlocksByDsetAndRun(dset, runWhite, dbsUrl)
                except Exception as exc:
                    msg = "Failed to retrieve blocks by dataset '%s'and run: %s\n" % (dset, runWhite)
                    msg += "Error details: %s" % str(exc)
                    self.logger.error(msg)
                    raise
                for block in blocks:
                    if block in blocksDict:
                        finalBlocks[block] = deepcopy(blocksDict[block])
                    else:
                        self.logger.info("Dropping block with no replicas in PhEDEx: %s", block)
        elif runBlack:
            # only run blacklist set
            self.logger.info("Fetching runs in blocks for RunBlacklist for %s", wflow.getName())
            try:
                blockRuns = getRunsInBlock(list(blocksDict), dbsUrl)
            except Exception as exc:
                self.logger.error("Failed to bulk retrieve runs per block. Details: %s", str(exc))
                raise
            for block, runs in blockRuns.items():
                if not set(runs).difference(runBlack):
                    self.logger.info("Dropping block with only blacklisted runs: %s", block)
                elif block in blocksDict:
                    finalBlocks[block] = deepcopy(blocksDict[block])

        if lumiList:
            self.logger.info("Fetching block/lumi information for %d blocks in %s",
                             len(finalBlocks), wflow.getName())
            self.logger.debug("with the following run whitelist: %s", runWhite)
            goodBlocks = set()
            # now with a smaller set of blocks in hand, we collect their lumi
            # information and discard any blocks not matching the lumi list
            for blockSlice in grouper(finalBlocks, 10):
                try:
                    blockFileLumis = getFileLumisInBlock(blockSlice, dbsUrl, validFileOnly=1)
                except Exception as exc:
                    self.logger.error("Failed to bulk retrieve run/lumi per block. Details: %s", str(exc))
                    raise
                for block, fileLumis in blockFileLumis.items():
                    for fileLumi in fileLumis:
                        if int(fileLumi['run_num']) not in runWhite:
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

        if not finalBlocks:
            finalBlocks = blocksDict
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

        for dbsUrl, blocks in blocksByDbs.items():
            if not blocks:
                continue
            self.logger.debug("Fetching DBS parent blocks for %d children blocks...", len(blocks))
            # first find out what's the parent dataset name
            parentageMap.update(findBlockParents(blocks, dbsUrl))

        # now check if any of our calls failed; if so, workflow needs to be skipped from this cycle
        # FIXME: isn't there a better way to do this?!?
        for dset, value in parentageMap.items():
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

    # FIXME: get rid of this method and use the Workflow objects instead
    def _getRequestWorkflows(self, requestNames):
        "Helper function to get all specs for given set of request names"
        urls = [str('%s/data/request/%s' % (self.msConfig['reqmgr2Url'], r)) for r in requestNames]
        self.logger.debug("getRequestWorkflows")
        for u in urls:
            self.logger.debug("url %s", u)
        data = multi_getdata(urls, ckey(), cert())
        rdict = {}
        for row in data:
            req = row['url'].split('/')[-1]
            try:
                data = json.loads(row['data'])
                rdict[req] = data['result'][0]  # we get back {'result': [workflow]} dict
            except Exception as exp:
                self.logger.error("fail to process row %s", row)
                self.logger.exception("fail to load data as json record, error=%s", str(exp))
        return rdict

    def _getRequestSpecs(self, requestNames):
        "Helper function to get all specs for given set of request names"
        urls = [str('%s/%s/spec' % (self.msConfig['reqmgrCacheUrl'], r)) for r in requestNames]
        data = multi_getdata(urls, ckey(), cert())
        rdict = {}
        for row in data:
            req = row['url'].split('/')[-2]
            rdict[req] = pickle.loads(row['data'])
        return rdict

    def _getSiteWhiteList(self, uConfig, request, siteInfo, reqSpecs=None, pickone=False):
        "Return site list for given request"
        lheinput, primary, parent, secondary = getIO(request, self.msConfig['dbsUrl'])
        allowedSites = []
        if lheinput:
            allowedSites = sorted(siteInfo.sites_eos)
        elif secondary:
            if self.heavyRead(request):
                allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodIO))
            else:
                allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodAAA))
        elif primary:
            allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_T2s + siteInfo.sites_T3s))
        else:
            # no input at all all site should contribute
            allowedSites = sorted(set(siteInfo.sites_T2s + siteInfo.sites_T1s + siteInfo.sites_T3s))
        if pickone:
            allowedSites = sorted([siteInfo.pick_CE(allowedSites)])

        # do further restrictions based on memory
        # do further restrictions based on blow-up factor
        minChildJobPerEvent, rootJobPerEvent, blowUp = self._getBlowupFactors(request, reqSpecs=reqSpecs)
        maxBlowUp, neededCores = uConfig.get('blow_up_limits', (0, 0))
        if blowUp > maxBlowUp:
            # then restrict to only sites with >4k slots
            siteCores = [site for site in allowedSites
                         if siteInfo.cpu_pledges[site] > neededCores]
            newAllowedSites = list(set(allowedSites) & set(siteCores))
            if newAllowedSites:
                allowedSites = newAllowedSites
                msg = "restricting site white list because of blow-up factor: "
                msg += 'minChildJobPerEvent=%s ' % minChildJobPerEvent
                msg += 'rootJobPerEvent=%s' % rootJobPerEvent
                msg += 'maxBlowUp=%s' % maxBlowUp
                self.logger.debug(msg)

        for campaign in self.getCampaigns(request):
            # for testing purposes add post campaign call
            # res = reqmgrAux.postCampaignConfig(campaign, {'%s_name' % campaign: {"Key1": "Value1"}})
            campaignConfig = self.reqmgrAux.getCampaignConfig(campaign)
            if isinstance(campaignConfig, list):
                campaignConfig = campaignConfig[0]
            campSites = campaignConfig.get('SiteWhitelist', [])
            if campSites:
                msg = "Using site whitelist restriction by campaign=%s " % campaign
                msg += "configuration=%s" % sorted(campSites)
                self.logger.debug(msg)
                allowedSites = list(set(allowedSites) & set(campSites))
                if not allowedSites:
                    allowedSites = list(campSites)

            campBlackList = campaignConfig.get('SiteBlacklist', [])
            if campBlackList:
                self.logger.debug("Reducing the whitelist due to black list in campaign configuration")
                self.logger.debug("Removing %s", campBlackList)
                allowedSites = list(set(allowedSites) - set(campBlackList))

        ncores = self.getMulticore(request)
        memAllowed = siteInfo.sitesByMemory(float(request['Memory']), maxCore=ncores)
        if memAllowed is not None:
            msg = "sites allowing %s " % request['Memory']
            msg += "MB and ncores=%s" % ncores
            msg += "core are %s" % sorted(memAllowed)
            self.logger.debug(msg)
            # mask to sites ready for mcore
            if ncores > 1:
                memAllowed = list(set(memAllowed) & set(siteInfo.sites_mcore_ready))
            allowedSites = list(set(allowedSites) & set(memAllowed))
        return lheinput, list(primary), list(parent), list(secondary), list(sorted(allowedSites))

    def _getBlowupFactors(self, request, reqSpecs=None):
        "Return blowup factors for given request"
        if request['RequestType'] != 'TaskChain':
            return 1., 1., 1.
        minChildJobPerEvent = None
        rootJobPerEvent = None
        maxBlowUp = 0
        splits = self._getSplittings(request, reqSpecs=reqSpecs)
        for item in splits:
            cSize = None
            pSize = None
            task = item['splittingTask']
            for key in ['events_per_job', 'avg_events_per_job']:
                if key in item:
                    cSize = item[key]
            parents = [s for s in splits
                       if task.startswith(s['splittingTask']) and task != s['splittingTask']]
            if parents:
                for parent in parents:
                    for key in ['events_per_job', 'avg_events_per_job']:
                        if key in parent:
                            pSize = parent[key]
                    if not minChildJobPerEvent or minChildJobPerEvent > cSize:
                        minChildJobPerEvent = cSize
            else:
                rootJobPerEvent = cSize
            if cSize and pSize:
                blowUp = float(pSize) / cSize
                if blowUp > maxBlowUp:
                    maxBlowUp = blowUp
        return minChildJobPerEvent, rootJobPerEvent, maxBlowUp

    def _getSplittings(self, request, reqSpecs=None):
        "Return splittings for given request"
        spl = []
        for task in self.getWorkTasks(request, reqSpecs=reqSpecs):
            tsplit = task.input.splitting
            spl.append({"splittingAlgo": tsplit.algorithm, "splittingTask": task.pathName})
            get_those = ['events_per_lumi', 'events_per_job', 'lumis_per_job',
                         'halt_job_on_file_boundaries', 'job_time_limit',
                         'halt_job_on_file_boundaries_event_aware']
            translate = {'EventAwareLumiBased': [('events_per_job', 'avg_events_per_job')]}
            include = {'EventAwareLumiBased': {'halt_job_on_file_boundaries_event_aware': 'True'},
                       'LumiBased': {'halt_job_on_file_boundaries': 'True'}}
            if tsplit.algorithm in include:
                for key, val in include[tsplit.algorithm].items():
                    spl[-1][key] = val
            for get in get_those:
                if hasattr(tsplit, get):
                    setTo = get
                    if tsplit.algorithm in translate:
                        for src, des in translate[tsplit.algorithm]:
                            if src == get:
                                setTo = des
                                break
                    spl[-1][setTo] = getattr(tsplit, get)
        return spl

    def getWorkTasks(self, request, reqSpecs=None):
        "Return work tasks for given request"
        select = {'taskType': ['Production', 'Processing', 'Skim']}
        allTasks = []
        tasks = self.getSpec(request, reqSpecs).tasks
        for task in tasks.tasklist:
            node = getattr(tasks, task)
            allTasks.extend(self.taskDescending(node, select))
        return allTasks

    def getSpec(self, request, reqSpecs=None):
        "Get request from workload cache"
        if reqSpecs and request['RequestName'] in reqSpecs:
            return reqSpecs[request['RequestName']]
        url = str('%s/%s/spec' % (self.msConfig['reqmgrCacheUrl'], request['RequestName']))
        mgr = RequestHandler()
        data = mgr.getdata(url, params={}, cert=cert(), ckey=ckey())
        return pickle.loads(data)

    def taskDescending(self, node, select=None):
        "Helper function to walk through task nodes in descending order"
        allTasks = []
        if not select:
            allTasks.append(node)
        else:
            for key, value in select.items():
                if (isinstance(value, list) and getattr(node, key) in value) or \
                        (not isinstance(value, list) and getattr(node, key) == value):
                    allTasks.append(node)
                    break

        for child in node.tree.childNames:
            chItem = getattr(node.tree.children, child)
            allTasks.extend(self.taskDescending(chItem, select))
        return allTasks

    def getCampaigns(self, request):
        "Return campaigns of given request"
        if 'Chain' in request['RequestType'] and not self.isRelval(request):
            return list(set(self.collectinchain(request, 'AcquisitionEra').values()))
        return [request['Campaign']]

    def heavyRead(self, request):
        """
        Return True by default. False if 'premix' appears in the
        output datasets or in the campaigns
        """
        response = True
        if any(['premix' in c.lower() for c in self.getCampaigns(request)]):
            response = False
        if any(['premix' in o.lower() for o in request['OutputDatasets']]):
            response = False
        return response

    def isRelval(self, request):
        "Return if given request is RelVal sample"
        if 'SubRequestType' in request and 'RelVal' in request['SubRequestType']:
            return True
        return False

    def collectinchain(self, request, member, func=None, default=None):
        "Helper function to return dictionary of collection chain"
        if request['RequestType'] == 'StepChain':
            return self.collectionHelper(request, member, func, default, base='Step')
        elif request['RequestType'] == 'TaskChain':
            return self.collectionHelper(request, member, func, default, base='Task')
        else:
            raise Exception("should not call collectinchain on non-chain request")

    def collectionHelper(self, request, member, func=None, default=None, base=None):
        "Helper function to return uhm chain as a dictionary"
        coll = {}
        item = 1
        while '%s%d' % (base, item) in request:
            if member in request['%s%d' % (base, item)]:
                if func:
                    coll[request['%s%d' % (base, item)]['%sName' % base]] = \
                        func(request['%s%d' % (base, item)][member])
                else:
                    coll[request['%s%d' % (base, item)]['%sName' % base]] = \
                        request['%s%d' % (base, item)].get(member, default)
            item += 1
        return coll

    def getMulticore(self, request):
        "Return max number of cores for a given request"
        mcores = [int(request.get('Multicore', 1))]
        if 'Chain' in request['RequestType']:
            mcoresCol = self.collectinchain(request, 'Multicore', default=1)
            mcores.extend([int(v) for v in mcoresCol.values()])
        return max(mcores)
