"""
Workflow class provides all the workflow data
required by MS Transferor
"""
from __future__ import division, print_function

from builtins import range, object
from future.utils import viewitems, viewvalues, listvalues

from copy import copy, deepcopy
from WMCore.DataStructs.LumiList import LumiList
from WMCore.MicroService.Tools.Common import getMSLogger, isRelVal
from WMCore.Services.Rucio.RucioUtils import GROUPING_DSET, GROUPING_ALL, NUM_COPIES_DEFAULT


class Workflow(object):
    """
    Class to represent a workflow and some helpers to access
    its information within MS
    """

    def __init__(self, reqName, reqData, logger=None, verbose=False):
        self.reqName = reqName
        self.data = reqData
        # TODO: this replace can be removed in one year from now, thus March 2022
        self.data['DbsUrl'] = self.data['DbsUrl'].replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
        # stripping any end slashes, which no longer work in the Go-based server
        self.data['DbsUrl'] = self.data['DbsUrl'].rstrip("/")

        self.logger = getMSLogger(verbose, logger)

        self.inputDataset = ""
        self.parentDataset = ""
        self.pileupDatasets = set()
        self.pileupRSEList = set()

        self.campaigns = set()
        self.dataCampaignMap = []
        # these blocks structure will be key'ed by the block name and value'd by the block size
        self.primaryBlocks = {}
        self.parentBlocks = {}
        # sort of duplicate info, but we need to have a way to link input to parent block(s)
        self.childToParentBlocks = {}
        # pileup don't need to get resolved into blocks, store only their location
        self.secondarySummaries = {}

        self.setDataCampaignMap()
        self.setInputData()

    def __str__(self):
        """
        Write out useful information for this object
        :return:
        """
        res = {'reqName': self.reqName, 'inputDataset': self.inputDataset,
               'parentDataset': self.parentDataset, 'pileupDatasets': self.pileupDatasets,
               'campaigns': self.campaigns, 'dataCampaignMap': self.dataCampaignMap}
        return str(res)

    def getName(self):
        """
        Get this request name
        """
        return self.reqName

    def getDbsUrl(self):
        """
        Get the DbsUrl defined in this request
        """
        return self.data['DbsUrl']

    def getReqType(self):
        """
        Return the request type for this workflow
        """
        return self.data['RequestType']

    def getReqParam(self, param):
        """
        Return a top level parameter for this request.
        Read: parameters internal to Task/Steps are not looked up.
        """
        if param not in self.data:
            self.logger.warning("Request parameter '%s' not found in the workflow %s", param, self.getName())
        return self.data.get(param)

    def getSitelist(self):
        """
        Get the SiteWhitelist minus the black list for this request
        """
        return sorted(list(set(self.data['SiteWhitelist']) - set(self.data['SiteBlacklist'])))

    def setPURSElist(self, rseSet):
        """
        Hook/hack to make sure that multiple pileup datasets are placed
        in the same storage (unless secondaryAAA is enabled).
        It will be used to place the primary/parent blocks under the same
        location too (in case primaryAAA is enabled)
        Set this location only once, any further updates should not work!
        :param rseSet: a set of RSE names
        """
        if isinstance(rseSet, set):
            self.pileupRSEList = rseSet
        elif isinstance(rseSet, list):
            self.pileupRSEList = set(rseSet)
        else:
            # assume it's a string
            self.pileupRSEList = {rseSet}

    def getPURSElist(self):
        """
        Retrieve the final list of RSEs where ALL the data needs to be placed
        """
        return self.pileupRSEList

    def getRunWhitelist(self):
        """
        Get the RunWhitelist for this request
        """
        res = set(self._getValue('RunWhitelist', []))
        return sorted(list(res))

    def getRunBlacklist(self):
        """
        Get the RunBlacklist for this request
        """
        res = set(self._getValue('RunBlacklist', []))
        return sorted(list(res))

    def getLumilist(self):
        """
        Get the LumiList parameter and return a LumiList object,
        in case the LumiList is not empty.
        """
        lumiDict = self._getValue('LumiList', {})
        if not lumiDict:
            return {}
        return LumiList(compactList=lumiDict)

    def setDataCampaignMap(self):
        """
        Set the association between input data, data type and campaign name
        in the format of:
        {"type": "type of input data", "name": "dataset name", "campaign": "campaign name"}
        Also set a flat set of campaign names
        """
        if self.data['RequestType'] in ("StoreResults", "Resubmission"):
            self.logger.info("Request type %s does not support input data placement", self.data['RequestType'])
            self.dataCampaignMap = []
            return

        inputMap = {"InputDataset": "primary", "MCPileup": "secondary", "DataPileup": "secondary"}

        if "TaskChain" in self.data or "StepChain" in self.data:
            innerDicts = []
            for i in range(1, self.data.get("TaskChain", self.data.get("StepChain")) + 1):
                innerDicts.append(self.data.get("Task%d" % i, self.data.get("Step%d" % i)))
        else:
            # ReReco and DQMHarvesting
            innerDicts = [self.data]

        data = {}
        for item in innerDicts:
            for key in inputMap:
                if key in item and item[key]:
                    # use top level campaign if not available in the inner dict
                    data[item[key]] = dict(type=inputMap[key], name=item[key],
                                           campaign=item.get('Campaign', self.data.get('Campaign')))
            # also create a flat list of campaign names
            if "Campaign" in item and item["Campaign"]:
                self.campaigns.add(item["Campaign"])

        self.dataCampaignMap = listvalues(data)

    def getDataCampaignMap(self):
        """
        Retrieve map of campaign, dataset, dataset type
        :return: list of dictionaries
        """
        return self.dataCampaignMap

    def getCampaigns(self):
        """
        Get a set of campaign names used within this request
        """
        return self.campaigns

    def setInputData(self):
        """
        Parse the request data and set the primary and secondary datasets
        """
        for item in self.getDataCampaignMap():
            if item["type"] == "primary":
                self.inputDataset = item["name"]
            elif item["type"] == "secondary":
                self.pileupDatasets.add(item["name"])

    def getInputDataset(self):
        """
        Get this request's input dataset name
        """
        return self.inputDataset

    def getPileupDatasets(self):
        """
        Get this request's secondary dataset names
        """
        return self.pileupDatasets

    def hasParents(self):
        """
        Check whether the request has IncludeParents
        :return:
        """
        if not self.inputDataset:
            return False
        return self._getValue("IncludeParents", False)

    def setParentDataset(self, parent):
        """
        Set the parent dataset name and update the data/campaign map
        :param parent: string corresponding to the parent dataset name
        """
        self.parentDataset = parent
        self._updateDataCampaignMap(parent)

    def getParentDataset(self):
        """
        Return the parent dataset name
        :return: parent dataset name string
        """
        return self.parentDataset

    def getBlockWhitelist(self):
        """
        Fetch the BlockWhitelist for this workflow
        :return: a list data type with the blocks white listed
        """
        return self._getValue("BlockWhitelist", [])

    def getBlockBlacklist(self):
        """
        Fetch the BlockBlacklist for this workflow
        :return: a list data type with the blocks white listed
        """
        return self._getValue("BlockBlacklist", [])

    def setPrimaryBlocks(self, blocksDict):
        """
        Sets a list of primary input blocks taking into consideration
        the BlockWhitelist and BlockBlacklist
        Data is in the form of:
        {"block_name": {"blockSize": 1234,
                        "locations": [list of locations]}}
        """
        blockWhite = self.getBlockWhitelist()
        if blockWhite:
            for block in blockWhite:
                if block in blocksDict:
                    self.primaryBlocks[block] = copy(blocksDict[block])
        else:
            self.primaryBlocks = deepcopy(blocksDict)

        for block in self.getBlockBlacklist():
            self.primaryBlocks.pop(block, None)

    def getPrimaryBlocks(self):
        """
        Retrieve list of input primary blocks
        """
        return self.primaryBlocks

    def setSecondarySummary(self, dsetName, locations=None):
        """
        Create a summary of the pileup dataset, with its total data size
        and locations where the whole dataset is subscribed and available
        :param dsetName: string with the secondary dataset name
        :param locations: locations hosting this dataset in full (and subscribed)
        Data is in the form of:
        {"dataset_name": {"locations": [list of locations]}}
        """
        self.secondarySummaries.setdefault(dsetName, {})
        self.secondarySummaries[dsetName]['locations'] = locations or []

    def getSecondarySummary(self):
        """
        Retrieve list of input secondary datasets and their sizes
        """
        return self.secondarySummaries

    def setParentBlocks(self, blocksDict):
        """
        Sets a list of parent input blocks and their size.
        NOTE: this list is solely based on the parent dataset, without
        considering what are the actual input primary blocks
        {"block_name": {"blockSize": 1234,
                        "locations": [list of locations]}}
        """
        self.parentBlocks = blocksDict

    def getParentBlocks(self):
        """
        Retrieve list of input parent blocks
        """
        return self.parentBlocks

    def setChildToParentBlocks(self, blocksDict):
        """
        Sets a relationship between input primary block and its parent block(s)
        This method guarantees that only parent blocks with valid replicas are
        kept around (and later transferred)
        The child block list is also final, meaning all the run/block/lumi lists
        have already been applied.
        :param blocksDict: dict key'ed by the primary block, with a list of parent blocks
        """
        # flat list with the final parent blocks
        parentBlocks = set()
        # remove parent blocks without any valid replica (only invalid files)
        for child, parents in viewitems(blocksDict):
            for parent in list(parents):
                if parent not in self.getParentBlocks():
                    # then drop this block
                    parents.remove(parent)
            self.childToParentBlocks[child] = blocksDict[child]
            parentBlocks = parentBlocks | set(blocksDict[child])

        # Now remove any parent block that don't need to be transferred
        for block in list(self.getParentBlocks()):
            if block not in parentBlocks:
                self.parentBlocks.pop(block, None)

    def getChildToParentBlocks(self):
        """
        Returns a dictionary of blocks and its correspondent list of parents
        """
        return self.childToParentBlocks

    def _getValue(self, keyName, defaultValue=None):
        """
        Provide a property/keyName, return its valid value if any
        :param property: dictionary key name
        :param defaultValue: default value in case the key is not found
        :return: valid value (non-empty/false) or None
        """
        resp = defaultValue
        if keyName in self.data and self.data[keyName]:
            resp = self.data[keyName]
        elif keyName in self.data.get("Task1", {}):
            resp = self.data["Task1"][keyName]
        elif keyName in self.data.get("Step1", {}):
            resp = self.data["Step1"][keyName]
        return resp

    def _updateDataCampaignMap(self, parentName):
        """
        Update the data/campaign map with a newly discovered parent dataset
        :param parentName: the parent dataset name
        """
        for item in self.getDataCampaignMap():
            if item["type"] == "primary":
                # this is the campaign name we're looking for
                newItem = dict(type="parent", name=parentName, campaign=item["campaign"])
                break
        self.dataCampaignMap.append(newItem)

    def getWorkflowGroup(self):
        """
        Defines a workflow according to its group/activity, such as:
          *) release validation workflows
          *) standard central production workflows
        :return: a string with the workflow class: relval, processing
        """
        if isRelVal(self.data):
            return "relval"
        return "production"

    def getOpenRunningTimeout(self):
        """
        Retrieve the OpenRunningTimeout parameter for this workflow
        :return: an integer with the amount of secs
        """
        return self.data.get("OpenRunningTimeout", 0)

    def getInputData(self):
        """
        Returns all the primary and parent data that has to be locked
        and transferred with Rucio
        :return: a list of unique block names and an integer
                 with their total size
        """
        blockList = list(self.getPrimaryBlocks())
        totalBlockSize = sum([blockInfo['blockSize'] for blockInfo in self.getPrimaryBlocks().values()])

        # if it has parent blocks, add all of them as well
        if self.getParentDataset():
            blockList.extend(list(self.getParentBlocks()))
            totalBlockSize += sum([blockInfo['blockSize'] for blockInfo in viewvalues(self.getParentBlocks())])
        return blockList, totalBlockSize

    def getRucioGrouping(self):
        """
        Returns the rucio rule grouping to be defined for a primary
        and/or parent input data placement, where:
            * ALL: all CMS blocks are placed under the same RSE
            * DATASET: CMS blocks can be scattered in multiple RSEs

        NOTE that this does not apply to secondary data placement,
        which is always "ALL" (whole container in the same RSE).

        :return: a string with the required DID grouping
        """
        if self.getParentDataset():
            return GROUPING_ALL
        return GROUPING_DSET

    def getReplicaCopies(self):
        """
        Returns the number of replica copies to be defined in
        a given rucio rule. Standard/default value is 1.

        :return: an integer with the number of copies
        """
        return NUM_COPIES_DEFAULT
