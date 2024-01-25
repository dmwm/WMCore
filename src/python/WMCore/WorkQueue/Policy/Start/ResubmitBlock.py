#!/usr/bin/env python
"""
WorkQueue splitting for Resubmission workflows

In this case we can't be agnostic of the specific splitting algorithm
for the top level task. Each algorithm may require a different way of generating
ACDC blocks.

Current implementations:

- SingleChunk: Harvest, ParentlessMergeBySize, MinFileBased, EventAwareLumiBased, LumiBased
- FixedSizeChunks: FileBased, FixedDelay, MergeBySize, RunBased, SizeBased, SplitFileBased, TwoFileAndEventBased, TwoFileBased

ACDC unsupported:

- WMBSMergeBySize
- SiblingProcessingBased

"""
import json
from math import ceil
from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueUtils import makeLocationsList
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.ACDC.DataCollectionService import DataCollectionService


class ResubmitBlock(StartPolicyInterface):
    """Split elements into blocks"""

    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        self.args.setdefault('SplittingAlgo', 'LumiBased')
        self.lumiType = "NumberOfLumis"

        # Define how to handle the different splitting algorithms
        self.algoMapping = {'Harvest': self.singleChunk,
                            'ParentlessMergeBySize': self.singleChunk,
                            'MinFileBased': self.singleChunk,
                            'LumiBased': self.singleChunk,
                            'EventAwareLumiBased': self.singleChunk,
                            'EventBased': self.singleChunk}
        self.unsupportedAlgos = ['WMBSMergeBySize', 'SiblingProcessingBased']
        self.defaultAlgo = self.fixedSizeChunk
        self.sites = []

    def split(self):
        """Apply policy to spec"""
        # Prepare a site list in case we need it
        siteWhitelist = self.initialTask.siteWhitelist()
        siteBlacklist = self.initialTask.siteBlacklist()
        self.sites = makeLocationsList(siteWhitelist, siteBlacklist)

        for block in self.validBlocks(self.initialTask):
            parentList = {}
            parentFlag = False
            if self.initialTask.parentProcessingFlag():
                parentFlag = True
                parentList[block["Name"]] = block['Sites']

            self.newQueueElement(Inputs={block['Name']: block['Sites']},
                                 ParentFlag=parentFlag,
                                 ParentData=parentList,
                                 NumberOfLumis=block[self.lumiType],
                                 NumberOfFiles=block['NumberOfFiles'],
                                 NumberOfEvents=block['NumberOfEvents'],
                                 Jobs=ceil(float(block[self.args['SliceType']]) /
                                           float(self.args['SliceSize'])),
                                 ACDC=block['ACDC'],
                                 NoInputUpdate=self.initialTask.getTrustSitelists().get('trustlists'),
                                 NoPileupUpdate=self.initialTask.getTrustSitelists().get('trustPUlists')
                                )

    def validate(self):
        """Check args and spec work with block splitting"""
        StartPolicyInterface.validateCommon(self)

    def validBlocks(self, task):
        """Return blocks that pass the input data restriction according
           to the splitting algorithm"""
        validBlocks = []

        acdcInfo = task.getInputACDC()
        if not acdcInfo:
            raise WorkQueueWMSpecError(self.wmspec, 'No acdc section for %s' % task.getPathName())
        acdc = DataCollectionService(acdcInfo["server"], acdcInfo["database"])

        self.logger.info("Policy self.data variable content: %s", self.data)
        if self.data:
            acdcBlockSplit = ACDCBlock.splitBlockName(next(iter(self.data)))
        else:
            # if self.data is not passed, assume the data is input dataset from the spec
            acdcBlockSplit = False

        self.logger.info("Using ACDC blockSplit:\n%s", json.dumps(acdcBlockSplit, indent=2))
        if acdcBlockSplit:
            block = acdc.getChunkInfo(acdcInfo['collection'],
                                      acdcBlockSplit['TaskName'],
                                      acdcBlockSplit['Offset'],
                                      acdcBlockSplit['NumOfFiles'])
            dbsBlock = {}
            dbsBlock['Name'] = next(iter(self.data))

            dbsBlock['NumberOfFiles'] = block['files']
            dbsBlock['NumberOfEvents'] = block['events']
            dbsBlock['NumberOfLumis'] = block['lumis']
            dbsBlock['ACDC'] = acdcInfo
            if task.getTrustSitelists().get('trustlists'):
                dbsBlock["Sites"] = self.sites
            else:
                dbsBlock["Sites"] = self.cric.PNNstoPSNs(block["locations"])
            validBlocks.append(dbsBlock)
        else:
            if self.args['SplittingAlgo'] in self.unsupportedAlgos:
                raise WorkQueueWMSpecError(self.wmspec, 'ACDC is not supported for %s' % self.args['SplittingAlgo'])
            splittingFunc = self.defaultAlgo
            if self.args['SplittingAlgo'] in self.algoMapping:
                splittingFunc = self.algoMapping[self.args['SplittingAlgo']]
            validBlocks = splittingFunc(acdc, acdcInfo, task)
        self.logger.info("ACDC with the following validBlocks summary:\n%s", json.dumps(validBlocks, indent=2))

        return validBlocks

    def fixedSizeChunk(self, acdc, acdcInfo, task):
        """Return a set of blocks with a fixed number of ACDC records"""
        fixedSizeBlocks = []
        chunkSize = 250
        acdcBlocks = acdc.chunkFileset(acdcInfo['collection'],
                                       acdcInfo['fileset'],
                                       chunkSize)
        for block in acdcBlocks:
            dbsBlock = {}
            dbsBlock['Name'] = ACDCBlock.name(self.wmspec.name(),
                                              acdcInfo["fileset"],
                                              block['offset'], block['files'])
            dbsBlock['NumberOfFiles'] = block['files']
            dbsBlock['NumberOfEvents'] = block['events']
            dbsBlock['NumberOfLumis'] = block['lumis']
            if task.getTrustSitelists().get('trustlists'):
                dbsBlock["Sites"] = self.sites
            else:
                dbsBlock["Sites"] = self.cric.PNNstoPSNs(block["locations"])
            dbsBlock['ACDC'] = acdcInfo
            if dbsBlock['NumberOfFiles']:
                fixedSizeBlocks.append(dbsBlock)
        return fixedSizeBlocks

    def singleChunk(self, acdc, acdcInfo, task):
        """Return a single block (inside a list) with all associated ACDC records"""
        result = []
        acdcBlock = acdc.singleChunkFileset(acdcInfo['collection'],
                                            acdcInfo['fileset'])
        dbsBlock = {}
        dbsBlock['Name'] = ACDCBlock.name(self.wmspec.name(),
                                          acdcInfo["fileset"],
                                          acdcBlock['offset'], acdcBlock['files'])
        dbsBlock['NumberOfFiles'] = acdcBlock['files']
        dbsBlock['NumberOfEvents'] = acdcBlock['events']
        dbsBlock['NumberOfLumis'] = acdcBlock['lumis']
        if task.getTrustSitelists().get('trustlists'):
            dbsBlock["Sites"] = self.sites
        else:
            dbsBlock["Sites"] = self.cric.PNNstoPSNs(acdcBlock["locations"])
        dbsBlock['ACDC'] = acdcInfo
        if dbsBlock['NumberOfFiles']:
            result.append(dbsBlock)

        return result
