#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueUtils import sitesFromStorageEelements

class Block(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        self.lumiType = "NumberOfLumis"
        
    def split(self):
        """Apply policy to spec"""
        for block in self.validBlocks(self.initialTask, self.dbs()):
            parents = []
            if self.initialTask.parentProcessingFlag():
                parents = block['Parents']
                if not parents:
                    msg = "Parentage required but no parents found for %s"
                    raise RuntimeError, msg % block['Name']

            self.newQueueElement(Inputs = {block['Name'] : self.data.get(block['Name'], [])},
                                 ParentData = parents,
                                 Jobs = ceil(float(block[self.args['SliceType']]) /
                                             float(self.args['SliceSize']))
                                 )


    def validate(self):
        """Check args and spec work with block splitting"""
        StartPolicyInterface.validateCommon(self)

        if not self.initialTask.inputDataset():
            raise WorkQueueWMSpecError(self.wmspec, 'No input dataset')

    def validBlocks(self, task, dbs):
        """Return blocks that pass the input data restriction"""
        datasetPath = task.getInputDatasetPath()
        validBlocks = []

        blockWhiteList = task.inputBlockWhitelist()
        blockBlackList = task.inputBlockBlacklist()
        runWhiteList = task.inputRunWhitelist()
        runBlackList = task.inputRunBlacklist()

        blocks = []
        # Take data inputs or from spec
        if not self.data:
            self.data = {datasetPath : []} # same structure as in WorkQueueElement
            #blocks = dbs.getFileBlocksInfo(datasetPath, locations = False)
        #else:
            #dataItems = self.data.keys()

        for data in self.data:
            if data.find('#') > -1:
                datasetPath = str(data.split('#')[0])
                blocks.extend(dbs.getFileBlocksInfo(datasetPath, blockName = str(data), locations = True))
            else:
                blocks.extend(dbs.getFileBlocksInfo(datasetPath, locations = True))

        for block in blocks:

            # check block restrictions
            if blockWhiteList and block['Name'] not in blockWhiteList:
                continue
            if block['Name'] in blockBlackList:
                continue

            # check run restrictions
            if runWhiteList or runBlackList:
                runs = set(dbs.listRuns(block = block['Name']))

                # apply blacklist
                runs = runs.difference(runBlackList)
                # if whitelist only accept listed runs
                if runWhiteList:
                    runs = runs.intersection(runWhiteList)
                # any runs left are ones we will run on, if none ignore block
                if not runs:
                    continue

            if self.args['SliceType'] == self.lumiType:
                blockSummary = dbs.getDBSSummaryInfo(block = block["Name"])
                block[self.lumiType] = blockSummary[self.lumiType]

            # save locations
            self.data[block['Name']] = sitesFromStorageEelements([x['Name'] for x in block['StorageElementList']])

            validBlocks.append(block)
        return validBlocks
