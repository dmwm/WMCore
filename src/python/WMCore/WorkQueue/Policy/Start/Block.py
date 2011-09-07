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
from WMCore import Lexicon

class Block(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        self.lumiType = "NumberOfLumis"
        
    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        for block in self.validBlocks(self.initialTask, dbs):
            #set the parent flag for processing only for clarity on the couch doc
            parentList = []
            parentFlag = False
            #TODO this is slow process needs to change in DBS3
            if self.initialTask.parentProcessingFlag():
                parentFlag = True
                for dbsBlock in dbs.listBlockParents(block["Name"]):
                    parentBlock = {'Name': dbsBlock["Name"],
                                   'Sites': sitesFromStorageEelements([x['Name'] for x in
                                            dbsBlock['StorageElementList']])}
                    parentList.append(parentBlock)

            self.newQueueElement(Inputs = {block['Name'] : self.data.get(block['Name'], [])},
                                 ParentFlag = parentFlag,
                                 ParentData = parentList,
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
                Lexicon.block(data) # check dataset name
                datasetPath = str(data.split('#')[0])
                blocks.extend(dbs.getFileBlocksInfo(datasetPath, blockName = str(data), locations = True))
            else:
                Lexicon.dataset(data) # check dataset name
                blocks.extend(dbs.getFileBlocksInfo(datasetPath, locations = True))

        for block in blocks:
            # blocks with 0 valid files should be ignored
            # - ideally they would be deleted but dbs can't delete blocks
            if not block['NumberOfFiles']:
                continue

            # check block restrictions
            if blockWhiteList and block['Name'] not in blockWhiteList:
                continue
            if block['Name'] in blockBlackList:
                continue

            # check run restrictions
            if runWhiteList or runBlackList:
                # listRuns returns a run number per lumi section
                full_lumi_list = dbs.listRuns(block = block['Name'])
                runs = set(full_lumi_list)

                # apply blacklist
                runs = runs.difference(runBlackList)
                # if whitelist only accept listed runs
                if runWhiteList:
                    runs = runs.intersection(runWhiteList)

                # any runs left are ones we will run on, if none ignore block
                if not runs:
                    continue

                # recalculate effective size of block
                # make a guess for new event/file numbers from ratio
                # of accepted lumi sections (otherwise have to pull file info)
                accepted_lumis = [x for x in full_lumi_list if x in runs]
                ratio_accepted = 1. * len(accepted_lumis) / len(full_lumi_list)
                block[self.lumiType] = len(accepted_lumis)
                block['NumberOfFiles'] *= ratio_accepted
                block['NumberOfEvents'] *= ratio_accepted

            # get lumi info if needed and not already available
            if self.args['SliceType'] == self.lumiType and not block.get(self.lumiType):
                blockSummary = dbs.getDBSSummaryInfo(block = block["Name"])
                block[self.lumiType] = blockSummary[self.lumiType]

            # save locations
            self.data[block['Name']] = sitesFromStorageEelements([x['Name'] for x in block['StorageElementList']])

            validBlocks.append(block)
        return validBlocks
