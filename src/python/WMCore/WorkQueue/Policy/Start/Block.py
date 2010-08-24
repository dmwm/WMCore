#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil

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
            
            #TODO: use this when dbs api is supported
            #if self.args['SliceType'] == self.lumiType:
                #blockSummary = dbs.getDBSSummary(block = block["Name"])
                #block[self.lumiType] = blockSummary[self.lumiType]
                
            self.newQueueElement(Data = block['Name'],
                                 ParentData = parents,
                                 Jobs = ceil(float(block[self.args['SliceType']]) /
                                             float(self.args['SliceSize']))
                                 )
                                 #Jobs = block[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass

    def validBlocks(self, task, dbs):
        """Return blocks that pass the input data restriction"""
        datasetPath = task.getInputDatasetPath()
        validBlocks = []

        blockWhiteList = task.inputBlockWhitelist()
        blockBlackList = task.inputBlockBlacklist()
        runWhiteList = task.inputRunWhitelist()
        runBlackList = task.inputRunBlacklist()

        # if a block has been passed take that instead of dataset in spec
        if self.data and self.data.find('#') > -1:
            datasetPath, self.data.split('#')[0]
            blocks = dbs.getFileBlocksInfo(datasetPath, blockName = self.data)
        else:
            blocks = dbs.getFileBlocksInfo(datasetPath)

        for block in blocks:

            # check block restrictions
            if blockWhiteList and block['Name'] not in blockWhiteList:
                continue
            if block['Name'] in blockBlackList:
                continue

            # check run restrictions
            if runWhiteList or runBlackList:
                lumis = sum([x['LumiList'] for x in \
                            dbs.listFilesInBlock(block['Name'])], [])
                runs = set([x['RunNumber'] for x in lumis])
                
                #TODO: use this one instead of above three lines when dbs api is supported
                #runs = dbs.listRuns(block = block['Name'])
                
                # apply blacklist
                runs = runs.difference(runBlackList)
                # if whitelist only accept listed runs
                if runWhiteList:
                    runs = runs.intersection(runWhiteList)
                # any runs left are ones we will run on, if none ignore block
                if not runs:
                    continue

            validBlocks.append(block)
        return validBlocks
