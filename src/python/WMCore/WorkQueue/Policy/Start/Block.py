#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []
__revision__ = "$Id: Block.py,v 1.14 2010/07/19 10:50:32 swakef Exp $"
__version__ = "$Revision: 1.14 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil

class Block(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)

    def split(self):
        """Apply policy to spec"""
        for block in self.validBlocks(self.initialTask, self.dbs()):
            parents = []
            if self.initialTask.parentProcessingFlag():
                parents = block['Parents']
                if not parents:
                    msg = "Parentage required but no parents found for %s"
                    raise RuntimeError, msg % block['Name']

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
        dbs = self.dbs()
        inputDataset = task.inputDataset()
        datasetPath = task.getInputDatasetPath()
        validBlocks = []

        blockWhiteList = inputDataset.blocks.whitelist
        blockBlackList = inputDataset.blocks.blacklist
        runWhiteList = [int(x) for x in \
                        inputDataset.runs.whitelist.dictionary_().keys()]
        runBlackList = [int(x) for x in \
                        inputDataset.runs.blacklist.dictionary_().keys()]

        for block in dbs.getFileBlocksInfo(datasetPath):

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