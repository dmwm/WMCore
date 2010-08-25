#!/usr/bin/env python
"""
WorkQueue splitting by dataset

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from math import ceil

class Dataset(StartPolicyInterface):
    """Split elements into datasets"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        
    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        work = 0
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        # dataset splitting can't have its data selection overridden
        if (self.data and self.data != datasetPath):
            raise RuntimeError, "Can't provide different data to split with"
        dataset = dbs.getDatasetInfo(datasetPath)
        #TODO: use this one instead of above one line when dbs api is supported
        #dataset = dbs.getDBSSummary(dataset = datasetPath)
        
        # apply input dataset restrictions
        blockWhiteList = self.initialTask.inputBlockWhitelist()
        blockBlackList = self.initialTask.inputBlockBlacklist()
        runWhiteList = self.initialTask.inputRunWhitelist()
        runBlackList = self.initialTask.inputRunBlacklist()
        if blockWhiteList or blockBlackList or runWhiteList or runBlackList:
            blocks = self.validBlocks(self.initialTask, self.dbs())
            if not blocks:
                raise RuntimeError, 'No blocks pass white/blacklist'

            for block in blocks:
                work += block[self.args['SliceType']]

        # parentage
        if self.initialTask.parentProcessingFlag():
            parents = dataset['Parents']
            if not parents:
                # Real data lacks dataset parentage - work with block parentage
                if not blocks:
                    blocks = dbs.getFileBlocksInfo(datasetPath)
                for block in blocks:
                    parents.extend(block['Parents'])
            if not parents:
                msg = "Parentage required but no parents found for %s"
                raise RuntimeError, msg % datasetPath
        else:
            parents = []

        if not work:
            work = dataset[self.args['SliceType']]
            
        self.newQueueElement(Data = dataset['path'],
                             ParentData = parents,
                             Jobs = ceil(float(work) /
                                         float(self.args['SliceSize']))
                             )
                             #Jobs = dataset[self.args['SliceType']])


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