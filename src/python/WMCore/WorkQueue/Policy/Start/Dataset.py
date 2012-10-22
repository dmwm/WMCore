#!/usr/bin/env python
"""
WorkQueue splitting by dataset

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from math import ceil
from WMCore.WorkQueue.WorkQueueUtils import sitesFromStorageEelements
from WMCore import Lexicon

class Dataset(StartPolicyInterface):
    """Split elements into datasets"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        self.lumiType = "NumberOfLumis"

    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        work = 0
        numFiles = 0
        numEvents = 0
        numLumis = 0
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        # dataset splitting can't have its data selection overridden
        if (self.data and self.data.keys() != [datasetPath]):
            raise RuntimeError, "Can't provide different data to split with"

        blocks = self.validBlocks(self.initialTask, self.dbs())
        if not blocks:
            return

        for block in blocks:
            work += float(block[self.args['SliceType']])
            numLumis +=  int(block[self.lumiType])
            numFiles += int(block['NumberOfFiles'])
            numEvents += int(block['NumberOfEvents'])

        dataset = dbs.getDBSSummaryInfo(dataset = datasetPath)

        # If the dataset which is not in dbs is passed, just return.
        # The exception will be created in upper level
        # when there is no work created
        if not dataset:
            return

        # parentage
        if self.initialTask.parentProcessingFlag():
            parentFlag = True
        else:
            parentFlag = False

        if not work:
            work = dataset[self.args['SliceType']]

        self.newQueueElement(Inputs = {dataset['path'] : self.data.get(dataset['path'], [])},
                             ParentFlag = parentFlag,
                             NumberOfLumis = numLumis,
                             NumberOfFiles = numFiles,
                             NumberOfEvents = numEvents,
                             Jobs = ceil(float(work) /
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
        Lexicon.dataset(datasetPath) # check dataset name
        validBlocks = []
        locations = None

        blockWhiteList = task.inputBlockWhitelist()
        blockBlackList = task.inputBlockBlacklist()
        runWhiteList = task.inputRunWhitelist()
        runBlackList = task.inputRunBlacklist()

        for blockName in dbs.listFileBlocks(datasetPath):
            block = dbs.getDBSSummaryInfo(datasetPath, block = blockName)

            # check block restrictions
            if blockWhiteList and block['block'] not in blockWhiteList:
                continue
            if block['block'] in blockBlackList:
                continue

            # check run restrictions
            if runWhiteList or runBlackList:
                # listRuns returns a run number per lumi section
                full_lumi_list = dbs.listRuns(block = block['block'])
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
                block['NumberOfFiles'] = float(block['NumberOfFiles']) * ratio_accepted
                block['NumberOfEvents'] = float(block['NumberOfEvents']) * ratio_accepted

            validBlocks.append(block)
            if locations is None:
                locations = set(sitesFromStorageEelements(dbs.listFileBlockLocation(block['block'])))
            else:
                locations = locations.intersection(set(sitesFromStorageEelements(dbs.listFileBlockLocation(block['block']))))

        # all needed blocks present at these sites
        if locations:
            self.data[datasetPath] = list(locations)
        return validBlocks
