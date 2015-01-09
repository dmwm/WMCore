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
        siteWhiteList = task.siteWhitelist()

        for blockName in dbs.listFileBlocks(datasetPath):
            block = dbs.getDBSSummaryInfo(datasetPath, block = blockName)

            # check block restrictions
            if blockWhiteList and block['block'] not in blockWhiteList:
                continue
            if block['block'] in blockBlackList:
                continue

            # check run restrictions
            if runWhiteList or runBlackList:
                # listRunLumis returns a dictionary with the lumi sections per run
                runLumis = dbs.listRunLumis(block = block['block'])
                runs = set(runLumis.keys())
                recalculateLumiCounts = False
                if len(runs) > 1:
                    # If more than one run in the block
                    # Then we must calculate the lumi counts after filtering the run list
                    # This has to be done rarely and requires calling DBS file information
                    recalculateLumiCounts = True

                # apply blacklist
                runs = runs.difference(runBlackList)
                # if whitelist only accept listed runs
                if runWhiteList:
                    runs = runs.intersection(runWhiteList)
                # any runs left are ones we will run on, if none ignore block
                if not runs:
                    continue
                
                if recalculateLumiCounts:
                    # get correct lumi count
                    # Recalculate effective size of block
                    # We pull out file info, since we don't do this often
                    acceptedLumiCount = 0
                    acceptedEventCount = 0
                    acceptedFileCount = 0
                    fileInfo = dbs.listFilesInBlock(fileBlockName = block['block'])
                    for fileEntry in fileInfo:
                        acceptedFile = False
                        acceptedFileLumiCount = 0
                        for lumiInfo in fileEntry['LumiList']:
                            runNumber = lumiInfo['RunNumber']
                            if runNumber in runs:
                                acceptedFile = True
                                acceptedFileLumiCount += 1
                        if acceptedFile:
                            acceptedFileCount += 1
                            acceptedLumiCount += acceptedFileLumiCount
                            if len(fileEntry['LumiList']) != acceptedFileLumiCount:
                                acceptedEventCount += float(acceptedFileLumiCount) * fileEntry['NumberOfEvents']/len(fileEntry['LumiList'])
                            else:
                                acceptedEventCount += fileEntry['NumberOfEvents']
                else:
                    acceptedLumiCount = block["NumberOfLumis"]
                    acceptedFileCount = block['NumberOfFiles']
                    acceptedEventCount = block['NumberOfEvents']
                    
                # recalculate effective size of block
                # make a guess for new event/file numbers from ratio
                # of accepted lumi sections (otherwise have to pull file info)
                
                fullLumiCount = block["NumberOfLumis"]
                block[self.lumiType] = acceptedLumiCount
                block['NumberOfFiles'] = acceptedFileCount
                block['NumberOfEvents'] = acceptedEventCount

            validBlocks.append(block)
            if locations is None:
                locations = set(dbs.listFileBlockLocation(block['block']))
            else:
                locations = locations.intersection(dbs.listFileBlockLocation(block['block']))
            
            if self.wmspec.locationDataSourceFlag():
                locations = locations.union(siteWhiteList)

        # all needed blocks present at these sites
        if locations:
            self.data[datasetPath] = list(locations)
        return validBlocks
