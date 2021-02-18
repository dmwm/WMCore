#!/usr/bin/env python
"""
WorkQueue splitting by dataset, it creates one single
workqueue element per dataset.

This policy is specifically used by DQMHarvest workflows, which requires
some special handling based on run information. Nonetheless, trying to
make it generic enough that could be used by other spec types.
"""

import logging
from math import ceil
from WMCore import Lexicon
from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueUtils import makeLocationsList


class Dataset(StartPolicyInterface):
    """Split elements into datasets"""

    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfRuns')
        self.args.setdefault('SliceSize', 1)
        self.lumiType = "NumberOfLumis"
        self.sites = []

    def split(self):
        """Apply policy to spec"""
        work = set() if self.args['SliceType'] == 'NumberOfRuns' else 0
        numFiles = 0
        numEvents = 0
        numLumis = 0
        datasetPath = self.initialTask.getInputDatasetPath()

        # dataset splitting can't have its data selection overridden
        if self.data and list(self.data) != [datasetPath]:
            raise RuntimeError("Can't provide different data to split with")

        blocks = self.validBlocks(self.initialTask, self.dbs())
        if not blocks:
            return

        for block in blocks:
            if self.args['SliceType'] == 'NumberOfRuns':
                work = work.union(block[self.args['SliceType']])
            else:
                work += float(block[self.args['SliceType']])
            numLumis += int(block[self.lumiType])
            numFiles += int(block['NumberOfFiles'])
            numEvents += int(block['NumberOfEvents'])

        if self.args['SliceType'] == 'NumberOfRuns':
            numJobs = ceil(len(work) / float(self.args['SliceSize']))
        else:
            numJobs = ceil(float(work) / float(self.args['SliceSize']))

        # parentage
        parentFlag = True if self.initialTask.parentProcessingFlag() else False

        self.newQueueElement(Inputs={datasetPath: self.data.get(datasetPath, [])},
                             ParentFlag=parentFlag,
                             NumberOfLumis=numLumis,
                             NumberOfFiles=numFiles,
                             NumberOfEvents=numEvents,
                             Jobs=numJobs,
                             NoInputUpdate=self.initialTask.getTrustSitelists().get('trustlists'),
                             NoPileupUpdate=self.initialTask.getTrustSitelists().get('trustPUlists')
                            )

    def validate(self):
        """Check args and spec work with block splitting"""
        StartPolicyInterface.validateCommon(self)
        if not self.initialTask.inputDataset():
            raise WorkQueueWMSpecError(self.wmspec, 'No input dataset')

    def validBlocks(self, task, dbs):
        """Return blocks that pass the input data restriction"""
        datasetPath = task.getInputDatasetPath()
        Lexicon.dataset(datasetPath)  # check dataset name
        validBlocks = []
        locations = None

        blockWhiteList = task.inputBlockWhitelist()
        blockBlackList = task.inputBlockBlacklist()
        runWhiteList = task.inputRunWhitelist()
        runBlackList = task.inputRunBlacklist()
        lumiMask = task.getLumiMask()
        if lumiMask:
            maskedBlocks = self.getMaskedBlocks(task, dbs, datasetPath)

        for blockName in dbs.listFileBlocks(datasetPath):
            # check block restrictions
            if blockWhiteList and blockName not in blockWhiteList:
                continue
            if blockName in blockBlackList:
                continue

            blockSummary = dbs.getDBSSummaryInfo(block=blockName)
            if int(blockSummary.get('NumberOfFiles', 0)) == 0:
                logging.warning("Block %s being rejected for lack of valid files to process", blockName)
                self.badWork.append(blockName)
                continue

            if self.args['SliceType'] == 'NumberOfRuns':
                blockSummary['NumberOfRuns'] = dbs.listRuns(block=blockName)

            # check lumi restrictions
            if lumiMask:
                if blockName not in maskedBlocks:
                    logging.warning("Block %s doesn't pass the lumi mask constraints", blockName)
                    self.rejectedWork.append(blockName)
                    continue

                acceptedLumiCount = sum([len(maskedBlocks[blockName][lfn].getLumis()) for lfn in maskedBlocks[blockName]])
                ratioAccepted = 1. * acceptedLumiCount / float(blockSummary['NumberOfLumis'])
                maskedRuns = [maskedBlocks[blockName][lfn].getRuns() for lfn in maskedBlocks[blockName]]
                acceptedRuns = set(lumiMask.getRuns()).intersection(set().union(*maskedRuns))

                blockSummary['NumberOfFiles'] = len(maskedBlocks[blockName])
                blockSummary['NumberOfEvents'] = float(blockSummary['NumberOfEvents']) * ratioAccepted
                blockSummary[self.lumiType] = acceptedLumiCount
                blockSummary['NumberOfRuns'] = acceptedRuns
            # check run restrictions
            elif runWhiteList or runBlackList:
                runs = set(dbs.listRuns(block=blockName))
                # multi run blocks need special account, requires more DBS calls
                recalculateLumiCounts = True if len(runs) > 1 else False

                # apply blacklist and whitelist
                runs = runs.difference(runBlackList)
                if runWhiteList:
                    runs = runs.intersection(runWhiteList)
                # any runs left are ones we will run on, if none ignore block
                if not runs:
                    logging.warning("Block %s doesn't pass the runs constraints", blockName)
                    self.rejectedWork.append(blockName)
                    continue

                if recalculateLumiCounts:
                    # Recalculate the number of files, lumis and ~events accepted
                    acceptedLumiCount = 0
                    acceptedEventCount = 0
                    acceptedFileCount = 0
                    fileInfo = dbs.listFilesInBlock(fileBlockName=blockName)

                    for fileEntry in fileInfo:
                        acceptedFile = False
                        for lumiInfo in fileEntry['LumiList']:
                            if lumiInfo['RunNumber'] in runs:
                                acceptedFile = True
                                acceptedLumiCount += len(lumiInfo['LumiSectionNumber'])
                        if acceptedFile:
                            acceptedFileCount += 1
                            acceptedEventCount += fileEntry['NumberOfEvents']

                else:
                    acceptedLumiCount = blockSummary["NumberOfLumis"]
                    acceptedFileCount = blockSummary['NumberOfFiles']
                    acceptedEventCount = blockSummary['NumberOfEvents']

                blockSummary[self.lumiType] = acceptedLumiCount
                blockSummary['NumberOfFiles'] = acceptedFileCount
                blockSummary['NumberOfEvents'] = acceptedEventCount
                blockSummary['NumberOfRuns'] = runs

            validBlocks.append(blockSummary)
            blockLocation = set(self.blockLocationRucioPhedex(blockName))
            if locations is None:
                locations = blockLocation
            else:
                locations = locations.intersection(blockLocation)

        # all needed blocks present at these sites
        if task.getTrustSitelists().get('trustlists'):
            siteWhitelist = task.siteWhitelist()
            siteBlacklist = task.siteBlacklist()
            self.sites = makeLocationsList(siteWhitelist, siteBlacklist)
            self.data[datasetPath] = self.sites
        elif locations:
            self.data[datasetPath] = list(set(self.cric.PNNstoPSNs(locations)))

        return validBlocks
