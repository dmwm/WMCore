#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
from __future__ import print_function, division

import logging
from math import ceil

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueUtils import makeLocationsList
from WMCore import Lexicon


class Block(StartPolicyInterface):
    """Split elements into blocks"""

    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        self.lumiType = "NumberOfLumis"

        # Initialize a list of sites where the data is
        self.sites = []

        # Initialize modifiers of the policy
        self.blockBlackListModifier = []

    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        for block in self.validBlocks(self.initialTask, dbs):
            # set the parent flag for processing only for clarity on the couch doc
            parentList = {}
            parentFlag = False
            # TODO this is slow process needs to change in DBS3
            if self.initialTask.parentProcessingFlag():
                parentFlag = True
                parentBlocks = dbs.listBlockParents(block["block"])
                for blockName in parentBlocks:
                    if self.initialTask.getTrustSitelists().get('trustlists'):
                        parentList[blockName] = self.sites
                    else:
                        blockLocations = self.blockLocationRucioPhedex(blockName)
                        parentList[blockName] = self.cric.PNNstoPSNs(blockLocations)

            # there could be 0 event files in that case we can't estimate the number of jobs created.
            # We set Jobs to 1 for that case.
            # If we need more realistic estimate, we need to dry run the spliting the jobs.
            estimateJobs = max(1, ceil(block[self.args['SliceType']] / self.args['SliceSize']))

            self.newQueueElement(Inputs={block['block']: self.data.get(block['block'], [])},
                                 ParentFlag=parentFlag,
                                 ParentData=parentList,
                                 NumberOfLumis=int(block[self.lumiType]),
                                 NumberOfFiles=int(block['NumberOfFiles']),
                                 NumberOfEvents=int(block['NumberOfEvents']),
                                 Jobs=estimateJobs,
                                 OpenForNewData=False,
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
        validBlocks = []

        blockWhiteList = task.inputBlockWhitelist()
        blockBlackList = task.inputBlockBlacklist()
        runWhiteList = task.inputRunWhitelist()
        runBlackList = task.inputRunBlacklist()
        if task.getLumiMask():  # if we have a lumi mask get only the relevant blocks
            maskedBlocks = self.getMaskedBlocks(task, dbs, datasetPath)
        if task.getTrustSitelists().get('trustlists'):
            siteWhitelist = task.siteWhitelist()
            siteBlacklist = task.siteBlacklist()
            self.sites = makeLocationsList(siteWhitelist, siteBlacklist)

        blocks = []
        # Take data inputs or from spec
        if not self.data:
            if blockWhiteList:
                self.data = dict((block, []) for block in blockWhiteList)
            else:
                self.data = {datasetPath: []}  # same structure as in WorkQueueElement

        for data in self.data:
            if data.find('#') > -1:
                Lexicon.block(data)  # check block name
                datasetPath = str(data.split('#')[0])
                blocks.append(str(data))
            else:
                Lexicon.dataset(data)  # check dataset name
                for block in dbs.listFileBlocks(data):
                    blocks.append(str(block))

        for blockName in blocks:
            # check block restrictions
            if blockWhiteList and blockName not in blockWhiteList:
                continue
            if blockName in blockBlackList:
                continue
            if blockName in self.blockBlackListModifier:
                # Don't duplicate blocks rejected before or blocks that were included and therefore are now in the blacklist
                continue
            if task.getLumiMask() and blockName not in maskedBlocks:
                logging.warning("Block %s doesn't pass the lumi mask constraints", blockName)
                self.rejectedWork.append(blockName)
                continue

            block = self._getBlockSummary(dbs, datasetPath, blockName)
            if not block:
                continue

            # check lumi restrictions
            if task.getLumiMask():
                accepted_lumis = sum([len(maskedBlocks[blockName][lfn].getLumis()) for lfn in maskedBlocks[blockName]])
                # use the information given from getMaskedBlocks to compute che size of the block
                block['NumberOfFiles'] = len(maskedBlocks[blockName])
                # ratio =  lumis which are ok in the block / total num lumis
                ratioAccepted = accepted_lumis / block['NumberOfLumis']
                block['NumberOfEvents'] = block['NumberOfEvents'] * ratioAccepted
                block[self.lumiType] = accepted_lumis
            # check run restrictions
            elif runWhiteList or runBlackList:
                # listRunLumis returns a dictionary with the lumi sections per run
                runLumis = dbs.listRunLumis(block=block['block'])
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
                    logging.warning("Block %s doesn't pass the runs constraints", blockName)
                    self.rejectedWork.append(blockName)
                    continue

                if len(runs) == len(runLumis):
                    # If there is no change in the runs, then we can skip recalculating lumi counts
                    recalculateLumiCounts = False

                if recalculateLumiCounts:
                    # Recalculate effective size of block
                    # We pull out file info, since we don't do this often
                    acceptedLumiCount = 0
                    acceptedEventCount = 0
                    acceptedFileCount = 0
                    fileInfo = dbs.listFilesInBlock(fileBlockName=block['block'])
                    for fileEntry in fileInfo:
                        acceptedFile = False
                        acceptedFileLumiCount = 0
                        for lumiInfo in fileEntry['LumiList']:
                            runNumber = lumiInfo['RunNumber']
                            if runNumber in runs:
                                acceptedFile = True
                                acceptedFileLumiCount += 1
                                acceptedLumiCount += len(lumiInfo['LumiSectionNumber'])
                        if acceptedFile:
                            acceptedFileCount += 1
                            if len(fileEntry['LumiList']) != acceptedFileLumiCount:
                                acceptedEventCount += acceptedFileLumiCount * fileEntry['NumberOfEvents'] / len(fileEntry['LumiList'])
                            else:
                                acceptedEventCount += fileEntry['NumberOfEvents']
                    block[self.lumiType] = acceptedLumiCount
                    block['NumberOfFiles'] = acceptedFileCount
                    block['NumberOfEvents'] = acceptedEventCount
            # save locations
            if task.getTrustSitelists().get('trustlists'):
                self.data[block['block']] = self.sites
            else:
                blockLocations = self.blockLocationRucioPhedex(block['block'])
                self.data[block['block']] = self.cric.PNNstoPSNs(blockLocations)

            # TODO: need to decide what to do when location is no find.
            # There could be case for network problem (no connection to dbs, phedex)
            # or DBS se is not recorded (This will be retried anyway by location mapper)
            if not self.data[block['block']]:
                self.data[block['block']] = ["NoInitialSite"]
            # # No sites for this block, move it to rejected
            #    self.rejectedWork.append(blockName)
            #    continue

            validBlocks.append(block)
        return validBlocks

    def _getBlockSummary(self, dbsObj, datasetPath, blockName):
        """
        Retrieve a summary for this block from both DBS and Rucio. If the block
        has 0 valid files in DBS, or 0 files in Rucio, it is then marked as
        rejected and skipped from the work creation. Otherwise, the DBS summary
        is returned.
        :param dbsObj: instance to the DBS3Reader object
        :param datasetPath: string with the input dataset name
        :param blockName: string with the block name
        :return: either an empty dictionary, or the DBS summary dictionary
        """
        # blocks with 0 valid files should be ignored
        # - ideally they would be deleted but dbs can't delete blocks
        block = dbsObj.getDBSSummaryInfo(dataset=datasetPath, block=blockName)
        if int(block.get('NumberOfFiles', 0)) == 0:
            logging.warning("Block %s being rejected for lack of valid files in DBS to process", blockName)
            self.badWork.append(blockName)
            return dict()
        # blocks with 0 files in Rucio should be ignored as well
        blockRucio = self.rucio.getDID(didName=blockName, dynamic=False)
        if not blockRucio.get('length'):
            logging.warning("Block %s being rejected for lack of files in Rucio to process", blockName)
            self.badWork.append(blockName)
            return dict()
        return block

    def modifyPolicyForWorkAddition(self, inboxElement):
        """
        A block blacklist modifier will be created,
        this policy object will split excluding the blocks in both the spec
        blacklist and the blacklist modified
        """
        # Get the already processed input blocks from the inbox element
        self.blockBlackListModifier = inboxElement.get('ProcessedInputs', [])
        self.blockBlackListModifier.extend(inboxElement.get('RejectedInputs', []))

    def newDataAvailable(self, task, inbound):
        """
            In the case of the block policy, the new data available
            returns True if it finds at least one open block.
        """
        self.initialTask = task
        dbs = self.dbs()
        allBlocks = dbs.listFileBlocks(task.getInputDatasetPath())
        newBlocks = set(allBlocks) - set(self.rejectedWork) - set(self.badWork)
        return bool(newBlocks)

    @staticmethod
    def supportsWorkAddition():
        """
        Block start policy supports continuous addition of work
        """
        return True
