#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueUtils import sitesFromStorageEelements, cmsSiteNames
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
            #set the parent flag for processing only for clarity on the couch doc
            parentList = {}
            parentFlag = False
            #TODO this is slow process needs to change in DBS3
            if self.initialTask.parentProcessingFlag():
                parentFlag = True
                for dbsBlock in dbs.listBlockParents(block["block"]):
                    if self.initialTask.inputLocationFlag():
                        parentList[dbsBlock["Name"]] = self.sites
                    else:
                        parentList[dbsBlock["Name"]] = sitesFromStorageEelements(dbsBlock['StorageElementList'])

            self.newQueueElement(Inputs = {block['block'] : self.data.get(block['block'], [])},
                                 ParentFlag = parentFlag,
                                 ParentData = parentList,
                                 NumberOfLumis = int(block[self.lumiType]),
                                 NumberOfFiles = int(block['NumberOfFiles']),
                                 NumberOfEvents = int(block['NumberOfEvents']),
                                 Jobs = ceil(float(block[self.args['SliceType']]) /
                                             float(self.args['SliceSize'])),
                                 OpenForNewData = True if str(block.get('OpenForWriting')) == '1' else False,
                                 NoLocationUpdate = self.initialTask.inputLocationFlag()
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
        if task.getLumiMask(): #if we have a lumi mask get only the relevant blocks
            maskedBlocks = self.getMaskedBlocks(task, dbs, datasetPath)
        if task.inputLocationFlag():
            # Then get the locations from the site whitelist/blacklist + SiteDB
            siteWhitelist = task.siteWhitelist()
            siteBlacklist = task.siteBlacklist()
            if siteWhitelist:
                # Just get the ses matching the whitelists
                self.sites = siteWhitelist
            elif siteBlacklist:
                # Get all CMS sites less the blacklist
                allSites = cmsSiteNames()
                self.sites = list(set(allSites) - set (siteBlacklist))
            else:
                # Run at any CMS site
                self.sites = cmsSiteNames()

        blocks = []
        # Take data inputs or from spec
        if not self.data:
            if blockWhiteList:
                self.data = dict((block, []) for block in blockWhiteList)
            else:
                self.data = {datasetPath : []} # same structure as in WorkQueueElement

        for data in self.data:
            if data.find('#') > -1:
                Lexicon.block(data) # check block name
                datasetPath = str(data.split('#')[0])
                blocks.append(str(data))
            else:
                Lexicon.dataset(data) # check dataset name
                for block in dbs.listFileBlocks(data, onlyClosedBlocks = True):
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
                self.rejectedWork.append(blockName)
                continue

            block = dbs.getDBSSummaryInfo(datasetPath, block = blockName)
            # blocks with 0 valid files should be ignored
            # - ideally they would be deleted but dbs can't delete blocks
            if not block['NumberOfFiles'] or block['NumberOfFiles'] == '0':
                self.rejectedWork.append(blockName)
                continue

            #check lumi restrictions
            if task.getLumiMask():
                accepted_lumis = sum( [ len(maskedBlocks[blockName][file]) for file in maskedBlocks[blockName] ] )
                #use the information given from getMaskedBlocks to compute che size of the block
                block['NumberOfFiles'] = len(maskedBlocks[blockName])
                #ratio =  lumis which are ok in the block / total num lumis
                ratioAccepted = 1. * accepted_lumis / float(block['NumberOfLumis'])
                block['NumberOfEvents'] = float(block['NumberOfEvents']) * ratioAccepted
                block[self.lumiType] = accepted_lumis
            # check run restrictions
            elif runWhiteList or runBlackList:
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
                    block[self.lumiType] = acceptedLumiCount
                    block['NumberOfFiles'] = acceptedFileCount
                    block['NumberOfEvents'] = acceptedEventCount
            # save locations
            if task.inputLocationFlag():
                self.data[block['block']] = self.sites
            else:
                self.data[block['block']] = sitesFromStorageEelements(dbs.listFileBlockLocation(block['block']))

            if not self.data[block['block']]:
                # No sites for this block, move it to rejected
                self.rejectedWork.append(blockName)
                continue

            validBlocks.append(block)
        return validBlocks

    def getMaskedBlocks(self, task, dbs, datasetPath):
        """ Get the blocks which pass the lumi mask restrictions. For each block return the list of lumis
            which were ok (given the lumi mask). The data structure returned is the following:

            {
                "block1" : {"file1" : [2,3,4,6,7], "file5" : [10,12,13,15,17], ...}
                "block2" : {"file2" : [22,23,24,26,27], "file7" : [310,312,313,315,317], ...}
            }

        """
        maskedBlocks = {}
        lumiMask = task.getLumiMask()

        files = dbs.dbs.listFiles(datasetPath, retriveList=["retrive_lumi", "retrive_run"])
        #go through the lumi and get a list of blocks after applying the lumi mask
        for file in files:
            for lumi in file['LumiList']:
                if self._applyMask(lumi['LumiSectionNumber'], lumi['RunNumber'], lumiMask):
                    #initialize the block if needed
                    if file['Block']['Name'] not in maskedBlocks:
                        maskedBlocks[ file['Block']['Name'] ] = {}
                    #initialize the file if needed
                    if file['LogicalFileName'] not in maskedBlocks[ file['Block']['Name'] ]:
                        maskedBlocks[ file['Block']['Name'] ][ file['LogicalFileName'] ] = []
                    #append the lumi
                    maskedBlocks[ file['Block']['Name'] ][ file['LogicalFileName'] ].append( lumi['LumiSectionNumber'] )

        return maskedBlocks

    def _applyMask(self, lumi, run, lumiMask):
        """
            Return True if the lumi and the run can be found in lumiMask
            E.g.: lumi=3, run=5, lumiMask={'1':[...], '5':[[1,7],...]} => True
        """
        if str(run) in lumiMask:
            for section in lumiMask[str(run)]:
                if int(section[0]) <= int(lumi) <= int(section[1]):
                    return True
        return False

    def modifyPolicyForWorkAddition(self, inboxElement):
        """
            A block blacklist modifier will be created,
            this policy object will split excluding the blocks in both the spec
            blacklist and the blacklist modified
        """
        # Get the already processed input blocks from the inbox element
        existingBlocks = inboxElement.get('ProcessedInputs', [])
        self.blockBlackListModifier = existingBlocks
        self.blockBlackListModifier.extend(inboxElement.get('RejectedInputs', []))
        return

    def newDataAvailable(self, task, inbound):
        """
            In the case of the block policy, the new data available
            returns True if it finds at least one open block.
        """
        self.initialTask = task
        dbs = self.dbs()
        openBlocks = dbs.listOpenFileBlocks(task.getInputDatasetPath())
        if openBlocks:
            return True
        return False

    @staticmethod
    def supportsWorkAddition():
        """
            Block start policy supports continuous addition of work
        """
        return True
