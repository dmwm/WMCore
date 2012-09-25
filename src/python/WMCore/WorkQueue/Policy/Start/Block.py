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
            parentList = {}
            parentFlag = False
            #TODO this is slow process needs to change in DBS3
            if self.initialTask.parentProcessingFlag():
                parentFlag = True
                for dbsBlock in dbs.listBlockParents(block["block"]):
                    parentList[dbsBlock["Name"]] = sitesFromStorageEelements(dbsBlock['StorageElementList'])

            self.newQueueElement(Inputs = {block['block'] : self.data.get(block['block'], [])},
                                 ParentFlag = parentFlag,
                                 ParentData = parentList,
                                 NumberOfLumis = int(block[self.lumiType]),
                                 NumberOfFiles = int(block['NumberOfFiles']),
                                 NumberOfEvents = int(block['NumberOfEvents']),
                                 Jobs = ceil(float(block[self.args['SliceType']]) /
                                             float(self.args['SliceSize'])),
                                 OpenForNewData = True if str(block.get('OpenForWriting')) == '1' else False
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
                for block in dbs.listFileBlocks(data):
                    blocks.append(str(block))


        for blockName in blocks:
            # check block restrictions
            if blockWhiteList and blockName not in blockWhiteList:
                continue
            if blockName in blockBlackList:
                continue
            if task.getLumiMask() and blockName not in maskedBlocks:
                continue

            block = dbs.getDBSSummaryInfo(datasetPath, block = blockName)
            # blocks with 0 valid files should be ignored
            # - ideally they would be deleted but dbs can't delete blocks
            if not block['NumberOfFiles'] or block['NumberOfFiles'] == '0':
                continue

            #check lumi restrictions
            if task.getLumiMask():
                accepted_lumis = sum( [ len(maskedBlocks[blockName][file]) for file in maskedBlocks[blockName] ] )
                #use the information given from getMaskedBlocks to compute che size of the block
                block['NumberOfFiles'] = len(maskedBlocks[blockName])
                #ratio =  lumis which are ok in the block / total num lumis
                ratio_accepted = 1. * accepted_lumis / float(block['NumberOfLumis'])
                block['NumberOfEvents'] = float(block['NumberOfEvents']) * ratio_accepted
                block[self.lumiType] = accepted_lumis
            # check run restrictions
            elif runWhiteList or runBlackList:
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

            # save locations
            self.data[block['block']] = sitesFromStorageEelements(dbs.listFileBlockLocation(block['block']))

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
