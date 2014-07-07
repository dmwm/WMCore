#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import *

from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from WMCore.Services.DBS.DBSErrors import DBSReaderError, formatEx
from WMCore.Services.EmulatorSwitch import emulatorHook

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

def remapDBS3Keys(data, stringify = False, **others):
    """Fields have been renamed between DBS2 and 3, take fields from DBS3
    and map to DBS2 values
    """
    mapping = {'num_file' : 'NumberOfFiles', 'num_event' : 'NumberOfEvents',
                   'num_block' : 'NumberOfBlocks', 'num_lumi' : 'NumberOfLumis',
                   'event_count' : 'NumberOfEvents', 'run_num' : 'RunNumber',
                   'file_size' : 'FileSize', 'block_size' : 'BlockSize',
                   'file_count' : 'NumberOfFiles', 'open_for_writing' : 'OpenForWriting',
                   'logical_file_name' : 'LogicalFileName'}
    mapping.update(others)
    format = lambda x: str(x) if stringify and type(x) == unicode else x
    for name, newname in mapping.iteritems():
        if data.has_key(name):
            data[newname] = format(data[name])
    return data

# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMQuality.Emulators.EmulatorSetup module for the values
#@emulatorHook
class DBS3Reader:
    """
    _DBSReader_

    General API for reading data from DBS


    """
    def __init__(self, url, **contact):

        # instantiate dbs api object
        try:
            self.dbs = DbsApi(url, **contact)
        except DbsException, ex:
            msg = "Error in DBSReader with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        
        # connection to PhEDEx (Use default endpoint url)
        self.phedex = PhEDEx(responseType = "json") 

    def listPrimaryDatasets(self, match = '*'):
        """
        _listPrimaryDatasets_

        return a list of primary datasets, The full dataset name must be provided
        pattern based mathcing is no longer supported.
        If no expression is provided, all datasets are returned
        """
        try:
            result = self.dbs.listPrimaryDatasets(primary_ds_name = match)
        except DbsException, ex:
            msg = "Error in DBSReader.listPrimaryDataset(%s)\n" % match
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = [ x['primary_ds_name'] for x in result ]
        return result

    def matchProcessedDatasets(self, primary, tier, process):
        """
        _matchProcessedDatasets_

        return a list of Processed datasets
        """
        result = []
        try:
            datasets = self.dbs.listDatasets(primary_ds_name = primary, data_tier_name = tier, detail = True)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        for dataset in datasets:
            dataset = remapDBS3Keys(dataset, processed_ds_name = 'Name')
            dataset['PathList'] = [dataset['dataset']]
            if dataset['Name'] == process:
                result.append(dataset)
        return result

    def listRuns(self, dataset = None, block = None):
        """
        it gets list of DbsRun object but for our purpose
        only list of number is collected.
        DbsRun (RunNumber,
                NumberOfEvents,
                NumberOfLumiSections,
                TotalLuminosity,
                StoreNumber,
                StartOfRungetLong,
                EndOfRun,
                CreationDate,
                CreatedBy,
                LastModificationDate,
                LastModifiedBy
                )
        """
        runs = []
        try:
            if block:
                results = self.dbs.listRuns(block_name = block)
            else:
                results = self.dbs.listRuns(dataset = dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listRuns(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        [runs.extend(x['run_num']) for x in results]
        return runs

    def listRunLumis(self, dataset = None, block = None):
        """
        It gets a list of DBSRun objects and returns the number of lumisections per run
        DbsRun (RunNumber,
                NumberOfEvents,
                NumberOfLumiSections,
                TotalLuminosity,
                StoreNumber,
                StartOfRungetLong,
                EndOfRun,
                CreationDate,
                CreatedBy,
                LastModificationDate,
                LastModifiedBy
                )
        """
        try:
            if block:
                results = self.dbs.listRuns(block_name = block)
            else:
                results = self.dbs.listRuns(dataset = dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listRuns(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        
        # send runDict format as result, this format is for sync with dbs2 call 
        # which has {run_number: num_lumis} but dbs3 call doesn't return num Lumis
        # So it returns {run_number: None}
        # TODO: After DBS2 is completely removed change the return format more sensible one
        
        runDict = {}
        for x in results:
            for runNumber in x["run_num"]:
                runDict[runNumber] = None
        return runDict

    def listProcessedDatasets(self, primary, dataTier = '*'):
        """
        _listProcessedDatasets_

        return a list of Processed datasets for the primary and optional
        data tier value

        """
        try:
            result = self.dbs.listDatasets(primary_ds_name = primary, data_tier_name = dataTier)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = [ x['dataset'].split('/')[2] for x in result ]
        return result


    def listDatasetFiles(self, datasetPath):
        """
        _listDatasetFiles_

        Get list of files for dataset

        """
        return [ x['logical_file_name'] for x in self.dbs.listFiles(dataset = datasetPath)]

    def listDatasetFileDetails(self, datasetPath, getParents=False):
        """
        _listDatasetFileDetails_

        Get list of lumis, events, and parents for each file in a dataset
        Return a dict where the keys are the files, and for each file we have something like:
            { 'NumberOfEvents': 545,
              'BlockName': '/HighPileUp/Run2011A-v1/RAW#dd6e0796-cbcc-11e0-80a9-003048caaace',
              'Lumis': {173658: [8, 12, 9, 14, 19, 109, 105]},
              'Parents': [],
              'Checksums': {'Checksum': '22218315', 'Adler32': 'a41a1446', 'Md5': 'NOTSET'},
              'Size': 286021145
            }

        """
        fileDetails = self.dbs.listFiles(dataset=datasetPath, detail=True)
        blocks = set() #the set of blocks of the dataset
        #Iterate over the files and prepare the set of blocks and a dict where the keys are the files
        files = {}
        for f in fileDetails:
            blocks.add(f['block_name'])
            files[f['logical_file_name']] = {
                "BlockName" : f['block_name'],
                "NumberOfEvents" : f['event_count'],
                "Lumis" : {},
                "Parents" : [],
                "Size" : f['file_size'],
                "Checksums" : {'Adler32': f['adler32'], 'Checksum': f['check_sum'], 'Md5': f['md5']}
            }

        #Iterate over the blocks and get parents and lumis
        for blockName in blocks:
            #get the parents
            if getParents:
                parents = self.dbs.listFileParents(block_name=blockName)
                for p in parents:
                    files[p['logical_file_name']]['Parents'].extend(p['parent_logical_file_name'])
            #get the lumis
            file_lumis = self.dbs.listFileLumis(block_name=blockName)
            for f in file_lumis:
                if f['run_num'] in files[f['logical_file_name']]['Lumis']:
                    files[f['logical_file_name']]['Lumis'][f['run_num']].extend(f['lumi_section_num'])
                else:
                    files[f['logical_file_name']]['Lumis'][f['run_num']] = f['lumi_section_num']

        return files


    def crossCheck(self, datasetPath, *lfns):
        """
        _crossCheck_

        For the dataset provided, check that the lfns listed all exist
        in the dataset.

        Return the list of lfns that are in the dataset

        """
        allLfns = self.listDatasetFiles(datasetPath)
        setOfAllLfns = set(allLfns)
        setOfKnownLfns = set(lfns)
        return list(setOfAllLfns.intersection(setOfKnownLfns))

    def crossCheckMissing(self, datasetPath, *lfns):
        """
        _crossCheckMissing_

        As cross check, but return value is a list of files that
        are *not* known by DBS

        """
        allLfns = self.listDatasetFiles(datasetPath)
        setOfAllLfns = set(allLfns)
        setOfKnownLfns = set(lfns)
        knownFiles = setOfAllLfns.intersection(setOfKnownLfns)
        unknownFiles = setOfKnownLfns.difference(knownFiles)
        return list(unknownFiles)


    def getDBSSummaryInfo(self, dataset = None, block = None):
        """
        Get dataset summary includes # of files, events, blocks and total size
        """
        #FIXME: Doesnt raise exceptions on missing data as old api did
        if dataset:
            self.checkDatasetPath(dataset)
        try:
            if block:
                summary = self.dbs.listFileSummaries(block_name = block)
            else: # dataset case dataset shouldn't be None
                summary = self.dbs.listFileSummaries(dataset = dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listDatasetSummary(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        if not summary or summary[0].get('file_size') is None: # appears to indicate missing dataset
            msg = "DBSReader.listDatasetSummary(%s, %s): No matching data"
            raise DBSReaderError(msg % (dataset, block))
        result = remapDBS3Keys(summary[0], stringify = True)
        result['path'] = dataset if not block else ''
        result['block'] = block if block else ''
        return result

    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = False,
                          blockName = None, locations = True):
        """
        """
        self.checkDatasetPath(dataset)
        args = {'dataset' : dataset, 'detail' : True}
        if blockName:
            args['block_name'] = blockName
        try:
            blocks = self.dbs.listBlocks(**args)
        except DbsException, ex:
            msg = "Error in DBSReader.getFileBlocksInfo(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        blocks = [remapDBS3Keys(block, stringify = True, block_name = 'Name') for block in blocks]
        # only raise if blockName not specified - mimic dbs2 error handling
        if not blocks and not blockName:
            msg = "DBSReader.getFileBlocksInfo(%s, %s): No matching data"
            raise DBSReaderError(msg % (dataset, blockName))
        if locations:
            for block in blocks:
                block['StorageElementList'] = [{'Name' : x} for x in self.listFileBlockLocation(block['Name'])]

        if onlyClosedBlocks:
            return [x for x in blocks if str(x['OpenForWriting']) != "1"]

        return blocks

    def listFileBlocks(self, dataset, onlyClosedBlocks = False,
                       blockName = None):
        """
        _listFileBlocks_

        Retrieve a list of fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        args = {'dataset' : dataset, 'detail' : False}
        if blockName:
            args['block_name'] = blockName
        if onlyClosedBlocks:
            args['detail'] = True
        try:
            blocks = self.dbs.listBlocks(**args)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if onlyClosedBlocks:
            result = [
                x['block_name'] for x in blocks \
                  if str(x['open_for_writing']) != "1"
                ]

        else:
            result = [ x['block_name'] for x in blocks ]

        return result

    def listOpenFileBlocks(self, dataset):
        """
        _listOpenFileBlocks_

        Retrieve a list of open fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        try:
            blocks = self.dbs.listBlocks(dataset = dataset, detail = True)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)


        result = [
            x['block_name'] for x in blocks \
            if str(x['open_for_writing']) == "1"
        ]


        return result

    def blockExists(self, fileBlockName):
        """
        _blockExists_

        Check to see if block with name provided exists in the DBS
        Instance.

        Return True if exists, False if not

        """
        self.checkBlockName(fileBlockName)
        try:

            blocks = self.dbs.listBlocks(block_name = fileBlockName)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.blockExists(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if len(blocks) == 0:
            return False
        return True


    def listFilesInBlock(self, fileBlockName, lumis = True):
        """
        _listFilesInBlock_

        Get a list of files in the named fileblock
        TODO: lumis can be false when lumi splitting is not required
        However WMBSHelper expect file['LumiList'] to get the run number
        so for now it will be always true.
        We need to clean code up when dbs2 is completely deprecated.
        calling lumis for run number is expensive.
        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.listFilesInBlock(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        try:
            files = self.dbs.listFiles(block_name = fileBlockName, detail = True)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if lumis:
            try:
                lumiLists = self.dbs.listFileLumis(block_name = fileBlockName)
            except DbsException, ex:
                msg = "Error in "
                msg += "DBSReader.listFileLumis(%s)\n" % fileBlockName
                msg += "%s\n" % formatEx(ex)
                raise DBSReaderError(msg)

            lumiDict = {}
            for lumisItem in lumiLists:
                lumiDict.setdefault(lumisItem['logical_file_name'], [])
                item = {}
                item["RunNumber"] = lumisItem['run_num']
                item['LumiSectionNumber'] = lumisItem['lumi_section_num']
                lumiDict[lumisItem['logical_file_name']].append(item)

        result = []
        for file in files:
            if lumis:
                file["LumiList"] = lumiDict[file['logical_file_name']]
            result.append(remapDBS3Keys(file, stringify = True))
        return result

    def listFilesInBlockWithParents(self, fileBlockName):
        """
        _listFilesInBlockWithParents_

        Get a list of files in the named fileblock including
        the parents of that file.

        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.listFilesInBlockWithParents(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        try:
            files = self.dbs.listFileParents(block_name = fileBlockName)

        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlockWithParents(%s)\n" % (
                fileBlockName,)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = []
        for f in files:
            result.append({'Block' : {'Name' : fileBlockName},
                           'LogicalFileName' : f['logical_file_name'],
                           'ParentList' : [{'LogicalFileName' : x} for x in f['parent_logical_file_name']]
                           })
        return result


    def lfnsInBlock(self, fileBlockName):
        """
        _lfnsInBlock_

        LFN list only for block, details = False => faster query

        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.lfnsInBlock(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        try:
            files = self.dbs.listFiles(block_name = fileBlockName, detail = False)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        return [x['logical_file_name'] for x in files]


    def listFileBlockLocation(self, fileBlockName, dbsOnly = False, phedex_node=False):
        """
        _listFileBlockLocation_

        Get origin_site_name of a block

        """
        self.checkBlockName(fileBlockName)
        
        if not dbsOnly:
            try:
                blockInfo = self.phedex.getReplicaSEForBlocks(block=[fileBlockName],complete='y')
            except Exception, ex:
                msg = "Error while getting block location from PhEDEx for block_name=%s)\n" % fileBlockName
                msg += "%s\n" % str(ex)
                raise Exception(msg)
            
            if not blockInfo: # if we couldnt get data location from PhEDEx, try to look into origin site location from dbs
                dbsOnly = True
            else:
                location = set()
                location.update(blockInfo[fileBlockName])
        
        if dbsOnly:
            try:
                blockInfo = self.dbs.listBlockOrigin(block_name = fileBlockName)
            except DbsException, ex:
                msg = "Error in DBSReader: dbsApi.listBlocks(block_name=%s)\n" % fileBlockName
                msg += "%s\n" % formatEx(ex)
                raise DBSReaderError(msg)
            
            if not blockInfo: # no data location from dbs
                return list()
            
            location = set()
            location.update([blockInfo[0]['origin_site_name']])
            
            location.difference_update(['UNKNOWN']) # remove entry when SE name is 'UNKNOWN'
             
        if phedex_node:
            return [self.phedex.getNodeNames(l) for l in location]
        return list(location)

    def getFileBlock(self, fileBlockName):
        """
        _getFileBlock_

        return a dictionary:
        { blockName: {
             "StorageElements" : [<se list>],
             "Files" : { LFN : Events },
             }
        }


        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.getFileBlock(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        result = { fileBlockName: {
            "StorageElements" : self.listFileBlockLocation(fileBlockName),
            "Files" : self.listFilesInBlock(fileBlockName),
            "IsOpen" : self.blockIsOpen(fileBlockName),

            }
                   }
        return result

    def getFileBlockWithParents(self, fileBlockName):
        """
        _getFileBlockWithParents_

        return a dictionary:
        { blockName: {
             "StorageElements" : [<se list>],
             "Files" : dictionaries representing each file
             }
        }

        files

        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.getFileBlockWithParents(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        result = { fileBlockName: {
            "StorageElements" : self.listFileBlockLocation(fileBlockName),
            "Files" : self.listFilesInBlockWithParents(fileBlockName),
            "IsOpen" : self.blockIsOpen(fileBlockName),

            }
                   }
        return result



    def getFiles(self, dataset, onlyClosedBlocks = False):
        """
        _getFiles_

        Returns a dictionary of block names for the dataset where
        each block constists of a dictionary containing the StorageElements
        for that block and the files in that block by LFN mapped to NEvents

        """
        result = {}
        blocks = self.listFileBlocks(dataset, onlyClosedBlocks)

        [ result.update(self.getFileBlock(x)) for x in blocks ]

        return result


    def listBlockParents(self, blockName):
        """Get parent blocks for block"""
        result = []
        self.checkBlockName(blockName)
        blocks = self.dbs.listBlockParents(block_name = blockName)
        for block in blocks:
            toreturn = {'Name' : block['parent_block_name']}
            toreturn['StorageElementList'] = self.listFileBlockLocation(toreturn['Name'])
            result.append(toreturn)
        return result


    def blockIsOpen(self, blockName):
        """
        _blockIsOpen_

        Return True if named block is open, false if not, or if block
        doenst exist

        """
        self.checkBlockName(blockName)
        blockInstance = self.dbs.listBlocks(block_name = blockName, detail = True)
        if len(blockInstance) == 0:
            return False
        blockInstance = blockInstance[0]
        isOpen = blockInstance.get('open_for_writing', 1)
        if isOpen == 0:
            return False
        return True



    def blockToDatasetPath(self, blockName):
        """
        _blockToDatasetPath_

        Given a block name, get the dataset Path associated with that
        Block.

        Returns the dataset path, or None if not found

        """
        self.checkBlockName(blockName)
        try:
            blocks = self.dbs.listBlocks(block_name = blockName, detail = True)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.blockToDataset(%s)\n" % blockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if blocks == []:
            return None

        pathname = blocks[-1].get('dataset', None)
        return pathname

    def listDatasetLocation(self, datasetName, dbsOnly = False):
        """
        _listDatasetLocation_

        List the origin SEs where there is at least a block of the given
        dataset.
        """
        self.checkDatasetPath(datasetName)
        
        if not dbsOnly:
            try:
                blocksInfo = self.phedex.getReplicaSEForBlocks(dataset=[datasetName],complete='y')
            except Exception, ex:
                msg = "Error while getting block location from PhEDEx for dataset=%s)\n" % datasetName
                msg += "%s\n" % str(ex)
                raise Exception(msg)
            
            if not blocksInfo: # if we couldnt get data location from PhEDEx, try to look into origin site location from dbs
                dbsOnly = True
            else:
                locations = set(blocksInfo.values()[0])
                for blockSites in blocksInfo.values():
                    locations.intersection_update(blockSites)
        
        if dbsOnly:
            try:
                blocksInfo = self.dbs.listBlockOrigin(dataset = datasetName)
            except DbsException, ex:
                msg = "Error in DBSReader: dbsApi.listBlocks(dataset=%s)\n" % datasetName
                msg += "%s\n" % formatEx(ex)
                raise DBSReaderError(msg)
            
            if not blocksInfo: # no data location from dbs
                return list()
            
            locations = set()
            for blockInfo in blocksInfo:
                locations.update([blockInfo['origin_site_name']])
            
            locations.difference_update(['UNKNOWN']) # remove entry when SE name is 'UNKNOWN'
        
        return list(locations)

    def checkDatasetPath(self, pathName):
        """
         _checkDatasetPath_
        """
        if pathName in ("", None):
            raise DBSReaderError("Invalid Dataset Path name: => %s <=" % pathName)

    def checkBlockName(self, blockName):
        """
         _checkBlockName_
        """
        if blockName in ("", "*", None):
            raise DBSReaderError("Invalid Block name: => %s <=" % blockName)
