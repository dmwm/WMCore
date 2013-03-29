#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
import dlsClient
from dlsApi import DlsApiError

from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import *

from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from WMCore.Services.DBS.DBSErrors import DBSReaderError, formatEx
from WMCore.Services.EmulatorSwitch import emulatorHook

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

        # setup DLS api
        dlsType = 'DLS_TYPE_PHEDEX'
        dlsUrl = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod'
        try:
            self.dls = dlsClient.getDlsApi(dls_type = dlsType, dls_endpoint = dlsUrl, version = '')
        except DlsApiError, ex:
            msg = "Error in DBSReader with DlsApi\n"
            msg += "%s\n" % str(ex)
            raise DBSReaderError(msg)
        except DbsException, ex:
            msg = "Error in DBSReader with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)


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
                results = self.dbs.listRuns(block = block)
            else:
                results = self.dbs.listRuns(dataset = dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listRuns(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        return dict((x["run_num"], x["num_lumi"])
                    for x in results)

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
            for lumis in lumiLists:
                lumiDict.setdefault(lumis['logical_file_name'], [])
                item = {}
                item["RunNumber"] = lumis['run_num']
                item['LumiSectionNumber'] = lumis['lumi_section_num']
                lumiDict[lumis['logical_file_name']].append(item)

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


    def listFileBlockLocation(self, fileBlockName):
        """
        _listFileBlockLocation_

        Get a list of fileblock locations

        """
        self.checkBlockName(fileBlockName)
        try:
            entryList = self.dls.getLocations([fileBlockName], showProd = True)
        except DlsApiError, ex:
            msg = "DLS Error in listFileBlockLocation() for %s" % fileBlockName
            msg += "\n%s\n" % str(ex)
            raise DBSReaderError(msg)
        ses = set()
        for block in entryList:
            ses.update([str(location.host) for location in block.locations])
        return list(ses)


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

    def listDatasetLocation(self, dataset):
        """
        _listDatasetLocation_

        List the SEs where there is at least a block of the given
        dataset.
        """
        self.checkDatasetPath(dataset)
        args = {'dataset' : dataset, 'detail' : False}
        try:
            blocks = self.dbs.listBlocks(**args)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        blockNames = [ x['block_name'] for x in blocks ]
        locations = set()
        for blockName in blockNames:
            locations |= set(self.listFileBlockLocation(blockName))

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
