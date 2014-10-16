#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
import dlsClient
from dlsApi import DlsApiError

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from WMCore.Services.DBS.DBSErrors import DBSReaderError, formatEx
from WMCore.Services.EmulatorSwitch import emulatorHook

# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMQuality.Emulators.EmulatorSetup module for the values
#@emulatorHook
class DBS2Reader:
    """
    _DBSReader_

    General API for reading data from DBS


    """
    def __init__(self, url, **contact):
        args = { "url" : url, "level" : 'ERROR', "version" : ''}
        args.update(contact)
        #try:
        self.dbs = DbsApi(args)
        #except DbsException, ex:
        #    msg = "Error in DBSReader with DbsApi\n"
        #    msg += "%s\n" % formatEx(ex)
        #    raise DBSReaderError(msg)

        # setup DLS api - with either dbs or phedex depending on dbs instance
        if url.count('cmsdbsprod.cern.ch/cms_dbs_prod_global') or \
                        self.dbs.getServerInfo()['InstanceName'] == 'GLOBAL':
            dlsType = 'DLS_TYPE_PHEDEX'
            dlsUrl = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod'
        else:
            dlsType = 'DLS_TYPE_DBS'
            dlsUrl = url
        try:
            self.dls = dlsClient.getDlsApi(dls_type = dlsType, dls_endpoint = dlsUrl, version = args['version'])
        except DlsApiError, ex:
            msg = "Error in DBSReader with DlsApi\n"
            msg += "%s\n" % str(ex)
            raise DBSReaderError(msg)
        except DbsException, ex:
            msg = "Error in DBSReader with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)


    def listPrimaryDatasets(self, match = None):
        """
        _listPrimaryDatasets_

        return a list of primary datasets matching the glob expression.
        If no expression is provided, all datasets are returned
        """
        arg = "*"
        if match != None:
            arg = match
        try:
            result = self.dbs.listPrimaryDatasets(arg)
        except DbsException, ex:
            msg = "Error in DBSReader.listPrimaryDataset(%s)\n" % arg
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = [ x['Name'] for x in result ]
        return result

    def matchProcessedDatasets(self, primary, tier, process):
        """
        _matchProcessedDatasets_

        return a list of Processed datasets
        """
        try:
            result = self.dbs.listProcessedDatasets(primary, tier, process)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

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
        try:
            if block:
                results = self.dbs.listRuns(block = block)
            else:
                results = self.dbs.listRuns(dataset = dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listRuns(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        return [x["RunNumber"] for x in results]

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

        return dict((x["RunNumber"], x["NumberOfLumiSections"])
                    for x in results)

    def listProcessedDatasets(self, primary, dataTier = None):
        """
        _listProcessedDatasets_

        return a list of Processed datasets for the primary and optional
        data tier value

        """
        tier = "*"
        if dataTier != None:
            tier = dataTier

        try:
            result = self.dbs.listProcessedDatasets(primary, tier)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = [ x['Name'] for x in result ]
        return result


    def listDatasetFiles(self, datasetPath):
        """
        _listDatasetFiles_

        Get list of files for dataset

        """
        return [ x['LogicalFileName'] for x in self.dbs.listFiles(datasetPath)]

    def listDatasetFileDetails(self, datasetPath, getParents=False):
        """
        _listDatasetFileDetails_

        Get list of lumis, events, and parents for each file in a dataset
        """

        fileDetails = self.dbs.listFiles(datasetPath, retriveList=["retrive_lumi", "retrive_run", "retrive_block", "retrive_parent"])
        files = {}
        for f in fileDetails:
            #prepare the dict of lumis
            lumis = {}
            for l in f['LumiList']:
                if l['RunNumber'] in lumis:
                    lumis[l['RunNumber']].append(l['LumiSectionNumber'])
                else:
                    lumis[l['RunNumber']] = [ l['LumiSectionNumber'] ]

            files[f['LogicalFileName']] = {
                "BlockName" : f['Block']['Name'],
                "NumberOfEvents" : f['NumberOfEvents'],
                "Lumis" : lumis,
                "Parents" : [ x['LogicalFileName'] for x in f['ParentList'] ],
                "Size" : f['FileSize'],
                "Checksums" : {'Adler32': f['Adler32'], 'Checksum': f['Checksum'], 'Md5': f['Md5']}
            }

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
        if dataset:
            self.checkDatasetPath(dataset)
        try:
            if block:
                summary = self.dbs.getSummary(block = block)
            else: # dataset case dataset shouldn't be None
                summary = self.dbs.getSummary(dataset = dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listDatasetSummary(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        # summary returns
        # 'number_of_files', 'number_of_events', 'number_of_blocks', 'total_size', 'path'
        # map that to syncronize with other dbs key
        for k, v in summary.items():
            if k == 'number_of_files':
                summary['NumberOfFiles'] = v
            if k == 'number_of_events':
                summary['NumberOfEvents'] = v
            if k == 'number_of_lumis':
                summary['NumberOfLumis'] = v
            if k == 'number_of_blocks':
                summary['NumberOfBlocks'] = v
        return summary

    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = False,
                          blockName = '*', locations = True):
        """
        """
        self.checkDatasetPath(dataset)
        try:
            blocks = self.dbs.listBlocks(dataset, blockName, nosite = not locations)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if onlyClosedBlocks:
            return [x for x in blocks if str(x['OpenForWriting']) != "1"]

        return blocks

    def listFileBlocks(self, dataset, onlyClosedBlocks = False,
                       blockName = '*'):
        """
        _listFileBlocks_

        Retrieve a list of fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        try:
            blocks = self.dbs.listBlocks(dataset, blockName, nosite = True)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if onlyClosedBlocks:
            result = [
                x['Name'] for x in blocks \
                  if str(x['OpenForWriting']) != "1"
                ]

        else:
            result = [ x['Name'] for x in blocks ]

        return result

    def listOpenFileBlocks(self, dataset):
        """
        _listOpenFileBlocks_

        Retrieve a list of open fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        try:
            blocks = self.dbs.listBlocks(dataset, nosite = True)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)


        result = [
            x['Name'] for x in blocks \
            if str(x['OpenForWriting']) == "1"
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

            blocks = self.dbs.listBlocks(block_name = fileBlockName, nosite = True)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.blockExists(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if len(blocks) == 0:
            return False
        return True


    def listFilesInBlock(self, fileBlockName):
        """
        _listFilesInBlock_

        Get a list of files in the named fileblock

        """
        try:
            files = self.dbs.listFiles(
                 "", # path
                 "", #primary
                 "", # processed
                 [], #tier_list
                 "", #analysisDataset
                 fileBlockName, details = "True")

        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = []
        [ result.append(dict(x)) for x in files ]

        for x in result:
            x['ParentList'] = []

        return result

    def listFilesInBlockWithParents(self, fileBlockName):
        """
        _listFilesInBlockWithParents_

        Get a list of files in the named fileblock including
        the parents of that file.

        """
        try:
            files = self.dbs.listFiles(
                 "", # path
                 "", #primary
                 "", # processed
                 [], #tier_list
                 "", #analysisDataset
                 fileBlockName,
                 details = "True",
                 retriveList = ['retrive_parent' ])

        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlockWithParents(%s)\n" % (
                fileBlockName,)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = []
        [ result.append(dict(x)) for x in files ]
        return result


    def lfnsInBlock(self, fileBlockName):
        """
        _lfnsInBlock_

        LFN list only for block, details = False => faster query

        """
        try:
            files = self.dbs.listFiles(
                "", # path
                "", #primary
                "", # processed
                [], #tier_list
                "", #analysisDataset
                fileBlockName, details = "False")
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.lfnsInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = []
        [ result.append(x['LogicalFileName']) for x in files ]
        return result


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
             "PhEDExNodeNames" : [<pnn list>],
             "Files" : { LFN : Events },
             }
        }


        """
        result = { fileBlockName: {
            "PhEDExNodeNames" : self.listFileBlockLocation(fileBlockName),
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
             "PhEDExNodeNames" : [<pnn list>],
             "Files" : dictionaries representing each file
             }
        }

        files

        """

        blockParents = self.listBlockParents(fileBlockName)
        parentFiles = {}
        for parentBlock in blockParents:
            blockFiles = self.listFilesInBlock(parentBlock['Name'])
            #Make the filelist a dictionary, speed up things later
            for parentFile in blockFiles:
                parentFiles[parentFile['LogicalFileName']] = parentFile

        blockFiles = self.listFilesInBlockWithParents(fileBlockName)
        #Now populate the parent lists
        for blockFile in blockFiles:
            for idx, parentFile in enumerate(blockFile['ParentList']):
                blockFile['ParentList'][idx] = parentFiles[parentFile['LogicalFileName']]

        result = { fileBlockName: {
            "PhEDExNodeNames" : self.listFileBlockLocation(fileBlockName),
            "Files" : blockFiles,
            "IsOpen" : self.blockIsOpen(fileBlockName),
            }
                   }
        return result



    def getFiles(self, dataset, onlyClosedBlocks = False):
        """
        _getFiles_

        Returns a dictionary of block names for the dataset where
        each block constists of a dictionary containing the PhEDExNodeNames
        for that block and the files in that block by LFN mapped to NEvents

        """
        result = {}
        blocks = self.listFileBlocks(dataset, onlyClosedBlocks)

        [ result.update(self.getFileBlock(x)) for x in blocks ]

        return result


    def listBlockParents(self, blockName):
        """Get parent blocks for block"""
        self.checkBlockName(blockName)
        blocks = self.dbs.listBlockParents(blockName)
        # api appears broken as doesn't return locations, as dbs2 lifetime limited
        # lookup locations here
        for block in blocks:
            block['PhEDExNodeList'] = self.listFileBlockLocation(block['Name'])
        return blocks


    def blockIsOpen(self, blockName):
        """
        _blockIsOpen_

        Return True if named block is open, false if not, or if block
        doenst exist

        """
        self.checkBlockName(blockName)
        blockInstance = self.dbs.listBlocks(block_name = blockName, nosite = True)
        if len(blockInstance) == 0:
            return False
        blockInstance = blockInstance[0]
        isOpen = blockInstance.get('OpenForWriting', '1')
        if isOpen == "0":
            return False
        return True

    def listDatasetLocation(self, dataset):
        """
        _listDatasetLocation_

        List the SEs where there is at least a block of the given
        dataset.
        """
        self.checkDatasetPath(dataset)
        try:
            blocks = self.dbs.listBlocks(dataset, '*', nosite = False)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = set()
        for block in blocks:
            result |= set([x['Name'] for x in block['PhEDExNodeList']])

        return list(result)

    def blockToDatasetPath(self, blockName):
        """
        _blockToDatasetPath_

        Given a block name, get the dataset Path associated with that
        Block.

        Returns the dataset path, or None if not found

        """
        self.checkBlockName(blockName)
        try:
            blocks = self.dbs.listBlocks(block_name = blockName, nosite = True)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.blockToDataset(%s)\n" % blockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if blocks == []:
            return None

        pathname = blocks[-1].get('Path', None)
        return pathname


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
