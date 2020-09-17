#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
from __future__ import print_function, division

import logging
from collections import defaultdict

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException
from retry import retry

from Utils.IteratorTools import grouper
from WMCore.Services.DBS.DBSErrors import DBSReaderError, formatEx3


### Needed for the pycurl comment, leave it out for now
# from WMCore.Services.pycurl_manager import getdata as multi_getdata


def remapDBS3Keys(data, stringify=False, **others):
    """Fields have been renamed between DBS2 and 3, take fields from DBS3
    and map to DBS2 values
    """
    mapping = {'num_file': 'NumberOfFiles', 'num_files': 'NumberOfFiles', 'num_event': 'NumberOfEvents',
               'num_block': 'NumberOfBlocks', 'num_lumi': 'NumberOfLumis',
               'event_count': 'NumberOfEvents', 'run_num': 'RunNumber',
               'file_size': 'FileSize', 'block_size': 'BlockSize',
               'file_count': 'NumberOfFiles', 'open_for_writing': 'OpenForWriting',
               'logical_file_name': 'LogicalFileName',
               'adler32': 'Adler32', 'check_sum': 'Checksum', 'md5': 'Md5',
               'block_name': 'BlockName', 'lumi_section_num': 'LumiSectionNumber'}

    mapping.update(others)
    formatFunc = lambda x: str(x) if stringify and isinstance(x, unicode) else x
    for name, newname in mapping.iteritems():
        if name in data:
            data[newname] = formatFunc(data[name])
    return data


@retry(tries=3, delay=1)
def getDataTiers(dbsUrl):
    """
    Function to retrieve all the datatiers from DBS.
    NOTE: to be used with some caching (MemoryCacheStruct)
    :param dbsUrl: the DBS URL string
    :return: a list of strings/datatiers
    """
    dbs = DbsApi(dbsUrl)
    return [tier['data_tier_name'] for tier in dbs.listDataTiers()]


# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMQuality.Emulators.EmulatorSetup module for the values
# @emulatorHook
class DBS3Reader(object):
    """
    _DBSReader_

    General API for reading data from DBS
    """

    def __init__(self, url, logger=None, **contact):

        # instantiate dbs api object
        try:
            self.dbsURL = url
            self.dbs = DbsApi(url, **contact)
            self.logger = logger or logging.getLogger(self.__class__.__name__)
        except dbsClientException as ex:
            msg = "Error in DBSReader with DbsApi\n"
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

    def _getLumiList(self, blockName=None, lfns=None, validFileOnly=1):
        """
        currently only take one lfn but dbs api need be updated
        """
        try:
            if blockName:
                lumiLists = self.dbs.listFileLumis(block_name=blockName, validFileOnly=validFileOnly)
            elif lfns:
                lumiLists = []
                for slfn in grouper(lfns, 50):
                    lumiLists.extend(self.dbs.listFileLumiArray(logical_file_name=slfn))
            else:
                # shouldn't call this with both blockName and lfns empty
                # but still returns empty dict for that case
                return {}
        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBSReader.listFileLumiArray(%s)\n" % lfns
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        lumiDict = {}
        for lumisItem in lumiLists:
            lumiDict.setdefault(lumisItem['logical_file_name'], [])
            item = {}
            item["RunNumber"] = lumisItem['run_num']
            item['LumiSectionNumber'] = lumisItem['lumi_section_num']
            if lumisItem.get('event_count', None) is not None:
                item['EventCount'] = lumisItem['event_count']
            lumiDict[lumisItem['logical_file_name']].append(item)
            # TODO: add key for lumi and event pair.
        return lumiDict

    def checkDBSServer(self):
        """
        check whether dbs server is up and running
        returns {"dbs_instance": "prod/global", "dbs_version": "3.3.144"}
        """
        try:
            return self.dbs.serverinfo()
        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBS server is not up: %s" % self.dbsURL
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

    def listPrimaryDatasets(self, match='*'):
        """
        _listPrimaryDatasets_

        return a list of primary datasets, The full dataset name must be provided
        pattern based mathcing is no longer supported.
        If no expression is provided, all datasets are returned
        """
        try:
            result = self.dbs.listPrimaryDatasets(primary_ds_name=match)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listPrimaryDataset(%s)\n" % match
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        result = [x['primary_ds_name'] for x in result]
        return result

    def matchProcessedDatasets(self, primary, tier, process):
        """
        _matchProcessedDatasets_

        return a list of Processed datasets
        """
        result = []
        try:
            datasets = self.dbs.listDatasets(primary_ds_name=primary, data_tier_name=tier, detail=True)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        for dataset in datasets:
            dataset = remapDBS3Keys(dataset, processed_ds_name='Name')
            dataset['PathList'] = [dataset['dataset']]
            if dataset['Name'] == process:
                result.append(dataset)
        return result

    def listRuns(self, dataset=None, block=None):
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
                results = self.dbs.listRuns(block_name=block)
            else:
                results = self.dbs.listRuns(dataset=dataset)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listRuns(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)
        for x in results:
            runs.extend(x['run_num'])
        return runs

    def listRunLumis(self, dataset=None, block=None):
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
        # Pointless code in python3
        if isinstance(block, str):
            block = unicode(block)
        if isinstance(dataset, str):
            dataset = unicode(dataset)

        try:
            if block:
                results = self.dbs.listRuns(block_name=block)
            else:
                results = self.dbs.listRuns(dataset=dataset)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listRuns(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx3(ex)
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

    def listProcessedDatasets(self, primary, dataTier='*'):
        """
        _listProcessedDatasets_

        return a list of Processed datasets for the primary and optional
        data tier value

        """
        try:
            result = self.dbs.listDatasets(primary_ds_name=primary, data_tier_name=dataTier)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        result = [x['dataset'].split('/')[2] for x in result]
        return result

    def listDatasetFiles(self, datasetPath):
        """
        _listDatasetFiles_

        Get list of files for dataset

        """
        return [x['logical_file_name'] for x in self.dbs.listFileArray(dataset=datasetPath)]

    def listDatatiers(self):
        """
        _listDatatiers_

        Get a list of datatiers known by DBS.
        """
        return [tier['data_tier_name'] for tier in self.dbs.listDataTiers()]

    def listDatasetFileDetails(self, datasetPath, getParents=False, getLumis=True, validFileOnly=1):
        """
        TODO: This is completely wrong need to be redone. or be removed - getting dataset altogether
        might be to costly

        _listDatasetFileDetails_

        Get list of lumis, events, and parents for each file in a dataset
        Return a dict where the keys are the files, and for each file we have something like:
            { 'NumberOfEvents': 545,
              'BlockName': '/HighPileUp/Run2011A-v1/RAW#dd6e0796-cbcc-11e0-80a9-003048caaace',
              'Lumis': {173658: [8, 12, 9, 14, 19, 109, 105]},
              'Parents': [],
              'Checksum': '22218315',
              'Adler32': 'a41a1446',
              'FileSize': 286021145,
              'ValidFile': 1
            }

        """
        fileDetails = self.getFileListByDataset(dataset=datasetPath, validFileOnly=validFileOnly, detail=True)
        blocks = set()  # the set of blocks of the dataset
        # Iterate over the files and prepare the set of blocks and a dict where the keys are the files
        files = {}
        for f in fileDetails:
            blocks.add(f['block_name'])
            files[f['logical_file_name']] = remapDBS3Keys(f, stringify=True)
            files[f['logical_file_name']]['ValidFile'] = f['is_file_valid']
            files[f['logical_file_name']]['Lumis'] = {}
            files[f['logical_file_name']]['Parents'] = []

        # Iterate over the blocks and get parents and lumis
        for blockName in blocks:
            # get the parents
            if getParents:
                parents = self.dbs.listFileParents(block_name=blockName)
                for p in parents:
                    if p['logical_file_name'] in files:  # invalid files are not there if validFileOnly=1
                        files[p['logical_file_name']]['Parents'].extend(p['parent_logical_file_name'])

            if getLumis:
                # get the lumis
                file_lumis = self.dbs.listFileLumis(block_name=blockName)
                for f in file_lumis:
                    if f['logical_file_name'] in files:  # invalid files are not there if validFileOnly=1
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
        allLfns = self.dbs.listFileArray(dataset=datasetPath, validFileOnly=1, detail=False)
        setOfAllLfns = set(allLfns)
        setOfKnownLfns = set(lfns)
        return list(setOfAllLfns.intersection(setOfKnownLfns))

    def crossCheckMissing(self, datasetPath, *lfns):
        """
        _crossCheckMissing_

        As cross check, but return value is a list of files that
        are *not* known by DBS

        """
        allLfns = self.dbs.listFileArray(dataset=datasetPath, validFileOnly=1, detail=False)
        setOfAllLfns = set(allLfns)
        setOfKnownLfns = set(lfns)
        knownFiles = setOfAllLfns.intersection(setOfKnownLfns)
        unknownFiles = setOfKnownLfns.difference(knownFiles)
        return list(unknownFiles)

    def getDBSSummaryInfo(self, dataset=None, block=None):
        """
        Get dataset summary includes # of files, events, blocks and total size
        """
        if dataset:
            self.checkDatasetPath(dataset)
        try:
            if block:
                summary = self.dbs.listFileSummaries(block_name=block, validFileOnly=1)
            else:
                summary = self.dbs.listFileSummaries(dataset=dataset, validFileOnly=1)
        except Exception as ex:
            msg = "Error in DBSReader.getDBSSummaryInfo(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        if not summary:  # missing data or all files invalid
            return {}

        result = remapDBS3Keys(summary[0], stringify=True)
        result['path'] = dataset if dataset else ''
        result['block'] = block if block else ''
        return result

    def listFileBlocks(self, dataset, onlyClosedBlocks=False, blockName=None):
        """
        _listFileBlocks_

        Retrieve a list of fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        args = {'dataset': dataset, 'detail': False}
        if blockName:
            args['block_name'] = blockName
        if onlyClosedBlocks:
            args['detail'] = True
        try:
            blocks = self.dbs.listBlocks(**args)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        if onlyClosedBlocks:
            result = [x['block_name'] for x in blocks if str(x['open_for_writing']) != "1"]

        else:
            result = [x['block_name'] for x in blocks]

        return result

    def listOpenFileBlocks(self, dataset):
        """
        _listOpenFileBlocks_

        Retrieve a list of open fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        try:
            blocks = self.dbs.listBlocks(dataset=dataset, detail=True)
        except dbsClientException as ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        result = [x['block_name'] for x in blocks if str(x['open_for_writing']) == "1"]

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

            blocks = self.dbs.listBlocks(block_name=fileBlockName)
        except Exception as ex:
            msg = "Error in "
            msg += "DBSReader.blockExists(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        if len(blocks) == 0:
            return False
        return True

    def listFilesInBlock(self, fileBlockName, lumis=True, validFileOnly=1):
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
            files = self.dbs.listFileArray(block_name=fileBlockName, validFileOnly=validFileOnly, detail=True)
        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        if lumis:
            lumiDict = self._getLumiList(blockName=fileBlockName, validFileOnly=validFileOnly)

        result = []
        for fileInfo in files:
            if lumis:
                fileInfo["LumiList"] = lumiDict[fileInfo['logical_file_name']]
            result.append(remapDBS3Keys(fileInfo, stringify=True))
        return result

    def listFilesInBlockWithParents(self, fileBlockName, lumis=True, validFileOnly=1):
        """
        _listFilesInBlockWithParents_

        Get a list of files in the named fileblock including
        the parents of that file.
        TODO: lumis can be false when lumi splitting is not required
        However WMBSHelper expect file['LumiList'] to get the run number
        so for now it will be always true.

        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.listFilesInBlockWithParents(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        try:
            # TODO: shoud we get only valid block for this?
            files = self.dbs.listFileParents(block_name=fileBlockName)
            fileDetails = self.listFilesInBlock(fileBlockName, lumis, validFileOnly)

        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlockWithParents(%s)\n" % (
                fileBlockName,)
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        childByParents = defaultdict(list)
        for f in files:
            # Probably a child can have more than 1 parent file
            for fp in f['parent_logical_file_name']:
                childByParents[fp].append(f['logical_file_name'])

        parentsLFNs = childByParents.keys()

        if len(parentsLFNs) == 0:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlockWithParents(%s)\n There is no parents files" % (
                fileBlockName)
            raise DBSReaderError(msg)

        parentFilesDetail = []
        # TODO: slicing parentLFNs util DBS api is handling that.
        # Remove slicing if DBS api handles
        for pLFNs in grouper(parentsLFNs, 50):
            parentFilesDetail.extend(self.dbs.listFileArray(logical_file_name=pLFNs, detail=True))

        if lumis:
            parentLumis = self._getLumiList(lfns=parentsLFNs)

        parentsByLFN = defaultdict(list)

        for pf in parentFilesDetail:
            parentLFN = pf['logical_file_name']
            dbsFile = remapDBS3Keys(pf, stringify=True)
            if lumis:
                dbsFile["LumiList"] = parentLumis[parentLFN]

            for childLFN in childByParents[parentLFN]:
                parentsByLFN[childLFN].append(dbsFile)

        for fileInfo in fileDetails:
            fileInfo["ParentList"] = parentsByLFN[fileInfo['logical_file_name']]

        return fileDetails

    def lfnsInBlock(self, fileBlockName):
        """
        _lfnsInBlock_

        LFN list only for block, details = False => faster query

        """
        if not self.blockExists(fileBlockName):
            msg = "DBSReader.lfnsInBlock(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        try:
            lfns = self.dbs.listFileArray(block_name=fileBlockName, validFileOnly=1, detail=False)
            return lfns
        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

    def listFileBlockLocation(self, fileBlockNames):
        """
        _listFileBlockLocation_

        Get origin_site_name of a block

        """

        singleBlockName = None
        if isinstance(fileBlockNames, basestring):
            singleBlockName = fileBlockNames
            fileBlockNames = [fileBlockNames]

        for block in fileBlockNames:
            self.checkBlockName(block)

        locations = {}
        node_filter = set(['UNKNOWN', None])

        blocksInfo = {}
        try:
            for block in fileBlockNames:
                blocksInfo.setdefault(block, [])
                # there should be only one element with a single origin site string ...
                for blockInfo in self.dbs.listBlockOrigin(block_name=block):
                    blocksInfo[block].append(blockInfo['origin_site_name'])
        except dbsClientException as ex:
            msg = "Error in DBS3Reader: self.dbs.listBlockOrigin(block_name=%s)\n" % fileBlockNames
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        for block in fileBlockNames:
            valid_nodes = set(blocksInfo.get(block, [])) - node_filter
            locations[block] = list(valid_nodes)

        # returning single list if a single block is passed
        if singleBlockName:
            return locations[singleBlockName]

        return locations

    def getFileBlock(self, fileBlockName):
        """
        Retrieve a list of files in the block; a flag whether the
        block is still open or not; and it used to resolve the block
        location via PhEDEx.

        :return: a dictionary in the format of:
            {"PhEDExNodeNames" : [],
             "Files" : { LFN : Events },
             "IsOpen" : True|False}
        """
        result = {"PhEDExNodeNames": [],  # FIXME: we better get rid of this line!
                  "Files": self.listFilesInBlock(fileBlockName),
                  "IsOpen": self.blockIsOpen(fileBlockName)}
        return result

    def getFileBlockWithParents(self, fileBlockName):
        """
        Retrieve a list of parent files in the block; a flag whether the
        block is still open or not; and it used to resolve the block
        location via PhEDEx.

        :return: a dictionary in the format of:
            {"PhEDExNodeNames" : [],
             "Files" : { LFN : Events },
             "IsOpen" : True|False}
        """
        if isinstance(fileBlockName, str):
            fileBlockName = unicode(fileBlockName)

        if not self.blockExists(fileBlockName):
            msg = "DBSReader.getFileBlockWithParents(%s): No matching data"
            raise DBSReaderError(msg % fileBlockName)

        result = {"PhEDExNodeNames": [],  # FIXME: we better get rid of this line!
                  "Files": self.listFilesInBlockWithParents(fileBlockName),
                  "IsOpen": self.blockIsOpen(fileBlockName)}
        return result

    def listBlockParents(self, blockName):
        """
        Return a list of parent blocks for a given child block name
        """
        # FIXME: note the different returned data structure
        result = []
        self.checkBlockName(blockName)
        blocks = self.dbs.listBlockParents(block_name=blockName)
        result = [block['parent_block_name'] for block in blocks]
        return result

    def blockIsOpen(self, blockName):
        """
        _blockIsOpen_

        Return True if named block is open, false if not, or if block
        doenst exist

        """
        self.checkBlockName(blockName)
        blockInstance = self.dbs.listBlocks(block_name=blockName, detail=True)
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
            blocks = self.dbs.listBlocks(block_name=blockName, detail=True)
        except Exception as ex:
            msg = "Error in "
            msg += "DBSReader.blockToDatasetPath(%s)\n" % blockName
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        if blocks == []:
            return None

        pathname = blocks[-1].get('dataset', None)
        return pathname

    def listDatasetLocation(self, datasetName):
        """
        _listDatasetLocation_

        List the origin SEs where there is at least a block of the given
        dataset.
        """
        self.checkDatasetPath(datasetName)

        locations = set()
        try:
            blocksInfo = self.dbs.listBlockOrigin(dataset=datasetName)
        except dbsClientException as ex:
            msg = "Error in DBSReader: dbsApi.listBlocks(dataset=%s)\n" % datasetName
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

        if not blocksInfo:  # no data location from dbs
            return list()

        for blockInfo in blocksInfo:
            locations.update(blockInfo['origin_site_name'])

        locations.difference_update(['UNKNOWN', None])  # remove entry when SE name is 'UNKNOWN'

        return list(locations)

    def checkDatasetPath(self, pathName):
        """
         _checkDatasetPath_
        """
        if pathName in ("", None):
            raise DBSReaderError("Invalid Dataset Path name: => %s <=" % pathName)
        else:
            try:
                result = self.dbs.listDatasets(dataset=pathName, dataset_access_type='*')
                if len(result) == 0:
                    raise DBSReaderError("Dataset %s doesn't exist in DBS %s" % (pathName, self.dbsURL))
            except (dbsClientException, HTTPError) as ex:
                msg = "Error in "
                msg += "DBSReader.checkDatasetPath(%s)\n" % pathName
                msg += "%s\n" % formatEx3(ex)
                raise DBSReaderError(msg)
        return

    def checkBlockName(self, blockName):
        """
         _checkBlockName_
        """
        if blockName in ("", "*", None):
            raise DBSReaderError("Invalid Block name: => %s <=" % blockName)

    def getFileListByDataset(self, dataset, validFileOnly=1, detail=True):

        """
        _getFileListByDataset_

        Given a dataset, retrieves all blocks, lfns and number of events (among other
        not really important info).
        Returns a list of dict.
        """

        try:
            fileList = self.dbs.listFileArray(dataset=dataset, validFileOnly=validFileOnly, detail=detail)
            return fileList
        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBSReader.getFileListByDataset(%s)\n" % dataset
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

    def listDatasetParents(self, childDataset):
        """
        list the the parents dataset path given childDataset
        """
        try:
            parentList = self.dbs.listDatasetParents(dataset=childDataset)
            return parentList
        except dbsClientException as ex:
            msg = "Error in "
            msg += "DBSReader.listDatasetParents(%s)\n" % childDataset
            msg += "%s\n" % formatEx3(ex)
            raise DBSReaderError(msg)

    # def getListFilesByLumiAndDataset(self, dataset, files):
    #     "Unsing pycurl to get all the child parents pair for given dataset"
    #
    #     urls = ['%s/data/dbs/fileparentbylumis?block_name=%s' % (
    #              self.dbsURL, b["block_name"]) for b in self.dbs.listBlocks(dataset=dataset)]
    #
    #     data = multi_getdata(urls, ckey(), cert())
    #     rdict = {}
    #     for row in data:
    #         try:
    #             data = json.loads(row['data'])
    #             rdict[req] = data['result'][0]  # we get back {'result': [workflow]} dict
    #         except Exception as exp:
    #             print("ERROR: fail to load data as json record, error=%s" % str(exp))
    #             print(row)
    #     return rdict

    def getParentFilesGivenParentDataset(self, parentDataset, childLFNs):
        """
        returns parent files for given childLFN when DBS doesn't have direct parent child relationship in DB
        Only use this for finding missing parents

        :param parentDataset: parent dataset for childLFN
        :param childLFN: a file in child dataset
        :return: set of parent files for childLFN
        """
        fInfo = self.dbs.listFileLumiArray(logical_file_name=childLFNs)
        parentFiles = defaultdict(set)
        for f in fInfo:
            pFileList = self.dbs.listFiles(dataset=parentDataset, run_num=f['run_num'], lumi_list=f['lumi_section_num'])
            pFiles = set([x['logical_file_name'] for x in pFileList])
            parentFiles[f['logical_file_name']] = parentFiles[f['logical_file_name']].union(pFiles)
        return parentFiles

    def getParentFilesByLumi(self, childLFN):
        """
        get the parent file's lfns by lumi (This might not be the actual parentage relations in DBS just parentage by Lumis).
        use for only specific lfn for validating purpose, for the parentage fix use findAndInsertMissingParentage
        :param childLFN:
        :return: list of dictionary with parent files for given child LFN and parent dataset
        [{"ParentDataset": /abc/bad/ddd, "ParentFiles": [alf, baf, ...]]
        """
        childDatasets = self.dbs.listDatasets(logical_file_name=childLFN)
        result = []
        for i in childDatasets:
            parents = self.dbs.listDatasetParents(dataset=i["dataset"])
            for parent in parents:
                parentFiles = self.getParentFilesGivenParentDataset(parent['parent_dataset'], childLFN)
                result.append({"ParentDataset": parent['parent_dataset'], "ParentFiles": list(parentFiles)})
        return result

    def insertFileParents(self, childBlockName, childParentsIDPairs):
        """
        :param childBlockName: child block name
        :param childParentsIDPairs: list of list child and parent file ids, i.e. [[1,2], [3,4]...]
                dbs validate child ids from the childBlockName
        :return: None
        """
        return self.dbs.insertFileParents({"block_name": childBlockName, "child_parent_id_list": childParentsIDPairs})

    def findAndInsertMissingParentage(self, childBlockName, parentData, insertFlag=True):
        """
        :param childBlockName: child block name
        :param parentData: a dictionary with complete parent dataset file/run/lumi information
        :param insertFlag: boolean to allow parentage insertion into DBS or not
        :return: number of file parents pair inserted
        """
        # in the format of: {'fileid': [[run_num1, lumi1], [run_num1, lumi2], etc]
        # e.g. {'554307997': [[1, 557179], [1, 557178], [1, 557181],
        childBlockData = self.dbs.listBlockTrio(block_name=childBlockName)

        # runs the actual mapping logic, like {"child_id": ["parent_id", "parent_id2", ...], etc
        mapChildParent = {}
        # there should be only 1 item, but we better be safe
        for item in childBlockData:
            for childFileID in item:
                for runLumiPair in item[childFileID]:
                    frozenKey = frozenset(runLumiPair)
                    parentId = parentData.get(frozenKey)
                    if parentId is None:
                        msg = "Child file id: %s, with run/lumi: %s, has no match in the parent dataset"
                        self.logger.warning(msg, childFileID, frozenKey)
                        continue
                    mapChildParent.setdefault(childFileID, set())
                    mapChildParent[childFileID].add(parentId)

        if insertFlag and mapChildParent:
            # convert dictionary to list of unique childID, parentID tuples
            listChildParent = []
            for childID in mapChildParent:
                for parentID in mapChildParent[childID]:
                    listChildParent.append([int(childID), int(parentID)])
            self.dbs.insertFileParents({"block_name": childBlockName, "child_parent_id_list": listChildParent})
        return len(mapChildParent)

    def listBlocksWithNoParents(self, childDataset):
        """
        :param childDataset: child dataset for
        :return: set of child blocks with no parentBlock
        """
        allBlocks = self.dbs.listBlocks(dataset=childDataset)
        blockNames = []
        for block in allBlocks:
            blockNames.append(block['block_name'])
        parentBlocks = self.dbs.listBlockParents(block_name=blockNames)

        cblock = set()
        for pblock in parentBlocks:
            cblock.add(pblock['this_block_name'])

        noParentBlocks = set(blockNames) - cblock
        return noParentBlocks

    def listFilesWithNoParents(self, childBlockName):
        """
        :param childBlockName:
        :return:
        """
        allFiles = self.dbs.listFiles(block_name=childBlockName)
        parentFiles = self.dbs.listFileParents(block_name=childBlockName)

        allFileNames = set()
        for fInfo in allFiles:
            allFileNames.add(fInfo['logical_file_name'])

        cfile = set()
        for pFile in parentFiles:
            cfile.add(pFile['logical_file_name'])

        noParentFiles = allFileNames - cfile
        return list(noParentFiles)

    def fixMissingParentageDatasets(self, childDataset, insertFlag=True):
        """
        :param childDataset: child dataset need to set the parentage correctly.
        :return: blocks which failed to insert parentage. for retry
        """
        pDatasets = self.listDatasetParents(childDataset)
        self.logger.info("Parent datasets for %s are: %s", childDataset, pDatasets)
        # print("parent datasets %s\n" % pDatasets)
        # pDatasets format is
        # [{'this_dataset': '/SingleMuon/Run2016D-03Feb2017-v1/MINIAOD', 'parent_dataset_id': 13265209, 'parent_dataset': '/SingleMuon/Run2016D-23Sep2016-v1/AOD'}]
        if not pDatasets:
            self.logger.warning("No parent dataset found for child dataset %s", childDataset)
            return {}

        parentFullInfo = self.getParentDatasetTrio(childDataset)
        blocks = self.listBlocksWithNoParents(childDataset)
        failedBlocks = []
        self.logger.info("Found %d blocks without parentage information", len(blocks))
        for blockName in blocks:
            try:
                self.logger.info("Fixing parentage for block: %s", blockName)
                numFiles = self.findAndInsertMissingParentage(blockName, parentFullInfo, insertFlag=insertFlag)
                self.logger.debug("%s file parentage added for block %s", numFiles, blockName)
            except Exception as ex:
                self.logger.exception("Parentage updated failed for block %s", blockName)
                failedBlocks.append(blockName)

        return failedBlocks

    def getParentDatasetTrio(self, childDataset):
        """
        Provided a dataset name, return all the parent dataset information, such as:
          - file ids, run number and lumi section
        NOTE: This API is meant to be used by the StepChainParentage thread only!!!
        :param childDataset: name of the child dataset
        :return: a dictionary where the key is a set of run/lumi, its value is the fileid
        """
        # this will return data in the format of:
        # {'554307997': [[1, 557179], [1, 557178],...
        # such that: key is file id, in each list is [run_number, lumi_section_numer].
        parentFullInfo = self.dbs.listParentDSTrio(dataset=childDataset)

        # runs the actual mapping logic, like {"child_id": ["parent_id", "parent_id2", ...], etc
        parentFrozenData = {}
        for item in parentFullInfo:
            for fileId in item:
                for runLumiPair in item[fileId]:
                    frozenKey = frozenset(runLumiPair)
                    parentFrozenData[frozenKey] = fileId
        return parentFrozenData
