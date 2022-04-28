#!/usr/bin/env python
"""
_DBSWriterObjects_

Functions to instantiate and return DBS Objects and insert them
into DBS if required

"""
from __future__ import print_function

from builtins import int

import logging

from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsAlgorithm import DbsAlgorithm
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsStorageElement import DbsStorageElement
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsLumiSection import DbsLumiSection

def makeTierList(dataTier):
    """
    _makeTierList_

    Standard tool to split data tiers if they contain - chars
    *** Do not use outside of this module ***

    """
    tierList = dataTier.split("-")
    return tierList

def createPrimaryDataset(datasetInfo, apiRef = None):
    """
    _createPrimaryDataset_

    Create and return a Primary Dataset object.
    If apiRef is not None, it is used to insert the dataset into the
    DBS

    """
    if 'PrimaryDatasetType' in datasetInfo:
        PrimaryDatasetType = datasetInfo['PrimaryDatasetType']
    else:
        PrimaryDatasetType = 'mc'

    logging.debug("Inserting PrimaryDataset %s with Type %s", datasetInfo["PrimaryDataset"], PrimaryDatasetType)
    primary = DbsPrimaryDataset(Name = datasetInfo["PrimaryDataset"], Type=PrimaryDatasetType)

    if apiRef != None:
        apiRef.insertPrimaryDataset(primary)
    return primary


def createAlgorithm(datasetInfo, configMetadata = None, apiRef = None):
    """
    _createAlgorithm_

    Create an algorithm assuming that datasetInfo is a
    ProdCommon.MCPayloads.DatasetInfo like dictionary

    """

    exeName = datasetInfo['ApplicationName']
    appVersion = datasetInfo['ApplicationVersion']
    appFamily = datasetInfo["ApplicationFamily"]

    #
    # HACK:  Problem with large PSets (is this still relevant ?)
    #
    # Repacker jobs have no PSetContent/PSetHash
    #
    psetContent = datasetInfo.get('PSetContent',None)
    if psetContent == None:
        psetContent = "PSET_CONTENT_NOT_AVAILABLE"
    psetHash = datasetInfo.get('PSetHash',None)
    if psetHash == None:
        psetHash = "NO_PSET_HASH"
    else:
        if psetHash.find(";"):
            # no need for fake hash in new schema
            psetHash = psetHash.split(";")[0]
            psetHash = psetHash.replace("hash=", "")

    ## No more hacks
    #msg = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
    #msg += "TEST HACK USED FOR PSetContent\n"
    #msg += ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    #logging.warning(msg)
    #print msg
    #psetContent = "This is not a PSet"

    #
    # HACK: 100 char limit on cfg file name
    if configMetadata != None:
        cfgName = configMetadata['name']
        if len(cfgName) > 100:
            msg = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
            msg += "TEST HACK USED FOR Config File Name"
            msg += ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
            logging.warning(msg)
            print(msg)
            configMetadata['name'] = cfgName[-99]

        psetInstance = DbsQueryableParameterSet(
            Hash = psetHash,
            Name = configMetadata['name'],
            Version = configMetadata['version'],
            Type = configMetadata['Type'],
            Annotation = configMetadata['annotation'],
            Content = psetContent,
            )


        algorithmInstance = DbsAlgorithm(
            ExecutableName = exeName,
            ApplicationVersion = appVersion,
            ApplicationFamily = appFamily,
            ParameterSetID = psetInstance
            )
    else:
        psetInstance = DbsQueryableParameterSet(
                    Hash = psetHash)
        algorithmInstance = DbsAlgorithm(
            ExecutableName = exeName,
            ApplicationVersion = appVersion,
            ApplicationFamily = appFamily,
            ParameterSetID = psetInstance
            )

    if apiRef != None:
        apiRef.insertAlgorithm(algorithmInstance)
    return algorithmInstance

def createAlgorithmForInsert(datasetInfo):
    """
    _createPartialAlgorithm_

    Create an Algorithm instance that uses the minimal info needed
    to insert a file

    """
    exeName = datasetInfo['ApplicationName']
    appVersion = datasetInfo['ApplicationVersion']
    appFamily = datasetInfo["ApplicationFamily"]

    #
    # Repacker jobs have no PsetContent/PSetHash
    #
    psetContent = datasetInfo.get('PSetContent',None)
    if psetContent == None:
        psetContent = "PSET_CONTENT_NOT_AVAILABLE"
    psetHash = datasetInfo.get('PSetHash',None)
    if psetHash == None:
        psetHash = "NO_PSET_HASH"
    else:
        if psetHash.find(";"):
            # no need for fake hash in new schema
            psetHash = psetHash.split(";")[0]
            psetHash = psetHash.replace("hash=", "")

    psetInstance = DbsQueryableParameterSet(
        Hash = psetHash)
    algorithmInstance = DbsAlgorithm(
        ExecutableName = exeName,
        ApplicationVersion = appVersion,
        ApplicationFamily = appFamily,
        ParameterSetID = psetInstance
        )
    return algorithmInstance

def createMergeAlgorithm(datasetInfo, apiRef = None):
    """
    _createMergeAlgorithm_

    Create a DbsAlgorithm for a merge dataset

    """
    exeName = datasetInfo['ApplicationName']
    version = datasetInfo['ApplicationVersion']
    family = datasetInfo.get('ApplicationFamily', None)
    if (family == None) or not (family) :
        family = datasetInfo['OutputModuleName']


    mergeAlgo = DbsAlgorithm (
        ExecutableName = exeName,
        ApplicationVersion = version,
        ApplicationFamily = family,
        )

    if apiRef != None:
        apiRef.insertAlgorithm(mergeAlgo)
    return mergeAlgo




def createProcessedDataset(primaryDataset, algorithm, datasetInfo,
                           apiRef = None):
    """
    _createProcessedDataset_


    """

    physicsGroup = datasetInfo.get("PhysicsGroup", "NoGroup")
    status = datasetInfo.get("Status", "VALID")
    dataTier = datasetInfo['DataTier']
    globalTag = datasetInfo.get('Conditions', None)
    if globalTag is None: globalTag = ''

    parents = []
    inputDataset = datasetInfo.get('ParentDataset', None)
    if inputDataset != None:
        parents.append(inputDataset)

    tierList = makeTierList(datasetInfo['DataTier'])

    name = datasetInfo['ProcessedDataset']
    algolist=[]
    if algorithm not in ('', None):
        algolist=list(algorithm)

    processedDataset = DbsProcessedDataset (
        PrimaryDataset = primaryDataset,
        AlgoList=algolist,
        Name = name,
        TierList = tierList,
        ParentList = parents,
        PhysicsGroup = physicsGroup,
        Status = status,
        GlobalTag = globalTag,
        )

    if apiRef != None:
        apiRef.insertProcessedDataset(processedDataset)
    #
    logging.debug("PrimaryDataset: %s ProcessedDataset: %s DataTierList: %s  requested by PhysicsGroup: %s ", primaryDataset['Name'], name, tierList, physicsGroup)
    return processedDataset




def createDBSFiles(fjrFileInfo, jobType = None, apiRef = None):
    """
    _createDBSFiles_

    Create a list of DBS File instances from the file details contained
    in a FwkJobRep.FileInfo instance describing an output file
    Does not insert files, returns as list of DbsFile objects
    Does insert runs and lumisections if DBS API reference is passed

    """
    results = []
    inputLFNs = [ x['LFN'] for x in fjrFileInfo.inputFiles]
    checksum = fjrFileInfo.checksums['cksum']
    adler32sum = fjrFileInfo.checksums.get('adler32', '')

    nEvents = int(fjrFileInfo['TotalEvents'])

    if len(fjrFileInfo.dataset)<=0:
        logging.error("No dataset info found in FWJobReport!")
        return results

    #  //
    # // Set FileType
    #//
    if 'FileType' in fjrFileInfo:
        fileType = fjrFileInfo['FileType']
    else:
        fileType = 'EDM'

    #
    # FIXME: at this point I should use the mc or data event type from
    #        the jobreport. Until this is supported by the framework,
    #        we use the workaround that mc job reports have an empty
    #        lumisections list (stripped in DBSInterface)
    #
    lumiList = []
    if ( len(fjrFileInfo.getLumiSections()) > 0 ):

        #
        # insert runs (for data files from detector)
        #
        if ( apiRef != None ):

            for runinfo in fjrFileInfo.runs:

                run = DbsRun(
                    RunNumber = int(runinfo),
                    NumberOfEvents = 0,
                    NumberOfLumiSections = 0,
                    TotalLuminosity = 0,
                    StoreNumber = 0,
                    StartOfRun = 0,
                    EndOfRun = 0,
                    )

                apiRef.insertRun(run)

        #
        # insert lumisections (for data files from detector)
        # associate files with lumisections (for all data files)
        #
        for lumiinfo in fjrFileInfo.getLumiSections():

            lumi = DbsLumiSection(
                LumiSectionNumber = int(lumiinfo['LumiSectionNumber']),
                StartEventNumber = 0,
                EndEventNumber = 0,
                LumiStartTime = 0,
                LumiEndTime = 0,
                RunNumber = int(lumiinfo['RunNumber']),
                )

            # Isnt needed, causes monster slowdown
            #if ( apiRef != None ):
            #    apiRef.insertLumiSection(lumi)

            lumiList.append(lumi)

            logging.debug("Lumi associated to file is: %s" % ([x for x in lumiList]))

    #  //
    # // Dataset info related to files and creation of DbsFile object
    #//
    for dataset in fjrFileInfo.dataset:

        primary = createPrimaryDataset(dataset)
        if jobType == "Merge":
            algo = createMergeAlgorithm(dataset)
        else:
            algo = createAlgorithmForInsert(dataset)

        processed = createProcessedDataset(primary, algo, dataset)

        dbsFileInstance = DbsFile(
            Checksum = checksum,
            Adler32 = adler32sum,
            NumberOfEvents = nEvents,
            LogicalFileName = fjrFileInfo['LFN'],
            FileSize = int(fjrFileInfo['Size']),
            Status = "VALID",
            ValidationStatus = 'VALID',
            FileType = fileType,
            Dataset = processed,
            TierList = makeTierList(dataset['DataTier']),
            AlgoList = [algo],
            LumiList = lumiList,
            ParentList = inputLFNs,
            BranchList = fjrFileInfo.branches,
            )

        results.append(dbsFileInstance)
    return results


def createDBSStorageElement(pnn):
    """
    _createDBSStorageElement_

    """
    return DbsStorageElement(Name = pnn)


def createDBSFileBlock(blockName):
    """
    _createDBSFileBlock_

    return a DbsFileBlock object with the block name provided

    NOTE: This method DOES NOT create a new block in DBS

    """
    return DbsFileBlock(Name=blockName)
