#! /usr/bin/env python
#pylint: disable-msg=W6501
# W6501: Allow logging messages to have string formatting
"""
_DBSInterface_

Holds both the individual functions to insert and list
objects into DBS, and the massive interface that runs the
DBSUploader
"""

__revision__ = "$Id: DBSInterface.py,v 1.1 2010/05/19 20:46:18 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import time
#import threading

from DBSAPI.dbsApi            import DbsApi
from DBSAPI.dbsException      import DbsException
#from DBSAPI.dbsStorageElement import *
#from DBSAPI.dbsApiException   import *


# Things still in Services
from WMCore.Services.DBS.DBSErrors import DBSInterfaceError, formatEx


# For creating algorithms
from DBSAPI.dbsAlgorithm             import DbsAlgorithm
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet

# For creating datasets
from DBSAPI.dbsPrimaryDataset        import DbsPrimaryDataset
from DBSAPI.dbsProcessedDataset      import DbsProcessedDataset

# For blocks and files
from DBSAPI.dbsFile                  import DbsFile
#from DBSAPI.dbsFileBlock             import DbsFileBlock
from DBSAPI.dbsRun                   import DbsRun
from DBSAPI.dbsLumiSection           import DbsLumiSection




# Useful insertion functions
def createPrimaryDataset(primaryName, primaryDatasetType = 'mc', apiRef = None):
    """
    _createPrimaryDataset_
    
    
    """
    logging.debug("Inserting PrimaryDataset %s with Type %s" \
                  % (primaryName, primaryDatasetType))
    primary = DbsPrimaryDataset(Name = primaryName,
                                Type = primaryDatasetType)
    
    if apiRef:
        apiRef.insertPrimaryDataset(primary)
    return primary




def createProcessedDataset(algorithm, apiRef, primary, processedName, dataTier,
                           group = "NoGroup", status = "VALID",
                           globalTag = ''):
    """
    _createProcessedDataset_

    Create a processed dataset
    """

    parents = []

    if not type(dataTier) == list:
        dataTier = [dataTier]

    if not type(algorithm) == list:
        algorithm = [algorithm]

    processedDataset = DbsProcessedDataset(PrimaryDataset = primary,
                                           AlgoList=algorithm,
                                           Name = processedName,
                                           TierList = dataTier,
                                           ParentList = parents,
                                           PhysicsGroup = group,
                                           Status = status,
                                           GlobalTag = globalTag )

    if apiRef != None:
        apiRef.insertProcessedDataset(processedDataset)

    logging.info("PrimaryDataset: %s ProcessedDataset: %s DataTierList: %s  requested by PhysicsGroup: %s " \
                 % (primary['Name'], processedName, dataTier, group))

    return processedDataset


# pylint: disable-msg=C0103
def createAlgorithm(apiRef, appName, appVer, appFam,
                    PSetHash = None):
    """
    _createAlgorithm_

    Create a new DBS Algorithm, explicitly passing in the arguments.
    We don't use configs.
    The insert tag tell you whether or not to actually write this to DBS
    """


    # Take care of PSetHash
    if not PSetHash:
        PSetHash = "NO_PSET_HASH2"
    elif PSetHash.find(";"):
        # no need for fake hash in new schema
        PSetHash = PSetHash.split(";")[0]
        PSetHash = PSetHash.replace("hash=", "")


    # Create PSetHash
    psetInstance = DbsQueryableParameterSet(Hash = PSetHash)
    algorithmInstance = DbsAlgorithm(ExecutableName = appName,
                                     ApplicationVersion = appVer,
                                     ApplicationFamily = appFam,
                                     ParameterSetID = psetInstance)

    if apiRef:
        apiRef.insertAlgorithm(algorithmInstance)
    return algorithmInstance

# pylint: enable-msg=C0103



def createFileBlock(apiRef, datasetPath, seName):
    """
    _createFileBlock_

    Create a new file block in the processed dataset
    """

    # First check for open blocks
    allBlocks = listBlocks(apiRef = apiRef,
                           datasetPath = datasetPath,
                           seName = seName)

    openBlocks = [b for b in allBlocks if str(b['OpenForWriting']) == "1"]

    # If you had open blocks, use last open
    blockRef = None
    if len(openBlocks) > 1:
        msg = "Too many open blocks for dataset:\n"
        msg += "SE: %s\n" % seName
        msg += "Dataset: %s\n" % datasetPath
        msg += "Using last open block\n"
        logging.error(msg)
        blockRef = openBlocks[-1]
    elif len(openBlocks) == 1:
        logging.warning("Attempted to open block while block already open")
        blockRef = openBlocks[0]



    try:
        newBlockName = apiRef.insertBlock(datasetPath, None ,
                                          storage_element_list = [seName])
        blocks = listBlocks(apiRef = apiRef, datasetPath = datasetPath,
                            blockName = newBlockName)
        if len(blocks) > 1:
            # We have created a duplicate of a primary key according to Anzar
            msg = "Created duplicate blocks with duplicate names.  Help!"
            msg += newBlockName
            raise DBSInterfaceError(msg)
        blockRef = blocks[0]
    except DbsException, ex:
        msg = "Error in DBSInterface.createFileBlock(%s)\n" % datasetPath
        msg += formatEx(ex)
        logging.error(msg)
        raise DBSInterfaceError(msg)


    # Add a files field, because we need it
    blockRef['newFiles'] = []

    return blockRef


def insertFiles(apiRef, datasetPath, files, block, maxFiles = 10):
    """
    _insertFiles_

    Insert files into a certain block
    files = list of file objects
    """

    # First break into small chunks
    listOfFileLists = []
    while len(files) > maxFiles:
        listOfFileLists.append(files[:maxFiles])
        files = files[maxFiles:]
    listOfFileLists.append(files)

    for fileList in listOfFileLists:
        try:
            apiRef.insertFiles(datasetPath, fileList, block)
        except DbsException, ex:
            msg = "Error in DBSInterface.insertFiles(%s)\n" % datasetPath
            msg += "%s\n" % formatEx(ex)
            raise DBSInterfaceError(msg)

    return



def closeBlock(apiRef, block):
    """
    _closeBlock_

    Close a block 
    """

    try:
        apiRef.closeBlock(block)
    except DbsException, ex:
        msg = "Error in DBSInterface.closeBlock(%s)\n" % block
        msg += "%s\n" % formatEx(ex)
        raise DBSInterfaceError(msg)

    return













# And now the read-out methods

def listPrimaryDatasets(apiRef, match = "*"):
    """
    List some Primary Datasets

    """
    try:
        result = apiRef.listPrimaryDatasets(match)
    except DbsException, ex:
        msg = "Error in DBSInterface.listPrimaryDataset(%s)\n" % match
        msg += "%s\n" % formatEx(ex)
        raise DBSInterfaceError(msg)

    result = [ x['Name'] for x in result ]
    return result


def listProcessedDatasets(apiRef, primary, dataTier = "*"):
    """
    _listProcessedDatasets_
    
    return a list of Processed datasets for the primary and optional
    data tier value
    
    """
    
    try:
        result = apiRef.listProcessedDatasets(primary, dataTier)
    except DbsException, ex:
        msg = "Error in DBSInterface.listProcessedDatasets(%s)\n" % primary
        msg += "%s\n" % formatEx(ex)
        raise DBSInterfaceError(msg)

    result = [ x['Name'] for x in result ]
    return result


def listDatasetFiles(apiRef, datasetPath):
    """
    _listDatasetFiles_
    
    Get list of files for dataset
    
    """
    return [ x['LogicalFileName'] for x in apiRef.listFiles(datasetPath)]


def listAlgorithms(apiRef, patternVer="*", patternFam="*",
                   patternExe="*", patternPS="*"):
    """
    List some Primary Datasets

    """
    try:
        result = apiRef.listAlgorithms(patternVer = patternVer,
                                       patternFam = patternFam,
                                       patternExe = patternExe,
                                       patternPS  = patternPS)
    except DbsException, ex:
        msg = "Error in DBSInterface.listPrimaryDataset()\n"
        msg += "%s\n" % formatEx(ex)
        raise DBSInterfaceError(msg)

    return result


def listBlocks(apiRef, datasetPath, blockName = "*", seName = "*"):
    """
    _listBlocks_

    List all the blocks in a primary dataset
    """

    try:
        blocks = apiRef.listBlocks(datasetPath,
                                   block_name = blockName,
                                   storage_element_name = seName)
    except DbsException, ex:
        msg = "Error in DBSInterface.listBlocks(%s)\n" % datasetPath
        msg += "%s\n" % formatEx(ex)
        raise DBSInterfaceError(msg)

    return blocks



# Transformation tools

def createDBSFileFromBufferFile(bufferFile, procDataset):
    """
    Take a DBSBufferFile and turn it into a DBSFile object
    
    """
    
    lumiList = []
    
    
    for run in bufferFile.getRuns():
        for l in run:
            lumi = DbsLumiSection(LumiSectionNumber = long(l),
                                  StartEventNumber = 0,
                                  EndEventNumber = 0,
                                  LumiStartTime = 0,
                                  LumiEndTime = 0,
                                  RunNumber = long(run.run) )
            lumiList.append(lumi)

    dbsfile = DbsFile(NumberOfEvents = bufferFile['events'],
                      LogicalFileName = bufferFile['lfn'],
                      FileSize = int(bufferFile['size']),
                      Status = "VALID",
                      ValidationStatus = 'VALID',
                      FileType = 'EDM',
                      Dataset = procDataset,
                      TierList = procDataset['TierList'],
                      AlgoList = procDataset['AlgoList'],
                      LumiList = lumiList,
                      ParentList = bufferFile.getParentLFNs() )


    for entry in bufferFile['checksums'].keys():
        # This should be a dictionary with a cktype key and cksum value
        if entry.lower() == 'cksum':
            dbsfile['Checksum'] = str(bufferFile['checksums'][entry])
        elif entry.lower() == 'adler32':
            dbsfile['Adler32'] = str(bufferFile['checksums'][entry])
        elif entry.lower() == 'md5':
            dbsfile['Md5'] = str(bufferFile['checksums'][entry])


    return dbsfile


def insertDBSRunsFromRun(apiRef, dSRun):
    """
    Take a DataStructs run and create a DBSRun out of it
    
    """
    
    run = DbsRun(RunNumber = long(dSRun.run),
                 NumberOfEvents = 0,
                 NumberOfLumiSections = 0,
                 TotalLuminosity = 0,
                 StoreNumber = 0,
                 StartOfRun = 0,
                 EndOfRun = 0)
    
    if apiRef:
        apiRef.insertRun(run)
        
    return run















class DBSInterface:
    """
    _DBSInterface_


    So once upon a time there was a guy named Anzar, and he wrote
    a thing called DBSWriter for a project called ProdAgent,
    which unfortunately sucked because, let's be honest, everything
    in ProdAgent was thrown together just to make stuff work.
    So eventually WMAgent came along and I had to rewrite the whole
    thing.
    """



    def __init__(self, config):
        """
        Use Configuration object from config

        """
        # Config should have DBSInterface element
        self.config = config.DBSInterface

        args = {'url': self.config.DBSUrl,
                'level': 'ERROR',
                "user" :'NORMAL',
                'version': self.config.DBSVersion}

        self.version          = self.config.DBSVersion
        self.globalDBSUrl     = None
        self.committedRuns    = []
        self.maxBlockFiles    = self.config.DBSBlockMaxFiles
        self.maxBlockTime     = self.config.DBSBlockMaxTime
        self.maxBlockSize     = self.config.DBSBlockMaxSize
        self.maxFilesToCommit = self.config.MaxFilesToCommit

        if hasattr(self.config, 'globalDBSUrl'):
            globalArgs = {'url': self.config.globalDBSUrl,
                          'level': 'ERROR',
                          "user" :'NORMAL',
                          'version': self.config.globalDBSVersion}
            self.globalDBSUrl = self.config.globalDBSUrl

        try:
            self.dbs       = DbsApi(args)
            if self.globalDBSUrl:
                self.globalDBS = DbsApi(globalArgs)
        except DbsException, ex:
            msg = "Error in DBSWriterError with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            logging.error(msg)
            raise DBSInterfaceError(msg)

        return


    def runDBSBuffer(self, algo, dataset, files, override = False):
        """
        _runDBSBuffer_

        Run the entire DBSBuffer chain
        """

        processed = self.insertDatasetAlgo(algo = algo, dataset = dataset,
                                           override = override)
        affBlocks = self.insertDBSBufferFiles(files = files,
                                              dataset = processed)

        return affBlocks
            

    def insertDatasetAlgo(self, algo, dataset, override = False):
        """
        _insertDatasetAlgo_

        Insert a dataset-algo combination
        The override re-inserts it if DBSBuffer thinks it's already there
        """

        dbsRef = None
        if override or not algo['InDBS']:
            # Then put the algo in DBS by referencing local DBS
            dbsRef = self.dbs

        # Create a DBS Algo
        dbsAlgo = createAlgorithm(apiRef = dbsRef,
                                  appName = algo['ApplicationName'],
                                  appVer = algo['ApplicationVersion'],
                                  appFam = algo['ApplicationFamily'],
                                  PSetHash = algo['PSetHash'])

        if dataset['PrimaryDataset'].lower() == 'bogus':
            # Do not commit bogus datasets!
            return None

        primary = createPrimaryDataset(apiRef = self.dbs,
                                       primaryName = dataset['PrimaryDataset'])

        processed = createProcessedDataset(apiRef = self.dbs,
                                           algorithm = dbsAlgo, 
                                           primary = primary,
                                           processedName = dataset['ProcessedDataset'],
                                           dataTier = dataset['DataTier'])


        return processed

        



    def insertDBSBufferFiles(self, files, dataset):
        """
        _insertDBSBufferFiles_

        Insert some files for a given datasetAlgo into blocks
        dataset is a DBS processed dataset object
        """

        # This will be a list of blocks to migrate
        affectedBlocks = []

        # This will be a dictionary of all the files, keyed by SE
        fileLocations = {}

        # Insert the runs for each file if not available
        # Split the files into locations
        for f in files:
            for run in f.getRuns():
                if not run.run in self.committedRuns:
                    insertDBSRunsFromRun(apiRef = self.dbs, dSRun = run)
                    self.committedRuns.append(run.run)
            if not len(f['locations']) == 1:
                logging.error("File with lfn %s does not have one SE" \
                              % (file['lfn']))
                continue
            fileLoc = f.getLocations()[0]
            if not fileLoc in fileLocations.keys():
                fileLocations[fileLoc] = []
            dbsfile = createDBSFileFromBufferFile(bufferFile = f,
                                                  procDataset = dataset)
            fileLocations[fileLoc].append(dbsfile)

        datasetPath = files[0]['datasetPath']

        for location in fileLocations.keys():
            # This actually inserts the files
            locBlocks = self.insertIntoBlocks(files = fileLocations[location],
                                              location = location,
                                              datasetPath = datasetPath)
            affectedBlocks.extend(locBlocks)


        if self.globalDBSUrl:
            self.migrateClosedBlocks(blocks = affectedBlocks)


        return affectedBlocks


    def insertIntoBlocks(self, files, location, datasetPath):
        """
        _insertIntoBlocks_

        Split files into blocks based on block max parameters
        When a block is full, close it and open a new one
        Return files in block[files]
        """

        if len(files) == 0:
            return

        affectedBlocks = []
        #datasetPath    = files[0]['datasetPath']

        # First, get a block
        # NOTE: The block as a 'newFiles' field
        # This only contains files not yet committed
        block = createFileBlock(apiRef = self.dbs,
                                datasetPath = datasetPath,
                                seName = location)

        blockFiles, blockSize, blockTime = self.blockLimits(block)

        
        
        # Run over all the files, committing the blocks when they
        # Exceed max values
        # NOTE: Logical flaw here, blocks cannot time out inside this loop
        # Don't think this is a problem.
        for newFile in files:
            # If the block is full, get a new one
            if blockFiles < 1 or blockSize < 1 or blockTime < 0:
                oldBlock = self.insertFilesAndCloseBlocks(block = block,
                                                          close = True)
                affectedBlocks.append(oldBlock)
                block = createFileBlock(apiRef = self.dbs,
                                        datasetPath = datasetPath,
                                        seName = location)
                blockFiles = self.maxBlockFiles
                blockTime  = self.maxBlockTime
                blockSize  = self.maxBlockSize
            block['newFiles'].append(newFile)
            blockFiles -= 1
            blockSize  -= newFile['FileSize']

        # When done, commit the block if it has files
        # Don't close it
        self.insertFilesAndCloseBlocks(block = block, close = False)
        affectedBlocks.append(block)

        return affectedBlocks


    def blockLimits(self, block):
        """
        _blockLimits_

        Return the number of files, the remaining size, and the remaining time
        """

        blockRunTime = int(time.time()) - int(block['CreationDate'])

        blockTime = self.maxBlockTime - blockRunTime
        blockFiles = self.maxBlockFiles - len(block['newFiles']) \
                     - int(block['NumberOfFiles'])
        blockSize = self.maxBlockSize - float(block.get('BlockSize', 0))

        return blockFiles, blockSize, blockTime


    def insertFilesAndCloseBlocks(self, block, close = False):
        """
        _insertFilesAndCloseBlocks_

        Insert files into blocks and close them.
        This does all the actual work of closing a block, first
        inserting the files, then actually closing if you have
        toggled the 'close' flag
        """

        # Insert all the files added to the block in this round
        insertFiles(apiRef = self.dbs, datasetPath = block['Path'],
                    files = block['newFiles'], block = block,
                    maxFiles = self.maxFilesToCommit)

        # Reset the block files
        block['NumberOfFiles'] += len(block['newFiles'])
        block['newFiles'] = []

        # Close the block if requested
        if close:
            closeBlock(apiRef = self.dbs, block = block)
            block['OpenForWriting'] = '0'

        return block
                
        


    def migrateClosedBlocks(self, blocks):
        """
        _migrateClosedBlocks_

        One at a time, migrate closed blocks
        This checks to see if blocks are closed.
        If they are, it migrates them.
        """


        for block in blocks:
            if block['OpenForWriting'] != '0':
                # Block is not done
                continue
            try:
                # Migrate each block
                self.dbs.dbsMigrateBlock(srcURL = self.config.DBSUrl, 
                                         dstURL = self.globalDBSUrl,
                                         block_name = block['Name'],
                                         srcVersion = self.version,
                                         dstVersion = self.config.globalDBSVersion)
            except DbsException, ex:
                msg = "Error in DBSInterface.migrateClosedBlocks()\n"
                msg += "%s\n" % formatEx(ex)
                raise DBSInterfaceError(msg)

        return
        



    def getAPIRef(self, globalRef = False):
        """
        Get a DBSAPI ref to either the local or global API

        """

        if globalRef:
            return self.globalDBS

        return self.dbs


