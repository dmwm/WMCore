#! /usr/bin/env python
#pylint: disable=W6501
# W6501: Allow logging messages to have string formatting
"""
_DBSInterface_

Holds both the individual functions to insert and list
objects into DBS, and the massive interface that runs the
DBSUploader
"""




import logging
import time
import traceback
import collections

from DBSAPI.dbsApi            import DbsApi
from DBSAPI.dbsException      import DbsException

from WMCore import Lexicon
from WMComponent.DBSUpload.DBSErrors import DBSInterfaceError, formatEx

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
    
    Lexicon.primaryDatasetType(primaryDatasetType)
    
    primary = DbsPrimaryDataset(Name = primaryName,
                                Type = primaryDatasetType)

    if apiRef:
        try:
            apiRef.insertPrimaryDataset(primary)
        except DbsException as ex:
            msg = "Error in DBSInterface.createPrimaryDataset(%s)\n" % primaryName
            msg += formatEx(ex)
            logging.error(msg)
            raise DBSInterfaceError(msg)
    return primary




def createProcessedDataset(algorithm, apiRef, primary, processedName, dataTier,
                           group = "NoGroup", status = "VALID",
                           globalTag = '', parent = None):
    """
    _createProcessedDataset_

    Create a processed dataset
    """
    if parent != None:
        parents = [parent]
    else:
        parents = []

    tierList = dataTier.split("-")

    if not type(algorithm) == list:
        algorithm = [algorithm]

    processedDataset = DbsProcessedDataset(PrimaryDataset = primary,
                                           AlgoList=algorithm,
                                           Name = processedName,
                                           TierList = tierList,
                                           ParentList = parents,
                                           PhysicsGroup = group,
                                           Status = status,
                                           GlobalTag = globalTag )

    if apiRef != None:
        try:
            apiRef.insertProcessedDataset(processedDataset)
        except DbsException as ex:
            msg = "Error in DBSInterface.createProcessedDataset(%s)\n" % processedName
            msg += formatEx(ex)
            logging.error(msg)
            raise DBSInterfaceError(msg)

    logging.info("PrimaryDataset: %s ProcessedDataset: %s DataTierList: %s  requested by PhysicsGroup: %s " \
                 % (primary['Name'], processedName, dataTier, group))

    return processedDataset


# pylint: disable=C0103
def createAlgorithm(apiRef, appName, appVer, appFam,
                    PSetHash = None, PSetContent = None):
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


    # Create PSetHash. dbsApi tries to base64encode the value of PSetContent
    # which blows up if it's None
    if not PSetContent:
        PSetContent = ""
    psetInstance = DbsQueryableParameterSet(Hash = PSetHash,
                                            Content = PSetContent)
    algorithmInstance = DbsAlgorithm(ExecutableName = appName,
                                     ApplicationVersion = appVer,
                                     ApplicationFamily = appFam,
                                     ParameterSetID = psetInstance)

    if apiRef:
        try:
            apiRef.insertAlgorithm(algorithmInstance)
        except DbsException as ex:
            msg = "Error in DBSInterface.createAlgorithm(%s)\n" % appVer
            msg += formatEx(ex)
            logging.error(msg)
            raise DBSInterfaceError(msg)
    return algorithmInstance

# pylint: enable=C0103


def createUncheckedBlock(apiRef, name, datasetPath, seName):
    """
    _createUncheckedBlock_

    Blindly create block with the established name
    Doesn't do any checks for open or existant blocks
    """

    try:
        newBlockName = apiRef.insertBlock(dataset = datasetPath,
                                          block = name,
                                          storage_element_list = [seName])

    except DbsException as ex:
        msg = "Error in DBSInterface.createUncheckedBlock(%s)\n" % name
        msg += formatEx(ex)
        logging.error(msg)
        raise DBSInterfaceError(msg)


    return newBlockName


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

    else:
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
        except DbsException as ex:
            msg = "Error in DBSInterface.createFileBlock(%s)\n" % datasetPath
            msg += formatEx(ex)
            logging.error(msg)
            raise DBSInterfaceError(msg)


    # Add a files field, because we need it
    blockRef['newFiles']      = []
    blockRef['insertedFiles'] = []

    return blockRef


def insertFiles(apiRef, datasetPath, files, block, maxFiles = 10):
    """
    _insertFiles_

    Insert files into a certain block
    files = list of file objects
    """

    if len(files) == 0:
        return

    # First break into small chunks
    listOfFileLists = []
    while len(files) > maxFiles:
        listOfFileLists.append(files[:maxFiles])
        files = files[maxFiles:]
    listOfFileLists.append(files)

    for fileList in listOfFileLists:
        try:
            apiRef.insertFiles(datasetPath, fileList, block)
        except DbsException as ex:
            msg = "Error in DBSInterface.insertFiles(%s)\n" % datasetPath
            msg += "%s\n" % formatEx(ex)
            msg += str(traceback.format_exc())
            raise DBSInterfaceError(msg)

    return



def closeBlock(apiRef, block):
    """
    _closeBlock_

    Close a block
    """
    logging.info("In closeBlock()")

    try:
        apiRef.closeBlock(block)
        logging.info("Back from closeBlock()")
    except DbsException as ex:
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
    except DbsException as ex:
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
    except DbsException as ex:
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
    except DbsException as ex:
        msg = "Error in DBSInterface.listPrimaryDataset()\n"
        msg += "%s\n" % formatEx(ex)
        raise DBSInterfaceError(msg)

    return result


def listBlocks(apiRef, datasetPath = None, blockName = "*", seName = "*"):
    """
    _listBlocks_

    List all the blocks in a primary dataset
    """

    try:
        blocks = apiRef.listBlocks(datasetPath,
                                   block_name = blockName,
                                   storage_element_name = seName)
    except DbsException as ex:
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

        self.version           = self.config.DBSVersion
        self.globalDBSUrl      = None
        self.committedRuns     = collections.deque(maxlen = 1000)
        self.maxFilesToCommit  = self.config.MaxFilesToCommit
        self.doGlobalMigration = getattr(self.config, 'doGlobalMigration', True)

        if getattr(self.config, 'globalDBSUrl', None) != None:
            globalArgs = {'url': self.config.globalDBSUrl,
                          'level': 'ERROR',
                          "user" :'NORMAL',
                          'version': self.config.globalDBSVersion}
            self.globalDBSUrl = self.config.globalDBSUrl
        else:
            self.doGlobalMigration = False

        try:
            self.dbs       = DbsApi(args)
            if self.globalDBSUrl:
                self.globalDBS = DbsApi(globalArgs)
        except DbsException as ex:
            msg = "Error in DBSWriterError with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            logging.error(msg)
            raise DBSInterfaceError(msg)

        return


    def runDBSBuffer(self, algo, dataset, blocks, override = False):
        """
        _runDBSBuffer_

        Run the entire DBSBuffer chain
        """


        # First create the dataset
        processed = self.insertDatasetAlgo(algo = algo, dataset = dataset,
                                           override = override)

        # Next create blocks
        affBlocks = self.createAndInsertBlocks(dataset = dataset,
                                               procDataset = processed,
                                               blocks = blocks)


        return affBlocks


    def createAndInsertBlocks(self, dataset, procDataset, blocks):
        """
        _createBlocks_

        Create all the blocks we use, and insert the
        files into them.
        """

        affectedBlocks = []

        for block in blocks:
            # Create each block one at a time and insert its files

            # First create the block
            createUncheckedBlock(apiRef = self.dbs, name = block['Name'],
                                 datasetPath = dataset['Path'],
                                 seName = block['location'])

            block['Path']               = dataset['Path']
            block['PhEDExNodeList'] = block['location']


            # Now assemble the files
            readyFiles = []
            for f in block['newFiles']:
                for run in f.getRuns():
                    if not run.run in self.committedRuns:
                        insertDBSRunsFromRun(apiRef = self.dbs, dSRun = run)
                        self.committedRuns.append(run.run)
                dbsfile = createDBSFileFromBufferFile(bufferFile = f,
                                                      procDataset = procDataset)
                readyFiles.append(dbsfile)

            block['readyFiles'] = readyFiles
            flag = False
            if block['open'] == 'Pending':
                logging.info("3Found block to close in DBSInterface.createAndInsertBlocks: %s" % (block['Name']))
                flag = True

            finBlock = self.insertFilesAndCloseBlocks(block = block, close = flag)
            affectedBlocks.append(finBlock)

        affBlocks = self.migrateClosedBlocks(blocks = affectedBlocks)


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
                                  PSetHash = algo['PSetHash'],
                                  PSetContent = algo['PSetContent'])

        if dataset['PrimaryDataset'].lower() == 'bogus':
            # Do not commit bogus datasets!
            return None

        dbsRef = self.dbs
        if dataset.get('DASInDBS', None):
            # Then this whole thing is already in DBS
            dbsRef = None

        primary = createPrimaryDataset(primaryName = dataset['PrimaryDataset'], 
                                       primaryDatasetType = self.config.primaryDatasetType,
                                       apiRef = dbsRef)

        processed = createProcessedDataset(apiRef = dbsRef,
                                           algorithm = dbsAlgo,
                                           primary = primary,
                                           processedName = dataset['ProcessedDataset'],
                                           dataTier = dataset['DataTier'],
                                           status = dataset['status'],
                                           globalTag = dataset['globalTag'],
                                           parent = dataset['parent'])


        return processed





    def insertFilesAndCloseBlocks(self, block, close = False):
        """
        _insertFilesAndCloseBlocks_

        Insert files into blocks and close them.
        This does all the actual work of closing a block, first
        inserting the files, then actually closing if you have
        toggled the 'close' flag
        """
        # Insert all the files added to the block in this round
        if len(block.get('readyFiles', [])) > 0:
            insertFiles(apiRef = self.dbs, datasetPath = block['Path'],
                        files = block['readyFiles'], block = block,
                        maxFiles = self.maxFilesToCommit)

            # Reset the block files
            block['insertedFiles'].extend(block['readyFiles'])
            block['readyFiles'] = []

        # Close the block if requested
        if close:
            logging.info("Calling close block...")
            closeBlock(apiRef = self.dbs, block = block)
            block['OpenForWriting'] = '0'
            block['open'] = 0

        return block



    def closeAndMigrateBlocksByName(self, blockNames):
        """
        _closeAndMigrateBlocksByName_

        This is basically for the timeout in DBSUploadPoller
        It allows you to close and migrate a block by name only
        It expects a list of names really, although it can
        take just one.
        """

        if type(blockNames) != list:
            blockNames = [blockNames]



        blocksToClose = []
        for name in blockNames:
            blockList = listBlocks(apiRef = self.dbs,
                                   blockName = name)
            if len(blockList) != 1:
                msg = "Error: We can't load blocks with this name\n"
                msg += str(name)
                msg += "\nRetrieved %i blocks" % (len(blockList))
                logging.error(msg)
                raise DBSInterfaceError(msg)
            block = blockList[0]
            block['open'] = 'Pending'
            b2 = self.insertFilesAndCloseBlocks(block = block,
                                                close = True)
            blocksToClose.append(b2)

        if self.doGlobalMigration:
            self.migrateClosedBlocks(blocks = blocksToClose)

        return blocksToClose




    def migrateClosedBlocks(self, blocks):
        """
        _migrateClosedBlocks_

        One at a time, migrate closed blocks
        This checks to see if blocks are closed.
        If they are, it migrates them.
        """
        if not self.doGlobalMigration:
            logging.debug("Skipping migration due to doGlobalMigration tag.")
            return blocks

        if type(blocks) != list:
            blocks = [blocks]


        for block in blocks:
            if block.get('OpenForWriting', 1) != '0':
                # logging.error("Attempt to migrate open block!")
                # Block is not done
                # Ignore this, because we send all blocks here
                continue
            try:
                # Migrate each block
                logging.info("About to migrate block %s" % (block['Name']))
                self.dbs.dbsMigrateBlock(srcURL = self.config.DBSUrl,
                                         dstURL = self.globalDBSUrl,
                                         block_name = block['Name'],
                                         srcVersion = self.version,
                                         dstVersion = self.config.globalDBSVersion)
                block['open'] = 'InGlobalDBS'
            except DbsException as ex:
                msg = "Error in DBSInterface.migrateClosedBlocks()\n"
                msg += "%s\n" % formatEx(ex)
                msg += str(traceback.format_exc())
                raise DBSInterfaceError(msg)

        return blocks




    def getAPIRef(self, globalRef = False):
        """
        Get a DBSAPI ref to either the local or global API

        """

        if globalRef:
            return self.globalDBS

        return self.dbs
