#!/usr/bin/env python
#pylint: disable-msg=W6501
# W6501: Allow logging messages to have string formatting

"""
_DBSUploadPoller_

Single threaded extremely slow version of the DBSUploader

Works in four steps:

1) Load DAS with uploadable files out of the database.  Take
those files that are already in blocks and put them back
into those blocks.  For the rest of the files, create new blocks.

2) Put those blocks into DBSBuffer with their status = Pending if full
of Open if they still have room for more files.  This
assures that we'll have some record if we fail during the DBS upload
part of things.  Set files to belong to those blocks, etc.

3) Insert things into DBS using DBSInterface.  Insert all blocks
into local, and then migrate blocks in Pending to global.  Then
mark the blocks and files as uploaded in DBS.

4) Poll over all Open and Pending blocks.  Close all blocks that
have timed out.  Close all Pending blocks.  Put contents into local
DBS and migrate all to global.  This is the error catching mechanism
that deals with exceptions raised in step 3 in DBS by attempting
to repeat (note that if a block is bad somehow, it will keep
repeating).
"""
import os
import sys
import time
import logging
import threading
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool        import ProcessPool

from WMCore.WMFactory     import WMFactory
from WMCore.DAOFactory    import DAOFactory
from WMCore.WMException   import WMException
from WMCore.Services.UUID import makeUUID

from WMComponent.DBSUpload.DBSInterface import DBSInterface
from WMComponent.DBSUpload.DBSErrors    import DBSInterfaceError


def createDatasetFromInfo(info):
    """
    Create a dataset object from basic information

    """
    dataset = {'ID':               info.get('Dataset'),
               'Path':             info.get('Path'),
               'ProcessedDataset': info.get('ProcessedDataset'),
               'PrimaryDataset':   info.get('PrimaryDataset'),
               'DataTier':         info.get('DataTier'),
               'Algo':             info.get('Algo'),
               'AlgoInDBS':        info.get('AlgoInDBS', None),
               'DASInDBS':         info.get('DASInDBS', None)
               }

    return dataset

def createAlgoFromInfo(info):
    """
    Create an Algo object from basic information

    """
    
    algo = {'ApplicationName':    info.get('ApplicationName'),
            'ApplicationFamily':  info.get('ApplicationFamily'),
            'ApplicationVersion': info.get('ApplicationVersion'),
            'PSetHash':           info.get('PSetHash'),
            'PSetContent':        info.get('PSetContent'),
            'InDBS':              info.get('AlgoInDBS', None)
            }

    return algo

def sortByDAS(incoming):
    """
    Sort a list of fileInfo into a dictionary keyed by dataset-algo
    assoc IDs

    """
    output = {}

    for entry in incoming:
        dasID = entry['DAS_ID']
        if not dasID in output.keys():
            output[dasID] = []
        output[dasID].append(entry)


    return output


def createConfigForJSON(config):
    """
    Turn a config object into a dictionary of dictionaries

    """

    final = {}
    for sectionName in config.listSections_():
        section = getattr(config, sectionName)
        if hasattr(section, 'dictionary_'):
            # Create a dictionary key for it
            final[sectionName] = createDictionaryFromConfig(section)


    return final


def sortListByKey(input, key):
    """
    Return list of dictionaries as a
    dictionary of lists of dictionaries
    keyed by one original key

    """
    final = {}

    for entry in input:
        value = entry.get(key)
        if type(value) == set:
            value = value.pop()
        if not value in final.keys():
            final[value] = []
        final[value].append(entry)

    return final

def createBlock(datasetPath, location):
    """
    Create a new block

    """

    block = {}
    block['ID']            = None
    block['Name']          = '%s#%s' % (datasetPath, makeUUID())
    block['NumberOfFiles'] = 0
    block['CreationDate']  = time.time()
    block['BlockSize']     = 0
    block['newFiles']      = []
    block['insertedFiles'] = []
    block['open']          = 1
    block['location']      = location


    return block


def preassignBlocks(files, blocks):
    """
    _preassignBlocks_
    
    Take a dictionary of predefined blocks and a dictionary
    of predefined files sorted by location, and assign files that were
    already in blocks to those blocks again

    Return result by memory
    """
    for location in files.keys():
        if not location in blocks.keys():
            # No blocks for this location
            continue
        localBlocks = blocks[location]
        filesToRemove = []
        for file in files[location]:
            if file.get('blockID', None) == None:
                continue
            for block in localBlocks:
                if block['ID'] == file['blockID']:
                    block['insertedFiles'].append(file)
                    filesToRemove.append(file)
        # Remove files that are already in blocks
        for file in filesToRemove:
            files[location].remove(file)

    return blocks, files



class DBSUploadPollerException(WMException):
    pass



class DBSUploadPoller(BaseWorkerThread):
    """
    Handles poll-based DBSUpload

    """


    def __init__(self, config, dbsconfig = None):
        """
        Initialise class members
        """

        myThread = threading.currentThread()
        
        BaseWorkerThread.__init__(self)
        self.config     = config

        # This is slightly dangerous, but DBSUpload depends
        # on DBSInterface anyway
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        factory = WMFactory("dbsUpload",
                            "WMComponent.DBSUpload.Database.Interface")
        self.uploadToDBS = factory.loadObject("UploadToDBS")

        addFactory = WMFactory("dbsBuffer",
                               "WMComponent.DBSBuffer.Database.Interface")
        self.addToBuffer = addFactory.loadObject("AddToBuffer")

        # Set DBSInterface
        self.dbsInterface = DBSInterface(config = config)

        # Set DAOs
        self.setBlock  = self.bufferFactory(classname = "DBSBufferFiles.SetBlock")

        # Set config parameters
        self.maxBlockFiles    = self.config.DBSInterface.DBSBlockMaxFiles
        self.maxBlockTime     = self.config.DBSInterface.DBSBlockMaxTime
        self.maxBlockSize     = self.config.DBSInterface.DBSBlockMaxSize
        logging.debug("Initializing with maxFiles %i, maxTime %i, maxSize %i" % (self.maxBlockFiles,
                                                                                 self.maxBlockTime,
                                                                                 self.maxBlockSize))

        if dbsconfig == None:
            self.dbsconfig = config

        return
    
    def algorithm(self, parameters = None):
        """
        Runs over all available DBSBuffer filesets/algos
        Commits them using DBSInterface
        Then checks blocks for timeout
        """
        logging.debug("Running subscription / fileset matching algorithm")
        try:
            self.uploadDatasets()
            self.closeBlocks()
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled exception in DBSUploadPoller\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise DBSUploadPollerException(msg)
        return

    def terminate(self, params):
        """
        Do one more pass, then terminate

        """
        logging.debug("Terminating. Doing one more pass before we die")
        self.algorithm(params)
        return


    def uploadDatasets(self):
        """
        _uploadDatasets_

        Find new datasets and upload them.

        First, find DAS with files to upload
        Then, sort those files into blocks
        Then, upload those blocks to DBSBuffer
        Then, upload those blocks to DBS using DBSInterface
        Then, close blocks in DBSBuffer
        """
        myThread = threading.currentThread()

        # Grab all the Dataset-Algo combindations
        dasList = self.uploadToDBS.findUploadableDAS()
        logging.debug("Recovered %i DAS to upload" % len(dasList))

        for dasInfo in dasList:
            # Go one DAS at a time
            dasID = dasInfo['DAS_ID']
            logging.info("Processing DAS %i" % dasID)

            # Initial values
            readyBlocks = []
            fileLFNs    = []

            # Get the dataset-algo information
            algo    = createAlgoFromInfo(info = dasInfo)
            dataset = createDatasetFromInfo(info = dasInfo)

            # Get the files and the blocks for the DAS
            files  = self.uploadToDBS.findUploadableFilesByDAS(das = dasID)
            blocks = self.uploadToDBS.loadBlocksByDAS(das = dasID)
            logging.debug("Retrieved %i files and %i blocks from DB." % (len(files), len(blocks)))

            if len(files) < 1:
                logging.error("Active DAS had no files!")
                return

            # Sort the files and blocks by location
            locationDict = sortListByKey(input = files, key = 'locations')
            blockDict    = sortListByKey(input = blocks, key = 'location')
            logging.debug("Active DAS file locations: %s" % locationDict.keys())
            logging.debug("Active Block file locations: %s" % blockDict.keys())

            try:
                # Sort files that are already in blocks
                # back into those blocks
                # pass by reference
                blockDict, locationDict = preassignBlocks(files = locationDict, blocks = blockDict)
                
                # Now go over all the files
                for location in locationDict:
                    # Split files into blocks
                    locFiles  = locationDict.get(location, [])
                    locBlocks = blockDict.get(location, [])
                    locBlocks = self.splitFilesIntoBlocks(files = locFiles,
                                                          blocks = locBlocks,
                                                          dataset = dataset,
                                                          location = location)
                    readyBlocks.extend(locBlocks)
            except WMException:
                raise
            except Exception, ex:
                msg =  "Unhandled exception while sorting files into blocks for DAS %i\n" % dasID
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                logging.debug("BlockDictionary: %s" % blockDict)
                logging.debug("FileDictionary: %s" % locationDict)
                raise DBSUploadPollerException(msg)


            # At this point, all blocks should be in readyBlocks
            # STEP TWO: Commit blocks to DBSBuffer
            fileLFNs = self.createBlocksInDBSBuffer(readyBlocks = readyBlocks)


            # STEP THREE: Insert Into DBS and Update DBSBuffer
            try:
                # Damn it Anzar: Why does DBS print stuff out?
                originalOut = sys.stdout
                originalErr = sys.stderr

                sys.stdout = open(os.devnull, 'w')
                sys.stderr = open(os.devnull, 'w')

                logging.info("About to send %i blocks to DBSInterface" % (len(readyBlocks)))
                for block in readyBlocks:
                    logging.info("About to send to DBS block %s with status %s" %(block['Name'], block.get('open', None)))
                
                
                affBlocks = self.dbsInterface.runDBSBuffer(algo = algo,
                                                           dataset = dataset,
                                                           blocks = readyBlocks)

                sys.stdout = originalOut
                sys.stderr = originalErr

                if getattr(self.config.DBSUpload, 'abortStepThree', False):
                    # Blow the stack for testing purposes
                    raise DBSUploadPollerException('None')

                myThread.transaction.begin()
                
                if not algo['InDBS']:
                    # List the algo as uploaded
                    self.addToBuffer.updateAlgo(algo, 1)
                if not dataset['DASInDBS']:
                    # List the datasetAlgo as uploaded
                    self.uploadToDBS.setDatasetAlgo(datasetAlgoInfo =  {'DAS_ID': dasID},
                                                    inDBS = 1)
                for block in affBlocks:
                    logging.info("About to set in DBSBuffer with status %s block %s" %(str(block['open']),
                                                                                       block['Name']))
                    if block['open'] == 0:
                        self.uploadToDBS.setBlockStatus(block = block['Name'],
                                                        locations = [block['location']],
                                                        openStatus = block['open'])

                # Update file status
                for block in readyBlocks:
                    self.uploadToDBS.closeBlockFiles(blockname = block['Name'])

                logging.info("Commmitting DBSBuffer transaction at end of DBS injection.")
                myThread.transaction.commit()

            except WMException:
                if getattr(myThread, 'transaction', None) != None: 
                    myThread.transaction.rollback()
                raise
            except Exception, ex:
                msg =  'Error in committing files to DBS\n'
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                if getattr(myThread, 'transaction', None) != None: 
                    myThread.transaction.rollback()
                raise DBSUploadPollerException(msg)


        return


    def closeBlocks(self):
        """
        _closeBlocks_

        Check blocks for timeout and close them if
        they exceed it.

        Load blocks in Pending state and close and migrate
        them - they are the result of an exception raised
        in uploadDatasets()
        """
        
        myThread = threading.currentThread()

        try:
            myThread.transaction.begin()

            # Now it's time to do a block check and look for
            # open blocks that we should timeout
            blocks = self.uploadToDBS.findOpenBlocks()

            logging.info("Found %i open or pending blocks" % (len(blocks)))

            blocksToClose = []
            doneBlocks    = []

            for buffBlock in blocks:
                if time.time() - buffBlock['create_time'] > self.maxBlockTime:
                    # Then we have to load a block
                    blocksToClose.append(buffBlock['blockname'])
                    logging.info("Going to close and migrate block due to timeout: %s" % (buffBlock['blockname']))
                elif buffBlock.get('status', None) == 'Pending':
                    logging.info("Found block to migrate due to Pending status: %s" % (buffBlock['blockname']))
                    blocksToClose.append(buffBlock['blockname'])

            # Actually finish the blocks
            if len(blocksToClose) > 0:
                # Damn it Anzar: Why does DBS print stuff out?
                originalOut = sys.stdout
                originalErr = sys.stderr
                sys.stdout = open(os.devnull, 'w')
                sys.stderr = open(os.devnull, 'w')
                
                doneBlocks = self.dbsInterface.closeAndMigrateBlocksByName(blockNames = blocksToClose)

                sys.stdout = originalOut
                sys.stderr = originalErr

            for block in doneBlocks:
                # Now close 'em in DBSBuffer
                logging.info("About to close block in DBSBuffer due to timeout: %s" % (block['Name']))
                self.uploadToDBS.setBlockStatus(block['Name'], locations = None,
                                                openStatus = 0)
                self.uploadToDBS.closeBlockFiles(blockname = block['Name'])

            logging.debug("Committing transaction at end of closeBlocks().")
            myThread.transaction.commit()

        except DBSInterfaceError, ex:
            msg =  'Error in DBSInterface while closing timed-out blocks\n'
            msg += str(ex)
            logging.error(msg)
            if getattr(myThread, 'transaction', None) != None: 
                myThread.transaction.rollback()
            raise 
        except Exception, ex:
            msg =  'Error in closing blocks in DBS\n'
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            if getattr(myThread, 'transaction', None) != None: 
                myThread.transaction.rollback()
            raise DBSUploadPollerException(msg)

        return



    def splitFilesIntoBlocks(self, files, blocks, dataset, location):
        """
        Break the files into blocks based on config params

        Create a new block when necessary.
        """

        blocksToHandle = []

        if len(blocks) > 1:
            # Well, then we have a bit of a problem
            # Decide what to do about this later
            logging.error("More then one open block for this DAS")

        if len(blocks) == 0:
            currentBlock = createBlock(datasetPath = dataset['Path'],
                                       location = location)
        else:
            currentBlock = blocks[0]

        for newFile in files:
            # Check to see if blocks are full
            if not self.isBlockOpen(block = currentBlock):
                # Add old block to return list
                # Create a new block
                currentBlock['open'] = 'Pending'
                blocksToHandle.append(currentBlock)
                currentBlock = createBlock(datasetPath = dataset['Path'],
                                           location = location)

            # Now process the file
            currentBlock['newFiles'].append(newFile)
            currentBlock['BlockSize']     += newFile['size']
            currentBlock['NumberOfFiles'] += 1

        if currentBlock['NumberOfFiles'] > 0:
            blocksToHandle.append(currentBlock)

                
        return blocksToHandle



    def isBlockOpen(self, block):
        """
        _isBlockOpen_

        Tells you if the block should be closed
        """

        if time.time() - int(block.get('CreationDate', 0)) >= self.maxBlockTime:
            # We've timed out on this block
            return False
        if block['NumberOfFiles'] >= self.maxBlockFiles:
            # We've got too many files
            return False
        if float(block.get('BlockSize')) >= self.maxBlockSize:
            return False

        return True



    def createBlocksInDBSBuffer(self, readyBlocks):
        """
        _createBlocksInDBSBuffer_

        Create the blocks in the local database in
        their initial states.
        """
        myThread = threading.currentThread()
        fileLFNs = []
        try:
            # Do this in its own transaction
            myThread.transaction.begin()
            
            for block in readyBlocks:
                # First insert each block
                logging.info("Prepping block %s for DBS with status %s" % (block['Name'], block['open']))
                self.uploadToDBS.setBlockStatus(block = block['Name'],
                                                locations = [block['location']],
                                                openStatus = block['open'],
                                                time = int(block['CreationDate']))
                
                # Then insert files from each block
                blockFileList = []
                for f in block.get('newFiles', []):
                    blockFileList.append(f['lfn'])

                if len(blockFileList) > 0:
                    self.setBlock.execute(lfn = blockFileList,
                                          blockName = block['Name'],
                                          conn = myThread.transaction.conn,
                                          transaction = myThread.transaction)
                    fileLFNs.extend(blockFileList)

            if getattr(self.config.DBSUpload, 'abortStepTwo', False):
                # Blow the stack for testing purposes
                raise DBSUploadPollerException('None')

            logging.debug("Committing transaction at the end of DBSBuffer insertion.")
            myThread.transaction.commit()

        except WMException, ex:
            if getattr(myThread, 'transaction', None) != None: 
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            msg =  'Error in committing blocks to DBSBuffer\n'
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            if getattr(myThread, 'transaction', None) != None: 
                myThread.transaction.rollback()
            raise DBSUploadPollerException(msg)

        return fileLFNs
