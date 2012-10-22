#!/usr/bin/env python
#pylint: disable-msg=W6501
# W6501: Allow logging messages to have string formatting

"""
_DBSUploadPoller_

Single threaded extremely slow version of the DBSUploader

Works in four steps:

1) Load DAS out of the database.  For now, loads all DAS
with files in the NOTUPLOADED status.  For each DAS, loads
all files that are NOTUPLOADED and have parents in GLOBAL

2) Splits new files into blocks.  Marks the files as status
READY.  Puts the blocks into DBSBuffer, and assigns the files
to the blocks.  If blocks are full, marks blocks as Pending.

3) Load all blocks in state Open or Pending out of the database.
Load all READY status files from the database.  Assign files
to blocks for upload to DBS.  Also check Open blocks for timeout.
If they are timed out set them to Pending.

4)  Send blocks to DBS.  Open blocks are loaded only into Local
DBS instance.  Pending blocks have any READY files put into local,
and are then migrated to global.  Once this process is done
mark the files as status GLOBAL or LOCAL in DBSBuffer (depending
on whether the block was migrated to global or not), and mark
the block accordingly.

Notes:

You can avoid going to global altogether by setting:
self.config.DBSInterface.DoGlobalMigration to False

This version takes advantage of the separate dbsbuffer_file.in_phedex column
"""
import os
import sys
import time
import logging
import threading
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool        import ProcessPool
from WMCore.Cache.WMConfigCache            import ConfigCache
from WMCore.Algorithms.MiscAlgos           import sortListByKey

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
    dataset = {'ID':               info['Dataset'],
               'Path':             info['Path'],
               'ProcessedDataset': info['ProcessedDataset'],
               'PrimaryDataset':   info['PrimaryDataset'],
               'DataTier':         info['DataTier'],
               'Algo':             info['Algo'],
               'AlgoInDBS':        info['AlgoInDBS'],
               'DASInDBS':         info['DASInDBS'],
               'status':           info['ValidStatus'],
               'globalTag':        info['GlobalTag'],
               'parent':           info['Parent']
               }

    if dataset['status'] == None:
        dataset['status'] = 'PRODUCTION'

    if dataset['globalTag'] == None:
        dataset['globalTag'] = ''

    return dataset

def createAlgoFromInfo(info):
    """
    Create an Algo object from basic information

    """

    algo = {'ApplicationName':    info['ApplicationName'],
            'ApplicationFamily':  info['ApplicationFamily'],
            'ApplicationVersion': info['ApplicationVersion'],
            'PSetHash':           info['PSetHash'],
            'PSetContent':        None,
            'InDBS':              info['AlgoInDBS']
            }

    configString = info.get('PSetContent')
    if configString:
        try:
            split = configString.split(';;')
            cacheURL = split[0]
            cacheDB  = split[1]
            configID = split[2]
        except IndexError:
            msg =  "configCache not properly formatted\n"
            msg += "configString\n: %s" % configString
            msg += "Not attempting to put configCache content in DBS for this algo"
            msg += "AlgoInfo: %s" % algo
            logging.error(msg)
            return algo
        if cacheURL == "None" or cacheDB == "None" or configID == "None":
            # No Config for this DB
            logging.debug("No configCache for this algo")
            return algo
        try:
            configCache = ConfigCache(cacheURL, cacheDB)
            configCache.loadByID(configID)
            algo['PSetContent'] = configCache.getConfig()
        except Exception, ex:
            msg =  "Exception in getting configCache from DB\n"
            msg += "Ignoring this exception and continuing without config.\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            logging.debug("URL: %s,  DB: %s,  ID: %s" % (cacheURL, cacheDB, configID))

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
    """
    Considering how many times you're likely to see this
    you would think it would do something, but you would
    be wrong.
    """
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
        self.setStatus = self.bufferFactory(classname = "DBSBufferFiles.SetStatus")

        # Set config parameters
        self.maxBlockFiles    = self.config.DBSInterface.DBSBlockMaxFiles
        self.maxBlockTime     = self.config.DBSInterface.DBSBlockMaxTime
        self.maxBlockSize     = self.config.DBSInterface.DBSBlockMaxSize
        self.doMigration      = getattr(self.config.DBSInterface, 'doGlobalMigration', True)
        logging.debug("Initializing with maxFiles %i, maxTime %i, maxSize %i" % (self.maxBlockFiles,
                                                                                 self.maxBlockTime,
                                                                                 self.maxBlockSize))

        if dbsconfig == None:
            self.dbsconfig = config

        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available
        self.initAlerts(compName = "DBSUpload")

        return


    def algorithm(self, parameters = None):
        """
        Runs over all available DBSBuffer filesets/algos
        Commits them using DBSInterface
        Then checks blocks for timeout
        """
        logging.debug("Running subscription / fileset matching algorithm")
        try:
            self.sortBlocks()
            self.uploadBlocks()
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled exception in DBSUploadPoller\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            raise DBSUploadPollerException(msg)
        return

    def terminate(self, params):
        """
        Do one more pass, then terminate

        """
        logging.debug("Terminating. Doing one more pass before we die")
        self.algorithm(params)
        return


    def sortBlocks(self):
        """
        _sortBlocks_

        Find new files to upload, sort them into blocks
        Save the blocks in DBSBuffer
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
            #algo    = createAlgoFromInfo(info = dasInfo)
            dataset = createDatasetFromInfo(info = dasInfo)

            # Get the files for the DAS
            files  = self.uploadToDBS.findUploadableFilesByDAS(das = dasID)
            if len(files) < 1:
                # Then we have no files for this DAS
                logging.debug("DAS %i has no available files.  Continuing." % dasID)
                continue

            # Load the blocks for the DAS
            blocks = self.uploadToDBS.loadBlocksByDAS(das = dasID)
            logging.debug("Retrieved %i files and %i blocks from DB." % (len(files), len(blocks)))

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
                for location in locationDict.keys():
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
                self.sendAlert(6, msg = msg)
                logging.debug("BlockDictionary: %s" % blockDict)
                logging.debug("FileDictionary: %s" % locationDict)
                raise DBSUploadPollerException(msg)


            # At this point, all blocks should be in readyBlocks
            # STEP TWO: Commit blocks to DBSBuffer
            fileLFNs = self.createBlocksInDBSBuffer(readyBlocks = readyBlocks)

            # Now we should have all the blocks in DBSBuffer
            # Time to set the status of the files

            lfnList = [x['lfn'] for x in files]
            self.setStatus.execute(lfns = lfnList, status = "READY",
                                   conn = myThread.transaction.conn,
                                   transaction = myThread.transaction)

        # All files that were in NOTUPLOADED
        # And had uploaded parents
        # Should now be in assigned to blocks in DBSBuffer, and in the READY status
        return


    def uploadBlocks(self):
        """
        _uploadBlocks_

        Load all OPEN blocks out of the database with all their necessary files
        Once we have the blocks, determine which ones are ready to be uploaded
        Also determine which ones are ready to be migrated
        Upload them
        """
        myThread = threading.currentThread()


        # Get the blocks
        # This should grab all Pending and Open blocks
        blockInfo = self.uploadToDBS.loadBlocks()
        blocks = []

        if len(blockInfo) < 1:
            # Then we have no block, and probably no files
            logging.info("No blocks in this iteration.  Returning")
            return

        # Assemble the blocks
        for info in blockInfo:
            block = createBlock(datasetPath = 'blank', location = 'blank')
            block['id']           = info['id']
            block['das']          = info['das']
            block['Name']         = info['blockname']
            block['CreationDate'] = info['create_time']
            block['open']         = info['open']
            blocks.append(block)


        dasIDs = []
        for block in blocks:
            if block['das'] not in dasIDs:
                dasIDs.append(block['das'])

        dasAlgoDataset = {}
        dasAlgoInfo = self.uploadToDBS.loadDASInfoByID(ids = dasIDs)
        for dasInfo in dasAlgoInfo:
            algo    = createAlgoFromInfo(info = dasInfo)
            dataset = createDatasetFromInfo(info = dasInfo)
            dasAlgoDataset[dasInfo['DAS_ID']] = {'dataset': dataset,
                                                 'algo': algo}

        # At this point we should have the dataset and algo information
        # The blocks
        # And the files
        # Time to sort the files into blocks

        # the counter / watcher of the alertUploadQueueSize to possibly send alerts
        alertUploadQueueSize = getattr(self.config.DBSUpload, "alertUploadQueueSize", None)
        alertUploadQueueSizeCounter = 0
        for block in blocks:
            files = self.uploadToDBS.loadFilesFromBlocks(blockID = block['id'])
            for f in files:
                if f['blockID'] == block['id']:
                    # Put file in this block
                    logging.debug("Setting file %s to block %s" % (f['lfn'], block['Name']))
                    block['newFiles'].append(f)
                    alertUploadQueueSizeCounter += 1

        # check alertUploadQueueSize threshold (alert condition)
        if alertUploadQueueSize:
            if alertUploadQueueSizeCounter >= int(alertUploadQueueSize):
                msg = ("DBS upload queue size (%s) exceeded configured "
                       "threshold (%s)." % (alertUploadQueueSizeCounter, alertUploadQueueSize))
                self.sendAlert(6, msg = msg)

        # Check for block timeout
        for block in blocks:
            if time.time() - block['CreationDate'] > self.maxBlockTime:
                logging.info("Setting status to Pending due to timeout for block %s" % block['Name'])
                block['open'] = 'Pending'

        # Should have files in blocks, now assign them to DAS
        for dasID in dasAlgoDataset.keys():
            readyBlocks = []
            dataset = dasAlgoDataset[dasID]['dataset']
            algo    = dasAlgoDataset[dasID]['algo']
            for block in blocks:
                if len(block['newFiles']) > 0:
                    # Assign a location from the files
                    logging.debug("Block %s has %i files" % (block['Name'], len(block['newFiles'])))
                    block['location'] = list(block['newFiles'][0]['locations'])[0]
                if block['das'] == dasID:
                    if block['open'] == 'Pending':
                        # Always attach pending blocks
                        logging.debug("Attaching block %s" % block['Name'])
                        readyBlocks.append(block)
                    elif len(block['newFiles']) > 0:
                        # Else you only deal with blocks if they have new files
                        logging.debug("Attaching block %s" % block['Name'])
                        readyBlocks.append(block)

            if len(readyBlocks) < 1:
                # Nothing to do
                logging.debug("Nothing to do for DAS %i in uploadBlocks" % dasID)
                continue

            try:
                # Now do the real action of transferring crap
                # Damn it Anzar: Why does DBS print stuff out?

                for singleBlock in readyBlocks:
                    originalOut = sys.stdout
                    originalErr = sys.stderr
                    sys.stdout = open(os.devnull, 'w')
                    sys.stderr = open(os.devnull, 'w')

                    if getattr(self.config.DBSUpload, 'abortStepThree', False):
                        # Blow the stack for testing purposes
                        raise DBSUploadPollerException('None')

                    logging.info("About to upload to DBS for DAS %i with %i blocks" % (dasID, len(readyBlocks)))
                    affBlocks = self.dbsInterface.runDBSBuffer(algo = algo,
                                                               dataset = dataset,
                                                               blocks = [singleBlock])

                    sys.stdout = originalOut
                    sys.stderr = originalErr


                    # Update DBSBuffer with current information
                    myThread.transaction.begin()

                    for block in affBlocks:
                        logging.info("Successfully inserted %i files for block %s." % (len(block['insertedFiles']),
                                                                                       block['Name']))
                        self.uploadToDBS.setBlockStatus(block = block['Name'],
                                                        locations = [block['location']],
                                                        openStatus = block['open'])
                        if block['open'] == 'InGlobalDBS' or not self.doMigration:
                            # Set block files as in global if they've been migrated.
                            # If we aren't doing global migrations, all files are in global
                            logging.debug("Block %s now listed in global DBS" % block['Name'])
                            self.uploadToDBS.closeBlockFiles(blockname = block['Name'], status = 'GLOBAL')
                        else:
                            logging.debug("Block %s now uploaded to local DBS" % block['Name'])
                            self.uploadToDBS.closeBlockFiles(blockname = block['Name'], status = 'LOCAL')

                    logging.debug("About to do post-upload DBS commit for DAS %i" % dasID)
                    myThread.transaction.commit()

            # New plan: If we get an error in trying to commit a block to DBS
            # then we just rollback the transaction and continue to the next
            # block - ignoring the exception
            except WMException:
                if getattr(myThread, 'transaction', None) != None:
                    myThread.transaction.rollbackForError()
                pass
                #raise
            except Exception, ex:
                msg =  'Error in committing files to DBS\n'
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                self.sendAlert(6, msg = msg)
                if getattr(myThread, 'transaction', None) != None:
                    myThread.transaction.rollbackForError()
                pass
                #raise DBSUploadPollerException(msg)


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
            self.sendAlert(6, msg = msg)
            if getattr(myThread, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise DBSUploadPollerException(msg)

        return fileLFNs
