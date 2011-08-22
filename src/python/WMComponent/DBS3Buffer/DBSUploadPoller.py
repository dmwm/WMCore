#!/usr/bin/env python

"""
The DBSUpload algorithm

This code, all of which is in uploadDatasets works in three parts.
Each part is carried out in its own database transaction.

1) Find all files that have not yet been uploaded to DBS,
   and the fileset-algo pairs that go along with them.
2) Upload these files.  This is handled through DBSInterface
   which first sends the algo, then the dataset, and then
   commits all of the files.  It also handles migrating to
   global is you specify a global URL and the file blocks are
   full (have reached the limit in the DBS config)
3) The code cleans up at the end, check for any open blocks
   that have exceeded their timeout.


NOTE: This is complicated as hell, because you can have a
dataset-algo pair where BOTH the dataset and the algo are in
DBS attached to different objects.  You have to scan for a pair
that is not in DBS, decide whether its components are in DBS,
add them, and then add the files.  This is why everything is
so convoluted.
"""


import threading
import logging
import Queue
import traceback
import multiprocessing

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.UUID                  import makeUUID
from WMCore.WMException                    import WMException


from WMComponent.DBS3Buffer.DBSBufferUtil  import DBSBufferUtil
from WMComponent.DBS3Buffer.DBSBufferBlock import DBSBlock

from dbs.apis.dbsClient import DbsApi



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


def createDictionaryFromConfig(configSection):
    """
    Recursively create dictionaries from config

    """


    final = configSection.dictionary_()

    for key in final.keys():
        if hasattr(final[key], 'dictionary_'):
            # Then we can turn it into a dictionary
            final[key] = createDictionaryFromConfig(final[key])

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



def uploadWorker(input, results, dbsUrl):
    """
    _uploadWorker_

    Put JSONized blocks in the input
    Get confirmation in the output
    """

    # Init DBS Stuff

    dbsApi = DbsApi(url = dbsUrl)


    while True:

        try:
            work = input.get()
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            logging.error(crashMessage)
            break

        if work == 'STOP':
            # Then halt the process
            break

        name  = work.get('name', None)
        block = work.get('block', None)


        # Do stuff with DBS
        try:
            dbsApi.insertBlockBluk(blockDump = block)
            results.put({'name': name, 'success': True})        
        except Exception, ex:
            exString = str(ex)
            if exString.find('Duplicate entry') > 0:
                # Then this is probably a duplicate
                # Ignore this for now
                logging.error("Had duplicate entry for block %s\n" % name)
                logging.error("Ignoring for now.\n")
                results.put({'name': name, 'success': True})
            else:
                msg =  "Error trying to process block %s through DBS.\n" % name
                msg += exString
                msg += str(traceback.format_exc())
                results.put({'name': name, 'success': False, 'error': msg})

    return



class DBSUploadException(WMException):
    """
    Holds the exception info for
    all the things that will go wrong

    """

        


class DBSUploadPoller(BaseWorkerThread):
    """
    Handles poll-based DBSUpload

    """


    def __init__(self, config, dbsconfig = None):
        """
        Initialise class members
        """

        #myThread = threading.currentThread()
        
        BaseWorkerThread.__init__(self)
        self.config     = config

        # This is slightly dangerous, but DBSUpload depends
        # on DBSInterface anyway
        self.maxBlockFiles    = self.config.DBSUpload.DBSBlockMaxFiles
        self.maxBlockTime     = self.config.DBSUpload.DBSBlockMaxTime
        self.maxBlockSize     = self.config.DBSUpload.DBSBlockMaxSize
        self.dbsUrl           = self.config.DBSUpload.dbsUrl

        self.dbsUtil = DBSBufferUtil()


        self.pool = []
        self.input  = multiprocessing.Queue()
        self.result = multiprocessing.Queue()
        self.nProc  = getattr(self.config.DBSUpload, 'nProcesses', 4)
        self.wait   = getattr(self.config.DBSUpload, 'dbsWaitTime', 0.1)
        self.physicsGroup = getattr(self.config.DBSUpload, 'physicsGroup', 'DBS3Test')

        # List of blocks currently in processing
        self.queuedBlocks = []

        # Starting up the pool:
        for x in range(self.nProc):
            p = multiprocessing.Process(target = uploadWorker,
                                        args = (self.input, self.result, self.dbsUrl))
            p.start()
            self.pool.append(p)


        # Setting up any cache objects
        self.blockCache = {}
        self.dasCache   = {}

        self.filesToUpdate = []

        self.produceCopy = getattr(self.config.DBSUpload, 'copyBlock', False)
        self.copyPath    = getattr(self.config.DBSUpload, 'copyBlockPath',
                                   '/data/mnorman/block.json')

        return


    def __del__(self):
        """
        __del__
        
        Trigger a close of connections if necessary
        """
        self.close()
        #BaseWorkerThread.__del__(self)
        return


    def close(self):
        """
        _close_

        Kill all connections and terminate
        """
        terminate = False
        for x in self.pool:
            try:
                self.input.put('STOP')
            except Exception, ex:
                # Something very strange happens here
                # It's like it raises a blank exception
                # Upon being told to return
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.debug(msg)
                terminate = True
        self.input.close()
        self.result.close()
        for proc in self.pool:
            if terminate:
                proc.terminate()
            else:
                proc.join()
        return



    def terminate(self, params):
        """
        Do one more pass, then terminate

        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


    def algorithm(self, parameters = None):
        """
        _algorithm_

        First, load blocks
        Then, load files
        Then, move files into blocks
        Then add new blocks in DBSBuffer
        Then add blocks to DBS
        Then mark blocks as done in DBSBuffer
        """

        try:
            self.loadBlocks()
            self.loadFiles()
            self.checkTimeout()
            self.inputBlocks()
            self.retrieveBlocks()
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled Exception in DBSUploadPoller!\n"
            msg += str(ex)
            msg += str(str(traceback.format_exc()))
            logging.error(msg)
            raise DBSUploadException(msg)


    def loadBlocks(self):
        """
        _loadBlocks_

        Find all blocks; make sure they're in the cache
        """
        openBlocks = self.dbsUtil.findOpenBlocks()


        # Load them if we don't have them
        blocksToLoad = []
        for block in openBlocks:
            if not block['blockname'] in self.blockCache.keys():
                blocksToLoad.append(block['blockname'])


        # Now load the blocks
        try:
            loadedBlocks = self.dbsUtil.loadBlocks(blocknames = blocksToLoad)
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled exception while loading blocks.\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Blocks to load: %s\n" % blocksToLoad)
            raise DBSUploadException(msg)
        
        for blockInfo in loadedBlocks:
            das  = blockInfo['DatasetAlgo']
            loc  = blockInfo['location']
            block = DBSBlock(name = blockInfo['Name'],
                             location = loc, das = das)
            block.FillFromDBSBuffer(blockInfo)
            blockname = block.getName()

            # Now we have to load files...
            try:
                files = self.dbsUtil.loadFilesByBlock(blockname = blockname)
            except WMException:
                raise
            except Exception, ex:
                msg =  "Unhandled exception while loading files for existing blocks.\n"
                msg += str(ex)
                logging.error(msg)
                logging.debug("Blocks being loaded: %s\n" % blockname)
                raise DBSUploadException(msg)
            for file in files:
                block.addFile(file)

            # Add to the cache
            self.addNewBlock(block = block)


        # All blocks should now be loaded and present
        # in both the block cache (which has all the info)
        # and the dasCache (which is a list of name pointers
        # to the keys in the block cache).           

        return


    def loadFiles(self):
        """
        _loadFiles_

        Load all files that need to be loaded.

        I will do this by DAS for now to break
        the monstrous calls down into smaller chunks.
        """


        # Grab all the Dataset-Algo combindations
        dasList = self.dbsUtil.findUploadableDAS()

        if len(dasList) < 1:
            # Then there's nothing to do
            return []

        readyBlocks = []
        for dasInfo in dasList:

            dasID = dasInfo['DAS_ID']
            
            # Get the files
            try:
                loadedFiles = self.dbsUtil.findUploadableFilesByDAS(das = dasID)
            except WMException:
                raise
            except Exception, ex:
                msg =  "Unhandled exception while loading uploadable files for DAS.\n"
                msg += str(ex)
                logging.error(msg)
                logging.debug("DAS being loaded: %s\n" % dasID)
                raise DBSUploadException(msg)

            # Get the blocks
            if not dasID in self.dasCache.keys():
                # Then we have a new DAS
                # Add it
                self.dasCache[dasID] = {}
            dasBlocks = self.dasCache.get(dasID)

            # Sort the files and blocks by location
            fileDict = sortListByKey(input = loadedFiles, key = 'locations')

            # Now we have both files and blocks
            # We need a sorting algorithm of sorts...

            

            # Now add each file
            for location in fileDict.keys():
                files = fileDict.get(location)

                if len(files) < 1:
                    # Nothing to do here
                    continue
                
                dasBlocks = self.dasCache[dasID].get(location, [])
                if len(dasBlocks) > 0:
                    # Load from cache
                    currentBlock = self.blockCache.get(dasBlocks[0])
                else:
                    blockname = '%s#%s' % (files[0]['datasetPath'], makeUUID())
                    currentBlock = DBSBlock(name = blockname,
                                            location = location, das = dasID)
                    # Add the era info
                    currentBlock.setAcquisitionEra(era = dasInfo['AcquisitionEra'])
                    currentBlock.setProcessingVer(era = dasInfo['ProcessingVer'])
                    self.addNewBlock(block = currentBlock)
                    dasBlocks.append(currentBlock.getName())

                for newFile in files:
                    if not newFile.get('block', 1) == None:
                        # Then this file already has a block
                        # It should be accounted for somewhere
                        # Or loaded with the block
                        continue
                    
                    # Check if we can put files in this block
                    if not self.isBlockOpen(newFile = newFile,
                                            block = currentBlock):
                        # Then we have to close the block and get a new one
                        currentBlock.status = 'Pending'
                        readyBlocks.append(currentBlock)
                        dasBlocks.remove(currentBlock.getName())
                        currentBlock = self.getBlock(newFile = newFile,
                                                     dasBlocks = dasBlocks,
                                                     location = location,
                                                     das = dasID)
                        currentBlock.setAcquisitionEra(era = dasInfo['AcquisitionEra'])
                        currentBlock.setProcessingVer(era = dasInfo['ProcessingVer'])

                    # Now deal with the file
                    currentBlock.addFile(dbsFile = newFile)
                    self.filesToUpdate.append({'filelfn': newFile['lfn'],
                                               'block': currentBlock.getName()})
                # Done with the location
                readyBlocks.append(currentBlock)

            # Should be done with the DAS once we've added all files

        # Update the blockCache with what is now ready.
        for block in readyBlocks:
            self.blockCache[block.getName()] = block
                
        return


    def checkTimeout(self):
        """
        Check all blocks for a timeout

        """
        for block in self.blockCache.values():
            if block.status == 'Open' and block.getTime() > self.maxBlockTime:
                block.status = 'Pending'
                self.blockCache[block.getName()] = block

    def addNewBlock(self, block):
        """
        _addNewBlock_

        Add a new block everywhere it has to go
        """
        name     = block.getName()
        location = block.getLocation()
        das      = block.das
        self.blockCache[name] = block
        if not das in self.dasCache.keys():
            self.dasCache[das] = {}
            self.dasCache[das][location] = []
        elif not location in self.dasCache[das].keys():
            self.dasCache[das][location] = []
        if not name in self.dasCache[das][location]:
            self.dasCache[das][location].append(name)

        return

    def isBlockOpen(self, newFile, block, doTime = False):
        """
        _isBlockOpen_

        Check and see if a block is full
        This will check on time, but that's disabled by default
        The plan is to do a time check after we do everything else,
        so open blocks about to time out can still get more
        files put in them.
        """

        if block.status != 'Open':
            # Then somebody has dumped this already
            return False
        if block.getTime() > self.maxBlockTime and doTime:
            return False
        if block.getSize() + newFile['size'] > self.maxBlockSize:
            return False
        if block.getNFiles() >= self.maxBlockFiles:
            # Then we have to dump it because this file
            # will put it over the limit.
            return False

        return True
        

    def getBlock(self, newFile, dasBlocks, location, das):
        """
        _getBlock_

        This gets a new block by checking whether there is a
        pre-existant block. 
        """

        for block in dasBlocks:
            if not self.isBlockOpen(newFile = newFile, block = block):
                # Then the block can't fit the file
                # Close the block
                block.status = 'Pending'
                self.blockCache[block.getName()] = block
                dasBlocks.remove(block.getName())
            else:
                # Load it out of the cache
                currentBlock = self.blockCache.get(block.getName())
                return currentBlock
        # If there are no open blocks
        # Or we run out of blocks
        blockname = '%s#%s' % (newFile['datasetPath'],
                               makeUUID())
        newBlock = DBSBlock(name = blockname,
                            location = location, das = das)
        self.addNewBlock(block = newBlock)
        dasBlocks.append(blockname)
        return newBlock

        

    def inputBlocks(self):
        """
        _processBlocks_

        Process the blocks that have new files in them.
        1) Put them into DBSBuffer in state 'Pending'
        2) Upload them to DBS
        """

        myThread = threading.currentThread()

        # We want to run this over all pending blocks
        blocks            = []
        blockForDBSBuffer = []
        updateBlocks      = []
        for block in self.blockCache.values():
            if block.getName() in self.queuedBlocks:
                continue
            if block.status == 'Pending':
                blocks.append(block)
                # Either this is a new block
                # or it's one we need to update
                if not block.inBuff:
                    blockForDBSBuffer.append(block)
                else:
                    updateBlocks.append(block)
            if block.status == 'Open' and not block.inBuff:
                # Then this is a new block we need to
                # add to the buffer
                # but not process
                blockForDBSBuffer.append(block)

        if len(blocks) < 1:
            # Nothing to do
            return


        # First handle new and updated blocks
        try:
            myThread.transaction.begin()
            self.dbsUtil.createBlocks(blocks = blockForDBSBuffer)
            self.dbsUtil.updateBlocks(blocks = updateBlocks)
            myThread.transaction.commit()
        except WMException:
            myThread.transaction.rollback()
            raise
        except Exception, ex:
            msg =  "Unhandled exception while writing new blocks into DBSBuffer\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Blocks for DBSBuffer: %s\n" % blockForDBSBuffer)
            logging.debug("Blocks for Update: %s\n" % updateBlocks)
            myThread.transaction.rollback()
            raise DBSUploadException(msg)

        # List blocks as inBuff
        for block in blockForDBSBuffer:
            self.blockCache.get(block.getName()).inBuff = True


        # Now put in the files that we just added.
        try:
            myThread.transaction.begin()
            self.dbsUtil.setBlockFiles(binds = self.filesToUpdate)
            self.filesToUpdate = []
            myThread.transaction.commit()
        except WMException:
            myThread.transaction.rollback()
            raise
        except Exception, ex:
            msg =  "Unhandled exception while setting blocks in files.\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Files to Update: %s\n" % self.filesToUpdate)
            myThread.transaction.rollback()
            raise DBSUploadException(msg)


        # Now that things are in DBSBuffer, we can put them in DBS

        for block in blocks:
            logging.debug("Found block %s in blocks" % block.getName())
            block.setPhysicsGroup(group = self.physicsGroup)
            encodedBlock = block.data
            self.input.put({'name': block.getName(), 'block': encodedBlock})
            if self.produceCopy:
                import json
                f = open(self.copyPath, 'w')
                f.write(json.dumps(encodedBlock))
                f.close()
            self.queuedBlocks.append(block.getName())



        # And all work is in and we're done for now

        return



    def retrieveBlocks(self):
        """
        _retrieveBlocks_

        Once blocks are in DBS, we have to retrieve them and see what's
        in them.  What we do is get everything out of the result queue,
        and then update it in DBSBuffer.

        To do this, the result queue needs to pass back the blockname
        """
        myThread = threading.currentThread()
        
        blocksToClose = []
        while True:
            try:
                # Get stuff out of the queue with a ridiculously
                # short wait time
                blockresult = self.result.get(timeout = self.wait)
                blocksToClose.append(blockresult)
            except Queue.Empty:
                # This means the queue has no current results
                break

        loadedBlocks = []
        for result in blocksToClose:
            # Remove from list of work being processed
            self.queuedBlocks.remove(result.get('name'))
            if result.get('success', False):
                block = self.blockCache.get(result.get('name'))
                block.status = 'InDBS'
                loadedBlocks.append(block)
            else:
                logging.error("Error found in multiprocess during process of block %s" % result.get('name'))
                logging.error(result['error'])
                # Continue to the next block
                # Block will remain in pending status until it is transferred

        try:
            myThread.transaction.begin()
            self.dbsUtil.updateBlocks(blocks = loadedBlocks)
            myThread.transaction.commit()
        except WMException:
            myThread.transaction.rollback()
            raise
        except Exception, ex:
            msg =  "Unhandled exception while finished closed blocks in DBSBuffer\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Blocks for Update: %s\n" % loadedBlocks)
            myThread.transaction.rollback()
            raise DBSUploadException(msg)


        for block in loadedBlocks:
            # Clean things up
            name     = block.getName()
            location = block.getLocation()
            das      = block.das
            self.dasCache[das][location].remove(name)
            del self.blockCache[name]


        # And we're done
        return


    








        

