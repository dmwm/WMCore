#!/usr/bin/env python
#pylint: disable-msg=W6501, E1103, E1101
# W6501: Allow logging messages to have string formatting
# E1103: Use objects attached to thread
# E1101: Use config sections we know are there

"""
The DBSUploadWorker for uploading to DBS
"""
__all__ = []






import logging
import time
import traceback
import threading
import sys
import os


from WMCore.Agent.Configuration import Configuration
from WMCore.WMFactory           import WMFactory
from WMCore.DAOFactory          import DAOFactory
from WMCore.Services.UUID       import makeUUID

from WMComponent.DBSUpload.DBSInterface import DBSInterface
from WMComponent.DBSUpload.DBSErrors    import DBSInterfaceError




# Objects for creating dataset/algo info dictionaries
def createDatasetFromInfo(info):
    """
    Create a dataset object from basic information

    """
    dataset = {'ID':               str(info.get('Dataset')),
               'Path':             str(info.get('Path')),
               'ProcessedDataset': str(info.get('ProcessedDataset')),
               'PrimaryDataset':   str(info.get('PrimaryDataset')),
               'DataTier':         str(info.get('DataTier')),
               'Algo':             str(info.get('Algo')),
               'AlgoInDBS':        info.get('AlgoInDBS', None),
               'DASInDBS':         info.get('DASInDBS', None)
               }

    return dataset

def createAlgoFromInfo(info):
    """
    Create an Algo object from basic information

    """
    
    algo = {'ApplicationName':    str(info.get('ApplicationName')),
            'ApplicationFamily':  str(info.get('ApplicationFamily')),
            'ApplicationVersion': str(info.get('ApplicationVersion')),
            'PSetHash':           str(info.get('PSetHash')),
            'PSetContent':        str(info.get('PSetContent')),
            'InDBS':              info.get('AlgoInDBS', None)
            }

    return algo


def createConfigFromDictionary(input):
    """
    Create the DBS config object from the dictionary sent in

    """

    config = Configuration()

    for key in input:
        # All top level keys should be sections
        config.section_(key)
        if type(input[key]) == dict:
            recursiveConfigBuilder(getattr(config, key), input[key])

    return config


def recursiveConfigBuilder(configSection, input):
    """
    Recursively build configuration sections
    from a dictionary.

    """

    for key in input:
        if type(input[key]) == dict:
            # Then this is another section
            configSection.section_(key)
            recursiveConfigBuilder(getattr(configSection, key),
                                   input[key])
        else:
            setattr(configSection, key, input[key])

    return configSection
    


def createBlock(datasetPath, location):
    """
    Create a new block

    """

    block = {}
    block['Name']          = '%s#%s' % (datasetPath, makeUUID())
    block['NumberOfFiles'] = 0
    block['CreationDate']  = time.time()
    block['BlockSize']     = 0
    block['newFiles']      = []
    block['insertedFiles'] = []
    block['open']          = 1
    block['location']      = location


    return block

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
    

class DBSUploadWorker:
    """
    _DBSUploadWorker_

    This is the class that does the work for the current DBSUploader

    Three steps here in __call__

    1) Load the files and break them into blocks based on the current
         settings (use only DBSBuffer information)
    2) Commit block information into DBSBuffer.  This duplicates and
         replaces the bookkeeping in DBS
    3) Put all information into DBS.  Insert the blocks we've named
         and the files to them.  Don't query DBS for permission.
    """


    def __init__(self, **configDict):
        """
        init a basic ProcessPool worker thread.
        """

        myThread = threading.currentThread()

        self.config = createConfigFromDictionary(configDict)

        self.dbsInterface = DBSInterface(config = self.config)

        # Open up the buffer wrapper
        uploadFactory = WMFactory("dbsUpload",
                                  "WMComponent.DBSUpload.Database.Interface")
        self.uploadToDBS = uploadFactory.loadObject("UploadToDBS")

        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)
        addFactory = WMFactory("dbsBuffer",
                               "WMComponent.DBSBuffer.Database.Interface")
        self.addToBuffer = addFactory.loadObject("AddToBuffer")

        # Set config parameters
        self.maxBlockFiles    = self.config.DBSInterface.DBSBlockMaxFiles
        self.maxBlockTime     = self.config.DBSInterface.DBSBlockMaxTime
        self.maxBlockSize     = self.config.DBSInterface.DBSBlockMaxSize
        self.maxFilesToCommit = self.config.DBSInterface.MaxFilesToCommit


        self.blocks = []



        return


    def __call__(self, parameters):
        """
        Does the work

        Expects parameters to be a list of DAS objects
        """
        myThread = threading.currentThread()

        for dasInfo in parameters:

            # Initial values
            readyBlocks = []
            fileLFNs    = []

            # Get the dataset-algo information
            algo    = createAlgoFromInfo(info = dasInfo)
            dataset = createDatasetFromInfo(info = dasInfo)


            # Get DAOs
            setBlock  = self.bufferFactory(classname = "DBSBufferFiles.SetBlock")

            dasID = dasInfo['DAS_ID']

            # Get the files
            files = self.uploadToDBS.findUploadableFilesByDAS(das = dasID)

            # Get all the blocks
            blocks = self.uploadToDBS.loadBlocksByDAS(das = dasID)


            # STEP ONE: Sort files into blocks

            # Sort the files and blocks by location
            locationDict = sortListByKey(input = files, key = 'locations')
            blockDict    = sortListByKey(input = blocks, key = 'location')


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



            # At this point, all blocks should be in readyBlocks
            # STEP TWO: Commit blocks to DBSBuffer
            try:
                # Do this in its own transaction
                myThread.transaction.begin()

                for block in readyBlocks:
                    # First insert each block
                    logging.error("About to put block into DBS")
                    logging.error(block['open'])
                    self.uploadToDBS.setBlockStatus(block = block['Name'],
                                                    locations = [block['location']],
                                                    openStatus = block['open'],
                                                    time = int(block['CreationDate']))

                    # Then insert files from each block
                    blockFileList = []
                    for f in block.get('newFiles', []):
                        blockFileList.append(f['lfn'])
                        setBlock.execute(lfn = blockFileList,
                                         blockName = block['Name'],
                                         conn = myThread.transaction.conn,
                                         transaction = myThread.transaction)
                        fileLFNs.extend(blockFileList)

                myThread.transaction.commit()


            except Exception, ex:
                msg =  'Error in committing blocks to DBSBuffer\n'
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                myThread.transaction.rollback()
                raise Exception(msg)

            # STEP THREE: Insert Into DBS and Update DBSBuffer
            try:


                # Damn it Anzar: Why does DBS print stuff out?
                originalOut = sys.stdout
                originalErr = sys.stderr

                sys.stdout = open(os.devnull, 'w')
                sys.stderr = open(os.devnull, 'w')

                logging.info("About to send %i blocks to DBSInterface" % (len(readyBlocks)))
                for block in readyBlocks:
                    logging.info("About to send to DBS block %s" %(block['Name']))
                    logging.info("Block status: %s" % (block.get('open', None) ))
                
                
                affBlocks = self.dbsInterface.runDBSBuffer(algo = algo,
                                                           dataset = dataset,
                                                           blocks = readyBlocks)

                sys.stdout = originalOut
                sys.stderr = originalErr


                myThread.transaction.begin()
                
                if not algo['InDBS']:
                    # List the algo as uploaded
                    self.addToBuffer.updateAlgo(algo, 1)
                if not dataset['DASInDBS']:
                    # List the datasetAlgo as uploaded
                    self.uploadToDBS.setDatasetAlgo(datasetAlgoInfo = \
                                                    {'DAS_ID': dasID},
                                                    inDBS = 1)
                for block in affBlocks:
                    logging.info("About to set in DBSBuffer with status %s block %s" %(str(block['open']), block['Name']))
                    if block['open'] == 0:
                        self.uploadToDBS.setBlockStatus(block = block['Name'],
                                                        locations = [block['location']],
                                                        openStatus = block['open'])

                # Update file status
                self.uploadToDBS.updateFilesStatus(fileLFNs, "InDBS")

                logging.info("Commmitting DBSBuffer transaction")

                myThread.transaction.commit()

                
            except DBSInterfaceError, ex:
                msg =  'Error in DBS Interface\n'
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                myThread.transaction.rollback()
                raise Exception(msg)
            except Exception, ex:
                msg =  'Error in committing files to DBS\n'
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                myThread.transaction.rollback()
                raise Exception(msg)

        return parameters




    def splitFilesIntoBlocks(self, files, blocks, dataset, location):
        """
        Break the files into blocks based on config params

        Create a new block when necessary.
        """

        if len(blocks) > 1:
            # Well, then we have a bit of a problem
            # Decide what to do about this later
            logging.error("More then one open block for this DAS")

        if len(blocks) == 0:
            blocks.append(createBlock(datasetPath = dataset['Path'],
                                      location = location))


        currentBlock = blocks[0]
        blockFiles, blockSize, blockTime = self.blockLimits(block = currentBlock)

        for newFile in files:
            # Check to see if blocks are full
            if blockSize < 1 or blockFiles < 1 or blockTime < 0:
                # Create a new block
                currentBlock['open'] = 'Pending'
                currentBlock = createBlock(datasetPath = dataset['Path'],
                                           location = location)
                blockFiles = self.maxBlockFiles
                blockTime  = self.maxBlockTime
                blockSize  = self.maxBlockSize
                blocks.append(currentBlock)

            # Now process the file
            currentBlock['newFiles'].append(newFile)
            blockFiles -= 1
            blockSize  -= newFile['size']
            currentBlock['BlockSize']     += newFile['size']
            currentBlock['NumberOfFiles'] += 1

                
        return blocks


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
