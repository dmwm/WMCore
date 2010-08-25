#!/usr/bin/env python
#pylint: disable-msg=W6501
# W6501: Allow logging messages to have string formatting
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

__revision__ = "$Id: DBSUploadPoller.py,v 1.26 2010/05/26 19:26:10 mnorman Exp $"
__version__ = "$Revision: 1.26 $"

import threading
import logging
import time
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory  import WMFactory
from WMCore.DAOFactory import DAOFactory

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
        self.DBSBlockTimeout = self.config.DBSInterface.DBSBlockMaxTime

        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        factory = WMFactory("dbsUpload",
                            "WMComponent.DBSUpload.Database.Interface")
        self.dbinterface = factory.loadObject("UploadToDBS")

        bufferFactory = WMFactory("dbsBuffer",
                                  "WMComponent.DBSBuffer.Database.Interface")
        self.addToBuffer = bufferFactory.loadObject("AddToBuffer")


        self.dbsInterface = DBSInterface(config = config)

        if dbsconfig == None:
            self.dbsconfig = config
    
    def setup(self, parameters):
        """
        Do nothing
        """

        return

    def uploadDatasets(self):
        """
        This should do the hard work of adding things to DBS.
        It essentially replaces BufferSuccess

        """
        dbinterface = self.dbinterface
        addToBuffer = self.addToBuffer

        myThread = threading.currentThread()


        # Get DAOs
        setBlock  = self.bufferFactory(classname = "DBSBufferFiles.SetBlock")

        # Get all files that need to be uploaded
        unsortedFileList = dbinterface.findUploadableFiles()
        fileList = sortByDAS(incoming = unsortedFileList)
        logging.debug('Have retrieved %i files to be uploaded from DBSBuffer' \
                      % (len(fileList)))



        for datasetAlgo in fileList.keys():
            # Now we process each datasetAlgo, with all its files.

            algo    = createAlgoFromInfo(info = fileList[datasetAlgo][0])
            dataset = createDatasetFromInfo(info = fileList[datasetAlgo][0])


            
            # Create a new transaction once per datasetAlgo
            myThread.transaction.begin()

            # Get the files
            files    = []
            fileLFNs = []
            files = addToBuffer.loadDBSBufferFilesBulk(fileObjs = fileList[datasetAlgo])
            fileLFNs.extend([f['lfn'] for f in files])
            

            try:
                # Do all actual useful operations inside
                # a single try/error loop
                # This includes all of DBS
                
                # Do all DBS Operations
                affectedBlocks = self.dbsInterface.runDBSBuffer(algo = algo,
                                                                dataset = dataset,
                                                                files = files)

                if not algo['InDBS']:
                    # List the algo as uploaded
                    addToBuffer.updateAlgo(algo, 1)
                if not dataset['DASInDBS']:
                    # List the datasetAlgo as uploaded
                    dbinterface.setDatasetAlgo(datasetAlgoInfo = \
                                               {'DAS_ID': datasetAlgo},
                                               inDBS = 1)
                for block in affectedBlocks:
                    info = block['StorageElementList']
                    locations = []
                    for loc in info:
                        locations.append(loc['Name'])
                    self.dbinterface.setBlockStatus(block['Name'], locations,
                                                    block['OpenForWriting'],
                                                    int(block['CreationDate']))
                    
                    blockFileList = []
                    for f in block.get('insertedFiles', []):
                        blockFileList.append(f['LogicalFileName'])
                        setBlock.execute(lfn = blockFileList,
                                         blockName = block['Name'],
                                         conn = myThread.transaction.conn,
                                         transaction = myThread.transaction)

                # Update the file status, and then recount UnMigrated Files
                dbinterface.updateFilesStatus(fileLFNs, "InDBS")

                # Commit transaction and finish
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




        # And we're done with putting stuff in.
        # Check to see if any blocks have timed out
        try:
            myThread.transaction.begin()

            # Now it's time to do a block check and look for
            # open blocks that we should timeout
            blocks = dbinterface.findOpenBlocks()

            blocksToClose = []
            doneBlocks    = []

            for buffBlock in blocks:
                if time.time() - buffBlock['create_time'] \
                       > self.DBSBlockTimeout:
                    # Then we have to load a block
                    blocksToClose.append(buffBlock['blockname'])

            # Actually finish the blocks
            if len(blocksToClose) > 0:
                doneBlocks = self.dbsInterface.closeAndMigrateBlocksByName(blockNames = blocksToClose)

            for block in doneBlocks:
                # Now close 'em in DBSBuffer
                dbinterface.setBlockStatus(block['Name'], locations = None,
                                           openStatus = 0,
                                           time = block['CreationDate'])


            myThread.transaction.commit()

        except DBSInterfaceError, ex:
            msg =  'Error in DBSInterface while closing timed-out blocks\n'
            msg += str(ex)
            logging.error(msg)
            myThread.transaction.rollback()
            raise Exception(msg)
        except Exception, ex:
            msg =  'Error in closing blocks in DBS\n'
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            myThread.transaction.rollback()
            raise Exception(msg)




        return
        

    def terminate(self, params):
        """
        Do one more pass, then terminate

        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        
    def algorithm(self, parameters = None):
        """
        Runs over all available DBSBuffer filesets/algos
        Commits them using DBSInterface
        Then checks blocks for timeout
        """
        logging.debug("Running subscription / fileset matching algorithm")
        self.uploadDatasets()




