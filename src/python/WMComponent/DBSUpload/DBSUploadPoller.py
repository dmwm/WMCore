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




import threading
import logging
import time
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool        import ProcessPool

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
        self.dbsBlockTimeout = self.config.DBSInterface.DBSBlockMaxTime

        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        factory = WMFactory("dbsUpload",
                            "WMComponent.DBSUpload.Database.Interface")
        self.dbinterface = factory.loadObject("UploadToDBS")

        self.dbsInterface = DBSInterface(config = config)

        if dbsconfig == None:
            self.dbsconfig = config

        configDict = createConfigForJSON(config)

        self.processPool = ProcessPool("DBSUpload.DBSUploadWorker",
                                       totalSlaves = self.config.DBSUpload.workerThreads,
                                       componentDir = self.config.DBSUpload.componentDir,
                                       config = self.config,
                                       slaveInit = configDict)



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

        myThread = threading.currentThread()

        # Grab all the Dataset-Algo combindations
        dasList = dbinterface.findUploadableDAS()

        if len(dasList) > 0:
            # Then send off some work

            self.processPool.enqueue(work = dasList)
            
            self.processPool.dequeue(totalItems = len(dasList))

        # And we're done with putting stuff in.
        # Check to see if any blocks have timed out
        try:
            myThread.transaction.begin()

            # Now it's time to do a block check and look for
            # open blocks that we should timeout
            blocks = dbinterface.findOpenBlocks()

            logging.info("Found %i open blocks" % (len(blocks)))

            blocksToClose = []
            doneBlocks    = []

            for buffBlock in blocks:
                if time.time() - buffBlock['create_time'] \
                       > self.dbsBlockTimeout:
                    # Then we have to load a block
                    blocksToClose.append(buffBlock['blockname'])
                    logging.info("Going to close and migrate block due to timeout: %s" % (buffBlock['blockname']))

            # Actually finish the blocks
            if len(blocksToClose) > 0:
                doneBlocks = self.dbsInterface.closeAndMigrateBlocksByName(blockNames = blocksToClose)

            for block in doneBlocks:
                # Now close 'em in DBSBuffer
                logging.info("About to close block in DBSBuffer due to timeout: %s" % (block['Name']))
                dbinterface.setBlockStatus(block['Name'], locations = None,
                                           openStatus = 0)


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




