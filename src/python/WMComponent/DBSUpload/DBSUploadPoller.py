#!/usr/bin/env python
"""
The DBSUpload algorithm

This code, all of which is in uploadDatasets works in three parts.
Each part is carried out in its own database transaction.

1) The code looks for all dataset-algo pair in DBSBuffer that are
   listed as not being in DBS.  It then uploads datasets or
   algos that do not have an in_dbs flag in the DBSBuffer.
2) For each pair, the code also looks for files that need to
   be uploaded, those files attached to the dataset-algo pair, with
   a NOTUPLOADED status, and once this is done, label the
   dataset-algo pair as being in DBS.
3) The code cleans up at the end, check for any open blocks
   that have exceeded their timeout.


NOTE: This is complicated as hell, because you can have a
dataset-algo pair where BOTH the dataset and the algo are in
DBS attached to different objects.  You have to scan for a pair
that is not in DBS, decide whether its components are in DBS,
add them, and then add the files.  This is why everything is
so convoluted.
"""

__revision__ = "$Id: DBSUploadPoller.py,v 1.22 2010/05/14 18:56:55 mnorman Exp $"
__version__ = "$Revision: 1.22 $"

import threading
import logging
import re
import os
import time
import traceback
import operator

import inspect

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory  import WMFactory
from WMCore.DAOFactory import DAOFactory

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

from DBSAPI.dbsApi import DbsApi

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Services.DBS.DBSWriter import DBSWriter
from WMCore.Services.DBS           import DBSWriterObjects
from WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from WMCore.Services.DBS.DBSReader import DBSReader


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
               'AlgoInDBS':        info.get('AlgoInDBS', None)
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

def sortByDAS(input):
    """
    Sort a list of fileInfo into a dictionary keyed by dataset-algo
    assoc IDs

    """
    output = {}

    for entry in input:
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
        self.dbsurl     = self.config.DBSUpload.dbsurl
        self.dbsversion = self.config.DBSUpload.dbsversion
        self.uploadFileMax = self.config.DBSUpload.uploadFileMax

        self.DBSMaxFiles     = self.config.DBSUpload.DBSMaxFiles
        self.DBSMaxSize      = self.config.DBSUpload.DBSMaxSize
        self.DBSBlockTimeout = self.config.DBSUpload.DBSBlockTimeout

        self.bufferFactory   = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                          logger = myThread.logger,
                                          dbinterface = myThread.dbi)

        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        self.dbinterface=factory.loadObject("UploadToDBS")

        bufferFactory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
        self.addToBuffer=bufferFactory.loadObject("AddToBuffer")

        if dbsconfig == None:
            self.dbsconfig = config
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()

        logging.info("DBSURL %s"%self.dbsurl)
        args = { "url" : self.dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" : self.dbsversion }
        self.dbsapi = DbsApi(args)
        self.dbswriter = DBSWriter(self.dbsurl, level='ERROR', user='NORMAL', version=self.dbsversion, \
                                   globalDBSUrl  = self.config.DBSUpload.globalDBSUrl, \
                                   globalVersion =  self.config.DBSUpload.globalDBSVer)
        self.dbsreader = DBSReader(self.dbsurl, level='ERROR', user='NORMAL', version=self.dbsversion)

        return

    def uploadDatasets(self):
        """
        This should do the hard work of adding things to DBS.
        It essentially replaces BufferSuccess

        """
        dbinterface = self.dbinterface
        addToBuffer = self.addToBuffer

        
        #Get datasets out of DBS, along with algos
        datasetAlgos=self.dbinterface.findUploadableDatasets()
        logging.debug('Have retrieved %i dataset-algo pairs from DBSBuffer' %(len(datasetAlgos)))

        myThread = threading.currentThread()

        

        setBlock      = self.bufferFactory(classname = "DBSBufferFiles.SetBlock")

        #Now check them
        fileLFNs = []


        # Okay, out of the dock
        # This will have a list of all dataset-algo combinations
        try:
            myThread.transaction.begin()
            for datasetAlgo in datasetAlgos:
                logging.info('Have begun to process dataset %i' %(datasetAlgo['ID']))

                # First step: commit dataset-algo combination
            

                newAlgos = []
                algo    = createAlgoFromInfo(info = datasetAlgo)
                dataset = createDatasetFromInfo(info = datasetAlgo)

                if not algo['InDBS']:
                    # Well then we better add it
                    logging.info('About to insert algo into DBS')
                    newAlgos.append(DBSWriterObjects.createAlgorithm(algo, configMetadata = None, apiRef = self.dbsapi))
                    addToBuffer.updateAlgo(algo, 1)
                else:
                    # Then the algo is already in DBS
                    # We still need a DBSInstance of this
                    # By calling this without an apiRef, we should create
                    # An instance of DBSAlgorithm without trying
                    # To check it back into DBS
                    newAlgos.append(DBSWriterObjects.createAlgorithm(algo, configMetadata = None, apiRef = None))


                # WARNING!  This is a temporary fix
                # Don't count on it staying around!
                # Also, I know it's clumsy.  Not particularly caring about that now.
                if not datasetAlgo['DatasetInDBS'] and dataset['PrimaryDataset'].lower() != 'bogus':
                    # We should add the dataset too

                    logging.info('Entering primary, processed datasets into DBS')
                    logging.info(dataset)
                    primary = DBSWriterObjects.createPrimaryDataset(datasetInfo = dataset,
                                                                    apiRef = self.dbsapi)
                    processed = DBSWriterObjects.createProcessedDataset(primaryDataset = primary,
                                                                        algorithm = newAlgos, 
                                                                        datasetInfo = dataset,
                                                                        apiRef = self.dbsapi)
                    addToBuffer.addDataset(datasetAlgo, 1)

                self.dbinterface.setDatasetAlgo(datasetAlgoInfo = datasetAlgo, inDBS = 1)
                    
            myThread.transaction.commit()
                
        except Exception, ex:
            msg =  'Error in committing algo/dataset pair to DBS\n'
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            myThread.transaction.rollback()
            raise Exception(msg)

        #Second step: commit files that need committing
        #myThread.transaction.begin()
        unsortedFileList = self.dbinterface.findUploadableFiles()
        fileList = sortByDAS(input = unsortedFileList)
        #myThread.transaction.commit()
        # Should now have a whole bunch of information
        for datasetAlgo in fileList:
            
            try:
                #myThread.transaction.begin()
                
                # Dataset/Algo info should be uniform
                # Just take from the first file
                files = []
                algo    = createAlgoFromInfo(info = fileList[datasetAlgo][0])
                dataset = createDatasetFromInfo(info = fileList[datasetAlgo][0])

                files = addToBuffer.loadDBSBufferFilesBulk(fileObjs = fileList[datasetAlgo])
                
                for f in files:
                    fileLFNs.append(f["lfn"])



                myThread.transaction.begin()
                    
                # Now that you have the files, insert them as a list
                if len(files) > 0:
                    logging.info('Preparing to insert %i files' % (len(files)))
                    affectedBlocks = self.dbswriter.insertFilesForDBSBuffer(files = files,
                                                                            procDataset = dict(dataset), 
                                                                            algos = [algo],
                                                                            jobType = "NotMerge",
                                                                            insertDetectorData = False, 
                                                                            maxFiles = self.DBSMaxFiles,
                                                                            maxSize = self.DBSMaxSize, 
                                                                            timeOut = self.DBSBlockTimeout)
                    logging.info('Have inserted files and received back blocks %s' %(affectedBlocks))
                    for block in affectedBlocks:
                        info = block['StorageElementList']
                        locations = []
                        for loc in info:
                            locations.append(loc['Name'])
                            self.dbinterface.setBlockStatus(block['Name'], locations,
                                                            block['OpenForWriting'],
                                                            int(block['CreationDate']))
                        if "files" in block.keys():
                            for file in block["files"]:
                                setBlock.execute(lfn = file, blockName = block['Name'],
                                                 conn = myThread.transaction.conn,
                                                 transaction = myThread.transaction)


                    # Update the file status, and then recount UnMigrated Files
                    dbinterface.updateFilesStatus(fileLFNs, "InDBS")
                    
                    # And we're done
                    myThread.transaction.commit()

                    
            except Exception, ex:
                msg =  'Error in committing files to DBS\n'
                msg += str(ex)
                msg += str(traceback.format_exc())
                msg += '\n\n'
                logging.error(msg)
                myThread.transaction.rollback()
                # Check that algo actually got to DBS
                algo    = createAlgoFromInfo(info = fileList[datasetAlgo][0])
                addToBuffer.addDataset(datasetAlgo, 0)
                addToBuffer.updateAlgo(algo, 0)
                raise Exception(msg)
                    
            
                


        # And we're done.  Wrap up the dataset-algo stuff and the blocks and go home
        try:
            myThread.transaction.begin()

            # Now it's time to do a block check and look for open blocks that we should timeout
            blocks = dbinterface.findOpenBlocks()

            for block in blocks:
                if time.time() - block['create_time'] > self.DBSBlockTimeout:
                    timedOut = self.dbswriter.manageFileBlock(block['blockname'], maxFiles = self.DBSMaxFiles,
                                                              maxSize = self.DBSMaxSize, timeOut = self.DBSBlockTimeout)
                    if timedOut:
                        dbinterface.setBlockStatus(block['blockname'], locations = None,
                                                   openStatus = 0, time = block['create_time'])

            myThread.transaction.commit()

        except Exception, ex:
            msg =  'Error in committing files to DBS\n'
            msg += str(ex)
            logging.error(msg)
            myThread.transaction.rollback()
            raise Exception(msg)


        return
        
#        for dataset in datasets:
#            #If we're here, then we have a dataset that needs to be uploaded.
#            #First task, are the algos registered?
#            logging.debug('Have begun to process dataset %s' %(str(dataset['ID'])))
#            algos = self.dbinterface.findAlgos(dataset)
#
#            #Necessary for creating Process Datasets
#            dataset['Conditions'] = None
#
#            newAlgos = []
#
#            for algo in algos:
#                #if algo['InDBS'] == 0:
#                #Then we have an algo that's not in DBS.  It needs to go there
#                logging.debug('About to insert algo into DBS')
#                newAlgos.append(DBSWriterObjects.createAlgorithm(dict(algo), configMetadata = None, apiRef = self.dbsapi))
#                #Algo should be in DBS now.
#                addToBuffer.updateAlgo(algo, 1)
#
#
#                    
#            #Now all the algos should be there, so we can create the dataset
#            #I'm unhappy about this because I don't know how to put a dataset in more then one algo
#            logging.debug('About to begin entering primary, processed datasets into DBS')
#            primary = DBSWriterObjects.createPrimaryDataset(datasetInfo = dataset, apiRef = self.dbsapi)
#            logging.debug('Created Primary Dataset')
#            processed = DBSWriterObjects.createProcessedDataset(primaryDataset = primary, algorithm = newAlgos, \
#                                                                datasetInfo = dataset, apiRef = self.dbsapi)
#            logging.debug('Created Processed Dataset')
#            addToBuffer.addDataset(dataset, 1)
#
#            #Once the registration is done, you need to upload the individual files
#            file_ids = self.dbinterface.findUploadableFiles(dataset, self.uploadFileMax)
#            logging.debug('Have retrieved %i files from dataset %s' %(len(file_ids), str(dataset['ID'])))
#            
#    	    files=[]
#    	    #Making DBSBufferFile objects for easy manipulation
#            for an_id in file_ids:
#                logging.debug('Beginning to process file %s for dataset %s' %(str(an_id), str(dataset['ID'])))
#                file = DBSBufferFile(id=an_id['ID'])
#                file.load(parentage=1)
#                fileLFNs.append(file["lfn"])
#                #Now really stupid stuff has to happen.
#                initSet = file['locations']
#                locations = set()
#                for loc in initSet:
#                    locations.add(str(loc))
#                file['locations'] = locations
#                files.append(file)
#                logging.info('I have prepared the file %s for uploading to DBS' %(an_id))
#
#            #Now that you have the files, insert them as a list
#            if len(files) > 0:
#                logging.debug('Preparing to insert %i files' %(len(files)))
#            	affectedBlocks = self.dbswriter.insertFilesForDBSBuffer(files = files, procDataset = dict(dataset), \
#                                                                        algos = algos, jobType = "NotMerge", insertDetectorData = False, \
#                                                                        maxFiles = self.DBSMaxFiles, maxSize = self.DBSMaxSize, \
#                                                                        timeOut = self.DBSBlockTimeout)
#                logging.debug('Have inserted files and received back blocks %s' %(affectedBlocks))
#                for block in affectedBlocks:
#                    info = block['StorageElementList']
#                    locations = []
#                    for loc in info:
#                        locations.append(loc['Name'])
#                    self.dbinterface.setBlockStatus(block['Name'], locations, block['OpenForWriting'], int(block['CreationDate']))
#                    if "files" in block.keys():
#                        for file in block["files"]:
#                            setBlock.execute(lfn = file, blockName = block['Name'], conn = myThread.transaction.conn, transaction = myThread.transaction)
#
#
#                #Update the file status, and then recount UnMigrated Files
#            	dbinterface.updateFilesStatus(fileLFNs, "InDBS")



#        #Now it's time to do a block check
#        blocks=dbinterface.findOpenBlocks()
#        for block in blocks:
#            if time.time() - block['create_time'] > self.DBSBlockTimeout:
#                timedOut = self.dbswriter.manageFileBlock(block['blockname'], maxFiles = self.DBSMaxFiles,
#                                                          maxSize = self.DBSMaxSize, timeOut = self.DBSBlockTimeout)
#                if timedOut:
#                    dbinterface.setBlockStatus(block['blockname'], locations = None, openStatus = 0, time = block['create_time'])
#
#
#        return


    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        
    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions. Wraps in transaction.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        self.uploadDatasets()




