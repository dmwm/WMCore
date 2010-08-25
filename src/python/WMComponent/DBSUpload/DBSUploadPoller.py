#!/usr/bin/env python
"""
The DBSUpload algorithm
"""

__revision__ = "$Id: DBSUploadPoller.py,v 1.15 2009/12/07 21:55:09 mnorman Exp $"
__version__ = "$Revision: 1.15 $"

import threading
import logging
import re
import os
import time
from sets import Set

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

class DBSUploadPoller(BaseWorkerThread):
    """
    Handles poll-based DBSUpload

    """


    def __init__(self, config, dbsconfig = None):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config     = config
        self.dbsurl     = self.config.DBSUpload.dbsurl
        self.dbsversion = self.config.DBSUpload.dbsversion
        self.uploadFileMax = self.config.DBSUpload.uploadFileMax

        self.DBSMaxFiles     = self.config.DBSUpload.DBSMaxFiles
        self.DBSMaxSize      = self.config.DBSUpload.DBSMaxSize
        self.DBSBlockTimeout = self.config.DBSUpload.DBSBlockTimeout

        if dbsconfig == None:
            self.dbsconfig = config
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()

        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        self.dbinterface=factory.loadObject("UploadToDBS")

        bufferFactory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
        self.addToBuffer=bufferFactory.loadObject("AddToBuffer")
        
        

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

        
        #Get datasets out of DBS
        datasets=dbinterface.findUploadableDatasets()
        logging.debug('Have retrieved %i datasets from DBSBuffer' %(len(datasets)))

        myThread = threading.currentThread()

        bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        setBlock      = bufferFactory(classname = "DBSBufferFiles.SetBlock")

        #Now check them
        fileLFNs = []
        for dataset in datasets:
            #If we're here, then we have a dataset that needs to be uploaded.
            #First task, are the algos registered?
            logging.debug('Have begun to process dataset %s' %(str(dataset['ID'])))
            algos = dbinterface.findAlgos(dataset)

            #Necessary for creating Process Datasets
            dataset['Conditions'] = None

            newAlgos = []

            for algo in algos:
                #if algo['InDBS'] == 0:
                #Then we have an algo that's not in DBS.  It needs to go there
                logging.debug('About to insert algo into DBS')
                newAlgos.append(DBSWriterObjects.createAlgorithm(dict(algo), configMetadata = None, apiRef = self.dbsapi))
                #Algo should be in DBS now.
                addToBuffer.updateAlgo(algo, 1)


                    
            #Now all the algos should be there, so we can create the dataset
            #I'm unhappy about this because I don't know how to put a dataset in more then one algo
            logging.debug('About to begin entering primary, processed datasets into DBS')
            primary = DBSWriterObjects.createPrimaryDataset(datasetInfo = dataset, apiRef = self.dbsapi)
            logging.debug('Created Primary Dataset')
            processed = DBSWriterObjects.createProcessedDataset(primaryDataset = primary, algorithm = newAlgos, \
                                                                datasetInfo = dataset, apiRef = self.dbsapi)
            logging.debug('Created Processed Dataset')
            addToBuffer.addDataset(dataset, 1)

            #Once the registration is done, you need to upload the individual files
            file_ids = dbinterface.findUploadableFiles(dataset, self.uploadFileMax)
            logging.debug('Have retrieved %i files from dataset %s' %(len(file_ids), str(dataset['ID'])))
            
    	    files=[]
    	    #Making DBSBufferFile objects for easy manipulation
            for an_id in file_ids:
                logging.debug('Beginning to process file %s for dataset %s' %(str(an_id), str(dataset['ID'])))
                file = DBSBufferFile(id=an_id['ID'])
                file.load(parentage=1)
                fileLFNs.append(file["lfn"])
                #Now really stupid stuff has to happen.
                initSet = file['locations']
                locations = Set()
                for loc in initSet:
                    locations.add(str(loc))
                file['locations'] = locations
                files.append(file)
                logging.info('I have prepared the file %s for uploading to DBS' %(an_id))

            #Now that you have the files, insert them as a list
            if len(files) > 0:
                logging.debug('Preparing to insert %i files' %(len(files)))
            	affectedBlocks = self.dbswriter.insertFilesForDBSBuffer(files = files, procDataset = dict(dataset), \
                                                                        algos = algos, jobType = "NotMerge", insertDetectorData = False, \
                                                                        maxFiles = self.DBSMaxFiles, maxSize = self.DBSMaxSize, \
                                                                        timeOut = self.DBSBlockTimeout)
                logging.debug('Have inserted files and received back blocks %s' %(affectedBlocks))
                for block in affectedBlocks:
                    info = block['StorageElementList']
                    locations = []
                    for loc in info:
                        locations.append(loc['Name'])
                    dbinterface.setBlockStatus(block['Name'], locations, block['OpenForWriting'], int(block['CreationDate']))
                    if "files" in block.keys():
                        myThread.transaction.begin()
                        for file in block["files"]:
                            setBlock.execute(lfn = file, blockName = block['Name'])
                        myThread.transaction.commit()


                #Update the file status, and then recount UnMigrated Files
            	dbinterface.updateFilesStatus(fileLFNs, "InDBS")



        #Now it's time to do a block check
        blocks=dbinterface.findOpenBlocks()
        for block in blocks:
            if time.time() - block['create_time'] > self.DBSBlockTimeout:
                timedOut = self.dbswriter.manageFileBlock(block['blockname'], maxFiles = self.DBSMaxFiles,
                                                          maxSize = self.DBSMaxSize, timeOut = self.DBSBlockTimeout)
                if timedOut:
                    dbinterface.setBlockStatus(block['blockname'], locations = None, openStatus = 0, time = block['create_time'])


        return


    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        
    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions. Wraps in transaction.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()
        try:
            #myThread.transaction.begin()
            self.uploadDatasets()
            #myThread.transaction.commit()
        except:
            #myThread.transaction.rollback()
            raise
