#!/usr/bin/env python
"""
_UploadToDBS_

APIs related to adding file to DBS

"""
__version__ = "$Revision: 1.13 $"
__revision__ = "$Id: UploadToDBS.py,v 1.13 2010/02/24 21:42:04 mnorman Exp $"

import logging
import threading
from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMConnectionBase import WMConnectionBase


class UploadToDBS (WMConnectionBase):
    """
    APIs relating to adding files, etc. to DBS

    """

    def __init__(self, logger=None, dbfactory = None):
        pass
    
    def findUploadableDatasets(self):
        """
        Call to findUploadableDatasets

        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findDatasets = factory.loadObject("FindUploadableDatasets")
        # Add the file to the buffer (API Call)
        results = findDatasets.execute(conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return results  

    def findUploadableFiles(self):
        """
        Call to findUploadableFiles

        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findFiles = factory.loadObject("FindUploadableFiles")
        # Add the file to the buffer (API Call)
        
        #results = findFiles.execute(conn = self.getDBConn(), transaction=self.existingTransaction())
        results = findFiles.execute(conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return results  

    def findAlgos(self, dataset):
        """
        Call to findAlgos

        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findAlgos = factory.loadObject("FindAlgos")
        # Add the file to the buffer (API Call)
        
        #results = findFiles.execute(conn = self.getDBConn(), transaction=self.existingTransaction())
        results = findAlgos.execute(datasetInfo=dataset, conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return results  

    def updateFilesStatus(self, files, status):
        """
        _updateFileStatus_

        Update the status of a series of files in DBSBuffer.  The files must be
        passed in as a list of LFNs.
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        statusAction = factory(classname = "DBSBufferFiles.SetStatus")
        statusAction.execute(lfns = files, status = status,
                             conn = self.getDBConn(),
                             transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return
    
    def setBlockStatus(self, block, locations, openStatus = 0, time = 0):
        """
        _setBlockStatus_

        Adds information about the block from DBSWriter to the database
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("SetBlockStatus")
        newDS.execute(block = block, locations = locations, open_status = openStatus, time = time, \
                      conn = self.getDBConn(), transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def findOpenBlocks(self):
        """
        _findOpenBlocks_
        
        This should find all blocks.
        """

        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()
        
        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        openBlocks = factory(classname = "GetOpenBlocks")

        result = openBlocks.execute(conn = self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return result


    def setDatasetAlgo(self, datasetAlgoInfo, inDBS = 1):
        """
        _setDatasetAlgo_

        Sets the datasetAlgo status to uploaded in_dbs
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        factory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        setDA = factory(classname = "SetDatasetAlgo")
        setDA.execute(datasetAlgo = datasetAlgoInfo['DAS_ID'], inDBS = inDBS,
                      conn = self.getDBConn(),
                      transaction=self.existingTransaction())


        self.commitTransaction(existingTransaction)

        return
