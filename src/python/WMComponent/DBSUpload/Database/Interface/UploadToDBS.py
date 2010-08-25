#!/usr/bin/env python
"""
_UploadToDBS_

APIs related to adding file to DBS

"""
__version__ = "$Revision: 1.11 $"
__revision__ = "$Id: UploadToDBS.py,v 1.11 2009/12/07 18:57:58 mnorman Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory

class UploadToDBS:

    def __init__(self, logger=None, dbfactory = None):
        pass
    
    def findUploadableDatasets(self):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findDatasets = factory.loadObject("FindUploadableDatasets")
        # Add the file to the buffer (API Call)
        results = findDatasets.execute(conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return results  

    def findUploadableFiles(self, dataset, maxfiles=10):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findFiles = factory.loadObject("FindUploadableFiles")
        # Add the file to the buffer (API Call)
        
        #results = findFiles.execute(conn = myThread.transaction.conn, transaction=myThread.transaction)
        results = findFiles.execute(datasetInfo=dataset, maxfiles=maxfiles, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return results  

    def findAlgos(self, dataset):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findAlgos = factory.loadObject("FindAlgos")
        # Add the file to the buffer (API Call)
        
        #results = findFiles.execute(conn = myThread.transaction.conn, transaction=myThread.transaction)
        results = findAlgos.execute(datasetInfo=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return results  

    def updateFilesStatus(self, files, status):
        """
        _updateFileStatus_

        Update the status of a series of files in DBSBuffer.  The files must be
        passed in as a list of LFNs.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                             logger = myThread.logger,
                             dbinterface = myThread.dbi)
        statusAction = factory(classname = "DBSBufferFiles.SetStatus")
        statusAction.execute(lfns = files, status = status,
                             conn = myThread.transaction.conn,
                             transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
    def setBlockStatus(self, block, locations, openStatus = 0, time = 0):
        """
        _setBlockStatus_

        Adds information about the block from DBSWriter to the database
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("SetBlockStatus")
        newDS.execute(block = block, locations = locations, open_status = openStatus, time = 0, \
                      conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return

        
