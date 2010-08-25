#!/usr/bin/env python
"""
_UploadToDBS_

APIs related to adding file to DBS

"""
__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: UploadToDBS.py,v 1.8 2009/08/12 23:11:48 meloam Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory

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

    def updateFilesStatus(self, files):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("UpdateFilesStatus")
        newDS.execute(files=files, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
    def setBlockStatus(self, blockname, locations):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("SetBlockStatus")
        newDS.execute(block=blockname, locations=locations, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return



