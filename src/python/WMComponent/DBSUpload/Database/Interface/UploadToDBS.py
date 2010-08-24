#!/usr/bin/env python
"""
_UploadToDBS_

APIs related to adding file to DBS

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: UploadToDBS.py,v 1.2 2008/10/23 19:18:36 afaq Exp $"
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

    def findUploadableFiles(self, dataset):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database."+ \
                        myThread.dialect)
        findFiles = factory.loadObject("FindUploadableFiles")
        # Add the file to the buffer (API Call)
        
        #results = findFiles.execute(conn = myThread.transaction.conn, transaction=myThread.transaction)
        results = findFiles.execute(datasetInfo=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return results  


