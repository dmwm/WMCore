#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.4 $"
__revision__ = "$Id: AddToBuffer.py,v 1.4 2008/11/18 23:25:31 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory


class AddToBuffer:

    def __init__(self, logger=None, dbfactory = None):
        pass
    
    def addFile(self, file, dataset):
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newFile = factory.loadObject("NewFile")
        # Add the file to the buffer (API Call)
        newFile.execute(file=file, dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return	
    
    def addDataset(self, dataset, algoInDBS):
        # Add the dataset to the buffer (API Call)
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("NewDataset")
        newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
    def addAlgo(self, dataset):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("NewAlgo")
        newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return

    def updateAlgo(self, dataset, psethash):
        # Add the algo to the buffer (API Call)
        # dataset object contains the algo information
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        newDS = factory.loadObject("UpdateAlgo")
        newDS.execute(dataset=dataset, psethash=psethash, conn = myThread.transaction.conn, transaction=myThread.transaction)
        myThread.transaction.commit()
        return
    
