#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: AddToBuffer.py,v 1.2 2008/10/20 22:05:08 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory


class AddToBuffer:

    def __init__(self, logger=None, dbfactory = None):
	pass
    
    def addFile(self, file):
	myThread = threading.currentThread()
	factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
	newFile = factory.loadObject("NewFile")
        # Add the file to the buffer (API Call)
	return newFile.execute(file=file, conn = myThread.transaction.conn, transaction=myThread.transaction)	


    def addDataset(self, dataset):
	# Add the dataset to the buffer (API Call)

	myThread = threading.currentThread()
	factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)

	newDS = factory.loadObject("NewDataset")
	return newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)


