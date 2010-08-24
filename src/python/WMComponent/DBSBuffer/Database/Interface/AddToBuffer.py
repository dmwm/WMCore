#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: AddToBuffer.py,v 1.1 2008/10/20 19:36:06 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory


class AddToBuffer:

    def __init__(self, logger=None, dbfactory = None):
	self.dbfactory = dbfactory
	pass
    
    def addFile(self, file):

	"""NOT doing anything at the moment"""
	return 

        # Add the file to the buffer (API Call)
	return WMFactory(classname='NewFile').execute(file)	


    def addDataset(self, dataset):
	# Add the dataset to the buffer (API Call)

	myThread = threading.currentThread()

	factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
	newDS = factory.loadObject("NewDataset")
	return newDS.execute(dataset=dataset, conn = myThread.transaction.conn, transaction=myThread.transaction)


