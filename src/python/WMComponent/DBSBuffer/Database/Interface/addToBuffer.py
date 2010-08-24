#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: addToBuffer.py,v 1.2 2008/10/10 21:41:00 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging

class AddToBuffer:

    def __init__(self, file, logger=None, dbfactory = None):
	pass
    
    def addFile(self, file):
        # Add the file to the buffer (API Call)
	return self.daofactory(classname='File.NewFile').execute(file)	


    def addDataset(self, dataset):
	# Add the dataset to the buffer (API Call)
	return self.daofactory(classname='File.NewDataset').execute(dataset)





