#!/usr/bin/env python
"""
_addToBuffer_

APIs related to adding file to DBS Buffer

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: addToBuffer.py,v 1.1 2008/10/02 19:57:13 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging

class AddToBuffer:

    def __init__(self, file, logger=None, dbfactory = None):

    
    def addFile(self, file):
        # Add the file to the buffer (API Call)
	return self.daofactory(classname='File.NewFile').execute(file)	
