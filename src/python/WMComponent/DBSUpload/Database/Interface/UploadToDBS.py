#!/usr/bin/env python
"""
_UploadToDBS_

APIs related to adding file to DBS

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: UploadToDBS.py,v 1.1 2008/10/22 17:20:49 afaq Exp $"
__author__ = "anzar@fnal.gov"

import logging
import threading
from WMCore.WMFactory import WMFactory

class UploadToDBS:

    def __init__(self, logger=None, dbfactory = None):
        pass
    
    def findUploadableDatasets(self):
        myThread = threading.currentThread()
        factory = WMFactory("dbsUpload", "WMComponent.DBSBuffer.Database."+ \
                        myThread.dialect)
        findDatasets = factory.loadObject("FindUploadableDatasets")
        # Add the file to the buffer (API Call)
        return findDatasets.execute(conn = myThread.transaction.conn, transaction=myThread.transaction)    


