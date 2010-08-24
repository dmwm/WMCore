#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.3 2008/11/18 23:25:29 afaq Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter

class FindUploadableDatasets(DBFormatter):
    
    sql = """SELECT * FROM dbsbuffer_dataset WHERE UnMigratedFiles >= 10 """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    """    
    def getBinds(self):
        return {} #Do we need this method at all here ?
            # binds a list of dictionaries
                  
    def format(self, result):
        return True

    def execute(self, conn=None, transaction = False):
        binds = self.getBinds()
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        import pdb
        pdb.set_trace()
        for aRow in result:
            print aRow
        return self.format(result)
    """

