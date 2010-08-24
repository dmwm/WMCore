#!/usr/bin/env python
"""
_DBSUpload.FindAlgos_

Find algos in datasets

"""
__revision__ = "$Id: FindAlgos.py,v 1.1 2008/11/05 01:20:45 afaq Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter

class FindAlgos(DBFormatter):
    
    sql = """SELECT * FROM dbsbuffer_algo where dataset=:dataset"""
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
    
    def getBinds(self, dataset):
        binds =  { 'dataset': dataset['ID']}
        return binds

    def execute(self, datasetInfo=None, conn=None, transaction = False):
        binds = self.getBinds(datasetInfo)
        print "SQL: %s" %(self.sql)
        print "BINDS: %s" %str(binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
    