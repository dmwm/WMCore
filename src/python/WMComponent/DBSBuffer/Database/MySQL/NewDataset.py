#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer

"""
__revision__ = "$Id: NewDataset.py,v 1.3 2008/10/20 19:22:04 afaq Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter

class NewDataset(DBFormatter):
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


    sql = """INSERT INTO dbsbuffer_dataset (Path)
                values (:path)"""

    def getBinds(self, dataset=None):
        # binds a list of dictionaries

        binds =  { 
                        'path': "/"+dataset['PrimaryDataset']+ \
                                "/"+dataset['ProcessedDataset']+ \
                                "/"+dataset['DataTier']
                }
        return binds

    def format(self, result):
        return True

    """

    def execute(self, sqlStr, args):
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.


        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args) 

    """

    def execute(self, dataset=None, conn=None, transaction = False):
        binds = self.getBinds(dataset)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)





