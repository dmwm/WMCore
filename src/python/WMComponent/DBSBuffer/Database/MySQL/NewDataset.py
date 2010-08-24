#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer

"""
__revision__ = "$Id: NewDataset.py,v 1.4 2008/10/20 20:03:26 afaq Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "anzar@fnal.gov"

import threading
from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy.exceptions import IntegrityError



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

	try:
        	result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

	except IntegrityError, ex:
		if ex.__str__().find("Duplicate entry") != -1 :
			#print "DUPLICATE: so what !!"
			return
		else:
			raise ex
        return 
        #return self.format(result)






