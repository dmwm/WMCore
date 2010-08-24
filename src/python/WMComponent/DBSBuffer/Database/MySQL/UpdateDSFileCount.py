#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: UpdateDSFileCount.py,v 1.2 2009/01/14 22:07:25 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class UpdateDSFileCount(DBFormatter):

        sql = """UPDATE dbsbuffer_dataset as A
                   inner join (
                      select * from dbsbuffer_dataset
                          where Path=:path
                   ) as B on A.ID = B.ID
                SET A.UnMigratedFiles = (select count(*) from dbsbuffer_file f where f.dataset = B.ID AND f.status = 'NOTUPLOADED')"""

	def __init__(self):
        	myThread = threading.currentThread()
        	DBFormatter.__init__(self, myThread.logger, myThread.dbi)
		
	def getBinds(self, dataset=None):
	    	# binds a list of dictionaries
	   	binds =  { 
			'path': dataset,
			}
	    	return binds
	   
	def format(self, result):
		return True

	def execute(self, dataset=None, conn=None, transaction = False):
		binds = self.getBinds(dataset)
		result = self.dbi.processData(self.sql, binds,
                        		conn = conn, transaction = transaction)



