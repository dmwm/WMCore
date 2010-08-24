#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: NewFile.py,v 1.10 2008/12/30 17:47:06 afaq Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class NewFile(DBFormatter):

	sql = """INSERT INTO dbsbuffer_file (WMBS_File_ID, Dataset, FileStatus) 
			values (
				(select ID from wmbs_file_details where lfn=:lfn), 
				(select ID from dbsbuffer_dataset where Path=:path),
				:status 
			)"""

	sqlUpdateDS = """UPDATE dbsbuffer_dataset as A
   				inner join (
      				select * from dbsbuffer_dataset
      					where Path=:path
   				) as B on A.ID = B.ID
				SET A.UnMigratedFiles = A.UnMigratedFiles + 1"""

	def __init__(self):
        	myThread = threading.currentThread()
        	DBFormatter.__init__(self, myThread.logger, myThread.dbi)
		
	def getBinds(self, file=None, dataset=None):
	    	# binds a list of dictionaries
	   	binds =  { 'lfn': file['LFN'],
			'path': '/'+dataset['PrimaryDataset']+'/'+ \
					dataset['ProcessedDataset']+'/'+ \
					dataset['DataTier'],
			'status' : 'NOTUPLOADED'
			}
	    	return binds
	   
	def format(self, result):
		return True

	def execute(self, file=None, dataset=None, conn=None, transaction = False):
		
		binds = self.getBinds(file, dataset)

		try:
			result = self.dbi.processData(self.sql, binds, 
                        		conn = conn, transaction = transaction)
			#Update the File Count in Dataset
			result = self.dbi.processData(self.sqlUpdateDS, binds,
                        		conn = conn, transaction = transaction)
		except Exception, ex:
			if ex.__str__().find("Duplicate entry") != -1 :
				pass
			else:
				raise ex



			
