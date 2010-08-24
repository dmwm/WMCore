#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: NewFile.py,v 1.7 2008/11/07 03:49:04 afaq Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
from WMCore.Database.DBFormatter import DBFormatter


#TODO:
# base64 encoding the Run/Lumi INFO, may come up with a beter way in future
#base64.binascii.b2a_base64(str(file.getLumiSections()))
#base64.decodestring('')

class NewFile(DBFormatter):

	sql = """INSERT INTO dbsbuffer_file (LFN, Dataset, Checksum, NumberOfEvents, FileSize, RunLumiInfo, FileStatus, SEName)
		values (:lfn, (select ID from dbsbuffer_dataset where Path=:path), :checksum, :events, :size, :runinfo, :status, :sename)"""

	sqlUpdateDS = """UPDATE dbsbuffer_dataset as A
   				inner join (
      				select * from dbsbuffer_dataset
      					where Path=:path
   				) as B on A.ID = B.ID
				SET A.UnMigratedFiles = A.UnMigratedFiles + 1"""

	#sqlUpdateDS = """UPDATE dbsbuffer_dataset SET UnMigratedFiles = UnMigratedFiles + 1 WHERE ID = (select ID from dbsbuffer_dataset where Path=:path)"""

    	def __init__(self):
        	myThread = threading.currentThread()
        	DBFormatter.__init__(self, myThread.logger, myThread.dbi)
		
	def getBinds(self, file=None):
	    	# binds a list of dictionaries
	   	binds =  { 'lfn': file['LFN'],
			'path': '/'+file.dataset[0]['PrimaryDataset']+'/'+ \
					file.dataset[0]['ProcessedDataset']+'/'+ \
					file.dataset[0]['DataTier'],
			'checksum' : file.checksums['cksum'],
			'events' : file['TotalEvents'],
			'size' : file['Size'],
			'runinfo' : base64.binascii.b2a_base64(str(file.getLumiSections())),
			'status' : 'NOTUPLOADED',
			'sename' : file['SEName']
			}
	    	return binds
	   
	def format(self, result):
		return True

	def execute(self, file=None, conn=None, transaction = False):
		binds = self.getBinds(file)
        
        	result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
		
		result = self.dbi.processData(self.sqlUpdateDS, binds,
                         conn = conn, transaction = transaction)
        	return 
        	#return self.format(result)

