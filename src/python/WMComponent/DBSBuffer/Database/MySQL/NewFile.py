#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: NewFile.py,v 1.8 2008/11/18 23:25:29 afaq Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "anzar@fnal.gov"

import threading
import base64
import exceptions

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
		
	def getBinds(self, file=None, dataset=None):
	    	# binds a list of dictionaries
	   	binds =  { 'lfn': file['LFN'],
			'path': '/'+dataset['PrimaryDataset']+'/'+ \
					dataset['ProcessedDataset']+'/'+ \
					dataset['DataTier'],
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

	def execute(self, file=None, dataset=None, conn=None, transaction = False):
		
		binds = self.getBinds(file, dataset)

		try:
			result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
			#Update the File Count in Dataset
			result = self.dbi.processData(self.sqlUpdateDS, binds,
                         conn = conn, transaction = transaction)
			print "I am here"
		except Exception, ex:
			if ex.__str__().find("Duplicate entry") != -1 :
				pass
			else:
				raise ex
			