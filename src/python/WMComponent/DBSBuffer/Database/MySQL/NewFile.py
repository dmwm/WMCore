#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: NewFile.py,v 1.4 2008/10/20 19:22:04 afaq Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "anzar@fnal.gov"

from WMCore.Database.DBFormatter import DBFormatter

class NewFile(DBFormatter):

	sql = """INSERT INTO dbsbuffer_file (LFN, Dataset, Checksum, NumberOfEvents, FileSize, RunLumiInfo)
		values (:lfn, (select ID from Dataset where Path=:path), :checksum, :events, :size, :runinfo)"""
		
	def getBinds(self, file=None):
	    	# binds a list of dictionaries
	   	binds =  { 'lfn': file['LFN'],
						'path': '/'+file.dataset[0]['PrimaryDataset']+'/'+file.dataset[0]['ProcessedDataset']+'/'+file.dataset[0]['DataTier'],
						'checksum' : file.checksums['cksum'],
						'events' : file['TotalEvents'],
						'size' : file['Size'],
						'runinfo' : str(file.getLumiSections())
				}
	    return binds
	   
	def format(self, result):
		return True

	def execute(self, file=None, transaction = False):
		binds = self.getBinds(files)
        
        	result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        	return self.format(result)

