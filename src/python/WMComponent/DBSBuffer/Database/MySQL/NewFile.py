#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: NewFile.py,v 1.1 2008/10/02 19:57:13 afaq Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"


from WMCore.Database.DBFormatter import DBFormatter

class NewFile(DBFormatter):

    sql = """INSERT INTO File (LFN, Path, BlockName, Checksum, NumberOfEvents, FileSize)
		values (:lfn, :path, :block, :checksum, :events, :size)"""

    def getBinds(self, file=None):
	# binds a list of dictionaries
	binds =  { 'lfn': file['LFN'],
			'path': file['path'],
			'block' : file['block'],
			'checksum' : file['checksum'],
			'events' : file['events'],
			'events' : file['events']
		}

        return binds
    
    def format(self, result):
        return True
    
    def execute(self, file=None, transaction = False):
        binds = self.getBinds(files)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)


