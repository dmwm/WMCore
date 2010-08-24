#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: NewFile.py,v 1.2 2008/10/15 12:53:37 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"


from WMCore.Database.DBFormatter import DBFormatter

		"""CREATE TABLE dbsbuffer_file
			( 
			    ID                    BIGINT UNSIGNED not null auto_increment,
			    LFN                   varchar(500)      unique not null,
			    Dataset 		  BIGINT UNSIGNED   not null,
			    BlockName             varchar(500)      not null,
			    Checksum              varchar(100)      not null,
			    NumberOfEvents        BIGINT UNSIGNED   not null,
			    FileSize              BIGINT UNSIGNED   not null,
			    FileStatus            BIGINT UNSIGNED,
			    FileType              BIGINT UNSIGNED,
			    RunLumiInfo           varchar(500),
			    LastModificationDate  BIGINT,
	



class NewFile(DBFormatter):

    sql = """INSERT INTO dbsbuffer_file (LFN, Dataset, Checksum, NumberOfEvents, FileSize, RunLumiInfo)
		values (:lfn, (select ID from Dataset where Path=:path), :checksum, :events, :size, :runinfo)"""

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

