#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Add a new file to DBS Buffer: Oracle version

"""




#This has been updated for use with Oracle


from WMComponent.DBSBuffer.Database.MySQL.UpdateDSFileCount import UpdateDSFileCount as MySQLUpdateDSFileCount

class UpdateDSFileCount(MySQLUpdateDSFileCount):
	"""
	_DBSBuffer.NewFile_
	
	Add a new file to DBS Buffer: Oracle version
	
	"""


	sql = """UPDATE dbsbuffer_dataset a
              SET UnMigratedFiles = (SELECT count(*) FROM dbsbuffer_file f
              WHERE f.status  = 'NOTUPLOADED'
              AND   f.dataset = a.ID)

	"""


#              AND   a.Path    = :path)



	def getBinds(self, datasetInfo=None):
	    	# binds a list of dictionaries

		#This is a dummy because the Oracle command needs no path

		return {}

