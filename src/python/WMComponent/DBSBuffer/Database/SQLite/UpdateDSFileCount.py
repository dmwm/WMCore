#!/usr/bin/env python
"""
_DBSBuffer.UpdateDSFileCount_

Add a new file to DBS Buffer

"""
__revision__ = "$Id: UpdateDSFileCount.py,v 1.1 2009/05/14 16:18:57 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

from WMComponent.DBSBuffer.Database.MySQL.UpdateDSFileCount import UpdateDSFileCount as MySQLUpdateDSFileCount

class UpdateDSFileCount(MySQLUpdateDSFileCount):
    """
    _DBSBuffer.UpdateDSFileCount_

    Add a new file to DBS Buffer
    
    """

    sql = """
UPDATE dbsbuffer_dataset
   SET UnMigratedFiles = (SELECT count(*) FROM dbsbuffer_file f
	WHERE f.status  = 'NOTUPLOADED'
	AND   f.dataset = ID
   )
   WHERE ID IN (SELECT ID from dbsbuffer_dataset WHERE Path=:path)
	"""

    def GetUpdateDSFileCountDialect(self):

        return 'SQLite'
