#!/usr/bin/env python
"""
_SetLocation_

MySQL implementation of DBSBuffer.SetLocation
"""

__revision__ = "$Id: SetLocation.py,v 1.4 2009/10/22 14:55:57 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetLocation(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_location (filename, location)
               VALUES (:fileid, :locationid)"""
    
    def execute(self, binds, conn = None, transaction = None):
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
