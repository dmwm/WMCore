#!/usr/bin/env python
"""
_SetLocation_

MySQL implementation of DBSBuffer.SetLocation
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetLocation(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_file_location (filename, location)
               VALUES (:fileid, :locationid)"""

    def execute(self, binds, conn = None, transaction = None):
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
