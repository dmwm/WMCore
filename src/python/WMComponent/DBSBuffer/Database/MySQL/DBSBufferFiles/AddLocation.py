#!/usr/bin/env python
"""
_AddLocation_

MySQL implementation of DBSBufferFiles.AddLocation
"""

from WMCore.Database.DBFormatter import DBFormatter

class AddLocation(DBFormatter):
    sql = """INSERT INTO dbsbuffer_location (se_name) 
               SELECT :location AS se_name FROM DUAL WHERE NOT EXISTS
                (SELECT se_name FROM dbsbuffer_location WHERE se_name = :location)"""
    
    def execute(self, siteName, conn = None, transaction = False):
        binds = {"location": siteName}
        self.dbi.processData(self.sql, binds, conn = conn, 
                             transaction = transaction)
        return
