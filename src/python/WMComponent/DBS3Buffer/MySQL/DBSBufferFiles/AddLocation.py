#!/usr/bin/env python
"""
_AddLocation_

MySQL implementation of DBSBufferFiles.AddLocation
"""

from WMCore.Database.DBFormatter import DBFormatter

class AddLocation(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_location (pnn)
               VALUES (:location)"""

    def execute(self, siteName, conn = None, transaction = False):
        binds = []
        binds.append({"location": siteName})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = True)
        return
