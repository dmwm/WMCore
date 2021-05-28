#!/usr/bin/env python
"""
_AddLocation_

MySQL implementation of DBSBufferFiles.AddLocation
"""

from builtins import str, bytes
from WMCore.Database.DBFormatter import DBFormatter

class AddLocation(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_location (pnn)
               VALUES (:location)"""

    def execute(self, siteName, conn = None, transaction = False):
        binds = []
        if isinstance(siteName, (str, bytes)):
            siteName = [siteName]

        for site in siteName:
            binds.append({"location": site})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = True)
        return
