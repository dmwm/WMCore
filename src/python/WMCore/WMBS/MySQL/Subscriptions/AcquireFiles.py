#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""

from WMCore.Database.DBFormatter import DBFormatter

class AcquireFiles(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_sub_files_acquired (subscription, fileid)
               VALUES (:subscription, :fileid)"""

    availDel = """DELETE FROM wmbs_sub_files_available
                  WHERE subscription = :subscription AND
                        fileid = :fileid"""

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        if isinstance(file, list):
            binds = []
            for fileid in file:
                binds.append({"subscription": subscription, "fileid": fileid})
        else:
            binds = {"subscription": subscription, "fileid": file}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.availDel, binds, conn = conn,
                             transaction = transaction)
        return
