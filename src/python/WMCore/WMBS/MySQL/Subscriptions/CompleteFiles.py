#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""

from WMCore.Database.DBFormatter import DBFormatter

class CompleteFiles(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_sub_files_complete (subscription, fileid)
               VALUES (:subscription, :fileid)"""

    delAcq = """DELETE FROM wmbs_sub_files_acquired
                WHERE subscription = :subscription AND
                      fileid = :fileid"""

    delAva = """DELETE FROM wmbs_sub_files_available
                WHERE subscription = :subscription AND
                      fileid = :fileid"""

    delFai = """DELETE FROM wmbs_sub_files_failed
                WHERE subscription = :subscription AND
                      fileid = :fileid"""

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        binds = self.getBinds(subscription = subscription, fileid = file)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.delAcq, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.delAva, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.delFai, binds, conn = conn,
                             transaction = transaction)

        return
