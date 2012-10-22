#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""

from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, fileid)
               VALUES(:subscription, :fileid)"""

    delAcq = """DELETE FROM wmbs_sub_files_acquired
                WHERE subscription = :subscription AND
                      fileid = :fileid"""

    delAva = """DELETE FROM wmbs_sub_files_available
                WHERE subscription = :subscription AND
                      fileid = :fileid"""

    delCom = """DELETE FROM wmbs_sub_files_complete
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
        self.dbi.processData(self.delCom, binds, conn = conn,
                             transaction = transaction)
        return
