#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_failed
                    WHERE file = :fileid AND subscription = :subscription)"""    

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        binds = self.getBinds(subscription=subscription, fileid=file)
        self.dbi.processData(self.sql, binds, 
                             conn = conn, transaction = transaction)
        return
