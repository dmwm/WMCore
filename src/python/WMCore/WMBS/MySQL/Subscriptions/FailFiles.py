#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""

__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.5 2009/03/23 19:05:09 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_failed
                    WHERE file = :fileid AND subscription = subscription)"""    

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file),
                             conn = conn, transaction = transaction)
        return
