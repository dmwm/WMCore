#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""

__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.5 2009/03/23 19:05:09 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class CompleteFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_complete
                    WHERE file = :fileid AND subscription = :subscription)"""

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file),
                             conn = conn, transaction = transaction)
        return
