#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""

__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.4 2009/03/20 14:29:18 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class CompleteFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_complete WHERE file = :fileid)"""

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file),
                             conn = conn, transaction = transaction)
        return
