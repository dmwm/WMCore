#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""

__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.4 2009/03/20 14:29:18 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_failed WHERE file = :fileid)"""    

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file),
                             conn = conn, transaction = transaction)
        return
