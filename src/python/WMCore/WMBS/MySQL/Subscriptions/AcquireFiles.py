#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.AcquireFiles
"""

__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.7 2009/03/20 14:29:18 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class AcquireFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_acquired (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_acquired WHERE file = :fileid)"""

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file),
                             conn = conn, transaction = transaction)
        return
