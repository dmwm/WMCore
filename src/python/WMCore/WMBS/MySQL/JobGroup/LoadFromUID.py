#!/usr/bin/env python
"""
_LoadFromUID_

MySQL implementation of JobGroup.LoadFromUID
"""

__all__ = []
__revision__ = "$Id: LoadFromUID.py,v 1.1 2009/01/14 16:35:26 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromUID(DBFormatter):
    sql = """SELECT id, subscription, uid, output, last_update
             FROM wmbs_jobgroup WHERE uid = :guid"""

    def execute(self, uid, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"guid": uid}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)[0]
