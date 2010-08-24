#!/usr/bin/env python
"""
_LoadIDFromUID__

MySQL implementation of JobGroup.LoadIDFromUID
"""

__all__ = []
__revision__ = "$Id: LoadIDFromUID.py,v 1.2 2009/01/12 19:26:03 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadIDFromUID(DBFormatter):
    sql = "select id from wmbs_jobgroup where uid = :guid"

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]        

    def execute(self, uid, conn = None, transaction = False):
        binds = self.getBinds(guid = uid)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
