#!/usr/bin/env python
"""
_LoadIDFromUID__

MySQL implementation of JobGroup.LoadIDFromUID
"""

__all__ = []
__revision__ = "$Id: LoadIDFromUID.py,v 1.1 2009/01/06 15:54:49 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadIDFromUID(DBFormatter):
    sql = "select id from wmbs_jobgroup where uid = :guid"

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]        

    def execute(self, uid, conn = None, transaction = False):
        binds = self.getBinds(guid = uid)
        result = self.dbi.processData(self.sql, binds)
        return self.format(result)
