#!/usr/bin/env python
"""
_Exists_

MySQL implementation of JobGroup.Exists
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = "SELECT id FROM wmbs_jobgroup WHERE guid = :guid"

    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]

    def getBinds(self, uid=None):
        return self.dbi.buildbinds(self.dbi.makelist(uid), "guid")

    def execute(self, uid, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(uid),
                         conn = conn, transaction = transaction)
        return self.format(result)
