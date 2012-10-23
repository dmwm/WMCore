#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Jobs.Exists
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = "select id from wmbs_job where name = :name"

    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]

    def getBinds(self, name):
        return self.dbi.buildbinds(self.dbi.makelist(name), "name")

    def execute(self, name, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(name),
                                      conn = conn, transaction = transaction)
        return self.format(result)
