#!/usr/bin/env python
"""
_ExistsByID_

MySQL implementation of Files.ExistsByID
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class ExistsByID(DBFormatter):
    sql = """select id from wmbs_fileset
             where id = :id"""

    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return int(result[0][0])

    def getBinds(self, id):
        return self.dbi.buildbinds(self.dbi.makelist(id), "id")

    def execute(self, id, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(id),
                                      conn = conn, transaction = transaction)
        return self.format(result)
