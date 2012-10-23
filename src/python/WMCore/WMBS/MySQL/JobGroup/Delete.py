#!/usr/bin/env python
"""
_Delete_

MySQL implementation of JobGroup.Delete

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_jobgroup where id = :id"

    def getBinds(self, id):
        return self.dbi.buildbinds(self.dbi.makelist(id), 'id')

    def execute(self, id, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id),
                         conn = conn, transaction = transaction)
        return True #or raise
