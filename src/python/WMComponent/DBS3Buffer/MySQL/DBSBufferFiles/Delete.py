#!/usr/bin/env python
"""
_Delete_

MySQL implementation of DeleteFile

"""




from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from dbsbuffer_file where lfn = :lfn"

    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'lfn')

    def execute(self, file = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(file),
                         conn = conn, transaction = transaction)
        return
