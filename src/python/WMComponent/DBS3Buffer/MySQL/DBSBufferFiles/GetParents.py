"""
MySQL implementation of File.GetParents

Return a list of lfn's which are parents for a file.
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetParents(DBFormatter):
    sql = """select lfn from dbsbuffer_file where id IN (
            select parent from dbsbuffer_file_parent where child = (
                select id from dbsbuffer_file where lfn = :lfn
            )
        )"""

    def getBinds(self, files=None):
        binds = []
        files = self.dbi.makelist(files)
        for f in files:
            binds.append({'lfn': f})
        return binds

    def format(self, result):
        out = []
        for r in result:
            for f in r.fetchall():
                out.append(f[0])
        return out

    def execute(self, files=None, conn = None, transaction = False):
        binds = self.getBinds(files)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
