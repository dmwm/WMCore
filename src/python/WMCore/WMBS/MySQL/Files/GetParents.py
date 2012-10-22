"""
MySQL implementation of File.GetParents

Return a list of lfn's which are parents for a file.
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetParents(DBFormatter):
    sql = """SELECT lfn FROM wmbs_file_details WHERE id IN (
              SELECT parent FROM wmbs_file_parent WHERE child = (
                SELECT id FROM wmbs_file_details WHERE lfn = :lfn
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
            r.close()
        return out

    def execute(self, files=None, conn = None, transaction = False):
        binds = self.getBinds(files)
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
