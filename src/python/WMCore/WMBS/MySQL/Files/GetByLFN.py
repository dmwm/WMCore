"""
MySQL implementation of File.Get
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetByLFN(DBFormatter):
    sql = """SELECT id, lfn, size, events, cksum from wmbs_file_details
             WHERE lfn = :lfn"""

    def execute(self, lfn = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"lfn": lfn},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]



