"""
MySQL implementation of Files.InFileset
"""
from WMCore.Database.DBFormatter import DBFormatter

class InFileset(DBFormatter):
    sql = "SELECT DISTINCT fileid AS fileid FROM wmbs_fileset_files WHERE fileset = :fileset"

    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
