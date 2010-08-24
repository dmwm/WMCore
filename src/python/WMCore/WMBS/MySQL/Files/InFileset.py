"""
MySQL implementation of Files.InFileset
"""
from WMCore.Database.DBFormatter import DBFormatter

class InFileset(DBFormatter):
    sql = """SELECT DISTINCT id FROM wmbs_file_details WHERE id IN
             (SELECT file FROM wmbs_fileset_files WHERE fileset =
             (SELECT id FROM wmbs_fileset WHERE name = :fileset))"""
                
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
