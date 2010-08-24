"""
MySQL implementation of File.Get
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetByID(DBFormatter):
    sql = """SELECT id, lfn, size, events, cksum FROM wmbs_file_details  
             WHERE id = :fileid"""
    
    def execute(self, file = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"fileid": file}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
