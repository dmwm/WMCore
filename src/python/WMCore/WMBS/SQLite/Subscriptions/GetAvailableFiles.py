"""
SQLite implementation of Files.InFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL, SQLiteBase):
    sql = GetAvailableFilesMySQL.sql
    
    def execute(self, subscription=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription), 
                         conn = conn, transaction = transaction)
        result = self.format(result)
        return result