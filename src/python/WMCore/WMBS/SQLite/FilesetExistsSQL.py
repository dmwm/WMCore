"""
SQLite implementation of FilesetExists
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.FilesetExistsSQL import FilesetExists as FilesetExistsMySQL

class FilesetExists(SQLiteBase, FilesetExistsMySQL):
    sql = FilesetExistsMySQL.sql
    
    def execute(self, fileset = None, conn = None, transaction = False):
        return FilesetExistsMySQL.execute(self, fileset = fileset, 
                                          conn = conn, transaction = transaction)
    
    def getBinds(self, fileset = None):
        return FilesetExistsMySQL.getBinds(self, fileset = fileset)
    
    def format(self, result):
        return FilesetExistsMySQL.format(self, result)