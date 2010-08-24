"""
SQLite implementation of Files.DeleteAcquiredFiles
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.DeleteAcquiredFiles import DeleteAcquiredFiles as DeleteAcquiredFilesMySQL

class DeleteAcquiredFiles(DeleteAcquiredFilesMySQL, SQLiteBase):
    sql = DeleteAcquiredFilesMySQL.sql