"""
SQLite implementation of Files.GetFailedFiles
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL, SQLiteBase):
    sql = GetFailedFilesMySQL.sql