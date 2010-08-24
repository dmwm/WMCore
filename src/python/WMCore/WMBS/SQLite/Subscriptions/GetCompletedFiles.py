"""
SQLite implementation of Files.GetCompletedFiles
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL, SQLiteBase):
    sql = GetCompletedFilesMySQL.sql