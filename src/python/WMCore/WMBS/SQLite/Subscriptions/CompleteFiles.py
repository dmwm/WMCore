"""
SQLite implementation of Files.CompleteFiles
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL, SQLiteBase):
    sql = CompleteFilesMySQL.sql