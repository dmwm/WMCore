"""
SQLite implementation of Files.InFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.FailFiles import FailFiles as FailFilesMySQL

class FailFiles(FailFilesMySQL, SQLiteBase):
    sql = FailFilesMySQL.sql