"""
SQLite implementation of Files.AcquireFiles
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL, SQLiteBase):
    sql = AcquireFilesMySQL.sql