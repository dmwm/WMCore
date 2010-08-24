"""
SQLite implementation of GetFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.Get import Get as GetFileMySQL

class Get(GetFileMySQL, SQLiteBase):
    sql = GetFileMySQL.sql