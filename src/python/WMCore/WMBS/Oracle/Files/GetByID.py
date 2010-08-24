"""
SQLite implementation of GetFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.GetByID import GetByID as GetFileMySQL

class GetByID(GetFileMySQL, SQLiteBase):
    sql = GetFileMySQL.sql