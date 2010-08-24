"""
SQLite implementation of GetLocationFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.GetLocation import GetLocation as GetLocationFileMySQL

class GetLocation(GetLocationFileMySQL, SQLiteBase):
    sql = GetLocationFileMySQL.sql