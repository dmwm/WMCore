"""
SQLite implementation of SetFileLocation
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.SetFileLocationSQL import SetFileLocation as SetFileLocationMySQL

class SetFileLocation(SetFileLocationMySQL, SQLiteBase):
    sql = SetFileLocationMySQL.sql