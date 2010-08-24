"""
SQLite implementation of SetFileLocation
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.SetLocationSQL import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL, SQLiteBase):
    sql = SetLocationMySQL.sql