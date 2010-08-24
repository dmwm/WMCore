"""
SQLite implementation of AddLocation
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.AddLocationSQL import AddLocation as AddLocationMySQL

class AddLocation(AddLocationMySQL, SQLiteBase):
    sql = AddLocationMySQL.sql