"""
SQLite implementation of AddLocation
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL, SQLiteBase):
    sql = NewLocationMySQL.sql